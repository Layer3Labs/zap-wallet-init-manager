// zap-wallet-init-manager/src/manager.rs

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Instant;
use fuels::prelude::*;
use fuels::types::Bytes32;
use tracing::{debug, info, warn};

use crate::error::{InitializationError, InitializationResult};
use crate::types::{InitializationJob, InitializationConfig, WalletInitRequest, CompletionCallback};
use crate::stats::{ManagerStats, WorkerStats};
use crate::worker::FuelEOAWorker;
use crate::UnlockedWallet;


pub struct InitializationManager {
    /// Channel to send jobs to the load balancer
    job_sender: mpsc::Sender<InitializationJob>,

    /// Statistics
    stats: Arc<Mutex<ManagerStats>>,

    /// Configuration
    config: InitializationConfig,
}

impl InitializationManager {
    /// Create a new initialization manager
    pub fn new(
        eoa_wallets: Vec<UnlockedWallet>,
        provider: Arc<Provider>,
        config: InitializationConfig,
    ) -> Self {
        let (job_tx, job_rx) = mpsc::channel(config.max_queue_size);
        let stats = Arc::new(Mutex::new(ManagerStats::new()));

        // Store worker count before creating load balancer
        let worker_count = eoa_wallets.len();

        // Create and spawn the load balancer
        let load_balancer = LoadBalancer::new(
            eoa_wallets,
            provider,
            config.clone(),
            job_rx,
            stats.clone()
        );

        tokio::spawn(load_balancer.run());

        info!(
            worker_count = worker_count,
            max_queue_size = config.max_queue_size,
            "Initialization manager started"
        );

        Self {
            job_sender: job_tx,
            stats,
            config,
        }
    }

    /// Get the current configuration
    pub fn config(&self) -> &InitializationConfig {
        &self.config
    }

    /// Submit a wallet for initialization
    pub async fn initialize_wallet(&self, request: WalletInitRequest) -> InitializationResult<Bytes32> {
        let (response_tx, response_rx) = oneshot::channel();

        let job = InitializationJob {
            wallet_address: request.wallet_address,
            wallet_version: request.wallet_version,
            response: response_tx,
            submitted_at: Instant::now(),
            completion_callback: None,
        };

        self.submit_job(job).await?;

        // Wait for response
        response_rx.await?
    }

    /// Submit a wallet for initialization with a completion callback
    pub async fn initialize_wallet_with_callback(
        &self,
        request: WalletInitRequest,
        callback: CompletionCallback,
    ) -> InitializationResult<()> {
        let (response_tx, response_rx) = oneshot::channel();

        let job = InitializationJob {
            wallet_address: request.wallet_address,
            wallet_version: request.wallet_version,
            response: response_tx,
            submitted_at: Instant::now(),
            completion_callback: Some(callback),
        };

        self.submit_job(job).await?;

        // Spawn a task to handle the response
        tokio::spawn(async move {
            let _ = response_rx.await;
            // Callback will be invoked by the worker
        });

        Ok(())
    }

    async fn submit_job(&self, job: InitializationJob) -> InitializationResult<()> {
        // Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.record_submission();
        }

        // Send job to load balancer
        self.job_sender
            .send(job)
            .await
            .map_err(|_| InitializationError::ServiceShuttingDown)?;

        Ok(())
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> ManagerStats {
        self.stats.lock().await.clone()
    }

    /// Get a summary of current status
    pub async fn get_status_summary(&self) -> String {
        let stats = self.stats.lock().await;
        stats.get_summary()
    }

    /// Check if the manager is healthy
    pub async fn health_check(&self) -> bool {
        // Check if we can send to the channel
        self.job_sender.capacity() > 0
    }
}

struct LoadBalancer {
    /// Receiver for incoming jobs
    job_receiver: mpsc::Receiver<InitializationJob>,

    /// Sender for each worker
    worker_senders: Vec<mpsc::Sender<InitializationJob>>,

    /// Track queue depths for load balancing
    queue_depths: Vec<Arc<AtomicUsize>>,

    /// Worker statistics
    worker_stats: Vec<Arc<Mutex<WorkerStats>>>,

    /// Number of workers
    worker_count: usize,

    /// Manager statistics
    stats: Arc<Mutex<ManagerStats>>,
}

impl LoadBalancer {
    fn new(
        eoa_wallets: Vec<UnlockedWallet>,
        provider: Arc<Provider>,
        config: InitializationConfig,
        job_receiver: mpsc::Receiver<InitializationJob>,
        stats: Arc<Mutex<ManagerStats>>,
    ) -> Self {
        let worker_count = eoa_wallets.len();
        let mut worker_senders = Vec::with_capacity(worker_count);
        let mut queue_depths = Vec::with_capacity(worker_count);
        let mut worker_stats = Vec::with_capacity(worker_count);

        // Create a worker for each EOA
        for (id, eoa_wallet) in eoa_wallets.into_iter().enumerate() {
            let (worker_tx, worker_rx) = mpsc::channel(config.worker_queue_size);
            let queue_depth = Arc::new(AtomicUsize::new(0));

            let worker = FuelEOAWorker::new(
                id,
                eoa_wallet,
                worker_rx,
                provider.clone(),
                config.clone(),
            );

            let worker_stat = worker.get_stats();
            worker_stats.push(worker_stat.clone());

            // Spawn the worker
            tokio::spawn(worker.run());

            worker_senders.push(worker_tx);
            queue_depths.push(queue_depth.clone());

            info!(worker_id = id, "Spawned EOA worker");
        }

        Self {
            job_receiver,
            worker_senders,
            queue_depths,
            worker_stats,
            worker_count,
            stats,
        }
    }

    async fn run(mut self) {
        info!("Load balancer started with {} workers", self.worker_count);

        while let Some(job) = self.job_receiver.recv().await {
            // Find worker with smallest queue
            let worker_index = self.select_worker();

            debug!(
                worker_id = worker_index,
                queue_depth = self.queue_depths[worker_index].load(Ordering::Relaxed),
                "Routing job to worker"
            );

            // Update queue depth
            self.queue_depths[worker_index].fetch_add(1, Ordering::Relaxed);

            // Update worker stats
            {
                let mut stats = self.worker_stats[worker_index].lock().await;
                stats.current_queue_depth += 1;
            }

            // Send to selected worker
            match self.worker_senders[worker_index].send(job).await {
                Ok(_) => {
                    // Successfully sent
                }
                Err(e) => {
                    warn!(
                        worker_id = worker_index,
                        error = %e,
                        "Failed to send job to worker"
                    );

                    // Worker might be dead, update stats
                    self.queue_depths[worker_index].store(0, Ordering::Relaxed);

                    // Update manager stats for failure
                    let mut stats = self.stats.lock().await;
                    stats.record_completion(worker_index, false);
                }
            }

            // Spawn task to track completion
            let queue_depth = self.queue_depths[worker_index].clone();
            let worker_stat = self.worker_stats[worker_index].clone();
            let manager_stats = self.stats.clone();
            let worker_id = worker_index;

            tokio::spawn(async move {
                // This is a simplified approach - in production, you'd want
                // the worker to signal completion directly
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

                queue_depth.fetch_sub(1, Ordering::Relaxed);

                let mut stats = worker_stat.lock().await;
                stats.current_queue_depth = stats.current_queue_depth.saturating_sub(1);

                // Update manager stats
                let mut m_stats = manager_stats.lock().await;
                m_stats.record_completion(worker_id, true);
            });
        }

        info!("Load balancer shutting down");
    }

    fn select_worker(&self) -> usize {
        // Select worker with minimum queue depth
        self.queue_depths
            .iter()
            .enumerate()
            .min_by_key(|(_, depth)| depth.load(Ordering::Relaxed))
            .map(|(idx, _)| idx)
            .unwrap_or(0)
    }
}