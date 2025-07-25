// zap-wallet-init-manager/src/manager.rs

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Instant;
use fuels::prelude::*;
use tracing::{debug, info, warn, error};

use crate::error::{InitializationError, InitializationResult};
use crate::types::{CompletionCallback, InitCallSuccessData, InitializationConfig, InitializationJob, WalletInitRequest};
use crate::stats::{ManagerStats, WorkerStats};
use crate::worker::FuelEOAWorker;
use crate::UnlockedWallet;
use crate::db::InitializationDb;

/// Internal job structure that includes database record ID
pub struct InternalJob {
    pub job: InitializationJob,
    pub record_id: Option<i64>,
}

pub struct InitializationManager {
    /// Channel to send jobs to the load balancer
    job_sender: mpsc::Sender<InternalJob>,
    /// Statistics
    stats: Arc<Mutex<ManagerStats>>,
    /// Configuration
    config: InitializationConfig,
    /// Database for tracking
    db: Option<Arc<InitializationDb>>,
}

impl InitializationManager {
    /// Create a new initialization manager
    pub fn new(
        eoa_wallets: Vec<UnlockedWallet>,
        provider: Arc<Provider>,
        config: InitializationConfig,
    ) -> Self {
        Self::new_with_db(eoa_wallets, provider, config, None)
    }

    /// Create a new initialization manager with database
    pub fn new_with_db(
        eoa_wallets: Vec<UnlockedWallet>,
        provider: Arc<Provider>,
        config: InitializationConfig,
        database_url: Option<String>,
    ) -> Self {
        let (job_tx, job_rx) = mpsc::channel(config.max_queue_size);
        let stats = Arc::new(Mutex::new(ManagerStats::new()));

        // Initialize database if URL provided
        let db = if let Some(url) = database_url {
            match tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(InitializationDb::new(&url))
            }) {
                Ok(database) => {
                    info!("Connected to initialization tracking database");
                    Some(Arc::new(database))
                }
                Err(e) => {
                    error!("Failed to connect to database: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Store worker count before creating load balancer
        let worker_count = eoa_wallets.len();

        // Create and spawn the load balancer
        let load_balancer = LoadBalancer::new(
            eoa_wallets,
            provider,
            config.clone(),
            job_rx,
            stats.clone(),
            db.clone(),
        );

        tokio::spawn(load_balancer.run());

        info!(
            worker_count = worker_count,
            max_queue_size = config.max_queue_size,
            database_enabled = db.is_some(),
            "Initialization manager started"
        );

        Self {
            job_sender: job_tx,
            stats,
            config,
            db,
        }
    }

    /// Get database statistics
    pub async fn get_db_stats(&self) -> Option<crate::db::InitStats> {
        if let Some(db) = &self.db {
            match db.get_stats().await {
                Ok(stats) => Some(stats),
                Err(e) => {
                    error!("Failed to get database stats: {}", e);
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get worker performance from database
    pub async fn get_worker_performance(&self) -> Option<Vec<crate::db::WorkerPerformance>> {
        if let Some(db) = &self.db {
            match db.get_worker_stats().await {
                Ok(stats) => Some(stats),
                Err(e) => {
                    error!("Failed to get worker performance: {}", e);
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get the current configuration
    pub fn config(&self) -> &InitializationConfig {
        &self.config
    }

    /// Submit a wallet for initialization
    pub async fn initialize_wallet(&self, request: WalletInitRequest) -> InitializationResult<InitCallSuccessData> {
        let (response_tx, response_rx) = oneshot::channel();

        // Record in database when job is queued
        let record_id = if let Some(db) = &self.db {
            match db.record_job_queued(&request.wallet_address).await {
                Ok(id) => {
                    debug!("Recorded job queued in database with ID: {}", id);
                    Some(id)
                }
                Err(e) => {
                    error!("Failed to record job in database: {}", e);
                    None
                }
            }
        } else {
            None
        };

        let job = InitializationJob {
            wallet_address: request.wallet_address,
            wallet_version: request.wallet_version,
            response: response_tx,
            submitted_at: Instant::now(),
            completion_callback: None,
        };

        let internal_job = InternalJob { job, record_id };

        self.submit_job(internal_job).await?;

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

        // Record in database when job is queued
        let record_id = if let Some(db) = &self.db {
            match db.record_job_queued(&request.wallet_address).await {
                Ok(id) => {
                    debug!("Recorded job queued in database with ID: {}", id);
                    Some(id)
                }
                Err(e) => {
                    error!("Failed to record job in database: {}", e);
                    None
                }
            }
        } else {
            None
        };

        let job = InitializationJob {
            wallet_address: request.wallet_address,
            wallet_version: request.wallet_version,
            response: response_tx,
            submitted_at: Instant::now(),
            completion_callback: Some(callback),
        };

        let internal_job = InternalJob { job, record_id };

        self.submit_job(internal_job).await?;

        // Spawn a task to handle the response
        tokio::spawn(async move {
            let _ = response_rx.await;
            // Callback will be invoked by the worker
        });

        Ok(())
    }

    async fn submit_job(&self, job: InternalJob) -> InitializationResult<()> {
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
    job_receiver: mpsc::Receiver<InternalJob>,

    /// Sender for each worker
    worker_senders: Vec<mpsc::Sender<InternalJob>>,

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
        job_receiver: mpsc::Receiver<InternalJob>,
        stats: Arc<Mutex<ManagerStats>>,
        db: Option<Arc<InitializationDb>>,
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
                db.clone(), // Pass database to worker
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

        while let Some(internal_job) = self.job_receiver.recv().await {
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
            match self.worker_senders[worker_index].send(internal_job).await {
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