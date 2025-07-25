// zap-wallet-init-manager/src/worker.rs

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use fuels::prelude::*;
use fuels::types::AssetId;
use tracing::{debug, error, info};
use hex;

use crate::consts::FUEL_BASE_ASSET;
use crate::error::{InitializationError, InitializationResult};
use crate::types::{InitializationConfig, CompletionStatus, InitCallSuccessData};
use crate::stats::WorkerStats;
use crate::UnlockedWallet;
use crate::ZapManager;
use crate::db::InitializationDb;
use crate::consts::*;
use zap_rs_sdk::core::version::ZapVersion;
use crate::manager::InternalJob;

pub struct FuelEOAWorker {
    pub id: usize,
    wallet: UnlockedWallet,
    job_receiver: mpsc::Receiver<InternalJob>,
    provider: Arc<Provider>,
    config: InitializationConfig,
    /// Worker statistics
    stats: Arc<Mutex<WorkerStats>>,
    /// Database for tracking initializations
    db: Option<Arc<InitializationDb>>,
}

impl FuelEOAWorker {
    pub fn new(
        id: usize,
        wallet: UnlockedWallet,
        job_receiver: mpsc::Receiver<InternalJob>,
        provider: Arc<Provider>,
        config: InitializationConfig,
        db: Option<Arc<InitializationDb>>,
    ) -> Self {
        let stats = Arc::new(Mutex::new(WorkerStats {
            id,
            ..Default::default()
        }));

        Self {
            id,
            wallet,
            job_receiver,
            provider,
            config,
            stats,
            db,
        }
    }

    pub async fn run(mut self) {
        info!(
            worker_id = self.id,
            eoa_address = ?self.wallet.address(),
            "Fuel EOA worker started"
        );

        // Initial UTXO refresh
        if let Err(e) = self.refresh_utxos().await {
            error!(worker_id = self.id, error = %e, "Failed to get initial UTXOs");
        }

        while let Some(internal_job) = self.job_receiver.recv().await {
            let InternalJob { job, record_id } = internal_job;
            let start_time = Instant::now();
            let wait_time = job.submitted_at.elapsed();

            tracing::info!(
                "{}[Worker {}] Processing initialization job for wallet {} (wait time: {}ms){}",
                MAGENTA,
                self.id,
                hex::encode(job.wallet_address.value().0),
                wait_time.as_millis(),
                RESET
            );

            // Update stats
            {
                let mut stats = self.stats.lock().await;
                stats.current_queue_depth = stats.current_queue_depth.saturating_sub(1);
            }

            // Record job started in database
            if let Some(db) = &self.db {
                if let Some(rec_id) = record_id {
                    if let Err(e) = db.record_job_started(
                        rec_id,
                        self.id,
                        &self.wallet.address().to_string(),
                    ).await {
                        error!(
                            worker_id = self.id,
                            error = %e,
                            "Failed to record job started in database"
                        );
                    }
                }
            }

            // Process the job
            let result: InitializationResult<InitCallSuccessData> = self.initialize_wallet_on_chain(
                job.wallet_address,
                job.wallet_version,
                record_id,
            ).await;

            // result is (tx_id, total_gas)
            // Record result in database
            if let Some(db) = &self.db {
                if let Some(rec_id) = record_id {
                    match &result {
                        Ok(result) => {
                            // Update with transaction hash first
                            if let Err(e) = db.update_tx_hash(rec_id, &result.tx_id).await {
                                error!(
                                    worker_id = self.id,
                                    error = %e,
                                    "Failed to update transaction hash in database"
                                );
                            }

                            // Then record completion
                            if let Err(e) = db.record_job_completed(
                                rec_id,
                                Some(&result.tx_id),
                                Some(result.total_gas),
                                Some(result.total_fee),
                            ).await {
                                error!(
                                    worker_id = self.id,
                                    error = %e,
                                    "Failed to record job completion in database"
                                );
                            }
                        }
                        Err(e) => {
                            // Record failure
                            if let Err(db_err) = db.record_job_failed(rec_id, &e.to_string()).await {
                                error!(
                                    worker_id = self.id,
                                    error = %db_err,
                                    "Failed to record job failure in database"
                                );
                            }
                        }
                    }
                }
            }

            // Handle completion callback
            if let Some(callback) = job.completion_callback {
                match &result {
                    Ok(result) => {
                        callback(CompletionStatus::Success {
                            tx_hash: result.tx_id,
                            wallet_address: job.wallet_address,
                            wallet_version: job.wallet_version,
                        });
                    }
                    Err(e) => {
                        callback(CompletionStatus::Failed {
                            error: InitializationError::WorkerError(e.to_string()),
                            wallet_address: job.wallet_address,
                        });
                    }
                }
            }

            // Send result back
            let is_error = result.is_err();
            let _ = job.response.send(result);

            // Update stats
            let processing_time = start_time.elapsed();
            {
                let mut stats = self.stats.lock().await;
                stats.total_processed += 1;
                stats.last_job_at = Some(Instant::now());
                let processing_time_ms = processing_time.as_millis() as u64;

                // Update average processing time
                stats.average_processing_time_ms =
                    (stats.average_processing_time_ms * (stats.total_processed - 1) + processing_time_ms)
                    / stats.total_processed;

                // Track min/max
                if stats.min_processing_time_ms == 0 || processing_time_ms < stats.min_processing_time_ms {
                    stats.min_processing_time_ms = processing_time_ms;
                }
                if processing_time_ms > stats.max_processing_time_ms {
                    stats.max_processing_time_ms = processing_time_ms;
                }

                // Update failure count if needed
                if is_error {
                    stats.total_failures += 1;
                }
            }

            // Emit metrics if enabled
            #[cfg(feature = "metrics")]
            if let Some(metrics) = crate::stats::prometheus_metrics::get_metrics() {
                metrics.processing_time
                    .with_label_values(&[&self.id.to_string()])
                    .observe(processing_time.as_secs_f64());
            }

            // Refresh UTXOs after each transaction
            if let Err(e) = self.refresh_utxos().await {
                error!(worker_id = self.id, error = %e, "Failed to refresh UTXOs");
            }
        }

        info!(worker_id = self.id, "Fuel EOA worker shutting down");
    }

    async fn initialize_wallet_on_chain(
        &mut self,
        target_wallet: fuels::types::EvmAddress,
        _version: ZapVersion,
        _record_id: Option<i64>,
    ) -> InitializationResult<InitCallSuccessData> {
        tracing::info!(
            "{}Starting wallet initialization - worker_id: {}{}, {}target_wallet: {}{}",
            MAGENTA, self.id, RESET,
            MAGENTA, hex::encode(target_wallet.value().0), RESET
        );

        // 1. Get the ZapManager contract instance
        let zap_manager = Self::get_zapmanager_instance(
            self.wallet.clone(),
            self.config.init_contract
        )?;

        tracing::info!("{}self.config.gas_limit            : {}{}", MAGENTA, self.config.gas_limit, RESET);
        tracing::info!("{}self.config.initialization_amount: {}{}", MAGENTA, self.config.initialization_amount, RESET);

        let tx_policies = TxPolicies::default()
            .with_tip(1);

        // 2. Build and send the contract call transaction
        let tx_response = zap_manager
            .methods()
            .initialize_wallet(target_wallet)
            .with_variable_output_policy(VariableOutputPolicy::Exactly(10)) // 9 modules + nonce
            .with_tx_policies(tx_policies)
            .call()
            .await
            .map_err(|e| InitializationError::BuildError(
                format!("Failed to initialize wallet: {}", e)
            ))?;

        // Wait a bit for the transaction to be processed
        // tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;


        // let o = tx_response.decode_logs().results;
        // println!("\no:\n{:?}\n", o);

        // get total fee and gas used
        let tx_total_fee = tx_response.tx_status.total_fee;
        let tx_total_gas = tx_response.tx_status.total_gas;
        tracing::info!(
            "{}total_fee: {}, total_gas: {}{}",
            MAGENTA, tx_total_fee, tx_total_gas, RESET);


        // let u = tx_response.tx_status.receipts;
        // println!("\nu:\n{:?}\n", u);

        // Extract the transaction ID
        let tx_id = tx_response.tx_id
            .ok_or_else(|| InitializationError::BuildError(
                "Transaction ID not found in response".to_string()
            ))?;

        tracing::info!("📨 {}ZapWallet Initialization transaction sent, TXID: {}{}", MAGENTA, tx_id, RESET);

        // Return the txid as success, and total_gas
        Ok(InitCallSuccessData {
            tx_id,
            total_gas: tx_total_gas,
            total_fee: tx_total_fee,
        })
    }

    /// Get ZapManager instance from environment variable
    fn get_zapmanager_instance(
        wallet: UnlockedWallet,
        contract_id: ContractId,
    ) -> Result<ZapManager<UnlockedWallet>> {

        println!("Using ZapManager at: 0x{}", hex::encode(&contract_id));

        Ok(ZapManager::new(contract_id, wallet))
    }

    async fn refresh_utxos(&mut self) -> InitializationResult<()> {
        // Since we're using contract calls, we don't need to manually manage UTXOs
        // The Fuel SDK handles this for us when making contract calls
        // This method is kept for compatibility but simplified

        // Just check the wallet balance for monitoring
        let balance = self.provider
            .get_asset_balance(self.wallet.address(), AssetId::from(FUEL_BASE_ASSET))
            .await
            .map_err(|e| InitializationError::UTXOError(format!("Failed to get balance: {}", e)))?;

        // Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.available_balance = balance;
            // We don't track individual UTXOs when using contract calls
            stats.total_utxos = 0;
        }

        debug!(
            worker_id = self.id,
            balance = balance,
            "Checked wallet balance"
        );

        Ok(())
    }

    pub fn get_stats(&self) -> Arc<Mutex<WorkerStats>> {
        self.stats.clone()
    }
}