// zap-wallet-init-manager/src/worker.rs

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use fuels::prelude::*;
use fuels::types::{Bytes32, AssetId};
use tracing::{debug, error, info};
use hex;

use crate::consts::FUEL_BASE_ASSET;
use crate::error::{InitializationError, InitializationResult};
use crate::types::{InitializationJob, InitializationConfig, CompletionStatus};
use crate::stats::WorkerStats;
use crate::UnlockedWallet;
use crate::ZapManager;
use zap_rs_sdk::core::version::ZapVersion;

use crate::consts::{RESET, MAGENTA};


pub struct FuelEOAWorker {
    pub id: usize,
    wallet: UnlockedWallet,
    job_receiver: mpsc::Receiver<InitializationJob>,
    provider: Arc<Provider>,
    config: InitializationConfig,
    /// Worker statistics
    stats: Arc<Mutex<WorkerStats>>,
}

impl FuelEOAWorker {
    pub fn new(
        id: usize,
        wallet: UnlockedWallet,
        job_receiver: mpsc::Receiver<InitializationJob>,
        provider: Arc<Provider>,
        config: InitializationConfig,
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

        while let Some(job) = self.job_receiver.recv().await {
            let start_time = Instant::now();
            let wait_time = job.submitted_at.elapsed();

            // info!(
            //     worker_id = self.id,
            //     wallet = %hex::encode(job.wallet_address.value().0),
            //     wait_time_ms = wait_time.as_millis(),
            //     "Processing initialization job"
            // );
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

            // Process the job
            let result: InitializationResult<Bytes32> = self.initialize_wallet_on_chain(
                job.wallet_address,
                job.wallet_version
            ).await;

            // Handle completion callback
            if let Some(callback) = job.completion_callback {
                match &result {
                    Ok(tx_hash) => {
                        callback(CompletionStatus::Success {
                            tx_hash: *tx_hash,
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
            let _ = job.response.send(result);

            // Update stats
            {
                let mut stats = self.stats.lock().await;
                stats.total_processed += 1;
                stats.last_job_at = Some(Instant::now());
                let processing_time = start_time.elapsed().as_millis() as u64;
                // Simple moving average
                stats.average_processing_time_ms =
                    (stats.average_processing_time_ms * (stats.total_processed - 1) + processing_time)
                    / stats.total_processed;
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
    ) -> InitializationResult<Bytes32> {
        info!(
            worker_id = self.id,
            target_wallet = %hex::encode(target_wallet.value().0),
            "Starting wallet initialization"
        );

        // 1. Get the ZapManager contract instance
        let zap_manager = Self::get_zapmanager_instance(
            self.wallet.clone(),
            self.config.init_contract
        )?;

        tracing::info!("{}self.config.gas_limit            : {}{}", MAGENTA, self.config.gas_limit, RESET);
        tracing::info!("{}self.config.initialization_amount: {}{}", MAGENTA, self.config.initialization_amount, RESET);

        // let tx_policies = TxPolicies::default()
        //     .with_tip(1)
        //     .with_script_gas_limit(self.config.gas_limit)
        //     .with_max_fee(self.config.initialization_amount);

        let tx_policies = TxPolicies::default()
            .with_tip(1);


        // 2. Build the contract call transaction
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

        // Extract the transaction ID
        let tx_id = tx_response.tx_id
            .ok_or_else(|| InitializationError::BuildError(
                "Transaction ID not found in response".to_string()
            ))?;

        // info!("tx_response:\n{:?}\n", tx_response);

        // info!("📨 ZapWallet Initialization transaction sent, TXID: {}", tx_id);
        tracing::info!("📨 {}ZapWallet Initialization transaction sent, TXID: {}{}", MAGENTA, tx_id, RESET);


        // Wait a bit for the transaction to be processed
        // tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Return the transaction ID as success
        Ok(tx_id)
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
