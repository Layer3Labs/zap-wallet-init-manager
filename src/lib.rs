// zap-wallet-init-manager/src/lib.rs

//! # Fuel Wallet Initialization Manager
//!
//! A channel-based work queue system for managing wallet initialization on Fuel network.
//! Handles UTXO management and ensures sequential transaction processing per EOA.

pub mod error;
pub mod manager;
pub mod worker;
pub mod types;
pub mod stats;
pub mod utils;
pub mod consts;
pub mod db;
pub mod zap_manager;

pub use error::{InitializationError, InitializationResult};
pub use manager::InitializationManager;
pub use types::{InitializationJob, InitializationConfig, WalletInitRequest};
pub use stats::{ManagerStats, WorkerStats};
pub use utils::{get_test_wallet, create_test_wallets};
pub use db::{InitializationDb, InitStats, WorkerPerformance};

pub use zap_manager::ZapManager;

// Re-export commonly used types
pub use fuels::accounts::wallet::{Wallet, Unlocked};
pub use fuels::accounts::signers::private_key::PrivateKeySigner;

/// Type alias for unlocked wallet
pub type UnlockedWallet = Wallet<Unlocked<PrivateKeySigner>>;