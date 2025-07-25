// zap-wallet-init-manager/src/types.rs

use tokio::sync::oneshot;
use tokio::time::Instant;
use fuels::types::{Bytes32, ContractId, EvmAddress};

use crate::error::InitializationError;

use zap_rs_sdk::core::version::ZapVersion;
pub use ZapVersion::*;


/// Key type for cache operations
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct EvmAddressKey(pub [u8; 32]);

impl From<EvmAddress> for EvmAddressKey {
    fn from(addr: EvmAddress) -> Self {
        let bits: fuels::types::Bits256 = addr.value();
        EvmAddressKey(bits.0)
    }
}

/// Placeholder types for cache integration
/// Replace these with your actual types when integrating
pub type ZapWalletInitialized = ();  // Replace with Arc<ZapWallet<Initialized>>
pub type ZapWalletMaybeInitialized = ();  // Replace with Arc<ZapWallet<MaybeInitialized>>
pub type AtomicInitState = ();  // Replace with Arc<AtomicInitState>

/// Configuration for initialization manager
#[derive(Debug, Clone)]
pub struct InitializationConfig {
    /// Maximum number of jobs to queue
    pub max_queue_size: usize,

    /// Per-worker queue size
    pub worker_queue_size: usize,

    /// Contract ID for initialization
    pub init_contract: ContractId,

    /// Amount of fuel tokens needed for initialization
    pub initialization_amount: u64,

    /// Gas limit for initialization transaction
    pub gas_limit: u64,

    /// Enable detailed logging
    pub verbose_logging: bool,
}

impl Default for InitializationConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 1000,
            worker_queue_size: 100,
            init_contract: ContractId::default(),
            initialization_amount: 1_000_000,
            gas_limit: 1_000_000,
            verbose_logging: false,
        }
    }
}

/// Request to initialize a wallet
#[derive(Debug)]
pub struct WalletInitRequest {
    pub wallet_address: EvmAddress,
    pub wallet_version: ZapVersion,
}

#[derive(Debug)]
pub struct InitCallSuccessData {
    pub tx_id: Bytes32,
    pub total_gas: u64,
    pub total_fee: u64,
}

/// Internal job structure with cache references
pub struct InitializationJob {
    /// The wallet we're initializing
    pub wallet_address: EvmAddress,
    pub wallet_version: ZapVersion,

    /// Channel to send result back
    pub response: oneshot::Sender<Result<InitCallSuccessData, InitializationError>>,

    /// When the job was submitted (for metrics)
    pub submitted_at: Instant,

    /// Callback for updating caches after completion
    pub completion_callback: Option<CompletionCallback>,
}

impl std::fmt::Debug for InitializationJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitializationJob")
            .field("wallet_address", &self.wallet_address)
            .field("wallet_version", &self.wallet_version)
            .field("submitted_at", &self.submitted_at)
            .field("has_callback", &self.completion_callback.is_some())
            .finish()
    }
}

/// Callback function type for cache updates
pub type CompletionCallback = Box<dyn FnOnce(CompletionStatus) + Send>;

/// Status passed to completion callback
#[derive(Debug)]
pub enum CompletionStatus {
    Success {
        tx_hash: Bytes32,
        wallet_address: EvmAddress,
        wallet_version: ZapVersion,
    },
    Failed {
        error: InitializationError,
        wallet_address: EvmAddress,
    },
}