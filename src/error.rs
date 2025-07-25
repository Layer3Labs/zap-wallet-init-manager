// zap-wallet-init-manager/src/error.rs

use thiserror::Error;
use fuels::types::errors::Error as FuelError;

pub type InitializationResult<T> = Result<T, InitializationError>;

#[derive(Error, Debug)]
pub enum InitializationError {
    #[error("Insufficient balance in EOA wallet")]
    InsufficientBalance,

    #[error("UTXO error: {0}")]
    UTXOError(String),

    #[error("Transaction failed: {0}")]
    TransactionFailed(String),

    #[error("Initialization timeout")]
    Timeout,

    #[error("Service shutting down")]
    ServiceShuttingDown,

    #[error("Worker error: {0}")]
    WorkerError(String),

    #[error("Fuel SDK error: {0}")]
    FuelError(#[from] FuelError),

    #[error("Channel send error")]
    ChannelSendError,

    #[error("Channel receive error")]
    ChannelReceiveError,

    #[error("Build error: {0}")]
    BuildError(String),
}

impl From<tokio::sync::mpsc::error::SendError<crate::types::InitializationJob>> for InitializationError {
    fn from(_: tokio::sync::mpsc::error::SendError<crate::types::InitializationJob>) -> Self {
        InitializationError::ChannelSendError
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for InitializationError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        InitializationError::ChannelReceiveError
    }
}