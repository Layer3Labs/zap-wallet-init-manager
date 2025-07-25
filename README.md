# Fuel Wallet Initialization Manager

A channel-based work queue system for managing wallet initialization on the Fuel network. Handles UTXO management and ensures sequential transaction processing per EOA wallet.

## Features

- **UTXO-Safe**: Each EOA wallet processes transactions sequentially
- **Load Balancing**: Jobs distributed to the least busy worker
- **Non-Blocking**: Integrates with async systems without blocking
- **Automatic UTXO Management**: Refreshes UTXOs after each transaction
- **Comprehensive Stats**: Track performance and health metrics
- **Configurable**: Adjust queue sizes, gas limits, and more

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
zap-wallet-init-manager = { path = "../zap-wallet-init-manager" }
```

## Basic Usage

```rust
use fuel_wallet_init_manager::{
    InitializationManager,
    InitializationConfig,
    WalletInitRequest,
};
use fuels::prelude::*;

// Create EOA wallets
let eoa_wallets = vec![
    WalletUnlocked::new_from_private_key(key1, Some(provider.clone()))?,
    WalletUnlocked::new_from_private_key(key2, Some(provider.clone()))?,
];

// Configure
let config = InitializationConfig {
    max_queue_size: 1000,
    worker_queue_size: 100,
    init_contract: contract_id,
    initialization_amount: 1_000_000,
    gas_limit: 1_000_000,
    verbose_logging: true,
};

// Create manager
let manager = InitializationManager::new(
    eoa_wallets,
    Arc::new(provider),
    config,
);

// Initialize a wallet
let request = WalletInitRequest {
    wallet_address: target_wallet,
    wallet_version: ZapVersion::V1,
};

let tx_hash = manager.initialize_wallet(request).await?;
```

## Integration with TieredWalletCache

In your cache's `create_new_wallet` method:

```rust
if init_state.try_start_init() {
    if eth_balance > 0 {
        let init_manager = self.init_manager.clone();
        let wallet_address = *addr;

        tokio::spawn(async move {
            let request = WalletInitRequest {
                wallet_address,
                wallet_version: version,
            };

            match init_manager.initialize_wallet(request).await {
                Ok(tx_hash) => {
                    // Update caches on success
                    init_cache.insert(key, initialized_wallet);
                    maybe_cache.remove(&key);
                    init_state.mark_completed();
                }
                Err(e) => {
                    init_state.mark_failed();
                }
            }
        });
    }
}
```

## Monitoring

```rust
// Get statistics
let stats = manager.get_stats().await;
println!("Total jobs: {}", stats.total_jobs_submitted);
println!("Queue depth: {}", stats.current_queue_depth);

// Health check
if manager.health_check().await {
    println!("Manager is healthy");
}
```

## Architecture

```
┌─────────────────┐
│ TieredWallet    │
│     Cache       │
└────────┬────────┘
         │ spawn task
         ▼
┌─────────────────┐
│ Initialization  │
│    Manager      │
└────────┬────────┘
         │ channel
         ▼
┌─────────────────┐
│ Load Balancer   │
└────────┬────────┘
         │ routes to least busy
         ▼
┌─────┬─────┬─────┐
│EOA 1│EOA 2│EOA 3│  ← Workers (one per EOA)
└─────┴─────┴─────┘
```

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `max_queue_size` | Maximum pending jobs | 1000 |
| `worker_queue_size` | Per-worker queue size | 100 |
| `init_contract` | Contract ID for initialization | Required |
| `initialization_amount` | Fuel tokens for init | 1,000,000 |
| `gas_limit` | Gas limit per transaction | 1,000,000 |
| `verbose_logging` | Enable detailed logs | false |

## Thread Safety

- All components are `Send + Sync`
- Uses channels for communication
- UTXO management is worker-local
- Stats use `Arc<Mutex<_>>` for thread-safe updates

## Error Handling

The manager returns typed errors:

```rust
match manager.initialize_wallet(request).await {
    Ok(tx_hash) => { /* success */ },
    Err(InitializationError::InsufficientBalance) => { /* fund EOA */ },
    Err(InitializationError::UTXOError(_)) => { /* UTXO issue */ },
    Err(InitializationError::Timeout) => { /* took too long */ },
    Err(e) => { /* other error */ },
}
```

## TODOs

1. Implement actual transaction building in `build_init_transaction()`
2. Add UTXO consolidation strategies
3. Implement worker restart on failure
4. Add more sophisticated UTXO selection algorithms
5. Add metrics integration (Prometheus support is stubbed)

## License

MIT OR Apache-2.0