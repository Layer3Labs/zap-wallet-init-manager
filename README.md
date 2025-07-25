# Zap Wallet Initialization Manager

A high-performance, channel-based work queue system for managing wallet initialization on the Fuel network. Features UTXO management, sequential transaction processing per EOA wallet, comprehensive database tracking, and NTP time synchronization.

## Features

- **UTXO-Safe Architecture**: Each EOA wallet processes transactions sequentially to prevent conflicts
- **Load Balancing**: Jobs are distributed to the least busy worker automatically
- **Non-Blocking Integration**: Works seamlessly with async systems
- **Automatic UTXO Management**: Refreshes UTXOs after each transaction
- **Database Tracking**: SQLite database for initialization history and metrics
- **NTP Time Sync**: Accurate timestamps using NTP synchronization
- **Comprehensive Stats**: Real-time performance and health metrics
- **Fee Tracking**: Tracks transaction fees in ETH with 9 decimal precision
- **Worker Performance**: Detailed per-worker statistics and fee analysis
- **Configurable**: Adjust queue sizes, gas limits, and more

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
zap-wallet-init-manager = { path = "../zap-wallet-init-manager" }
```

## Basic Usage

```rust
use zap_wallet_init_manager::{
    InitializationManager,
    InitializationConfig,
    WalletInitRequest,
};
use fuels::prelude::*;
use zap_rs_sdk::core::version::ZapVersion::*;

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
    initialization_amount: 100_000_000,
    gas_limit: 500_000_000,
    verbose_logging: true,
};

// Create manager with database
let manager = InitializationManager::new_with_db(
    eoa_wallets,
    Arc::new(provider),
    config,
    Some("wallet_init.db".to_string()),
);

// Initialize a wallet
let request = WalletInitRequest {
    wallet_address: target_wallet,
    wallet_version: V1,
};

let result = manager.initialize_wallet(request).await?;
println!("Transaction ID: {}", result.tx_id);
println!("Gas used: {}", result.total_gas);
println!("Fee: {} Fuel units", result.total_fee);
```

## Database Integration

The manager includes SQLite database support for tracking initialization history, performance metrics, and fees.

### Database Schema

```sql
CREATE TABLE wallet_initializations (
    id INTEGER PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    eoa_worker_id INTEGER,
    eoa_address TEXT,
    tx_hash TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    job_queued_at TEXT NOT NULL,
    job_started_at TEXT,
    job_completed_at TEXT,
    duration_ms INTEGER,
    error_message TEXT,
    gas_used INTEGER,
    total_fee_eth REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### Configuration

Set the database location via environment variable:

```bash
# In your .env file
DATABASE_URL=wallet_init.db  # or :memory: for in-memory
```

Or programmatically:

```rust
let manager = InitializationManager::new_with_db(
    eoa_wallets,
    provider,
    config,
    Some("path/to/database.db".to_string()),
);
```

## NTP Time Synchronization

The system automatically synchronizes with NTP servers for accurate timestamps:
- Syncs on startup and every hour
- Falls back to system time if NTP fails
- All timestamps stored in UTC RFC3339 format

## Query Tools

### Database Query CLI

```bash
# Show all records in formatted table
cargo run --bin db-query -- show

# Query specific wallet
cargo run --bin db-query -- query 0x2222279484204e6e790c650281bb8ea18b35d4b6

# Show statistics
cargo run --bin db-query -- stats

# Show worker performance
cargo run --bin db-query -- workers
```

### Programmatic Access

```rust
// Get database statistics
let db_stats = manager.get_db_stats().await;

// Get worker performance
let worker_perf = manager.get_worker_performance().await;

// Direct database access
use zap_wallet_init_manager::InitializationDb;
let db = InitializationDb::new("wallet_init.db").await?;

// Print formatted table
db.print_all_records().await?;

// Query specific address
let records = db.query_evm_address("0x2222").await?;
```

## Example Output

### Initialization Table
```
Time source: NTP synchronized (offset: -29ms, last sync: 12:57:59 UTC)

┌────┬──────────────────────────────────────────────────────────────────────┬────────┬──────────────────────────────────────────────────────────────────────┬─────────────────────┬─────────────────────┬─────────────────────┬────────────┬──────────────┬───────────┬────────────┬─────────────────────┐
│ ID │ Wallet Address                                                       │ Worker │ Transaction Hash                                                     │ Queued At           │ Started At          │ Completed At        │ Status     │ Duration     │ Gas Used  │ Fee (ETH)  │ Error               │
├────┼──────────────────────────────────────────────────────────────────────┼────────┼──────────────────────────────────────────────────────────────────────┼─────────────────────┼─────────────────────┼─────────────────────┼────────────┼──────────────┼───────────┼────────────┼─────────────────────┤
│  1 │ 0000000000000000000000002222279484204e6e790c650281bb8ea18b35d4b6     │      0 │ edec62cd2d9bfa887dc676c3f44175d1146ed253eb6af1d43fbe55ab1b632733     │ 2025-07-25 12:55:07 │ 2025-07-25 12:55:07 │ 2025-07-25 12:55:11 │ ✓ Success  │       3.9 s  │   2851173 │ 0.000004960│ -                   │
└────┴──────────────────────────────────────────────────────────────────────┴────────┴──────────────────────────────────────────────────────────────────────┴─────────────────────┴─────────────────────┴─────────────────────┴────────────┴──────────────┴───────────┴────────────┴─────────────────────┘

SUMMARY:
├─ Total Initializations: 1
├─ Queued: 0
├─ Processing: 0
├─ Successful: 1 (100.0%)
└─ Failed: 0 (0.0%)

FEE SUMMARY:
├─ Total Fees: 0.000004960 ETH
└─ Average Fee per Init: 0.000004960 ETH
```

## Architecture

```
┌─────────────────┐
│ TieredWallet    │
│     Cache       │
└────────┬────────┘
         │ spawn task
         ▼
┌─────────────────┐      ┌──────────────┐
│ Initialization  │◄────►│   SQLite     │
│    Manager      │      │   Database   │
└────────┬────────┘      └──────────────┘
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

## Lifecycle Tracking

Each wallet initialization follows this lifecycle:
1. **Queued**: Job received by manager
2. **Processing**: Worker picks up the job
3. **Completed/Failed**: Final status with transaction details

All tracked in a single database row with accurate timestamps and fee information.

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `max_queue_size` | Maximum pending jobs | 1000 |
| `worker_queue_size` | Per-worker queue size | 100 |
| `init_contract` | Contract ID for initialization | Required |
| `initialization_amount` | Fuel tokens for init | 1,000,000 |
| `gas_limit` | Gas limit per transaction | 1,000,000 |
| `verbose_logging` | Enable detailed logs | false |

## Fee Tracking

Fees are tracked with full precision:
- Stored in ETH (1 ETH = 10^9 Fuel units)
- Always displayed with 9 decimal places
- No rounding in storage or display
- Worker statistics include total and average fees

## Testing

```bash
# Run all tests
cd crates/zap-wallet-init-manager
cargo test

# Run database tests with output
cargo test --lib db::tests -- --nocapture

# Run specific test
cargo test --lib db::tests::test_fee_conversion
```

## Integration with TieredWalletCache

```rust
// With database support
let zapwal_cache = TieredWalletCache::new_with_init_manager_and_db(
    CACHE_ZAPWALLET_CAPACITY,
    fuel_provider.clone(),
    eoa_wallets,
    init_contract_id,
    Some(database_url),
);

// Without database
let zapwal_cache = TieredWalletCache::new_with_init_manager(
    CACHE_ZAPWALLET_CAPACITY,
    fuel_provider.clone(),
    eoa_wallets,
    init_contract_id,
);
```

## Environment Variables

```bash
# Database location
DATABASE_URL=wallet_init.db
INIT_DATABASE_URL=wallet_init.db

# Contract configuration
INIT_CONTRACT_ID=0x8e046df8e45aeebaf4443498eced8102688e2628c3e4eb58893f061553b04cc7

# EOA wallet private keys
EOA_PRIVATE_KEY_1=0x...
EOA_PRIVATE_KEY_2=0x...
EOA_PRIVATE_KEY_3=0x...

# Logging
RUST_LOG=info,zap_wallet_init_manager=debug
```

## Thread Safety

- All components are `Send + Sync`
- Channel-based communication
- Database access protected by `Arc<Mutex<Connection>>`
- UTXO management is worker-local

## Error Handling

```rust
match manager.initialize_wallet(request).await {
    Ok(result) => {
        println!("Success! TX: {}", result.tx_id);
        println!("Gas: {}, Fee: {} ETH", result.total_gas, result.total_fee as f64 / 1e9);
    },
    Err(InitializationError::InsufficientBalance) => { /* fund EOA */ },
    Err(InitializationError::UTXOError(_)) => { /* UTXO issue */ },
    Err(InitializationError::Timeout) => { /* took too long */ },
    Err(e) => { /* other error */ },
}
```

## License

MIT OR Apache-2.0