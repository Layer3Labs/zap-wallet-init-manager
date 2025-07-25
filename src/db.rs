// zap-wallet-init-manager/src/db.rs

use rusqlite::{Connection, params, Result as SqliteResult, OptionalExtension};
use chrono::{DateTime, Utc, Duration};
use fuels::types::{Bytes32, EvmAddress};
use std::sync::{Arc, Mutex};
use anyhow::Result;
use rsntp::SntpClient;
use tokio::sync::RwLock;
use once_cell::sync::Lazy;
use crate::consts::*;

/// NTP time offset from system time
static NTP_OFFSET: Lazy<RwLock<Option<Duration>>> = Lazy::new(|| RwLock::new(None));
static NTP_LAST_SYNC: Lazy<RwLock<Option<DateTime<Utc>>>> = Lazy::new(|| RwLock::new(None));

/// Get current time with NTP synchronization
async fn get_ntp_time() -> DateTime<Utc> {
    // Check if we need to resync (every hour)
    let should_sync = {
        let last_sync = NTP_LAST_SYNC.read().await;
        match *last_sync {
            Some(last) => (Utc::now() - last).num_seconds() > 3600,
            None => true,
        }
    };

    if should_sync {
        // Try to sync with NTP
        if let Err(e) = sync_ntp_time().await {
            tracing::warn!("Failed to sync NTP time: {}. Using system time.", e);
        }
    }

    // Apply offset if available
    let offset = NTP_OFFSET.read().await;
    match *offset {
        Some(off) => Utc::now() + off,
        None => Utc::now(),
    }
}

/// Synchronize with NTP server
async fn sync_ntp_time() -> Result<()> {
    // Run NTP sync in blocking task since rsntp is not async
    let result = tokio::task::spawn_blocking(|| {
        let client = SntpClient::new();
        client.synchronize("pool.ntp.org")
    }).await??;

    // Calculate offset between NTP time and system time
    let ntp_time = result.datetime().into_chrono_datetime()?;
    let system_time = Utc::now();
    let offset = ntp_time.signed_duration_since(system_time);

    // Store the offset
    *NTP_OFFSET.write().await = Some(offset);
    *NTP_LAST_SYNC.write().await = Some(system_time);

    tracing::info!(
        "NTP time synchronized. Offset: {}ms",
        offset.num_milliseconds()
    );

    Ok(())
}

/// Database schema for initialization tracking
pub const INIT_TRACKING_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS wallet_initializations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_wallet_address ON wallet_initializations(wallet_address);
CREATE INDEX IF NOT EXISTS idx_status ON wallet_initializations(status);
CREATE INDEX IF NOT EXISTS idx_job_queued_at ON wallet_initializations(job_queued_at);
"#;

#[derive(Debug, Clone)]
pub struct InitializationRecord {
    pub id: i64,
    pub wallet_address: String,
    pub eoa_worker_id: Option<i32>,
    pub eoa_address: Option<String>,
    pub tx_hash: Option<String>,
    pub status: String,
    pub job_queued_at: DateTime<Utc>,
    pub job_started_at: Option<DateTime<Utc>>,
    pub job_completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub error_message: Option<String>,
    pub gas_used: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub enum InitStatus {
    Queued,      // Job is queued in the manager
    Processing,  // Worker has started processing
    Completed,   // Successfully completed
    Failed,      // Failed with error
}

impl InitStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            InitStatus::Queued => "queued",
            InitStatus::Processing => "processing",
            InitStatus::Completed => "completed",
            InitStatus::Failed => "failed",
        }
    }
}

pub struct InitializationDb {
    conn: Arc<Mutex<Connection>>,
}

impl InitializationDb {
    /// Create a new database connection
    pub async fn new(database_url: &str) -> Result<Self> {
        // Try to sync NTP time on startup
        if let Err(e) = sync_ntp_time().await {
            tracing::warn!("Failed to sync NTP time on startup: {}. Using system time.", e);
        }

        // For rusqlite, we need to handle the connection synchronously
        let conn = Connection::open(database_url)?;

        // Create tables
        conn.execute_batch(INIT_TRACKING_SCHEMA)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Record when a job is queued by the manager
    pub async fn record_job_queued(
        &self,
        wallet_address: &EvmAddress,
    ) -> Result<i64> {
        let wallet_addr = hex::encode(wallet_address.value().0);
        let queued_at = get_ntp_time().await.to_rfc3339();
        
        let record_id = {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO wallet_initializations
                (wallet_address, status, job_queued_at)
                VALUES (?1, ?2, ?3)",
                params![
                    wallet_addr,
                    InitStatus::Queued.as_str(),
                    queued_at
                ],
            )?;
            conn.last_insert_rowid()
        };

        Ok(record_id)
    }

    /// Record when a worker starts processing
    pub async fn record_job_started(
        &self,
        record_id: i64,
        worker_id: usize,
        eoa_address: &str,
    ) -> Result<()> {
        let started_at = get_ntp_time().await.to_rfc3339();

        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "UPDATE wallet_initializations
                SET status = ?1, job_started_at = ?2, eoa_worker_id = ?3, eoa_address = ?4
                WHERE id = ?5",
                params![
                    InitStatus::Processing.as_str(),
                    started_at,
                    worker_id as i32,
                    eoa_address,
                    record_id
                ],
            )?;
        }

        Ok(())
    }

    /// Update with transaction hash once available
    pub async fn update_tx_hash(
        &self,
        record_id: i64,
        tx_hash: &Bytes32,
    ) -> Result<()> {
        let tx_hash_str = hex::encode(tx_hash);

        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "UPDATE wallet_initializations
                SET tx_hash = ?1
                WHERE id = ?2",
                params![tx_hash_str, record_id],
            )?;
        }

        Ok(())
    }

    /// Record job completion (success)
    pub async fn record_job_completed(
        &self,
        record_id: i64,
        tx_hash: Option<&Bytes32>,
        gas_used: Option<u64>,
    ) -> Result<()> {
        let completed_at = get_ntp_time().await;
        
        let (started_at_opt, queued_at_opt, duration_ms) = {
            let conn = self.conn.lock().unwrap();

            // Get both started and queued times
            let times: (Option<String>, Option<String>) = conn.query_row(
                "SELECT job_started_at, job_queued_at FROM wallet_initializations WHERE id = ?1",
                params![record_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).unwrap_or((None, None));

            // Calculate duration from when job started (if available) or from when it was queued
            let duration_ms = if let Some(started_str) = &times.0 {
                let started_at = DateTime::parse_from_rfc3339(started_str)
                    .unwrap()
                    .with_timezone(&Utc);
                (completed_at - started_at).num_milliseconds()
            } else if let Some(queued_str) = &times.1 {
                let queued_at = DateTime::parse_from_rfc3339(queued_str)
                    .unwrap()
                    .with_timezone(&Utc);
                (completed_at - queued_at).num_milliseconds()
            } else {
                0
            };

            (times.0, times.1, duration_ms)
        };

        tracing::info!(
            "{}Job timing - Started: {}, Queued: {}, Duration: {} ms{}",
            MAGENTA,
            started_at_opt.as_ref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "pending".to_string()),
            queued_at_opt.as_ref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            duration_ms,
            RESET
        );

        {
            let conn = self.conn.lock().unwrap();

            let mut sql = "UPDATE wallet_initializations
                SET status = ?1, job_completed_at = ?2, duration_ms = ?3, gas_used = ?4".to_string();

            let mut param_count = 5;
            if tx_hash.is_some() {
                sql.push_str(", tx_hash = ?5");
                param_count = 6;
            }

            sql.push_str(&format!(" WHERE id = ?{}", param_count));

            if let Some(tx) = tx_hash {
                let tx_hash_str = hex::encode(tx);
                conn.execute(
                    &sql,
                    params![
                        InitStatus::Completed.as_str(),
                        completed_at.to_rfc3339(),
                        duration_ms,
                        gas_used.map(|g| g as i64),
                        tx_hash_str,
                        record_id
                    ],
                )?;
            } else {
                conn.execute(
                    &sql,
                    params![
                        InitStatus::Completed.as_str(),
                        completed_at.to_rfc3339(),
                        duration_ms,
                        gas_used.map(|g| g as i64),
                        record_id
                    ],
                )?;
            }
        }

        Ok(())
    }

    /// Record job failure
    pub async fn record_job_failed(
        &self,
        record_id: i64,
        error_message: &str,
    ) -> Result<()> {
        let completed_at = get_ntp_time().await;

        tracing::error!(
            "{}Job {} failed: {}{}",
            MAGENTA,
            record_id,
            error_message,
            RESET
        );

        let duration_ms = {
            let conn = self.conn.lock().unwrap();

            // Get start time or queue time
            let times: (Option<String>, Option<String>) = conn.query_row(
                "SELECT job_started_at, job_queued_at FROM wallet_initializations WHERE id = ?1",
                params![record_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).unwrap_or((None, None));

            if let Some(started_str) = times.0 {
                let started_at = DateTime::parse_from_rfc3339(&started_str)
                    .unwrap()
                    .with_timezone(&Utc);
                Some((completed_at - started_at).num_milliseconds())
            } else if let Some(queued_str) = times.1 {
                let queued_at = DateTime::parse_from_rfc3339(&queued_str)
                    .unwrap()
                    .with_timezone(&Utc);
                Some((completed_at - queued_at).num_milliseconds())
            } else {
                None
            }
        };

        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "UPDATE wallet_initializations
                SET status = ?1, job_completed_at = ?2, duration_ms = ?3, error_message = ?4
                WHERE id = ?5",
                params![
                    InitStatus::Failed.as_str(),
                    completed_at.to_rfc3339(),
                    duration_ms,
                    error_message,
                    record_id
                ],
            )?;
        }

        Ok(())
    }

    /// Get record by wallet address (most recent)
    pub async fn get_record_by_wallet(
        &self,
        wallet_address: &EvmAddress,
    ) -> Result<Option<i64>> {
        let wallet_addr = hex::encode(wallet_address.value().0);
        
        let record_id = {
            let conn = self.conn.lock().unwrap();
            conn.query_row(
                "SELECT id FROM wallet_initializations
                WHERE wallet_address = ?1
                ORDER BY job_queued_at DESC
                LIMIT 1",
                params![wallet_addr],
                |row| row.get(0),
            ).optional()?
        };

        Ok(record_id)
    }

    /// Get initialization statistics
    pub async fn get_stats(&self) -> Result<InitStats> {
        let (total, completed, failed, queued, processing, avg_duration) = {
            let conn = self.conn.lock().unwrap();

            let total: i64 = conn.query_row(
                "SELECT COUNT(*) FROM wallet_initializations",
                [],
                |row| row.get(0),
            ).unwrap_or(0);

            let completed: i64 = conn.query_row(
                "SELECT COUNT(*) FROM wallet_initializations WHERE status = ?1",
                params![InitStatus::Completed.as_str()],
                |row| row.get(0),
            ).unwrap_or(0);

            let failed: i64 = conn.query_row(
                "SELECT COUNT(*) FROM wallet_initializations WHERE status = ?1",
                params![InitStatus::Failed.as_str()],
                |row| row.get(0),
            ).unwrap_or(0);

            let queued: i64 = conn.query_row(
                "SELECT COUNT(*) FROM wallet_initializations WHERE status = ?1",
                params![InitStatus::Queued.as_str()],
                |row| row.get(0),
            ).unwrap_or(0);

            let processing: i64 = conn.query_row(
                "SELECT COUNT(*) FROM wallet_initializations WHERE status = ?1",
                params![InitStatus::Processing.as_str()],
                |row| row.get(0),
            ).unwrap_or(0);

            // Handle potential NULL from AVG() function
            let avg_duration: Option<f64> = conn.query_row(
                "SELECT AVG(duration_ms) FROM wallet_initializations WHERE status = ?1 AND duration_ms IS NOT NULL",
                params![InitStatus::Completed.as_str()],
                |row| row.get::<_, Option<f64>>(0),
            ).unwrap_or(None);

            (total, completed, failed, queued, processing, avg_duration)
        };

        let recent_inits = self.get_recent_initializations(10).await?;

        Ok(InitStats {
            total_initializations: total as u64,
            successful_initializations: completed as u64,
            failed_initializations: failed as u64,
            queued_initializations: queued as u64,
            processing_initializations: processing as u64,
            average_duration_ms: avg_duration.map(|d| d as u64),
            recent_initializations: recent_inits,
        })
    }

    /// Get recent initializations
    pub async fn get_recent_initializations(&self, limit: i64) -> Result<Vec<InitializationRecord>> {
        let records = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT
                    id, wallet_address, eoa_worker_id, eoa_address,
                    tx_hash, status, job_queued_at, job_started_at,
                    job_completed_at, duration_ms, error_message, gas_used
                FROM wallet_initializations
                ORDER BY job_queued_at DESC
                LIMIT ?1"
            )?;

            let records = stmt.query_map(params![limit], |row| {
                Ok(InitializationRecord {
                    id: row.get(0)?,
                    wallet_address: row.get(1)?,
                    eoa_worker_id: row.get(2)?,
                    eoa_address: row.get(3)?,
                    tx_hash: row.get(4)?,
                    status: row.get(5)?,
                    job_queued_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(6)?)
                        .unwrap()
                        .with_timezone(&Utc),
                    job_started_at: row.get::<_, Option<String>>(7)?
                        .map(|s| DateTime::parse_from_rfc3339(&s).unwrap().with_timezone(&Utc)),
                    job_completed_at: row.get::<_, Option<String>>(8)?
                        .map(|s| DateTime::parse_from_rfc3339(&s).unwrap().with_timezone(&Utc)),
                    duration_ms: row.get(9)?,
                    error_message: row.get(10)?,
                    gas_used: row.get(11)?,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

            records
        };

        Ok(records)
    }

    /// Get initialization history for a specific wallet
    pub async fn get_wallet_history(&self, wallet_address: &EvmAddress) -> Result<Vec<InitializationRecord>> {
        let wallet_addr = hex::encode(wallet_address.value().0);
        
        let records = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT
                    id, wallet_address, eoa_worker_id, eoa_address,
                    tx_hash, status, job_queued_at, job_started_at,
                    job_completed_at, duration_ms, error_message, gas_used
                FROM wallet_initializations
                WHERE wallet_address = ?1
                ORDER BY job_queued_at DESC"
            )?;

            let records = stmt.query_map(params![wallet_addr], |row| {
                Ok(InitializationRecord {
                    id: row.get(0)?,
                    wallet_address: row.get(1)?,
                    eoa_worker_id: row.get(2)?,
                    eoa_address: row.get(3)?,
                    tx_hash: row.get(4)?,
                    status: row.get(5)?,
                    job_queued_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(6)?)
                        .unwrap()
                        .with_timezone(&Utc),
                    job_started_at: row.get::<_, Option<String>>(7)?
                        .map(|s| DateTime::parse_from_rfc3339(&s).unwrap().with_timezone(&Utc)),
                    job_completed_at: row.get::<_, Option<String>>(8)?
                        .map(|s| DateTime::parse_from_rfc3339(&s).unwrap().with_timezone(&Utc)),
                    duration_ms: row.get(9)?,
                    error_message: row.get(10)?,
                    gas_used: row.get(11)?,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

            records
        };

        Ok(records)
    }

    /// Get worker performance stats
    pub async fn get_worker_stats(&self) -> Result<Vec<WorkerPerformance>> {
        let stats = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT
                    eoa_worker_id,
                    eoa_address,
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_jobs,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_jobs,
                    AVG(CASE WHEN status = 'completed' THEN duration_ms ELSE NULL END) as avg_duration_ms,
                    MIN(CASE WHEN status = 'completed' THEN duration_ms ELSE NULL END) as min_duration_ms,
                    MAX(CASE WHEN status = 'completed' THEN duration_ms ELSE NULL END) as max_duration_ms
                FROM wallet_initializations
                WHERE eoa_worker_id IS NOT NULL
                GROUP BY eoa_worker_id, eoa_address
                ORDER BY total_jobs DESC"
            )?;

            let stats = stmt.query_map([], |row| {
                Ok(WorkerPerformance {
                    worker_id: row.get::<_, i32>(0)? as usize,
                    eoa_address: row.get(1)?,
                    total_jobs: row.get::<_, i64>(2)? as u64,
                    successful_jobs: row.get::<_, i64>(3)? as u64,
                    failed_jobs: row.get::<_, i64>(4)? as u64,
                    avg_duration_ms: row.get::<_, Option<f64>>(5)?.map(|d| d as u64),
                    min_duration_ms: row.get::<_, Option<i64>>(6)?.map(|d| d as u64),
                    max_duration_ms: row.get::<_, Option<i64>>(7)?.map(|d| d as u64),
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

            stats
        };

        Ok(stats)
    }

    /// Print all database content in a formatted table
    pub async fn print_all_records(&self) -> Result<()> {
        let records = self.get_recent_initializations(1000).await?;

        if records.is_empty() {
            println!("No records found in database.");
            return Ok(());
        }

        // Print NTP sync status
        let ntp_status = {
            let offset = NTP_OFFSET.read().await;
            let last_sync = NTP_LAST_SYNC.read().await;
            match (*offset, *last_sync) {
                (Some(off), Some(sync)) => {
                    format!("NTP synchronized (offset: {}ms, last sync: {})",
                        off.num_milliseconds(),
                        sync.format("%H:%M:%S UTC"))
                },
                _ => "Using system time (NTP not synchronized)".to_string()
            }
        };
        println!("\nTime source: {}", ntp_status);

        // Use box drawing characters for better formatting
        println!("\n┌────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐");
        println!("│       WALLET INITIALIZATION RECORDS                                                                                                                                                                                                                                                       │");
        println!("├────┼──────────────────────────────────────────────────────────────────────┼────────┼──────────────────────────────────────────────────────────────────────┼─────────────────────┼─────────────────────┼─────────────────────┼────────────┼──────────────┼───────────┼─────────────────────┤");
        println!("│ ID │ Wallet Address                                                       │ Worker │ Transaction Hash                                                     │ Queued At           │ Started At          │ Completed At        │ Status     │ Duration     │ Gas Used  │ Error               │");
        println!("├────┼──────────────────────────────────────────────────────────────────────┼────────┼──────────────────────────────────────────────────────────────────────┼─────────────────────┼─────────────────────┼─────────────────────┼────────────┼──────────────┼───────────┼─────────────────────┤");

        for record in records {
            let queued = record.job_queued_at.format("%Y-%m-%d %H:%M:%S").to_string();
            let started = record.job_started_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "-".to_string());
            let completed = record.job_completed_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "-".to_string());

            let duration_str = match record.duration_ms {
                Some(ms) => {
                    if ms < 1000 {
                        format!("{:>10} ms", ms)
                    } else if ms < 60000 {
                        format!("{:>9.1} s", ms as f64 / 1000.0)
                    } else {
                        let minutes = ms / 60000;
                        let seconds = (ms % 60000) as f64 / 1000.0;
                        format!("{:>3}m {:>4.1}s", minutes, seconds)
                    }
                },
                None => "           -".to_string(),
            };

            let gas_str = match record.gas_used {
                Some(gas) => format!("{:>9}", gas),
                None => "        -".to_string(),
            };

            let error_str = match &record.error_message {
                Some(msg) => {
                    if msg.len() > 19 {
                        format!("{}...", &msg[0..16])
                    } else {
                        msg.clone()
                    }
                },
                None => "-".to_string(),
            };

            let status_str = match record.status.as_str() {
                "completed" => "✓ Success ",
                "failed" => "✗ Failed  ",
                "processing" => "⚡ Running ",
                "queued" => "⏳ Queued  ",
                _ => &record.status,
            };

            let worker_str = record.eoa_worker_id
                .map(|id| format!("{:>6}", id))
                .unwrap_or_else(|| "     -".to_string());

            let tx_hash_str = record.tx_hash.as_deref().unwrap_or("-");

            println!("│{:>3} │ {:68} │ {} │ {:68} │ {:19} │ {:19} │ {:19} │ {:9} │ {:12} │ {:9} │ {:19} │",
                record.id,
                record.wallet_address,
                worker_str,
                tx_hash_str,
                queued,
                started,
                completed,
                status_str,
                duration_str,
                gas_str,
                error_str
            );
        }

        println!("└────┴──────────────────────────────────────────────────────────────────────┴────────┴──────────────────────────────────────────────────────────────────────┴─────────────────────┴─────────────────────┴─────────────────────┴────────────┴──────────────┴───────────┴─────────────────────┘");

        // Print summary
        let stats = self.get_stats().await?;
        println!("\nSUMMARY:");
        println!("├─ Total Initializations: {}", stats.total_initializations);
        println!("├─ Queued: {}", stats.queued_initializations);
        println!("├─ Processing: {}", stats.processing_initializations);
        println!("├─ Successful: {} ({:.1}%)",
            stats.successful_initializations,
            if stats.total_initializations > 0 {
                (stats.successful_initializations as f64 / stats.total_initializations as f64) * 100.0
            } else {
                0.0
            }
        );
        println!("├─ Failed: {} ({:.1}%)",
            stats.failed_initializations,
            if stats.total_initializations > 0 {
                (stats.failed_initializations as f64 / stats.total_initializations as f64) * 100.0
            } else {
                0.0
            }
        );

        if let Some(avg_ms) = stats.average_duration_ms {
            // Convert to human-readable format
            let duration = if avg_ms < 1000 {
                format!("{} ms", avg_ms)
            } else if avg_ms < 60000 {
                format!("{:.1} seconds", avg_ms as f64 / 1000.0)
            } else {
                let minutes = avg_ms / 60000;
                let seconds = (avg_ms % 60000) as f64 / 1000.0;
                format!("{} min {:.1} sec", minutes, seconds)
            };
            println!("└─ Average Duration: {}", duration);
        } else {
            println!("└─ Average Duration: N/A");
        }

        Ok(())
    }

    /// Query and print records for a specific EVM address
    pub async fn query_evm_address(&self, evm_address: &str) -> Result<Vec<InitializationRecord>> {
        // Remove 0x prefix if present
        let clean_address = evm_address.trim_start_matches("0x").to_lowercase();

        let records = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT
                    id, wallet_address, eoa_worker_id, eoa_address,
                    tx_hash, status, job_queued_at, job_started_at,
                    job_completed_at, duration_ms, error_message, gas_used
                FROM wallet_initializations
                WHERE lower(wallet_address) LIKE ?1
                ORDER BY job_queued_at DESC"
            )?;

            let pattern = format!("%{}%", clean_address);
            let records = stmt.query_map(params![pattern], |row| {
                Ok(InitializationRecord {
                    id: row.get(0)?,
                    wallet_address: row.get(1)?,
                    eoa_worker_id: row.get(2)?,
                    eoa_address: row.get(3)?,
                    tx_hash: row.get(4)?,
                    status: row.get(5)?,
                    job_queued_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(6)?)
                        .unwrap()
                        .with_timezone(&Utc),
                    job_started_at: row.get::<_, Option<String>>(7)?
                        .map(|s| DateTime::parse_from_rfc3339(&s).unwrap().with_timezone(&Utc)),
                    job_completed_at: row.get::<_, Option<String>>(8)?
                        .map(|s| DateTime::parse_from_rfc3339(&s).unwrap().with_timezone(&Utc)),
                    duration_ms: row.get(9)?,
                    error_message: row.get(10)?,
                    gas_used: row.get(11)?,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

            records
        };

        if !records.is_empty() {
            println!("\nFound {} records for address containing '{}':", records.len(), clean_address);
            println!("─────────────────────────────────────────────────────────────────────────────────");

            for record in &records {
                println!("\nRecord ID: {}", record.id);
                println!("├─ Wallet Address: {}", record.wallet_address);
                if let Some(tx) = &record.tx_hash {
                    println!("├─ Transaction Hash: {}", tx);
                }
                println!("├─ Status: {}", record.status);
                if let Some(worker_id) = record.eoa_worker_id {
                    println!("├─ Worker ID: {} ({})", worker_id, record.eoa_address.as_deref().unwrap_or("unknown"));
                }
                println!("├─ Queued: {}", record.job_queued_at.format("%Y-%m-%d %H:%M:%S UTC"));

                if let Some(started) = &record.job_started_at {
                    println!("├─ Started: {}", started.format("%Y-%m-%d %H:%M:%S UTC"));
                }

                if let Some(completed) = &record.job_completed_at {
                    println!("├─ Completed: {}", completed.format("%Y-%m-%d %H:%M:%S UTC"));
                }

                if let Some(duration) = record.duration_ms {
                    println!("├─ Duration: {} ms", duration);
                }

                if let Some(gas) = record.gas_used {
                    println!("├─ Gas Used: {}", gas);
                }

                if let Some(error) = &record.error_message {
                    println!("└─ Error: {}", error);
                } else if record.status == "completed" {
                    println!("└─ Result: Success");
                } else {
                    println!("└─ Status: {}", record.status);
                }
            }
            println!("─────────────────────────────────────────────────────────────────────────────────");
        } else {
            println!("No records found for address containing '{}'", clean_address);
        }

        Ok(records)
    }
}

#[derive(Debug, Clone)]
pub struct InitStats {
    pub total_initializations: u64,
    pub successful_initializations: u64,
    pub failed_initializations: u64,
    pub queued_initializations: u64,
    pub processing_initializations: u64,
    pub average_duration_ms: Option<u64>,
    pub recent_initializations: Vec<InitializationRecord>,
}

#[derive(Debug, Clone)]
pub struct WorkerPerformance {
    pub worker_id: usize,
    pub eoa_address: String,
    pub total_jobs: u64,
    pub successful_jobs: u64,
    pub failed_jobs: u64,
    pub avg_duration_ms: Option<u64>,
    pub min_duration_ms: Option<u64>,
    pub max_duration_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuels::types::Bits256;

    fn create_test_address(index: u64) -> EvmAddress {
        let mut bytes = [0u8; 32];
        bytes[12..20].copy_from_slice(&index.to_be_bytes());
        EvmAddress::from(Bits256(bytes))
    }

    fn create_test_tx_hash(index: u64) -> Bytes32 {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&index.to_be_bytes());
        Bytes32::from(bytes)
    }

    #[tokio::test]
    async fn test_single_row_lifecycle() -> Result<()> {
        let db = InitializationDb::new(":memory:").await?;
        let wallet_addr = create_test_address(1);

        // 1. Record when job is queued
        let record_id = db.record_job_queued(&wallet_addr).await?;
        assert!(record_id > 0);

        // 2. Record when worker starts
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        db.record_job_started(record_id, 0, "0xworker0").await?;

        // 3. Update with tx hash when available
        let tx_hash = create_test_tx_hash(1);
        db.update_tx_hash(record_id, &tx_hash).await?;

        // 4. Record completion
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        db.record_job_completed(record_id, Some(&tx_hash), Some(250_000)).await?;

        // Verify single row with complete lifecycle
        let history = db.get_wallet_history(&wallet_addr).await?;
        assert_eq!(history.len(), 1);

        let record = &history[0];
        assert_eq!(record.status, "completed");
        assert!(record.job_queued_at < record.job_started_at.unwrap());
        assert!(record.job_started_at.unwrap() < record.job_completed_at.unwrap());
        assert!(record.duration_ms.unwrap() >= 100);
        assert_eq!(record.gas_used, Some(250_000));

        Ok(())
    }

    #[tokio::test]
    async fn test_queued_only() -> Result<()> {
        let db = InitializationDb::new(":memory:").await?;
        let wallet_addr = create_test_address(2);

        // Only queue the job
        let record_id = db.record_job_queued(&wallet_addr).await?;

        // Check stats
        let stats = db.get_stats().await?;
        assert_eq!(stats.total_initializations, 1);
        assert_eq!(stats.queued_initializations, 1);
        assert_eq!(stats.processing_initializations, 0);
        assert_eq!(stats.successful_initializations, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_failure_recording() -> Result<()> {
        let db = InitializationDb::new(":memory:").await?;
        let wallet_addr = create_test_address(3);

        // Queue and start job
        let record_id = db.record_job_queued(&wallet_addr).await?;
        db.record_job_started(record_id, 1, "0xworker1").await?;

        // Record failure
        db.record_job_failed(record_id, "Network timeout").await?;

        // Verify
        let history = db.get_wallet_history(&wallet_addr).await?;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].status, "failed");
        assert_eq!(history[0].error_message.as_deref(), Some("Network timeout"));

        Ok(())
    }

    #[tokio::test]
    async fn test_worker_stats() -> Result<()> {
        let db = InitializationDb::new(":memory:").await?;

        // Create test data for multiple workers
        for i in 0..10 {
            let wallet_addr = create_test_address(i);
            let worker_id = (i % 3) as usize;

            let record_id = db.record_job_queued(&wallet_addr).await?;
            db.record_job_started(record_id, worker_id, &format!("0xworker{}", worker_id)).await?;

            let tx_hash = create_test_tx_hash(i);
            db.update_tx_hash(record_id, &tx_hash).await?;

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            if i % 4 == 0 {
                db.record_job_failed(record_id, "Test failure").await?;
            } else {
                db.record_job_completed(record_id, Some(&tx_hash), Some(200_000 + (i * 10_000))).await?;
            }
        }

        // Get worker stats
        let worker_stats = db.get_worker_stats().await?;
        assert!(!worker_stats.is_empty());

        // Verify each worker has processed jobs
        for stat in &worker_stats {
            assert!(stat.total_jobs > 0);
            assert!(stat.successful_jobs > 0 || stat.failed_jobs > 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_print_database_content() -> Result<()> {
        let db = InitializationDb::new(":memory:").await?;

        // Create some test data
        for i in 0..5 {
            let wallet_addr = create_test_address(i);
            let record_id = db.record_job_queued(&wallet_addr).await?;

            if i < 3 {
                db.record_job_started(record_id, i as usize % 2, &format!("0xworker{}", i % 2)).await?;

                let tx_hash = create_test_tx_hash(i);
                db.update_tx_hash(record_id, &tx_hash).await?;

                tokio::time::sleep(tokio::time::Duration::from_millis(50 + i * 20)).await;

                if i % 2 == 0 {
                    db.record_job_completed(record_id, Some(&tx_hash), Some(250_000 + i * 10_000)).await?;
                } else {
                    db.record_job_failed(record_id, "Network timeout").await?;
                }
            }
            // Leave some in queued/processing state
        }

        // Print all records
        println!("\n=== Testing print_all_records ===");
        db.print_all_records().await?;

        Ok(())
    }
}