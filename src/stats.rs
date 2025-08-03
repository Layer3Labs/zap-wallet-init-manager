// zap-wallet-init-manager/src/stats.rs

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::time::Instant;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManagerStats {
    pub total_jobs_submitted: u64,
    pub total_jobs_completed: u64,
    pub total_jobs_failed: u64,
    pub current_queue_depth: usize,
    pub worker_stats: HashMap<usize, WorkerStats>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerStats {
    pub id: usize,
    pub total_processed: u64,
    pub total_failures: u64,
    pub current_queue_depth: usize,
    pub total_utxos: usize,
    pub available_balance: u128,
    #[serde(skip)]
    pub last_job_at: Option<Instant>,
    pub average_processing_time_ms: u64,
    pub min_processing_time_ms: u64,
    pub max_processing_time_ms: u64,
}

impl ManagerStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_submission(&mut self) {
        self.total_jobs_submitted += 1;
        self.current_queue_depth += 1;
    }

    pub fn record_completion(&mut self, worker_id: usize, success: bool) {
        if success {
            self.total_jobs_completed += 1;
        } else {
            self.total_jobs_failed += 1;
        }
        self.current_queue_depth = self.current_queue_depth.saturating_sub(1);

        if let Some(worker_stats) = self.worker_stats.get_mut(&worker_id) {
            worker_stats.total_processed += 1;
            if !success {
                worker_stats.total_failures += 1;
            }
            worker_stats.last_job_at = Some(Instant::now());
        }
    }

    pub fn get_summary(&self) -> String {
        format!(
            "Manager Stats: submitted={}, completed={}, failed={}, queued={}, workers={}",
            self.total_jobs_submitted,
            self.total_jobs_completed,
            self.total_jobs_failed,
            self.current_queue_depth,
            self.worker_stats.len()
        )
    }
}

#[cfg(feature = "metrics")]
pub mod prometheus_metrics {
    use prometheus::{IntCounter, IntGauge, HistogramVec, Registry};
    use std::sync::OnceLock;

    static METRICS: OnceLock<Metrics> = OnceLock::new();

    pub struct Metrics {
        pub jobs_submitted: IntCounter,
        pub jobs_completed: IntCounter,
        pub jobs_failed: IntCounter,
        pub queue_depth: IntGauge,
        pub processing_time: HistogramVec,
    }

    pub fn init_metrics(registry: &Registry) -> Result<(), prometheus::Error> {
        let metrics = Metrics {
            jobs_submitted: IntCounter::new("init_jobs_submitted_total", "Total initialization jobs submitted")?,
            jobs_completed: IntCounter::new("init_jobs_completed_total", "Total initialization jobs completed")?,
            jobs_failed: IntCounter::new("init_jobs_failed_total", "Total initialization jobs failed")?,
            queue_depth: IntGauge::new("init_queue_depth", "Current queue depth")?,
            processing_time: HistogramVec::new(
                prometheus::HistogramOpts::new("init_processing_time_seconds", "Job processing time"),
                &["worker_id"]
            )?,
        };

        registry.register(Box::new(metrics.jobs_submitted.clone()))?;
        registry.register(Box::new(metrics.jobs_completed.clone()))?;
        registry.register(Box::new(metrics.jobs_failed.clone()))?;
        registry.register(Box::new(metrics.queue_depth.clone()))?;
        registry.register(Box::new(metrics.processing_time.clone()))?;

        METRICS.set(metrics).map_err(|_| prometheus::Error::AlreadyReg)?;
        Ok(())
    }

    pub fn get_metrics() -> Option<&'static Metrics> {
        METRICS.get()
    }
}