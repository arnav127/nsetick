//! Memory accounting and the out-of-memory guard.
//!
//! Two different things are tracked here, because they behave differently.
//!
//! **Row-group buffering** is what each open Parquet writer holds for the row group it is
//! building. It is genuinely elastic: flushing a row group early releases it. Measurement
//! says it is also not worth tuning - throughput was within noise (7.4s to 7.9s on an 8M
//! record session) across budgets from 64 MB to 3 GB - so the budget exists to bound memory,
//! not to buy speed.
//!
//! **Open partitions** are the real footprint, and they are not elastic. Writing a session
//! partitioned by symbol keeps one Parquet writer open per symbol for the whole run, because
//! NSE interleaves every symbol throughout the day and a closed Parquet file cannot be
//! reopened to append. Measured on a CM session: 620 partitions cost about 1.3 GB more than
//! a single file, so roughly 2 MB per open partition, regardless of page size or row-group
//! budget.
//!
//! That second figure is what decides whether a run fits. The guard turns it into an
//! arithmetic check made when each partition is opened, so a run that cannot fit stops with
//! an explanation and a suggested remedy instead of taking the machine down with it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{bail, Result};

/// Measured cost of holding one Parquet writer open: column writer state, page buffers and
/// dictionaries. Derived from (1559 MB - 269 MB) / 620 partitions on a 17-column CM layout,
/// rounded up for headroom.
pub const BYTES_PER_OPEN_PARTITION: usize = 2_500_000;

/// Fraction of currently available memory a run may plan to occupy.
const FOOTPRINT_FRACTION: f64 = 0.70;

/// Fraction of the footprint allowed to be elastic row-group buffering.
const BUFFER_FRACTION: f64 = 0.25;

const BUFFER_FLOOR: usize = 128 * 1024 * 1024;
const BUFFER_CEILING: usize = 2 * 1024 * 1024 * 1024;
const FOOTPRINT_FLOOR: usize = 1024 * 1024 * 1024;

#[derive(Debug)]
pub struct MemoryGuard {
    /// Bytes of row-group buffering currently held across every shard.
    used: AtomicUsize,
    peak: AtomicUsize,
    buffer_limit: usize,

    /// Parquet writers currently open across every shard.
    partitions: AtomicUsize,
    partitions_peak: AtomicUsize,
    /// Total process footprint the run is allowed to plan for.
    footprint_limit: usize,
    per_partition: usize,
}

impl MemoryGuard {
    pub fn new(footprint_limit: usize, buffer_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            buffer_limit: buffer_limit.max(1),
            partitions: AtomicUsize::new(0),
            partitions_peak: AtomicUsize::new(0),
            footprint_limit,
            per_partition: BYTES_PER_OPEN_PARTITION,
        })
    }

    /// A guard that never fires, for tests and single-partition writes.
    pub fn unlimited() -> Arc<Self> {
        MemoryGuard::new(usize::MAX, usize::MAX / 2)
    }

    /// Record that a partition's in-progress buffer changed from `before` to `after`.
    ///
    /// Taking both values rather than a delta is deliberate: `ArrowWriter` shrinks its own
    /// buffer when it rolls a row group internally, and an accounting scheme that only ever
    /// adds drifts upward until the budget fires on every write.
    pub fn adjust(&self, before: usize, after: usize) {
        if after >= before {
            let delta = after - before;
            let now = self.used.fetch_add(delta, Ordering::Relaxed) + delta;
            self.peak.fetch_max(now, Ordering::Relaxed);
        } else {
            let delta = before - after;
            self.used
                .fetch_sub(delta.min(self.used()), Ordering::Relaxed);
        }
    }

    pub fn release(&self, amount: usize) {
        self.used
            .fetch_sub(amount.min(self.used()), Ordering::Relaxed);
    }

    /// Account for one more open Parquet writer, refusing if the run would not fit.
    ///
    /// Called before the writer is created, so the failure happens with the machine still
    /// healthy rather than after it has started swapping.
    pub fn open_partition(&self, layout_hint: &str) -> Result<()> {
        let n = self.partitions.fetch_add(1, Ordering::Relaxed) + 1;
        self.partitions_peak.fetch_max(n, Ordering::Relaxed);

        let projected = n
            .saturating_mul(self.per_partition)
            .saturating_add(self.buffer_limit);
        if projected > self.footprint_limit {
            let fits = self.footprint_limit.saturating_sub(self.buffer_limit) / self.per_partition;
            bail!(
                "memory guard: {n} open partitions would need about {}, over the {} limit \
                 for this run.\n\
                 Each open Parquet writer costs roughly {} and cannot be closed early, \
                 because NSE interleaves every symbol throughout the session.\n\
                 About {fits} partitions fit in the current limit.\n\
                 Options: narrow the run with --where (for example a symbol list), write one \
                 file per date with --partition-by none, or raise the ceiling with \
                 --memory-limit-mb if the machine has the headroom.\n\
                 (partitioning by {layout_hint})",
                human(projected),
                human(self.footprint_limit),
                human(self.per_partition),
            );
        }
        Ok(())
    }

    pub fn close_partition(&self) {
        let cur = self.partitions.load(Ordering::Relaxed);
        if cur > 0 {
            self.partitions.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    pub fn buffer_limit(&self) -> usize {
        self.buffer_limit
    }

    pub fn footprint_limit(&self) -> usize {
        self.footprint_limit
    }

    pub fn partitions_peak(&self) -> usize {
        self.partitions_peak.load(Ordering::Relaxed)
    }

    /// Best estimate of what the process is actually occupying.
    pub fn projected_bytes(&self) -> usize {
        self.partitions_peak()
            .saturating_mul(self.per_partition)
            .saturating_add(self.peak())
    }

    pub fn over_buffer_limit(&self) -> bool {
        self.used() > self.buffer_limit
    }
}

/// Memory currently available on this machine, in bytes.
pub fn available_bytes() -> u64 {
    use sysinfo::System;
    let mut sys = System::new();
    sys.refresh_memory();
    // `available` counts reclaimable cache, unlike `free`.
    sys.available_memory()
}

pub fn total_bytes() -> u64 {
    use sysinfo::System;
    let mut sys = System::new();
    sys.refresh_memory();
    sys.total_memory()
}

/// Footprint and buffer limits for this machine, given the memory free right now.
///
/// `headroom` covers what neither figure tracks: chunks in flight on the channels and the
/// Arrow batches queued behind them.
pub fn default_limits(headroom: usize) -> (usize, usize) {
    let usable = (available_bytes() as usize).saturating_sub(headroom);
    let footprint = ((usable as f64 * FOOTPRINT_FRACTION) as usize).max(FOOTPRINT_FLOOR);
    let buffer =
        ((footprint as f64 * BUFFER_FRACTION) as usize).clamp(BUFFER_FLOOR, BUFFER_CEILING);
    (footprint, buffer)
}

pub fn human(bytes: usize) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u <= 1 {
        format!("{v:.0} {}", UNITS[u])
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjust_tracks_growth_and_shrinkage() {
        let g = MemoryGuard::new(FOOTPRINT_FLOOR, 1000);
        g.adjust(0, 100);
        assert_eq!(g.used(), 100);
        g.adjust(100, 250);
        assert_eq!(g.used(), 250);
        // The case the previous accounting got wrong: a writer rolling its own row group.
        g.adjust(250, 10);
        assert_eq!(g.used(), 10, "shrinkage must be subtracted, not ignored");
    }

    #[test]
    fn repeated_grow_then_shrink_does_not_drift() {
        let g = MemoryGuard::new(FOOTPRINT_FLOOR, 1_000_000);
        for _ in 0..1000 {
            g.adjust(0, 5000);
            g.adjust(5000, 0);
        }
        assert_eq!(g.used(), 0, "accounting drifted to {}", g.used());
    }

    #[test]
    fn peak_is_remembered() {
        let g = MemoryGuard::new(FOOTPRINT_FLOOR, 1000);
        g.adjust(0, 900);
        g.adjust(900, 10);
        assert_eq!(g.used(), 10);
        assert_eq!(g.peak(), 900);
    }

    #[test]
    fn one_guard_shared_by_shards_sums_rather_than_multiplies() {
        // The defect this replaced: each shard enforcing its own copy of the limit.
        let shared = MemoryGuard::new(FOOTPRINT_FLOOR, 300);
        let a = Arc::clone(&shared);
        let b = Arc::clone(&shared);
        a.adjust(0, 200);
        b.adjust(0, 200);
        assert_eq!(shared.used(), 400);
        assert!(
            shared.over_buffer_limit(),
            "two shards at 200 each must exceed a global 300"
        );
    }

    #[test]
    fn partitions_are_allowed_up_to_the_footprint_then_refused() {
        // An explicitly supplied limit is honoured exactly, so this leaves room for ten.
        let buffer = 1000;
        let footprint = 10 * BYTES_PER_OPEN_PARTITION + buffer;
        let g = MemoryGuard::new(footprint, buffer);
        for i in 0..10 {
            g.open_partition("symbol")
                .unwrap_or_else(|e| panic!("partition {i}: {e}"));
        }
        let err = g.open_partition("symbol").unwrap_err().to_string();
        assert!(err.contains("memory guard"), "{err}");
        // The message has to be actionable, not just a refusal.
        assert!(err.contains("--partition-by none"), "{err}");
        assert!(err.contains("--memory-limit-mb"), "{err}");
        assert!(err.contains("--where"), "{err}");
    }

    #[test]
    fn the_refusal_says_how_many_would_fit() {
        let g = MemoryGuard::new(FOOTPRINT_FLOOR, 1024);
        let mut err: Option<String> = None;
        for _ in 0..100_000 {
            if let Err(e) = g.open_partition("symbol") {
                err = Some(e.to_string());
                break;
            }
        }
        let err = err.expect("the guard must eventually refuse");
        let fits = (FOOTPRINT_FLOOR - 1024) / BYTES_PER_OPEN_PARTITION;
        assert!(err.contains(&fits.to_string()), "{err}");
    }

    #[test]
    fn a_full_cm_universe_fits_on_a_modest_machine() {
        // ~2000 symbols is the Capital Market universe; this must not be refused on 8 GB.
        let eight_gb = 8 * 1024 * 1024 * 1024usize;
        let (footprint, buffer) = (
            (eight_gb as f64 * FOOTPRINT_FRACTION) as usize,
            BUFFER_FLOOR,
        );
        let g = MemoryGuard::new(footprint, buffer);
        for i in 0..2000 {
            g.open_partition("symbol")
                .unwrap_or_else(|e| panic!("symbol {i} of 2000 refused on an 8 GB machine: {e}"));
        }
    }

    #[test]
    fn unlimited_never_refuses() {
        let g = MemoryGuard::unlimited();
        for _ in 0..50_000 {
            g.open_partition("symbol").unwrap();
        }
    }

    #[test]
    fn an_explicit_limit_is_honoured_rather_than_clamped_up() {
        // Passing --memory-limit-mb must mean what it says, even when it is very small.
        let g = MemoryGuard::new(3 * BYTES_PER_OPEN_PARTITION + 1, 0);
        g.open_partition("symbol").unwrap();
        g.open_partition("symbol").unwrap();
        g.open_partition("symbol").unwrap();
        assert!(
            g.open_partition("symbol").is_err(),
            "limit was silently raised"
        );
    }

    #[test]
    fn default_limits_are_sane_for_this_machine() {
        let (footprint, buffer) = default_limits(512 * 1024 * 1024);
        assert!(footprint >= FOOTPRINT_FLOOR);
        assert!((BUFFER_FLOOR..=BUFFER_CEILING).contains(&buffer));
        assert!(buffer < footprint);
    }

    #[test]
    fn the_machine_reports_plausible_memory() {
        assert!(total_bytes() > 0);
        assert!(available_bytes() <= total_bytes());
    }

    #[test]
    fn human_is_readable() {
        assert_eq!(human(512), "512 B");
        assert_eq!(human(2 * 1024 * 1024), "2.0 MB");
        assert_eq!(human(3 * 1024 * 1024 * 1024), "3.0 GB");
    }
}
