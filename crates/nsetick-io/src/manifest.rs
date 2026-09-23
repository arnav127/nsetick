//! Run manifests.
//!
//! Every parse writes one. Neither existing pipeline records what produced a parquet file,
//! so a directory of results cannot be traced back to a layout version, a filter, or a row
//! count, and a partial run is indistinguishable from a complete one. For research output
//! that has to be defensible months later, that is the difference between a result and a
//! guess.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionRecord {
    pub key: String,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub nsetick_version: String,
    pub created_utc: String,

    pub source_path: String,
    pub source_bytes: u64,
    pub bytes_decompressed: u64,

    pub layout_id: String,
    pub spec_version: String,
    pub record_length: usize,
    /// False when the layout version has never been checked against a real file.
    pub layout_verified: bool,
    pub session_date: String,

    /// Free-text note from the run spec, recording why this run was made.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,

    pub filter: String,
    pub selected_fields: Vec<String>,
    pub partition_by: Option<String>,
    pub strict: bool,

    pub rows_read: u64,
    pub rows_emitted: u64,
    pub rows_filtered: u64,
    pub rows_malformed: u64,
    pub elapsed_secs: f64,

    pub partitions: Vec<PartitionRecord>,
}

impl Manifest {
    /// Manifests are named per layout and date, so parsing orders and trades for the same
    /// session into one root does not have them overwrite each other.
    pub fn write(&self, root: &Path, layout_id: &str, date: NaiveDate) -> Result<PathBuf> {
        std::fs::create_dir_all(root).with_context(|| format!("creating {}", root.display()))?;
        let path = root.join(format!("_manifest.{layout_id}.{date}.json"));
        let json = serde_json::to_string_pretty(self).context("serialising manifest")?;
        std::fs::write(&path, json).with_context(|| format!("writing {}", path.display()))?;
        Ok(path)
    }

    pub fn read(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }

    /// Rows that reached the output, as a fraction of rows read. A run that filtered nothing
    /// and emitted nothing is a strong hint the filter is wrong.
    pub fn yield_ratio(&self) -> f64 {
        if self.rows_read == 0 {
            return 0.0;
        }
        self.rows_emitted as f64 / self.rows_read as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        Manifest {
            nsetick_version: "0.1.0".into(),
            created_utc: "2026-09-12T00:00:00+00:00".into(),
            source_path: "CASH_Orders_27012022.DAT.gz".into(),
            source_bytes: 8_260_000_000,
            bytes_decompressed: 58_000_000_000,
            layout_id: "cm_orders".into(),
            spec_version: "1.0".into(),
            record_length: 87,
            layout_verified: true,
            session_date: "2022-01-27".into(),
            note: None,
            filter: "series == 'EQ'".into(),
            selected_fields: vec!["symbol".into(), "txn_time".into()],
            partition_by: Some("symbol".into()),
            strict: true,
            rows_read: 1000,
            rows_emitted: 900,
            rows_filtered: 100,
            rows_malformed: 0,
            elapsed_secs: 1.5,
            partitions: vec![PartitionRecord {
                key: "RELIANCE".into(),
                rows: 900,
                bytes: 4096,
            }],
        }
    }

    #[test]
    fn round_trips_through_json() {
        let dir = std::env::temp_dir().join("nsetick_manifest_test");
        let _ = std::fs::remove_dir_all(&dir);
        let m = sample();
        let date = NaiveDate::from_ymd_opt(2022, 1, 27).unwrap();
        let path = m.write(&dir, "cm_orders", date).unwrap();

        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "_manifest.cm_orders.2022-01-27.json"
        );
        let back = Manifest::read(&path).unwrap();
        assert_eq!(back.rows_emitted, 900);
        assert_eq!(back.spec_version, "1.0");
        assert_eq!(back.filter, "series == 'EQ'");
    }

    #[test]
    fn orders_and_trades_manifests_do_not_collide() {
        let dir = std::env::temp_dir().join("nsetick_manifest_collide");
        let _ = std::fs::remove_dir_all(&dir);
        let date = NaiveDate::from_ymd_opt(2022, 1, 27).unwrap();
        let a = sample().write(&dir, "cm_orders", date).unwrap();
        let b = sample().write(&dir, "cm_trades", date).unwrap();
        assert_ne!(a, b);
        assert!(a.exists() && b.exists());
    }

    #[test]
    fn yield_ratio_reports_how_much_survived_the_filter() {
        assert!((sample().yield_ratio() - 0.9).abs() < 1e-9);
        let mut empty = sample();
        empty.rows_read = 0;
        assert_eq!(empty.yield_ratio(), 0.0);
    }
}
