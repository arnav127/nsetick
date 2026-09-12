//! JSON run specifications.
//!
//! A run spec is the same information a command line invocation carries, but as a file that
//! can be committed, diffed and cited. For research output that matters: months later, the
//! question "what exactly produced this parquet" has an answer that is a file rather than a
//! half-remembered shell command.
//!
//! Unknown fields are rejected. A silently ignored typo in a spec that is meant to be a
//! record of what was run would defeat the purpose.
//!
//! Single job:
//!
//! ```json
//! {
//!   "input": "data/raw/CASH_Orders_27012022.DAT.gz",
//!   "out":   "data/parquet",
//!   "where": "series == 'EQ'",
//!   "select": ["symbol", "txn_time", "limit_price"]
//! }
//! ```
//!
//! Several jobs sharing defaults:
//!
//! ```json
//! {
//!   "defaults": { "out": "data/parquet", "where": "series == 'EQ'" },
//!   "jobs": [
//!     { "input": "data/raw/CASH_Orders_27012022.DAT.gz" },
//!     { "input": "data/raw/CASH_Trades_27012022.DAT.gz" }
//!   ]
//! }
//! ```

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use parquet::basic::{Compression, ZstdLevel};
use serde::{Deserialize, Deserializer, Serialize};

use crate::pipeline::{default_threads, ParseRequest};
use crate::writer::WriterOptions;

/// One job. Every field is optional so it can also serve as a defaults block; `input` and
/// `out` must be present once a job is merged with the defaults.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    pub input: Option<PathBuf>,
    pub out: Option<PathBuf>,
    /// Layout id. Inferred from the file name when omitted.
    pub layout: Option<String>,
    /// Session date as YYYY-MM-DD. Inferred from the file name when omitted.
    pub date: Option<String>,
    pub select: Option<Vec<String>>,
    #[serde(rename = "where")]
    pub filter: Option<String>,
    /// Column to partition on, or null for a single file per date.
    ///
    /// Double option so an explicit `"partition_by": null` (do not partition) is
    /// distinguishable from the key being absent (use the default). Plain serde collapses
    /// both to None.
    #[serde(default, deserialize_with = "explicit_option")]
    pub partition_by: Option<Option<String>>,
    /// "zstd", "snappy" or "none".
    pub compression: Option<String>,
    pub row_group_rows: Option<usize>,
    pub max_buffered_mb: Option<usize>,
    pub chunk_mb: Option<usize>,
    pub threads: Option<usize>,
    /// Stop at the first malformed record. Defaults to true.
    pub strict: Option<bool>,
    /// Check the file size against its .trg sidecar. Defaults to true.
    pub verify_trigger: Option<bool>,
    pub max_records: Option<u64>,
    /// Free-text note carried into the manifest, e.g. why this run was made.
    pub note: Option<String>,
}

impl JobSpec {
    /// Fields set on `self` win; anything unset falls back to `defaults`.
    fn merged_over(&self, defaults: &JobSpec) -> JobSpec {
        macro_rules! pick {
            ($f:ident) => {
                self.$f.clone().or_else(|| defaults.$f.clone())
            };
        }
        JobSpec {
            input: pick!(input),
            out: pick!(out),
            layout: pick!(layout),
            date: pick!(date),
            select: pick!(select),
            filter: pick!(filter),
            partition_by: pick!(partition_by),
            compression: pick!(compression),
            row_group_rows: pick!(row_group_rows),
            max_buffered_mb: pick!(max_buffered_mb),
            chunk_mb: pick!(chunk_mb),
            threads: pick!(threads),
            strict: pick!(strict),
            verify_trigger: pick!(verify_trigger),
            max_records: pick!(max_records),
            note: pick!(note),
        }
    }
}

/// Deserialize a present-but-null field as `Some(None)` rather than `None`.
fn explicit_option<'de, T, D>(d: D) -> std::result::Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    T::deserialize(d).map(Some)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchSpec {
    #[serde(default)]
    defaults: JobSpec,
    jobs: Vec<JobSpec>,
}

fn parse_compression(name: &str) -> Result<Compression> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::try_new(3).expect("level 3 is valid")),
        "snappy" => Compression::SNAPPY,
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        other => bail!("unknown compression {other:?}; use zstd, snappy or none"),
    })
}

/// A job resolved into something the pipeline can run, plus the note for the manifest.
#[derive(Debug)]
pub struct ResolvedJob {
    pub request: ParseRequest,
    pub note: Option<String>,
}

/// Load a spec file and resolve it into runnable jobs.
///
/// Relative paths inside the spec resolve against the spec file's own directory, so a spec
/// can sit next to the data it describes and stay portable.
pub fn load(path: &Path) -> Result<Vec<ResolvedJob>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading run spec {}", path.display()))?;

    let value: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("parsing {} as JSON", path.display()))?;

    let (defaults, jobs) = if value.get("jobs").is_some() {
        let batch: BatchSpec = serde_json::from_value(value)
            .with_context(|| format!("reading {} as a batch spec", path.display()))?;
        (batch.defaults, batch.jobs)
    } else {
        let single: JobSpec = serde_json::from_value(value)
            .with_context(|| format!("reading {} as a single job spec", path.display()))?;
        (JobSpec::default(), vec![single])
    };

    if jobs.is_empty() {
        bail!("{}: \"jobs\" is empty, so there is nothing to run", path.display());
    }

    let base = path.parent().unwrap_or(Path::new("."));
    jobs.iter()
        .enumerate()
        .map(|(i, job)| resolve(&job.merged_over(&defaults), base, i))
        .collect()
}

fn resolve(job: &JobSpec, base: &Path, index: usize) -> Result<ResolvedJob> {
    let resolve_path = |p: &PathBuf| -> PathBuf {
        if p.is_absolute() {
            p.clone()
        } else {
            base.join(p)
        }
    };

    let input = job
        .input
        .as_ref()
        .map(resolve_path)
        .with_context(|| format!("job {index}: \"input\" is required"))?;
    let out = job
        .out
        .as_ref()
        .map(resolve_path)
        .with_context(|| format!("job {index}: \"out\" is required"))?;

    let layout_id = match &job.layout {
        Some(id) => id.clone(),
        None => crate::infer_layout(&input)
            .map(str::to_string)
            .with_context(|| {
                format!(
                    "job {index}: cannot infer a layout from {:?}; set \"layout\"",
                    input.file_name().unwrap_or_default()
                )
            })?,
    };

    let date = match &job.date {
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .with_context(|| format!("job {index}: parsing \"date\" {s:?} as YYYY-MM-DD"))?,
        None => crate::infer_date(&input).with_context(|| {
            format!(
                "job {index}: cannot infer a session date from {:?}; set \"date\"",
                input.file_name().unwrap_or_default()
            )
        })?,
    };

    let mut request = ParseRequest::new(input, layout_id, date, out);
    request.select = job.select.clone();
    request.filter = job.filter.clone().unwrap_or_default();
    request.strict = job.strict.unwrap_or(true);
    request.verify_trigger = job.verify_trigger.unwrap_or(true);
    request.max_records = job.max_records;
    request.threads = Some(job.threads.unwrap_or_else(default_threads));
    request.chunk_bytes = job.chunk_mb.unwrap_or(8) * 1024 * 1024;
    request.note = job.note.clone();

    let defaults = WriterOptions::default();
    request.writer = WriterOptions {
        compression: match &job.compression {
            Some(c) => parse_compression(c)
                .with_context(|| format!("job {index}: \"compression\""))?,
            None => defaults.compression,
        },
        row_group_rows: job.row_group_rows.unwrap_or(defaults.row_group_rows),
        max_buffered_bytes: job
            .max_buffered_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(defaults.max_buffered_bytes),
        // Distinguishes "absent" (use the default) from "present but null" (do not partition).
        partition_by: match &job.partition_by {
            None => defaults.partition_by,
            Some(v) => v.clone(),
        },
    };

    Ok(ResolvedJob {
        request,
        note: job.note.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_spec(name: &str, json: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("nsetick_spec_tests");
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join(name);
        std::fs::write(&p, json).unwrap();
        p
    }

    #[test]
    fn a_single_job_spec_resolves() {
        let p = write_spec(
            "single.json",
            r#"{
                "input": "CASH_Orders_27012022.DAT.gz",
                "out": "out",
                "where": "series == 'EQ'",
                "select": ["symbol", "txn_time"]
            }"#,
        );
        let jobs = load(&p).unwrap();
        assert_eq!(jobs.len(), 1);
        let r = &jobs[0].request;
        assert_eq!(r.layout_id, "cm_orders");
        assert_eq!(r.session_date, NaiveDate::from_ymd_opt(2022, 1, 27).unwrap());
        assert_eq!(r.filter, "series == 'EQ'");
        assert_eq!(r.select.as_deref().unwrap().len(), 2);
        // Relative paths resolve against the spec file, not the process directory.
        assert!(r.input.is_absolute());
    }

    #[test]
    fn defaults_apply_to_every_job_and_jobs_win() {
        let p = write_spec(
            "batch.json",
            r#"{
                "defaults": { "out": "out", "where": "series == 'EQ'", "threads": 2 },
                "jobs": [
                    { "input": "CASH_Orders_27012022.DAT.gz" },
                    { "input": "CASH_Trades_30062022.DAT.gz", "where": "symbol == 'TCS'" }
                ]
            }"#,
        );
        let jobs = load(&p).unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].request.filter, "series == 'EQ'");
        assert_eq!(jobs[0].request.layout_id, "cm_orders");
        assert_eq!(jobs[0].request.threads, Some(2));
        assert_eq!(jobs[1].request.filter, "symbol == 'TCS'");
        assert_eq!(jobs[1].request.layout_id, "cm_trades");
        assert_eq!(
            jobs[1].request.session_date,
            NaiveDate::from_ymd_opt(2022, 6, 30).unwrap()
        );
    }

    #[test]
    fn a_typo_in_a_field_name_is_an_error_not_a_silent_default() {
        let p = write_spec(
            "typo.json",
            r#"{ "input": "CASH_Orders_27012022.DAT.gz", "out": "o", "wheer": "x" }"#,
        );
        let err = format!("{:#}", load(&p).unwrap_err());
        assert!(err.contains("wheer") || err.contains("unknown field"), "{err}");
    }

    #[test]
    fn partition_by_null_means_do_not_partition() {
        let p = write_spec(
            "nopart.json",
            r#"{ "input": "CASH_Orders_27012022.DAT.gz", "out": "o", "partition_by": null }"#,
        );
        let jobs = load(&p).unwrap();
        assert_eq!(jobs[0].request.writer.partition_by, None);

        // Omitting the key entirely keeps the default.
        let p2 = write_spec(
            "defpart.json",
            r#"{ "input": "CASH_Orders_27012022.DAT.gz", "out": "o" }"#,
        );
        let jobs2 = load(&p2).unwrap();
        assert_eq!(
            jobs2[0].request.writer.partition_by.as_deref(),
            Some("symbol")
        );
    }

    #[test]
    fn a_missing_input_is_reported_with_the_job_index() {
        let p = write_spec("noinput.json", r#"{ "jobs": [ { "out": "o" } ] }"#);
        let err = format!("{:#}", load(&p).unwrap_err());
        assert!(err.contains("job 0"), "{err}");
        assert!(err.contains("input"), "{err}");
    }

    #[test]
    fn an_unparseable_date_is_rejected() {
        let p = write_spec(
            "baddate.json",
            r#"{ "input": "x.DAT.gz", "out": "o", "layout": "cm_orders", "date": "27-01-2022" }"#,
        );
        let err = format!("{:#}", load(&p).unwrap_err());
        assert!(err.contains("YYYY-MM-DD"), "{err}");
    }

    #[test]
    fn empty_job_list_is_refused() {
        let p = write_spec("empty.json", r#"{ "jobs": [] }"#);
        let err = format!("{:#}", load(&p).unwrap_err());
        assert!(err.contains("nothing to run"), "{err}");
    }
}
