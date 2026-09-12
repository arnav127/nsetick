//! Gzip reading and partitioned Parquet writing for NSE historical tick data.

use std::path::Path;

use chrono::NaiveDate;

pub mod manifest;
pub mod memory;
pub mod pipeline;
pub mod reader;
pub mod spec;
pub mod writer;

pub use manifest::Manifest;
pub use memory::MemoryGuard;
pub use pipeline::{probe_record_length, run, ParseRequest, RunReport};
pub use reader::{read_trigger, RecordReader, Trigger};
pub use spec::{JobSpec, ResolvedJob};
pub use writer::{PartitionSummary, PartitionedWriter, WriterOptions};

/// Infer a layout id from an NSE file name, e.g. `CASH_Orders_27012022.DAT.gz`.
pub fn infer_layout(path: &Path) -> Option<&'static str> {
    let name = path.file_name()?.to_str()?.to_ascii_uppercase();
    let candidates = [
        ("CASH_ORDERS", "cm_orders"),
        ("CASH_TRADES", "cm_trades"),
        ("CASH_INDEX", "cm_index"),
        ("FAO_ORDERS", "fao_orders"),
        ("FAO_TRADES", "fao_trades"),
        ("CDS_ORDERS", "cd_orders"),
        ("CDS_TRADES", "cd_trades"),
    ];
    candidates
        .iter()
        .find(|(prefix, _)| name.starts_with(prefix))
        .map(|(_, id)| *id)
}

/// Infer the session date from the DDMMYYYY component of an NSE file name.
pub fn infer_date(path: &Path) -> Option<NaiveDate> {
    let name = path.file_name()?.to_str()?;
    let bytes = name.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i - start == 8 {
                let s = &name[start..i];
                let d: u32 = s[0..2].parse().ok()?;
                let m: u32 = s[2..4].parse().ok()?;
                let y: i32 = s[4..8].parse().ok()?;
                return NaiveDate::from_ymd_opt(y, m, d);
            }
        } else {
            i += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_inferred_from_nse_file_names() {
        assert_eq!(infer_layout(Path::new("CASH_Orders_27012022.DAT.gz")), Some("cm_orders"));
        assert_eq!(infer_layout(Path::new("FAO_Orders_27012022_01.DAT.gz")), Some("fao_orders"));
        assert_eq!(infer_layout(Path::new("CDS_Trades_27012022.DAT.gz")), Some("cd_trades"));
        assert_eq!(infer_layout(Path::new("something_else.gz")), None);
    }

    #[test]
    fn date_is_inferred_from_the_ddmmyyyy_component() {
        assert_eq!(infer_date(Path::new("CASH_Orders_27012022.DAT.gz")), NaiveDate::from_ymd_opt(2022, 1, 27));
        assert_eq!(infer_date(Path::new("FAO_Orders_30062022_11.DAT.gz")), NaiveDate::from_ymd_opt(2022, 6, 30));
        assert_eq!(infer_date(Path::new("no_date_here.DAT.gz")), None);
        // 32 is not a day, so this must not silently produce a wrong date.
        assert_eq!(infer_date(Path::new("CASH_Orders_32012022.DAT.gz")), None);
    }
}
