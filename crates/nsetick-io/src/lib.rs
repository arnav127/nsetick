//! Gzip reading and partitioned Parquet writing for NSE historical tick data.

pub mod manifest;
pub mod pipeline;
pub mod reader;
pub mod writer;

pub use manifest::Manifest;
pub use pipeline::{probe_record_length, run, ParseRequest, RunReport};
pub use reader::{read_trigger, RecordReader, Trigger};
pub use writer::{PartitionedWriter, PartitionSummary, WriterOptions};
