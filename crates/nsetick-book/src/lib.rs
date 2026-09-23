//! Limit order book reconstruction for NSE order-level data.
//!
//! Exchange mechanics only: what the book looks like after a stream of order events, and
//! periodic L2 snapshots of that book. What to conclude from the resulting book is the
//! caller's business and deliberately stays out of this crate.

// Prices are written as rupees_paise (`100_00` is 100.00) throughout, which clippy reads as
// inconsistent grouping.
#![allow(clippy::inconsistent_digit_grouping)]

pub mod book;
pub mod fills;
pub mod from_parquet;
pub mod replay;
pub mod snapshot;
pub mod stream;

pub use book::{BookStats, Fill, OrderBook, OrderEvent, Side, CANCEL, ENTRY, MODIFY};
pub use from_parquet::ParquetReplayRequest;
pub use replay::{ReplayReport, ReplayRequest};
pub use snapshot::SnapshotBuilder;
