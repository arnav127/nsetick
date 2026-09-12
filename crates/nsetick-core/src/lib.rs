//! nsetick core: NSE historical fixed-width tick data for the CM, FAO and CD segments.
//!
//! Byte offsets live in exactly one place, `spec/layouts/*.toml`, shared verbatim with the
//! Python package. Everything here derives from those specs; nothing hardcodes an offset.

pub mod decode;
pub mod filter;
pub mod layout;
pub mod value;

pub use decode::{DecodeOptions, Decoder, Stats};
pub use filter::{compile as compile_filter, CmpOp, Predicate};
pub use layout::{
    available, is_pad, load, registry, unpad_left, Field, FieldType, Layout, Pad, Version,
    PAD_BYTES,
};
