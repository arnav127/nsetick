//! nsetick core: NSE historical fixed-width tick data for the CM, FAO and CD segments.
//!
//! Byte offsets live in exactly one place, `spec/layouts/*.toml`, shared verbatim with the
//! Python package. Everything here derives from those specs; nothing hardcodes an offset.

pub mod layout;

pub use layout::{
    available, load, registry, Field, FieldType, Layout, Pad, Version, PAD_BYTES,
};
