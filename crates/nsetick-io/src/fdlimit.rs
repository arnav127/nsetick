//! The open-file limit, raised to what partitioned output needs.
//!
//! A partitioned run keeps one Parquet writer open per partition for its whole length, because
//! NSE interleaves every symbol throughout the session and a closed Parquet file cannot be
//! appended to. A full Capital Market universe is well over a thousand symbols, and the usual
//! Linux soft limit is 1024 open files, so without this every large run fails part-way with
//! "Too many open files". The soft limit can be raised to the hard limit without privileges,
//! which is what this does, once, before the first partition opens. Where even the hard limit
//! is too low, [`check`] turns the eventual failure into an explanation up front.

use std::sync::OnceLock;

use anyhow::{bail, Result};

/// Descriptors kept back for everything that is not a partition: the input file, the
/// manifest, the standard streams, sockets a caller holds, and the allocator's own.
pub const RESERVED: u64 = 64;

/// The most this raises the soft limit to. Enough for any universe with room to spare, and
/// modest enough not to disturb anything that sizes a table by the limit.
#[cfg(unix)]
const TARGET: u64 = 65_536;

static LIMIT: OnceLock<Option<u64>> = OnceLock::new();

/// Raise the soft open-file limit towards the hard limit, once per process, and return the
/// limit now in force. `None` where there is no such limit (Windows) or it cannot be read.
pub fn ensure() -> Option<u64> {
    *LIMIT.get_or_init(raise)
}

/// Fail early, with the remedy, if `partitions` writers cannot all be open at once.
pub fn check(partitions: u64, layout_hint: &str) -> Result<()> {
    let Some(limit) = ensure() else {
        return Ok(());
    };
    if partitions + RESERVED > limit {
        bail!(
            "open-file limit: {partitions} open partitions need about {} file descriptors, \
             but this process may only open {limit}.\n\
             Each partition keeps its Parquet file open for the whole run, because NSE \
             interleaves every symbol throughout the session.\n\
             Options: raise the limit before running (`ulimit -n 65536`; if that is refused, \
             the hard limit needs raising in /etc/security/limits.conf), narrow the run with \
             --where, or write one file per date with --partition-by none.\n\
             (partitioning by {layout_hint})",
            partitions + RESERVED
        );
    }
    Ok(())
}

#[cfg(unix)]
fn raise() -> Option<u64> {
    // SAFETY: getrlimit and setrlimit only read and write the rlimit struct passed to them.
    unsafe {
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 {
            return None;
        }
        let hard = rl.rlim_max as u64;
        let current = rl.rlim_cur as u64;
        let mut want = TARGET.min(hard);
        // macOS reports an unlimited hard limit but refuses a soft limit above OPEN_MAX.
        if cfg!(target_os = "macos") {
            want = want.min(10_240);
        }
        if want > current {
            rl.rlim_cur = want as libc::rlim_t;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) != 0 {
                return Some(current);
            }
            return Some(want);
        }
        Some(current)
    }
}

#[cfg(not(unix))]
fn raise() -> Option<u64> {
    // Windows handles have no per-process limit of this kind.
    None
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn the_limit_is_raised_as_far_as_allowed() {
        let limit = ensure().expect("unix has a limit");
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
        assert_eq!(
            rl.rlim_cur as u64, limit,
            "the limit in force is the one reported"
        );
        let reachable = if cfg!(target_os = "macos") {
            10_240
        } else {
            TARGET
        };
        assert!(limit >= reachable.min(rl.rlim_max as u64));
    }

    #[test]
    fn a_run_beyond_the_limit_is_refused_with_the_remedy() {
        let limit = ensure().expect("unix has a limit");
        let err = check(limit, "symbol").unwrap_err().to_string();
        assert!(err.contains("ulimit -n"), "{err}");
        assert!(check(limit - RESERVED - 1, "symbol").is_ok());
    }
}
