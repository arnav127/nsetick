//! `nsetick` command line interface. Everything is in the library; see `lib.rs`.

// Arrow arrays are allocated on the decode workers and freed on the writer shards, so the
// pipeline generates a lot of cross-thread allocator traffic. The Windows system allocator
// serialises badly under that pattern; mimalloc's per-thread heaps do not.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    // Output piped into `head` and the like: exit quietly when the reader goes, as command
    // line tools do, rather than panic on the broken pipe. Rust ignores SIGPIPE by default.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    let code = nsetick_cli::run(std::env::args_os());
    std::process::ExitCode::from(u8::try_from(code).unwrap_or(1))
}
