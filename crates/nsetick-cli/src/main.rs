//! `nsetick` command line interface.

// Arrow arrays are allocated on the decode workers and freed on the writer shards, so the
// pipeline generates a lot of cross-thread allocator traffic. The Windows system allocator
// serialises badly under that pattern; mimalloc's per-thread heaps do not.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::{Args, Parser, Subcommand, ValueEnum};
use nsetick_core::decode::{DecodeOptions, Decoder, Stats};
use nsetick_core::{filter, layout};
use nsetick_io::pipeline::{self, ParseRequest};
use nsetick_io::writer::WriterOptions;
use nsetick_io::{infer_date, infer_layout, spec};

#[derive(Parser)]
#[command(
    name = "nsetick",
    version,
    about = "Parse NSE historical order and trade data into Parquet",
    long_about = "Parse NSE historical fixed-width order and trade files (CM, FAO, CD) into \
                  partitioned Parquet.\n\nByte layouts come from spec/layouts/*.toml, which is \
                  shared with the Python package, so offsets are defined exactly once."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Parse a .DAT.gz file into partitioned Parquet.
    Parse(ParseArgs),
    /// List the available layouts.
    Layouts,
    /// Print the fields of a layout.
    Describe {
        /// Layout id, e.g. cm_orders.
        layout: String,
        /// Session date, which selects the layout version. Defaults to today.
        #[arg(long)]
        date: Option<String>,
    },
    /// Reconstruct limit order books and write periodic L2 snapshots.
    Book(BookArgs),
    /// Run one or more parses described by a JSON spec file.
    Run {
        /// Path to the JSON run spec.
        spec: PathBuf,
        /// Print the resolved jobs and exit without parsing anything.
        #[arg(long)]
        dry_run: bool,
    },
    /// Decode and print the first few records of a file, without writing anything.
    Inspect {
        input: PathBuf,
        /// Layout id. Inferred from the file name when omitted.
        #[arg(long)]
        layout: Option<String>,
        /// Session date. Inferred from the file name when omitted.
        #[arg(long)]
        date: Option<String>,
        /// Number of records to show.
        #[arg(long, default_value_t = 5)]
        n: usize,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum CompressionArg {
    Zstd,
    Snappy,
    None,
}

#[derive(Args)]
struct ParseArgs {
    /// Input .DAT.gz (or .DAT) file.
    input: PathBuf,

    /// Output root directory.
    #[arg(long, short)]
    out: PathBuf,

    /// Layout id. Inferred from the file name when omitted.
    #[arg(long)]
    layout: Option<String>,

    /// Session date as YYYY-MM-DD. Inferred from the file name when omitted.
    #[arg(long)]
    date: Option<String>,

    /// Comma-separated fields to emit. Defaults to all of them.
    #[arg(long, value_delimiter = ',')]
    select: Option<Vec<String>>,

    /// Filter expression, e.g. "series == 'EQ' and symbol in ('RELIANCE','TCS')".
    #[arg(long = "where", default_value = "")]
    filter: String,

    /// Column to partition on, or "none".
    #[arg(long, default_value = "symbol")]
    partition_by: String,

    #[arg(long, value_enum, default_value_t = CompressionArg::Snappy)]
    compression: CompressionArg,

    /// Target rows per row group.
    #[arg(long, default_value_t = 256_000)]
    row_group_rows: usize,

    /// Kilobytes each column writer buffers before cutting a data page. The dominant cost
    /// when writing thousands of partitions at once.
    #[arg(long, default_value_t = 64)]
    data_page_kb: usize,

    /// Ceiling in megabytes on this run's total memory footprint. Omit to derive one from
    /// the memory currently available on this machine. The run refuses to start opening more
    /// partitions than fit rather than exhausting the machine.
    #[arg(long)]
    memory_limit_mb: Option<usize>,

    /// Count malformed records and carry on instead of stopping at the first one.
    #[arg(long)]
    lenient: bool,

    /// Skip the .trg size check.
    #[arg(long)]
    no_verify: bool,

    /// Stop after roughly this many records. For smoke tests on multi-gigabyte files.
    #[arg(long)]
    max_records: Option<u64>,

    /// Decompressed bytes read per work unit, in megabytes. Larger chunks mean larger
    /// per-symbol batches, which cuts per-partition write overhead.
    #[arg(long, default_value_t = 8)]
    chunk_mb: usize,

    /// Decode and writer threads. Defaults to the machine's parallelism less two, leaving a
    /// core for the inflate thread.
    #[arg(long, short = 'j')]
    threads: Option<usize>,
}

fn resolve_layout(explicit: &Option<String>, path: &Path) -> Result<String> {
    if let Some(id) = explicit {
        return Ok(id.clone());
    }
    infer_layout(path).map(str::to_string).with_context(|| {
        format!(
            "cannot infer a layout from {:?}; pass --layout (one of: {})",
            path.file_name().unwrap_or_default(),
            layout::available().join(", ")
        )
    })
}

fn resolve_date(explicit: &Option<String>, path: &Path) -> Result<NaiveDate> {
    if let Some(s) = explicit {
        return NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .with_context(|| format!("parsing --date {s:?} as YYYY-MM-DD"));
    }
    infer_date(path).with_context(|| {
        format!(
            "cannot infer a session date from {:?}; pass --date YYYY-MM-DD",
            path.file_name().unwrap_or_default()
        )
    })
}

#[derive(Args)]
struct BookArgs {
    /// Either a raw CASH_Orders .DAT.gz, or a directory of already-parsed orders (the
    /// date=... directory holding symbol=* partitions). Parsed input is preferred when it
    /// exists: it avoids a second pass over the compressed file and parallelises per symbol.
    input: PathBuf,

    /// Output root directory.
    #[arg(long, short)]
    out: PathBuf,

    /// Session date as YYYY-MM-DD. Inferred from the file name when omitted, and required
    /// for parsed input whose directory name does not carry it.
    #[arg(long)]
    date: Option<String>,

    /// Restrict to these symbols when replaying parsed input.
    #[arg(long, value_delimiter = ',')]
    symbols: Option<Vec<String>>,

    /// Filter applied before decoding raw input, e.g. "series == 'EQ'".
    #[arg(long = "where", default_value = "series == 'EQ'")]
    filter: String,

    /// Seconds between snapshots.
    #[arg(long, default_value_t = 1.0)]
    interval: f64,

    /// Depth captured per side. 20 covers most of the resting book on a liquid name; 5
    /// sees only the front of the queue.
    #[arg(long, default_value_t = 20)]
    levels: usize,

    #[arg(long, short = 'j')]
    threads: Option<usize>,

    #[arg(long, value_enum, default_value_t = CompressionArg::Snappy)]
    compression: CompressionArg,

    /// Stop after roughly this many records. For smoke tests.
    #[arg(long)]
    max_records: Option<u64>,
}

/// Pull DDMMYYYY or YYYY-MM-DD out of a `date=...` directory name.
fn date_from_dir(path: &Path) -> Option<NaiveDate> {
    let name = path.file_name()?.to_str()?;
    let v = name.strip_prefix("date=")?;
    NaiveDate::parse_from_str(v, "%Y-%m-%d")
        .ok()
        .or_else(|| NaiveDate::parse_from_str(v, "%d%m%Y").ok())
}

fn cmd_book(args: BookArgs) -> Result<()> {
    let compression = match args.compression {
        CompressionArg::Zstd => parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).expect("level 3 is valid"),
        ),
        CompressionArg::Snappy => parquet::basic::Compression::SNAPPY,
        CompressionArg::None => parquet::basic::Compression::UNCOMPRESSED,
    };

    // Parsed input is a directory; raw input is a file.
    if args.input.is_dir() {
        let date = match &args.date {
            Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .with_context(|| format!("parsing --date {s:?}"))?,
            None => date_from_dir(&args.input).with_context(|| {
                format!(
                    "cannot infer a session date from {:?}; pass --date YYYY-MM-DD",
                    args.input.file_name().unwrap_or_default()
                )
            })?,
        };
        let mut req = nsetick_book::ParquetReplayRequest::new(&args.input, &args.out, date);
        req.interval_secs = args.interval;
        req.levels = args.levels;
        req.threads = args.threads;
        req.symbols = args.symbols.clone().unwrap_or_default();
        req.writer = WriterOptions {
            compression,
            ..WriterOptions::default()
        };
        eprintln!(
            "nsetick book: {} (parsed) -> {}
  session {} | every {}s | {} levels",
            args.input.display(),
            args.out.display(),
            date,
            args.interval,
            args.levels
        );
        let r = nsetick_book::from_parquet::run(&req)?;
        print_book_report(&r);
        return Ok(());
    }

    let date = resolve_date(&args.date, &args.input)?;
    let mut req = nsetick_book::ReplayRequest::new(&args.input, &args.out, date);
    req.filter = args.filter.clone();
    req.interval_secs = args.interval;
    req.levels = args.levels;
    req.threads = args.threads;
    req.max_records = args.max_records;
    req.writer = WriterOptions {
        compression,
        ..WriterOptions::default()
    };

    eprintln!(
        "nsetick book: {} (raw) -> {}
  session {} | every {}s | {} levels | where {:?}",
        args.input.display(),
        args.out.display(),
        date,
        args.interval,
        args.levels,
        args.filter
    );

    let r = nsetick_book::replay::run(&req)?;
    print_book_report(&r);
    Ok(())
}

fn print_book_report(r: &nsetick_book::ReplayReport) {
    println!("rows read         {}", r.stats.rows_read);
    println!("events applied    {}", r.events_applied);
    println!("symbols           {}", r.symbols);
    println!("snapshots         {}", r.snapshots);
    println!("fills generated   {}", r.fills_generated);
    println!("replenishments    {}", r.replenishments);
    if r.crossed_symbols > 0 {
        println!(
            "crossed books     {}  <-- replay diverged from the exchange for these symbols",
            r.crossed_symbols
        );
    }
    println!("threads           {}", r.threads);
    println!(
        "elapsed           {:.1}s  ({:.2} M events/s)",
        r.elapsed_secs,
        r.events_per_sec() / 1e6
    );
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    format!("{v:.2} {}", UNITS[u])
}

fn cmd_parse(args: ParseArgs) -> Result<()> {
    let layout_id = resolve_layout(&args.layout, &args.input)?;
    let date = resolve_date(&args.date, &args.input)?;

    let partition_by = match args.partition_by.as_str() {
        "none" | "" => None,
        other => Some(other.to_string()),
    };

    let compression = match args.compression {
        CompressionArg::Zstd => parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).expect("level 3 is valid"),
        ),
        CompressionArg::Snappy => parquet::basic::Compression::SNAPPY,
        CompressionArg::None => parquet::basic::Compression::UNCOMPRESSED,
    };

    let mut req = ParseRequest::new(&args.input, &layout_id, date, &args.out);
    req.select = args.select.clone();
    req.filter = args.filter.clone();
    req.strict = !args.lenient;
    req.verify_trigger = !args.no_verify;
    req.max_records = args.max_records;
    req.threads = args.threads;
    req.memory_limit = args.memory_limit_mb.map(|mb| mb * 1024 * 1024);
    req.chunk_bytes = args.chunk_mb * 1024 * 1024;
    req.writer = WriterOptions {
        compression,
        row_group_rows: args.row_group_rows,
        max_buffered_bytes: 0, // superseded by req.memory_limit below
        data_page_size: args.data_page_kb * 1024,
        partition_by,
    };

    eprintln!(
        "nsetick: {} -> {}\n  layout {} | session {}",
        args.input.display(),
        args.out.display(),
        layout_id,
        date
    );

    let report = pipeline::run(&req)?;
    print_report(&report);
    Ok(())
}

fn print_report(report: &pipeline::RunReport) {
    let s = report.stats;
    println!(
        "layout version    {} ({} B records)",
        report.spec_version, report.record_length
    );
    println!(
        "decompressed      {}",
        human_bytes(report.bytes_decompressed)
    );
    println!("rows read         {}", s.rows_read);
    println!("rows written      {}", s.rows_emitted);
    println!("rows filtered out {}", s.rows_filtered);
    if s.rows_malformed > 0 {
        println!(
            "rows malformed    {}  <-- inspect before trusting this output",
            s.rows_malformed
        );
    }
    println!("partitions        {}", report.partitions);
    println!("threads           {}", report.threads);
    println!(
        "memory            ~{} peak of {} limit ({} partitions open)",
        nsetick_io::memory::human(report.memory_peak),
        nsetick_io::memory::human(report.memory_limit),
        report.partitions_peak
    );
    println!(
        "elapsed           {:.1}s  ({:.0} MB/s decompressed, {:.2} M rows/s)",
        report.elapsed_secs,
        report.throughput_mb_s(),
        report.rows_per_sec() / 1e6
    );
    println!("manifest          {}", report.manifest_path.display());
}

fn cmd_run(spec_path: &Path, dry_run: bool) -> Result<()> {
    let jobs = spec::load(spec_path)?;
    eprintln!(
        "nsetick: {} job(s) from {}",
        jobs.len(),
        spec_path.display()
    );

    for (i, job) in jobs.iter().enumerate() {
        let r = &job.request;
        eprintln!(
            "  [{}/{}] {} -> {}
        layout {} | session {} | threads {} | where {:?}",
            i + 1,
            jobs.len(),
            r.input.display(),
            r.out_root.display(),
            r.layout_id,
            r.session_date,
            r.threads.unwrap_or(0),
            r.filter
        );
    }
    if dry_run {
        eprintln!("dry run: nothing was parsed");
        return Ok(());
    }

    let started = std::time::Instant::now();
    let mut total_rows = 0u64;
    for (i, job) in jobs.iter().enumerate() {
        println!(
            "
--- job {}/{}: {} ---",
            i + 1,
            jobs.len(),
            job.request.input.display()
        );
        let report = pipeline::run(&job.request)?;
        total_rows += report.stats.rows_emitted;
        print_report(&report);
    }
    if jobs.len() > 1 {
        println!(
            "
all {} jobs done: {} rows written in {:.1}s",
            jobs.len(),
            total_rows,
            started.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

fn cmd_layouts() -> Result<()> {
    for id in layout::available() {
        let l = layout::load(id)?;
        let lengths: Vec<String> = l
            .versions
            .iter()
            .map(|v| {
                let mark = if v.verified { "" } else { "?" };
                format!("{}{mark}", v.record_length)
            })
            .collect();
        println!(
            "{:<11} {:<3} {:<7} {:<28} records: {}",
            id,
            l.meta.segment,
            l.meta.kind,
            l.meta.file_glob.replace("{date}", "DDMMYYYY"),
            lengths.join(", ")
        );
    }
    println!("\n? marks a layout version not yet checked against a real file.");
    Ok(())
}

fn cmd_describe(layout_id: &str, date: Option<String>) -> Result<()> {
    let l = layout::load(layout_id)?;
    let date = match date {
        Some(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
            .with_context(|| format!("parsing --date {s:?}"))?,
        None => chrono::Local::now().date_naive(),
    };
    let v = l.for_date(date)?;

    println!("{} - {}", l.meta.id, l.meta.description);
    println!(
        "spec {} valid {} to {}, {} byte records{}\n",
        v.spec_version,
        v.valid_from,
        v.valid_to,
        v.record_length,
        if v.verified { "" } else { "  [UNVERIFIED]" }
    );
    println!(
        "{:<22} {:>6} {:>5}  {:<11} notes",
        "field", "offset", "len", "type"
    );
    for f in &v.fields {
        let mut notes = Vec::new();
        if let Some(p) = f.pad {
            notes.push(format!("pad {p:?}"));
        }
        if let Some(s) = f.scale {
            notes.push(format!("scale {s}"));
        }
        println!(
            "{:<22} {:>6} {:>5}  {:<11} {}",
            f.name,
            f.offset,
            f.len,
            format!("{:?}", f.ty),
            notes.join(", ")
        );
    }
    Ok(())
}

fn cmd_inspect(
    input: PathBuf,
    layout_id: Option<String>,
    date: Option<String>,
    n: usize,
) -> Result<()> {
    let layout_id = resolve_layout(&layout_id, &input)?;
    let date = resolve_date(&date, &input)?;
    let l = layout::load(&layout_id)?;

    let observed = pipeline::probe_record_length(&input)?;
    let v = l.resolve(date, Some(observed))?;

    println!(
        "{}\n  layout {} spec {} | {} byte records | session {}\n",
        input.display(),
        layout_id,
        v.spec_version,
        v.record_length,
        date
    );

    let want = n * v.line_length();
    let bytes = pipeline::head_bytes(&input, want)?;
    let usable = bytes.len() - (bytes.len() % v.line_length());
    if usable == 0 {
        bail!("file has no complete records");
    }

    let decoder = Decoder::new(v, None, DecodeOptions { strict: true })?;
    let mut stats = Stats::default();
    let batch = decoder.decode(&bytes[..usable], &filter::Predicate::True, &mut stats)?;

    for row in 0..batch.num_rows() {
        println!("record {row}:");
        for (i, f) in batch.schema().fields().iter().enumerate() {
            let col = arrow::util::display::array_value_to_string(batch.column(i), row)
                .unwrap_or_else(|_| "<unprintable>".into());
            println!("  {:<22} {}", f.name(), col);
        }
        println!();
    }
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Parse(args) => cmd_parse(args),
        Command::Book(args) => cmd_book(args),
        Command::Run { spec, dry_run } => cmd_run(&spec, dry_run),
        Command::Layouts => cmd_layouts(),
        Command::Describe { layout, date } => cmd_describe(&layout, date),
        Command::Inspect {
            input,
            layout,
            date,
            n,
        } => cmd_inspect(input, layout, date, n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_inferred_from_nse_file_names() {
        assert_eq!(
            infer_layout(Path::new("CASH_Orders_27012022.DAT.gz")),
            Some("cm_orders")
        );
        assert_eq!(
            infer_layout(Path::new("CASH_Trades_27012022.DAT.gz")),
            Some("cm_trades")
        );
        assert_eq!(
            infer_layout(Path::new("FAO_Orders_27012022_01.DAT.gz")),
            Some("fao_orders")
        );
        assert_eq!(
            infer_layout(Path::new("CDS_Trades_27012022.DAT.gz")),
            Some("cd_trades")
        );
        assert_eq!(infer_layout(Path::new("something_else.gz")), None);
    }

    #[test]
    fn date_is_inferred_from_the_ddmmyyyy_component() {
        assert_eq!(
            infer_date(Path::new("CASH_Orders_27012022.DAT.gz")),
            NaiveDate::from_ymd_opt(2022, 1, 27)
        );
        // Split FAO files carry a stream suffix after the date.
        assert_eq!(
            infer_date(Path::new("FAO_Orders_30062022_11.DAT.gz")),
            NaiveDate::from_ymd_opt(2022, 6, 30)
        );
        assert_eq!(infer_date(Path::new("no_date_here.DAT.gz")), None);
        // 32 is not a day, so this must not silently produce a wrong date.
        assert_eq!(infer_date(Path::new("CASH_Orders_32012022.DAT.gz")), None);
    }

    #[test]
    fn byte_sizes_render_readably() {
        assert_eq!(human_bytes(0), "0.00 B");
        assert_eq!(human_bytes(8_260_000_000), "7.69 GB");
    }
}
