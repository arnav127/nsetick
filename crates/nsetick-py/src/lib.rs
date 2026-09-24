//! Python bindings for nsetick.
//!
//! Two ways in, because the existing projects want different things:
//!
//! * `parse` / `run_spec` write partitioned Parquet, the same as the CLI, for the one-time
//!   conversion of a session.
//! * `BatchReader` streams Arrow record batches straight into the process, so an analysis can
//!   consume filtered NSE data without a Parquet round trip at all. Batches arrive as real
//!   `pyarrow.RecordBatch` objects, which pandas and Polars both take for free.
//!
//! The GIL is released around every decode, so a `BatchReader` in one thread does not block
//! the rest of an application.

use std::path::PathBuf;

use nsetick_core::decode::{DecodeOptions, Decoder, Stats};
use nsetick_core::filter::{self, Predicate};
use nsetick_core::layout;
use nsetick_io::pipeline::{self, ParseRequest};
use nsetick_io::reader::RecordReader;
use nsetick_io::spec;
use nsetick_io::writer::WriterOptions;
use nsetick_io::{infer_date, infer_layout};

use arrow::pyarrow::PyArrowType;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use pyo3::exceptions::PyValueError;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// anyhow's chain is where the useful part of an nsetick error lives, so flatten the whole
/// chain into the Python exception rather than showing only the outermost context.
fn to_py_err(e: anyhow::Error) -> PyErr {
    // An interrupt must surface as KeyboardInterrupt, not as a ValueError whose message
    // happens to mention one: callers catch the exception type, and `run_all.py` needs to
    // distinguish "the user stopped this" from "the stage failed".
    let msg = format!("{e:#}");
    if msg.contains("interrupted: KeyboardInterrupt") {
        return pyo3::exceptions::PyKeyboardInterrupt::new_err("interrupted");
    }
    PyValueError::new_err(format!("{e:#}"))
}

fn resolve_layout(explicit: Option<&str>, path: &std::path::Path) -> PyResult<String> {
    match explicit {
        Some(id) => Ok(id.to_string()),
        None => infer_layout(path).map(str::to_string).ok_or_else(|| {
            PyValueError::new_err(format!(
                "cannot infer a layout from {:?}; pass layout=... (one of: {})",
                path.file_name().unwrap_or_default(),
                layout::available().join(", ")
            ))
        }),
    }
}

fn resolve_date(explicit: Option<&str>, path: &std::path::Path) -> PyResult<NaiveDate> {
    match explicit {
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| PyValueError::new_err(format!("date {s:?} is not YYYY-MM-DD: {e}"))),
        None => infer_date(path).ok_or_else(|| {
            PyValueError::new_err(format!(
                "cannot infer a session date from {:?}; pass date='YYYY-MM-DD'",
                path.file_name().unwrap_or_default()
            ))
        }),
    }
}

fn stats_dict<'py>(py: Python<'py>, s: &Stats) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("rows_read", s.rows_read)?;
    d.set_item("rows_emitted", s.rows_emitted)?;
    d.set_item("rows_filtered", s.rows_filtered)?;
    d.set_item("rows_malformed", s.rows_malformed)?;
    Ok(d)
}

/// Parse one file into partitioned Parquet.
/// A hook the long-running stages poll so Ctrl-C works.
///
/// Python raises `KeyboardInterrupt` from its eval loop. While the main thread is inside a
/// twenty-minute Rust call that loop is not running, so Ctrl-C sets a flag nobody reads until
/// the call returns - exactly the wait the user is trying to escape. Re-acquiring the GIL
/// briefly and running the pending handlers converts it into an error the pipeline can
/// propagate, which unwinds through the normal shutdown path and closes every writer.
fn interrupt_hook() -> Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync> {
    Arc::new(|| {
        Python::attach(|py| py.check_signals()).map_err(|e| anyhow::anyhow!("interrupted: {e}"))
    })
}

#[pyfunction]
#[pyo3(signature = (
    input, out, *, layout=None, date=None, select=None, where_=None,
    partition_by="symbol", compression="snappy", threads=None, strict=true,
    verify_trigger=true, memory_limit_mb=None, max_records=None, row_group_rows=None,
    note=None,
))]
#[allow(clippy::too_many_arguments)]
fn parse(
    py: Python<'_>,
    input: PathBuf,
    out: PathBuf,
    layout: Option<&str>,
    date: Option<&str>,
    select: Option<Vec<String>>,
    where_: Option<&str>,
    partition_by: Option<&str>,
    compression: &str,
    threads: Option<usize>,
    strict: bool,
    verify_trigger: bool,
    memory_limit_mb: Option<usize>,
    max_records: Option<u64>,
    row_group_rows: Option<usize>,
    note: Option<String>,
) -> PyResult<Py<PyDict>> {
    let layout_id = resolve_layout(layout, &input)?;
    let session = resolve_date(date, &input)?;

    let compression = match compression.to_ascii_lowercase().as_str() {
        "zstd" => parquet_zstd(),
        "snappy" => parquet::basic::Compression::SNAPPY,
        "none" | "uncompressed" => parquet::basic::Compression::UNCOMPRESSED,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown compression {other:?}; use zstd, snappy or none"
            )))
        }
    };

    let mut req = ParseRequest::new(&input, &layout_id, session, &out);
    req.select = select;
    req.filter = where_.unwrap_or("").to_string();
    req.strict = strict;
    req.verify_trigger = verify_trigger;
    req.threads = threads;
    req.max_records = max_records;
    req.memory_limit = memory_limit_mb.map(|mb| mb * 1024 * 1024);
    req.note = note;
    req.writer = WriterOptions {
        compression,
        row_group_rows: row_group_rows.unwrap_or(WriterOptions::default().row_group_rows),
        partition_by: match partition_by {
            None | Some("none") | Some("") => None,
            Some(c) => Some(c.to_string()),
        },
        ..WriterOptions::default()
    };

    // Parsing a session takes minutes and touches no Python objects, beyond the periodic
    // signal check the hook performs.
    req.interrupt = Some(interrupt_hook());
    let report = py.detach(|| pipeline::run(&req)).map_err(to_py_err)?;

    let d = PyDict::new(py);
    d.set_item("rows_read", report.stats.rows_read)?;
    d.set_item("rows_emitted", report.stats.rows_emitted)?;
    d.set_item("rows_filtered", report.stats.rows_filtered)?;
    d.set_item("rows_malformed", report.stats.rows_malformed)?;
    d.set_item("bytes_decompressed", report.bytes_decompressed)?;
    d.set_item("partitions", report.partitions)?;
    d.set_item("elapsed_secs", report.elapsed_secs)?;
    d.set_item("manifest", report.manifest_path.display().to_string())?;
    d.set_item("layout", layout_id)?;
    d.set_item("spec_version", report.spec_version)?;
    d.set_item("record_length", report.record_length)?;
    d.set_item("threads", report.threads)?;
    d.set_item("memory_peak_bytes", report.memory_peak)?;
    d.set_item("memory_limit_bytes", report.memory_limit)?;
    Ok(d.into())
}

fn parquet_zstd() -> parquet::basic::Compression {
    parquet::basic::Compression::ZSTD(
        parquet::basic::ZstdLevel::try_new(3).expect("level 3 is valid"),
    )
}

/// Run every job in a JSON run spec, returning one result dict per job.
#[pyfunction]
fn run_spec(py: Python<'_>, path: PathBuf) -> PyResult<Py<PyList>> {
    let jobs = spec::load(&path).map_err(to_py_err)?;
    let out = PyList::empty(py);
    for job in jobs {
        let report = py
            .detach(|| pipeline::run(&job.request))
            .map_err(to_py_err)?;
        let d = PyDict::new(py);
        d.set_item("input", job.request.input.display().to_string())?;
        d.set_item("layout", job.request.layout_id.clone())?;
        d.set_item("rows_emitted", report.stats.rows_emitted)?;
        d.set_item("rows_read", report.stats.rows_read)?;
        d.set_item("partitions", report.partitions)?;
        d.set_item("elapsed_secs", report.elapsed_secs)?;
        d.set_item("manifest", report.manifest_path.display().to_string())?;
        out.append(d)?;
    }
    Ok(out.into())
}

/// Streaming reader yielding `pyarrow.RecordBatch` objects.
///
/// Chunks are decoded one at a time, so memory stays bounded no matter how large the session
/// is: a 56 GB file streams through in `chunk_mb`-sized pieces.
#[pyclass(unsendable, module = "nsetick._native")]
struct BatchReader {
    reader: RecordReader,
    decoder: Decoder,
    predicate: Predicate,
    stats: Stats,
    line_len: usize,
    exhausted: bool,
}

#[pymethods]
impl BatchReader {
    #[new]
    #[pyo3(signature = (
        input, *, layout=None, date=None, select=None, where_=None, strict=true,
        chunk_mb=8,
    ))]
    fn new(
        input: PathBuf,
        layout: Option<&str>,
        date: Option<&str>,
        select: Option<Vec<String>>,
        where_: Option<&str>,
        strict: bool,
        chunk_mb: usize,
    ) -> PyResult<Self> {
        let layout_id = resolve_layout(layout, &input)?;
        let session = resolve_date(date, &input)?;
        let lay = layout::load(&layout_id).map_err(to_py_err)?;

        // Ground the layout version in the file itself, exactly as the CLI does.
        let observed = pipeline::probe_record_length(&input).map_err(to_py_err)?;
        let version = lay
            .resolve(session, Some(observed))
            .map_err(to_py_err)?
            .clone();

        let decoder = Decoder::new(&version, select.as_deref(), DecodeOptions { strict })
            .map_err(to_py_err)?;
        let predicate =
            filter::compile(where_.unwrap_or(""), &version, Some(session)).map_err(to_py_err)?;

        let line_len = version.line_length();
        let reader =
            RecordReader::open(&input, line_len, chunk_mb * 1024 * 1024).map_err(to_py_err)?;

        Ok(Self {
            reader,
            decoder,
            predicate,
            stats: Stats::default(),
            line_len,
            exhausted: false,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyArrowType<RecordBatch>>> {
        loop {
            if self.exhausted {
                return Ok(None);
            }
            // Inflate and decode without holding the GIL.
            let decoded = py.detach(|| -> anyhow::Result<Option<RecordBatch>> {
                match self.reader.next_chunk()? {
                    None => Ok(None),
                    Some(chunk) => {
                        let batch =
                            self.decoder
                                .decode(&chunk, &self.predicate, &mut self.stats)?;
                        Ok(Some(batch))
                    }
                }
            });

            match decoded.map_err(to_py_err)? {
                None => {
                    self.exhausted = true;
                    return Ok(None);
                }
                // A chunk where the filter rejected everything is not the end of the file;
                // keep reading rather than terminating the iterator early.
                Some(b) if b.num_rows() == 0 => continue,
                Some(b) => return Ok(Some(PyArrowType(b))),
            }
        }
    }

    /// Rows read, emitted, filtered and malformed so far.
    #[getter]
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        stats_dict(py, &self.stats)
    }

    /// The pyarrow schema of the batches this reader yields.
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = self.decoder.schema();
        PyArrowType(schema.as_ref().clone())
            .into_pyobject(py)
            .map(|b| b.unbind())
    }

    #[getter]
    fn record_length(&self) -> usize {
        self.line_len - 1
    }

    #[getter]
    fn bytes_read(&self) -> u64 {
        self.reader.bytes_read()
    }

    fn __repr__(&self) -> String {
        format!(
            "BatchReader(path={:?}, record_length={}, rows_read={})",
            self.reader.path().display(),
            self.line_len - 1,
            self.stats.rows_read
        )
    }
}

/// Reconstruct limit order books and write periodic L2 snapshots.
///
/// Every symbol's book is independent, so this fans out across threads and replays the whole
/// session in one pass over the file rather than one pass per symbol.
#[pyfunction]
#[pyo3(signature = (
    input, out, *, date=None, where_=None, interval_secs=1.0, levels=20, threads=None,
    compression="snappy", max_records=None, symbols=None, previous_close=None,
))]
#[allow(clippy::too_many_arguments)]
fn build_books(
    py: Python<'_>,
    input: PathBuf,
    out: PathBuf,
    date: Option<&str>,
    where_: Option<&str>,
    interval_secs: f64,
    levels: usize,
    threads: Option<usize>,
    compression: &str,
    max_records: Option<u64>,
    symbols: Option<Vec<String>>,
    previous_close: Option<std::collections::HashMap<String, i64>>,
) -> PyResult<Py<PyDict>> {
    let previous_close = previous_close.unwrap_or_default();
    let compression = match compression.to_ascii_lowercase().as_str() {
        "zstd" => parquet_zstd(),
        "snappy" => parquet::basic::Compression::SNAPPY,
        "none" | "uncompressed" => parquet::basic::Compression::UNCOMPRESSED,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown compression {other:?}; use zstd, snappy or none"
            )))
        }
    };

    // A directory means already-parsed orders, which replay far faster: no second pass over
    // the compressed file, and one parallel job per symbol partition.
    if input.is_dir() {
        let session = match date {
            Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| PyValueError::new_err(format!("date {s:?} is not YYYY-MM-DD: {e}")))?,
            None => input
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix("date="))
                .and_then(|v| {
                    NaiveDate::parse_from_str(v, "%Y-%m-%d")
                        .ok()
                        .or_else(|| NaiveDate::parse_from_str(v, "%d%m%Y").ok())
                })
                .ok_or_else(|| {
                    PyValueError::new_err(
                        "cannot infer a session date from the directory name; pass date=",
                    )
                })?,
        };
        let mut req = nsetick_book::ParquetReplayRequest::new(&input, &out, session);
        req.interval_secs = interval_secs;
        req.levels = levels;
        req.threads = threads;
        req.symbols = symbols.unwrap_or_default();
        req.previous_close = previous_close.clone();
        req.writer = WriterOptions {
            compression,
            ..WriterOptions::default()
        };
        req.interrupt = Some(interrupt_hook());
        let r = py
            .detach(|| nsetick_book::from_parquet::run(&req))
            .map_err(to_py_err)?;
        return book_report(py, &r, "parquet");
    }

    let session = resolve_date(date, &input)?;
    let mut req = nsetick_book::ReplayRequest::new(&input, &out, session);
    if let Some(w) = where_ {
        req.filter = w.to_string();
    }
    req.interval_secs = interval_secs;
    req.levels = levels;
    req.threads = threads;
    req.max_records = max_records;
    req.previous_close = previous_close;
    req.writer = WriterOptions {
        compression,
        ..WriterOptions::default()
    };

    let r = py
        .detach(|| nsetick_book::replay::run(&req))
        .map_err(to_py_err)?;
    book_report(py, &r, "raw")
}

fn book_report(
    py: Python<'_>,
    r: &nsetick_book::ReplayReport,
    source: &str,
) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("source", source)?;
    d.set_item("rows_read", r.stats.rows_read)?;
    d.set_item("events_applied", r.events_applied)?;
    d.set_item("symbols", r.symbols)?;
    d.set_item("snapshots", r.snapshots)?;
    d.set_item("fills_generated", r.fills_generated)?;
    d.set_item("replenishments", r.replenishments)?;
    d.set_item("crossed_symbols", r.crossed_symbols)?;
    d.set_item("elapsed_secs", r.elapsed_secs)?;
    d.set_item("threads", r.threads)?;
    d.set_item("bytes_decompressed", r.bytes_decompressed)?;
    Ok(d.into())
}

/// The layout ids nsetick knows about.
#[pyfunction]
fn layouts() -> Vec<&'static str> {
    layout::available()
}

/// Describe a layout version: its fields, offsets, types and scales.
#[pyfunction]
#[pyo3(signature = (layout_id, date=None))]
fn describe(py: Python<'_>, layout_id: &str, date: Option<&str>) -> PyResult<Py<PyDict>> {
    let lay = layout::load(layout_id).map_err(to_py_err)?;
    let on = match date {
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| PyValueError::new_err(format!("date {s:?} is not YYYY-MM-DD: {e}")))?,
        None => chrono::Local::now().date_naive(),
    };
    let v = lay.for_date(on).map_err(to_py_err)?;

    let fields = PyList::empty(py);
    for f in &v.fields {
        let d = PyDict::new(py);
        d.set_item("name", &f.name)?;
        d.set_item("offset", f.offset)?;
        d.set_item("len", f.len)?;
        d.set_item("type", format!("{:?}", f.ty))?;
        d.set_item("scale", f.scale)?;
        d.set_item("pad", f.pad.map(|p| format!("{p:?}")))?;
        d.set_item("doc", &f.doc)?;
        fields.append(d)?;
    }

    let out = PyDict::new(py);
    out.set_item("id", &lay.meta.id)?;
    out.set_item("segment", &lay.meta.segment)?;
    out.set_item("kind", &lay.meta.kind)?;
    out.set_item("description", &lay.meta.description)?;
    out.set_item("file_glob", &lay.meta.file_glob)?;
    out.set_item("spec_version", &v.spec_version)?;
    out.set_item("record_length", v.record_length)?;
    out.set_item("verified", v.verified)?;
    out.set_item("fields", fields)?;
    Ok(out.into())
}

/// Record length observed in a file, and the layout version it selects.
#[pyfunction]
#[pyo3(signature = (input, layout=None, date=None))]
fn probe(
    py: Python<'_>,
    input: PathBuf,
    layout: Option<&str>,
    date: Option<&str>,
) -> PyResult<Py<PyDict>> {
    let layout_id = resolve_layout(layout, &input)?;
    let session = resolve_date(date, &input)?;
    let lay = layout::load(&layout_id).map_err(to_py_err)?;
    let observed = pipeline::probe_record_length(&input).map_err(to_py_err)?;
    let v = lay.resolve(session, Some(observed)).map_err(to_py_err)?;

    let d = PyDict::new(py);
    d.set_item("layout", layout_id)?;
    d.set_item("session_date", session.to_string())?;
    d.set_item("observed_record_length", observed)?;
    d.set_item("spec_version", &v.spec_version)?;
    d.set_item("verified", v.verified)?;
    Ok(d.into())
}

/// Memory nsetick would plan for on this machine, and what a run of `partitions` would cost.
#[pyfunction]
#[pyo3(signature = (partitions=0))]
fn memory_estimate(py: Python<'_>, partitions: usize) -> PyResult<Py<PyDict>> {
    use nsetick_io::memory;
    let (footprint, buffer) = memory::default_limits(512 * 1024 * 1024);
    let d = PyDict::new(py);
    d.set_item("available_bytes", memory::available_bytes())?;
    d.set_item("total_bytes", memory::total_bytes())?;
    d.set_item("default_footprint_limit", footprint)?;
    d.set_item("default_buffer_limit", buffer)?;
    d.set_item("bytes_per_open_partition", memory::BYTES_PER_OPEN_PARTITION)?;
    d.set_item(
        "estimated_bytes",
        partitions * memory::BYTES_PER_OPEN_PARTITION + buffer,
    )?;
    d.set_item(
        "partitions_that_fit",
        footprint.saturating_sub(buffer) / memory::BYTES_PER_OPEN_PARTITION,
    )?;
    Ok(d.into())
}

/// Validate a filter expression against a layout without reading any data.
///
/// Useful for failing fast in a pipeline: a typo in a field name raises here rather than
/// producing an empty result after an hour of parsing.
#[pyfunction]
#[pyo3(signature = (expr, layout_id, date=None))]
fn check_filter(expr: &str, layout_id: &str, date: Option<&str>) -> PyResult<bool> {
    let lay = layout::load(layout_id).map_err(to_py_err)?;
    let on = match date {
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| PyValueError::new_err(format!("date {s:?} is not YYYY-MM-DD: {e}")))?,
        None => chrono::Local::now().date_naive(),
    };
    let v = lay.for_date(on).map_err(to_py_err)?;
    filter::compile(expr, v, Some(on)).map_err(to_py_err)?;
    Ok(true)
}

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Every trade the order book replay generates, for comparison with the trade file.
#[pyfunction]
#[pyo3(signature = (input, symbols=None, previous_close=None))]
fn replay_fills(
    py: Python<'_>,
    input: PathBuf,
    symbols: Option<Vec<String>>,
    previous_close: Option<std::collections::HashMap<String, i64>>,
) -> PyResult<PyArrowType<RecordBatch>> {
    let symbols = symbols.unwrap_or_default();
    let previous_close = previous_close.unwrap_or_default();
    let batch = py
        .detach(|| nsetick_book::fills::replay_fills(&input, &symbols, &previous_close))
        .map_err(to_py_err)?;
    Ok(PyArrowType(batch))
}

/// Run the `nsetick` command line with `argv` (program name first) and return its exit code.
/// Backs the `nsetick` console script and `python -m nsetick`.
#[pyfunction]
fn cli(py: Python<'_>, argv: Vec<String>) -> i32 {
    py.detach(|| nsetick_cli::run(argv))
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cli, m)?)?;
    m.add_class::<BatchReader>()?;
    m.add_function(wrap_pyfunction!(parse, m)?)?;
    m.add_function(wrap_pyfunction!(run_spec, m)?)?;
    m.add_function(wrap_pyfunction!(build_books, m)?)?;
    m.add_function(wrap_pyfunction!(replay_fills, m)?)?;
    m.add_function(wrap_pyfunction!(layouts, m)?)?;
    m.add_function(wrap_pyfunction!(describe, m)?)?;
    m.add_function(wrap_pyfunction!(probe, m)?)?;
    m.add_function(wrap_pyfunction!(memory_estimate, m)?)?;
    m.add_function(wrap_pyfunction!(check_filter, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add("__doc__", "Native core of nsetick.")?;
    Ok(())
}
