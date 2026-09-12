//! Record-aligned reading of gzipped fixed-width files.
//!
//! Because the format is fixed width, splitting the decompressed stream into work units is
//! exact arithmetic rather than a scan for newlines: a chunk is valid if its length is a
//! multiple of `record_length + 1`. That is what lets decoding be parallelised later without
//! any boundary heuristics.
//!
//! Decompression itself is the wall. A single NSE session of CM orders is 8.3 GB compressed
//! and 58 GB decompressed, and deflate streams are not randomly seekable, so exactly one
//! thread can inflate a given file. Everything downstream therefore runs concurrently with
//! the inflate rather than after it, and the `zlib-ng` feature swaps in a faster inflate for
//! builds that have cmake available.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use flate2::read::MultiGzDecoder;

/// Decompressed bytes held per chunk before it is handed downstream. Rounded down to a
/// record boundary at construction. 8 MB is large enough to amortise syscalls and small
/// enough that several chunks in flight do not dominate memory.
pub const DEFAULT_CHUNK_BYTES: usize = 8 * 1024 * 1024;

pub struct RecordReader {
    inner: Box<dyn Read + Send>,
    path: PathBuf,
    line_len: usize,
    buf: Vec<u8>,
    /// Bytes of `buf` currently holding data.
    filled: usize,
    /// Decompressed bytes produced so far, for progress reporting.
    bytes_read: u64,
    eof: bool,
}

impl RecordReader {
    /// Open a `.DAT.gz` (or plain `.DAT`) file for record-aligned reading.
    pub fn open(path: impl AsRef<Path>, line_len: usize, chunk_bytes: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        // 1 MB of read-ahead: these files are gigabytes and the default 8 KB is wasteful.
        let buffered = BufReader::with_capacity(1024 * 1024, file);

        let gzipped = path
            .extension()
            .map(|e| e.eq_ignore_ascii_case("gz"))
            .unwrap_or(false);

        // MultiGzDecoder rather than GzDecoder: concatenated gzip members are legal and a
        // plain GzDecoder silently stops at the end of the first one, which would truncate
        // the session without any error at all.
        let inner: Box<dyn Read + Send> = if gzipped {
            Box::new(MultiGzDecoder::new(buffered))
        } else {
            Box::new(buffered)
        };

        if line_len == 0 {
            bail!("line length must be non-zero");
        }
        let capacity = (chunk_bytes / line_len).max(1) * line_len;

        Ok(Self {
            inner,
            path,
            line_len,
            buf: vec![0u8; capacity + line_len],
            filled: 0,
            bytes_read: 0,
            eof: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Read the next chunk holding a whole number of records.
    ///
    /// Returns `None` at clean end of file. A trailing partial record is an error: it means
    /// the file is truncated or the layout's record length is wrong, and both are worth
    /// stopping for rather than quietly dropping data.
    pub fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        let target = self.buf.len() - self.line_len;

        while !self.eof && self.filled < target {
            let n = self
                .inner
                .read(&mut self.buf[self.filled..])
                .with_context(|| format!("decompressing {}", self.path.display()))?;
            if n == 0 {
                self.eof = true;
                break;
            }
            self.filled += n;
            self.bytes_read += n as u64;
        }

        if self.filled == 0 {
            return Ok(None);
        }

        let whole = self.filled - (self.filled % self.line_len);

        if whole == 0 {
            // Everything buffered is a partial record and there is no more input.
            bail!(
                "{}: file ends with {} trailing bytes, which is less than one {}-byte record. \
                 The file is truncated or the layout record length is wrong.",
                self.path.display(),
                self.filled,
                self.line_len
            );
        }

        let chunk = self.buf[..whole].to_vec();
        let remainder = self.filled - whole;
        if remainder > 0 {
            if self.eof {
                bail!(
                    "{}: file ends with {remainder} trailing bytes after the last complete \
                     {}-byte record. The file is truncated or the layout record length is wrong.",
                    self.path.display(),
                    self.line_len
                );
            }
            self.buf.copy_within(whole..self.filled, 0);
        }
        self.filled = remainder;

        Ok(Some(chunk))
    }
}

/// Read the `.trg` sidecar NSE ships next to each data file, if present.
///
/// Content is an MD5 and a byte count, available for files from 2020-12-01. The existing
/// pipelines merely exclude `.trg` from their globs; it is a free integrity check.
pub fn read_trigger(data_path: &Path) -> Result<Option<Trigger>> {
    let trg = PathBuf::from(format!("{}.trg", data_path.display()));
    if !trg.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&trg)
        .with_context(|| format!("reading {}", trg.display()))?;

    let mut md5 = None;
    let mut size = None;
    for token in text.split_whitespace() {
        if token.len() == 32 && token.bytes().all(|b| b.is_ascii_hexdigit()) {
            md5 = Some(token.to_ascii_lowercase());
        } else if let Ok(n) = token.parse::<u64>() {
            size = Some(n);
        }
    }
    Ok(Some(Trigger { md5, size }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub md5: Option<String>,
    pub size: Option<u64>,
}

impl Trigger {
    /// Check the on-disk size against the trigger file. The MD5 is deliberately not checked
    /// here: hashing 8 GB costs more than the parse itself and belongs behind a flag.
    pub fn check_size(&self, path: &Path) -> Result<()> {
        let Some(expected) = self.size else {
            return Ok(());
        };
        let actual = std::fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .len();
        if actual != expected {
            bail!(
                "{}: size {actual} does not match the {expected} recorded in the .trg file; \
                 the download is incomplete or corrupt",
                path.display()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn gz_temp(body: &[u8], name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(name);
        let f = File::create(&path).unwrap();
        let mut enc = GzEncoder::new(f, Compression::fast());
        enc.write_all(body).unwrap();
        enc.finish().unwrap();
        path
    }

    /// 88-byte lines: 87-byte CM order records plus LF.
    fn records(n: usize) -> Vec<u8> {
        let mut out = Vec::new();
        for i in 0..n {
            let line = format!(
                "POCASH1000000000002084870148472915{:02}S1BALKRISINDEQ00000000000000760023000000000000NNN13\n",
                i % 100
            );
            assert_eq!(line.len(), 88);
            out.extend_from_slice(line.as_bytes());
        }
        out
    }

    #[test]
    fn reads_all_records_across_several_chunks() {
        let body = records(1000);
        let path = gz_temp(&body, "nsetick_reader_ok.DAT.gz");
        // Force many small chunks: 10 records each.
        let mut r = RecordReader::open(&path, 88, 88 * 10).unwrap();

        let mut total = 0usize;
        while let Some(chunk) = r.next_chunk().unwrap() {
            assert_eq!(chunk.len() % 88, 0, "chunk must be record aligned");
            total += chunk.len();
        }
        assert_eq!(total, body.len());
        assert_eq!(r.bytes_read(), body.len() as u64);
    }

    #[test]
    fn a_truncated_final_record_is_an_error_not_a_silent_drop() {
        let mut body = records(10);
        body.truncate(body.len() - 30); // chop the last record in half
        let path = gz_temp(&body, "nsetick_reader_trunc.DAT.gz");
        let mut r = RecordReader::open(&path, 88, 88 * 4).unwrap();

        let mut err = None;
        loop {
            match r.next_chunk() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(e) => {
                    err = Some(e.to_string());
                    break;
                }
            }
        }
        let err = err.expect("truncation should be reported");
        assert!(err.contains("truncated"), "{err}");
    }

    #[test]
    fn concatenated_gzip_members_are_all_read() {
        // GzDecoder would stop after the first member and silently lose the rest.
        let a = records(5);
        let b = records(7);
        let path = std::env::temp_dir().join("nsetick_reader_multi.DAT.gz");
        {
            let mut f = File::create(&path).unwrap();
            for part in [&a, &b] {
                let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
                enc.write_all(part).unwrap();
                f.write_all(&enc.finish().unwrap()).unwrap();
            }
        }
        let mut r = RecordReader::open(&path, 88, 88 * 100).unwrap();
        let mut total = 0;
        while let Some(c) = r.next_chunk().unwrap() {
            total += c.len();
        }
        assert_eq!(total, a.len() + b.len(), "second gzip member was dropped");
    }

    #[test]
    fn plain_uncompressed_files_work_too() {
        let body = records(20);
        let path = std::env::temp_dir().join("nsetick_reader_plain.DAT");
        std::fs::write(&path, &body).unwrap();
        let mut r = RecordReader::open(&path, 88, 88 * 3).unwrap();
        let mut total = 0;
        while let Some(c) = r.next_chunk().unwrap() {
            total += c.len();
        }
        assert_eq!(total, body.len());
    }

    #[test]
    fn trigger_files_are_parsed_and_checked() {
        let body = records(3);
        let path = std::env::temp_dir().join("nsetick_trg.DAT.gz");
        std::fs::write(&path, &body).unwrap();
        let size = body.len();
        std::fs::write(
            std::env::temp_dir().join("nsetick_trg.DAT.gz.trg"),
            format!("a50a1646ae6dc59c66d3be3e15b9e1cf   nsetick_trg.DAT.gz\n{size}\n"),
        )
        .unwrap();

        let t = read_trigger(&path).unwrap().expect("trigger present");
        assert_eq!(t.md5.as_deref(), Some("a50a1646ae6dc59c66d3be3e15b9e1cf"));
        assert_eq!(t.size, Some(size as u64));
        t.check_size(&path).unwrap();

        // A size mismatch is reported rather than ignored.
        std::fs::write(&path, b"short").unwrap();
        assert!(t.check_size(&path).is_err());
    }

    #[test]
    fn a_missing_trigger_is_not_an_error() {
        let path = std::env::temp_dir().join("nsetick_no_trg.DAT.gz");
        std::fs::write(&path, b"x").unwrap();
        assert_eq!(read_trigger(&path).unwrap(), None);
    }
}
