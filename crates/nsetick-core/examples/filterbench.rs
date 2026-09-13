//! Isolate the cost of set membership: bucketed index against a flat scan.
//!
//! A parse benchmark cannot answer this. Changing the needle list also changes how many rows
//! are emitted and how many partitions stay open, and both dominate the runtime - a first
//! attempt measured the 1,129-symbol filter as *faster* than no symbol filter at all, purely
//! because it opened half as many partitions. Here nothing varies but the matching.

use std::time::Instant;

use nsetick_core::filter::TextSet;

/// Real NSE tickers, which cluster on first byte and length in ways synthetic strings do not.
fn real_sets() -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let raw = include_str!("symbols.txt");
    let norm: String = raw.chars().filter(|c| *c != '\r').collect();
    let (hits, misses) = norm.split_once("\n---\n").expect("separator in symbols.txt");
    let to_vecs = |s: &str| -> Vec<Vec<u8>> {
        s.lines().filter(|l| !l.is_empty()).map(|l| l.as_bytes().to_vec()).collect()
    };
    (to_vecs(hits), to_vecs(misses))
}

/// Synthetic tickers, for the needle-count sweep.
fn symbols(n: usize) -> Vec<Vec<u8>> {
    let alpha = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    (0..n)
        .map(|i| {
            let len = 3 + (i * 7) % 8;
            (0..len).map(|j| alpha[(i * 13 + j * 31) % 26]).collect()
        })
        .collect()
}

fn linear(needles: &[Vec<u8>], v: &[u8]) -> bool {
    needles.iter().any(|n| n.as_slice() == v)
}

fn time<F: FnMut() -> usize>(mut f: F) -> (f64, usize) {
    let t = Instant::now();
    let n = f();
    (t.elapsed().as_secs_f64(), n)
}

fn main() {
    const ROWS: usize = 20_000_000;

    // The measurement that matters: the real universe against real non-universe tickers, at
    // roughly the hit rate a session sees - about four fifths of EQ records on 25012022
    // belonged to the configured universe.
    let (hits, misses) = real_sets();
    let set = TextSet::new(hits.clone());
    let probes: Vec<Vec<u8>> = (0..2048)
        .map(|i| {
            if i % 5 == 0 {
                misses[i % misses.len()].clone()
            } else {
                hits[i % hits.len()].clone()
            }
        })
        .collect();

    let (lin, a) = time(|| {
        let mut c = 0;
        for i in 0..ROWS {
            if linear(&hits, &probes[i & 2047]) {
                c += 1;
            }
        }
        c
    });
    let (buck, b) = time(|| {
        let mut c = 0;
        for i in 0..ROWS {
            if set.contains(&probes[i & 2047]) {
                c += 1;
            }
        }
        c
    });
    assert_eq!(a, b, "the two implementations must agree");

    println!(
        "REAL {} NSE tickers, {ROWS} probes, 80% hit rate",
        hits.len()
    );
    println!("   linear   {lin:>6.2}s   {:>6.1} ns/record", lin / ROWS as f64 * 1e9);
    println!("   bucketed {buck:>6.2}s   {:>6.1} ns/record", buck / ROWS as f64 * 1e9);
    println!("   speedup  {:>6.1}x", lin / buck);
    println!(
        "   extrapolated over a 704M-record session: linear {:.0}s, bucketed {:.0}s\n",
        lin / ROWS as f64 * 704e6,
        buck / ROWS as f64 * 704e6
    );

    println!("needle-count sweep, synthetic tickers:");
    for count in [2usize, 50, 500, 1129] {
        let needles = symbols(count);
        let set = TextSet::new(needles.clone());
        let pool = symbols(4000);
        let probes: Vec<Vec<u8>> = (0..2048)
            .map(|i| {
                if i % 5 == 0 {
                    pool[2000 + (i % 2000)].clone()
                } else {
                    needles[i % count].clone()
                }
            })
            .collect();

        let (lin, a) = time(|| {
            let mut c = 0;
            for i in 0..ROWS {
                if linear(&needles, &probes[i & 2047]) {
                    c += 1;
                }
            }
            c
        });
        let (buck, b) = time(|| {
            let mut c = 0;
            for i in 0..ROWS {
                if set.contains(&probes[i & 2047]) {
                    c += 1;
                }
            }
            c
        });
        assert_eq!(a, b);
        println!(
            "  {count:>5}: linear {lin:>6.2}s   bucketed {buck:>6.2}s   speedup {:>5.1}x",
            lin / buck
        );
    }
}
