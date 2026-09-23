# Installation

Prebuilt downloads need nothing else installed: no Rust, no compiler. Pick whichever way you
intend to use the tool; both come from the same release.

## Python package

Requires Python 3.9 or newer. One wheel covers every Python version from 3.9 on.

```bash
pip install nsetick --find-links https://github.com/arnav127/nsetick/releases/expanded_assets/v0.2.0
```

`--find-links` points pip at the release page, and pip picks the wheel for your platform.
Replace `v0.2.0` with the [latest release](https://github.com/arnav127/nsetick/releases).
Wheels are published for:

| Platform | Wheel |
|---|---|
| Linux x86_64 and aarch64 | `manylinux2014` — runs on glibc 2.17 and newer, which covers CentOS 7, RHEL 7/8/9 and current Ubuntu |
| macOS Apple silicon and Intel | native wheels |
| Windows x86_64 | native wheel |

Optional extras for DataFrame output:

```bash
pip install pandas      # for nsetick.to_pandas
pip install polars      # for nsetick.to_polars
```

Check it worked:

```python
import nsetick
print(nsetick.__version__, nsetick.layouts())
```

If the package is also published on PyPI (see [Releasing](Releasing)), a plain
`pip install nsetick` works as well.

### Offline or cluster installs

Compute nodes often have no internet access. Download the wheel for the cluster's platform
from the release page on a machine that does, copy it across, and install the file:

```bash
pip install --user nsetick-0.2.0-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```

`pyarrow` is the only dependency; install it the same way if the cluster cannot reach PyPI.

## Command-line binary

Download the archive for your platform from the
[releases page](https://github.com/arnav127/nsetick/releases), unpack it, and put `nsetick`
somewhere on your `PATH`.

| Platform | Archive |
|---|---|
| Linux x86_64 | `nsetick-<version>-x86_64-unknown-linux-musl.tar.gz` |
| Linux aarch64 | `nsetick-<version>-aarch64-unknown-linux-musl.tar.gz` |
| macOS Apple silicon | `nsetick-<version>-aarch64-apple-darwin.tar.gz` |
| macOS Intel | `nsetick-<version>-x86_64-apple-darwin.tar.gz` |
| Windows | `nsetick-<version>-x86_64-pc-windows-msvc.zip` |

The Linux binaries are statically linked, so they run on any distribution regardless of its
C library version. The layout specifications are compiled in; the binary is self-contained.

```bash
tar xzf nsetick-0.2.0-x86_64-unknown-linux-musl.tar.gz
cd nsetick-0.2.0-x86_64-unknown-linux-musl
./nsetick --version
```

On macOS, a downloaded binary may be quarantined. If it is refused, clear the flag once:

```bash
xattr -d com.apple.quarantine ./nsetick
```

Every release lists SHA-256 checksums in `SHA256SUMS`:

```bash
sha256sum -c SHA256SUMS --ignore-missing
```

## From source

Needed only to modify `nsetick` or to build for a platform without a prebuilt download.
Requires [Rust](https://rustup.rs) 1.95 or newer; `rustup update stable` gets it.

```bash
git clone https://github.com/arnav127/nsetick
cd nsetick

# The command line
cargo build --release
./target/release/nsetick --help

# The Python package, into the active environment
pip install maturin
maturin develop --release
```

Or let pip build it directly from the repository:

```bash
pip install "nsetick @ git+https://github.com/arnav127/nsetick"
```

## Which version do I have?

```bash
nsetick --version
python -c "import nsetick; print(nsetick.__version__)"
```

Record the version alongside any results. Order books built by 0.1.0 differ from those built
by 0.2.0 and later; see [Validation and Accuracy](Validation-and-Accuracy).
