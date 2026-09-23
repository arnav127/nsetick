"""Layout registry: loads the TOML specs in spec/layouts and selects the right version.

The TOML files are the single source of truth shared by the Rust core and this Python
package. Offsets appear exactly once, in one place, in 0-based form. Nothing else in
nsetick is permitted to hardcode a byte offset.
"""

from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    import tomllib
else:  # tomllib joined the standard library in 3.11; tomli is its backport.
    import tomli as tomllib
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path

def _find_spec_dir() -> Path:
    """Locate spec/layouts, whether running from a checkout or an installed wheel."""
    here = Path(__file__).resolve()
    candidates = [
        here.parents[2] / "spec" / "layouts",   # source checkout: <repo>/spec/layouts
        here.parents[1] / "spec" / "layouts",   # installed wheel: site-packages/spec/layouts
        here.parent / "spec" / "layouts",
        here.parent / "layouts",
    ]
    for c in candidates:
        if c.is_dir():
            return c
    # The native module embeds the same specs, so this is only a problem for the pure-Python
    # reference decoder.
    return candidates[0]


SPEC_DIR = _find_spec_dir()

# NSE writes the literal character 'b' as the blank fill byte, because the layout document
# denotes a blank as "b" ("Symbol ABC will be 'bbbbbbbABC'") and the generator emitted the
# notation rather than the character it stands for. Real 0x20 spaces also occur, so both are
# stripped. Nothing else is: '&', '-', '*' and '.' are legal inside NSE symbols, and a regex
# like [A-Z0-9-]+ silently truncates M&M to M.
PAD_BYTES = b"b "


@dataclass(frozen=True)
class Field:
    name: str
    offset: int
    len: int
    type: str
    pad: str | None = None
    scale: int | None = None
    values: tuple = ()
    doc: str = ""

    @property
    def end(self) -> int:
        return self.offset + self.len


@dataclass(frozen=True)
class LayoutVersion:
    spec_version: str
    valid_from: date
    valid_to: date
    record_length: int
    verified: bool
    fields: tuple[Field, ...]

    @property
    def line_length(self) -> int:
        """Record length including the LF delimiter."""
        return self.record_length + 1

    def field(self, name: str) -> Field:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(f"no field {name!r} in {self.spec_version}")


@dataclass(frozen=True)
class Layout:
    id: str
    segment: str
    kind: str
    description: str
    file_glob: str
    split_files: bool
    versions: tuple[LayoutVersion, ...]

    def for_date(self, on: date) -> LayoutVersion:
        for v in self.versions:
            if v.valid_from <= on <= v.valid_to:
                return v
        raise ValueError(f"{self.id}: no layout version covers {on}")

    def for_record_length(self, length: int) -> LayoutVersion:
        matches = [v for v in self.versions if v.record_length == length]
        if not matches:
            known = sorted(v.record_length for v in self.versions)
            raise ValueError(
                f"{self.id}: observed record length {length} matches no known version "
                f"(known: {known}). Refusing to guess - a wrong layout shifts every field "
                f"after the mismatch and produces silently corrupt output."
            )
        return matches[0]

    def resolve(self, on: date, observed_length: int | None = None) -> LayoutVersion:
        """Pick a version by date, then cross-check it against the observed record length.

        Date boundaries taken from the spec's revision history are approximate; the observed
        length is ground truth. When they disagree the length wins, loudly.
        """
        by_date = self.for_date(on)
        if observed_length is None or observed_length == by_date.record_length:
            return by_date
        by_len = self.for_record_length(observed_length)
        import warnings

        warnings.warn(
            f"{self.id}: date {on} selects spec {by_date.spec_version} "
            f"({by_date.record_length}B) but the file has {observed_length}B records. "
            f"Using spec {by_len.spec_version}; the valid_from boundary in "
            f"spec/layouts/{self.id}.toml is likely wrong.",
            stacklevel=2,
        )
        return by_len

    def filename_pattern(self, date_str: str) -> str:
        return self.file_glob.replace("{date}", date_str)


def _parse_version(raw: dict) -> LayoutVersion:
    return LayoutVersion(
        spec_version=str(raw["spec_version"]),
        valid_from=raw["valid_from"],
        valid_to=raw["valid_to"],
        record_length=raw["record_length"],
        verified=raw.get("verified", True),
        fields=tuple(
            Field(
                name=f["name"],
                offset=f["offset"],
                len=f["len"],
                type=f["type"],
                pad=f.get("pad"),
                scale=f.get("scale"),
                values=tuple(f.get("values", ())),
                doc=f.get("doc", ""),
            )
            for f in raw["fields"]
        ),
    )


@lru_cache(maxsize=None)
def load(layout_id: str) -> Layout:
    path = SPEC_DIR / f"{layout_id}.toml"
    if not path.exists():
        raise FileNotFoundError(
            f"unknown layout {layout_id!r}; available: {sorted(available())}"
        )
    spec = tomllib.loads(path.read_text(encoding="utf-8"))
    meta = spec["layout"]
    return Layout(
        id=meta["id"],
        segment=meta["segment"],
        kind=meta["kind"],
        description=meta["description"],
        file_glob=meta["file_glob"],
        split_files=meta.get("split_files", False),
        versions=tuple(_parse_version(v) for v in spec["version"]),
    )


def available() -> list[str]:
    return sorted(p.stem for p in SPEC_DIR.glob("*.toml"))
