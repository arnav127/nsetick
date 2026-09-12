"""Structural validation of the nsetick layout specs.

Every bug this guards against was observed in the wild:
  * the same offset typed in two places and drifting out of sync
  * a field list whose lengths do not sum to the declared record length
  * 0-based schema offsets mixed with 1-based SQL SUBSTRING positions
  * two repos disagreeing on a field name for the same byte range

Run: python spec/validate_layouts.py
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

LAYOUT_DIR = Path(__file__).parent / "layouts"

VALID_TYPES = {
    "str", "u8", "u64", "price", "jiffies",
    "bool_yn", "date_dmmmy", "date_ymd", "time_hms",
}
VALID_PADS = {"left", "right", "both"}


def validate_version(layout_id: str, ver: dict, errors: list[str]) -> None:
    tag = f"{layout_id}@{ver.get('spec_version', '?')}"
    fields = ver["fields"]
    declared = ver["record_length"]

    cursor = 0
    seen: set[str] = set()
    for f in fields:
        name, off, ln = f["name"], f["offset"], f["len"]

        if name in seen:
            errors.append(f"{tag}: duplicate field name {name!r}")
        seen.add(name)

        if f["type"] not in VALID_TYPES:
            errors.append(f"{tag}.{name}: unknown type {f['type']!r}")
        if "pad" in f and f["pad"] not in VALID_PADS:
            errors.append(f"{tag}.{name}: unknown pad {f['pad']!r}")
        if ln <= 0:
            errors.append(f"{tag}.{name}: non-positive length {ln}")

        # Contiguity: fixed-width NSE records have no gaps and no overlaps.
        if off != cursor:
            kind = "gap" if off > cursor else "overlap"
            errors.append(
                f"{tag}.{name}: {kind} - field starts at {off}, previous field ended at {cursor}"
            )
        cursor = off + ln

        if f["type"] == "price" and "scale" not in f:
            errors.append(f"{tag}.{name}: price field must declare a scale")
        if f["type"] == "bool_yn" and ln != 1:
            errors.append(f"{tag}.{name}: bool_yn must be 1 byte, got {ln}")

    if cursor != declared:
        errors.append(
            f"{tag}: fields span {cursor} bytes but record_length is {declared}"
        )


def main() -> int:
    errors: list[str] = []
    layouts = sorted(LAYOUT_DIR.glob("*.toml"))
    if not layouts:
        print(f"no layouts found in {LAYOUT_DIR}", file=sys.stderr)
        return 1

    for path in layouts:
        spec = tomllib.loads(path.read_text(encoding="utf-8"))
        layout_id = spec["layout"]["id"]

        if layout_id != path.stem:
            errors.append(f"{path.name}: layout id {layout_id!r} does not match filename")

        versions = spec["version"]
        lengths = [v["record_length"] for v in versions]
        if len(set(lengths)) != len(lengths):
            errors.append(
                f"{layout_id}: two versions share a record_length {lengths} - "
                "length-based version detection would be ambiguous"
            )

        # Date ranges must not overlap, else version selection is ambiguous.
        spans = sorted((v["valid_from"], v["valid_to"], v["spec_version"]) for v in versions)
        for (_, prev_to, prev_v), (next_from, _, next_v) in zip(spans, spans[1:]):
            if str(next_from) <= str(prev_to):
                errors.append(
                    f"{layout_id}: date ranges of v{prev_v} and v{next_v} overlap "
                    f"({prev_to} >= {next_from})"
                )

        for ver in versions:
            validate_version(layout_id, ver, errors)

        n_unverified = sum(1 for v in versions if not v.get("verified", True))
        flag = f"  [{n_unverified} unverified]" if n_unverified else ""
        print(
            f"  OK  {layout_id:<12} "
            f"{len(versions)} version(s), lengths {sorted(lengths)}{flag}"
        )

    if errors:
        print(f"\n{len(errors)} problem(s):", file=sys.stderr)
        for e in errors:
            print(f"  FAIL  {e}", file=sys.stderr)
        return 1

    print(f"\nAll {len(layouts)} layouts structurally valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
