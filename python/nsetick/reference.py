"""Pure-Python reference decoder.

Deliberately simple and slow. Its job is to be obviously correct so it can serve as the
oracle the Rust core is tested against, and to decode small samples during layout
development. Production parsing goes through the Rust core.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from .layout import PAD_BYTES, Field, LayoutVersion

# 65536 jiffies = 1 second, counted from 1980-01-01. The values decode directly to IST
# wall-clock time (no timezone conversion is applied by NSE), which is why a pre-open record
# on 2022-01-27 decodes to 09:0x and not 03:3x.
JIFFIES_PER_SECOND = 65536
JIFFIES_EPOCH = datetime(1980, 1, 1)

_MONTHS = {
    m: i
    for i, m in enumerate(
        ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
         "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1
    )
}


class DecodeError(ValueError):
    pass


def jiffies_to_datetime(j: int) -> datetime:
    return JIFFIES_EPOCH + timedelta(microseconds=j * 1_000_000 // JIFFIES_PER_SECOND)


def _decode_field(raw: bytes, f: Field):
    if f.type == "str":
        if f.pad == "left":
            v = raw.lstrip(PAD_BYTES)
        elif f.pad == "right":
            v = raw.rstrip(PAD_BYTES)
        elif f.pad == "both":
            v = raw.strip(PAD_BYTES)
        else:
            v = raw  # verbatim: fixed-width codes (series, instrument, option_type) are exact
        return v.decode("ascii", errors="replace")

    if f.type in ("u8", "u64", "price", "jiffies"):
        s = raw.lstrip(PAD_BYTES)
        if not s:
            return None
        if not s.isdigit():
            raise DecodeError(f"{f.name}: expected digits, got {raw!r}")
        n = int(s)
        return jiffies_to_datetime(n) if f.type == "jiffies" else n

    if f.type == "bool_yn":
        if raw == b"Y":
            return True
        if raw == b"N":
            return False
        return None

    if f.type == "date_dmmmy":            # ddMMMyyyy
        s = raw.decode("ascii", "replace")
        try:
            return date(int(s[5:9]), _MONTHS[s[2:5].upper()], int(s[0:2]))
        except (KeyError, ValueError) as exc:
            raise DecodeError(f"{f.name}: bad ddMMMyyyy {raw!r}") from exc

    if f.type == "date_ymd":              # YYYYMMDD
        s = raw.decode("ascii", "replace")
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))

    if f.type == "time_hms":              # HH:MM:SS
        return raw.decode("ascii", "replace")

    raise DecodeError(f"{f.name}: unhandled type {f.type!r}")


def decode_record(line: bytes, version: LayoutVersion) -> dict:
    """Decode one record. `line` must exclude the LF delimiter."""
    if len(line) != version.record_length:
        raise DecodeError(
            f"record is {len(line)} bytes, layout {version.spec_version} expects "
            f"{version.record_length}: {line[:40]!r}..."
        )
    return {f.name: _decode_field(line[f.offset:f.end], f) for f in version.fields}


def decode_stream(lines, version: LayoutVersion, *, strict: bool = True):
    """Decode an iterable of raw lines, yielding dicts.

    Unlike the TRY_CAST-everything approach in the existing pipelines, a malformed record
    raises by default instead of silently becoming a row of NULLs.
    """
    for lineno, line in enumerate(lines, 1):
        line = line.rstrip(b"\r\n")
        if not line:
            continue
        try:
            yield decode_record(line, version)
        except DecodeError as exc:
            if strict:
                raise DecodeError(f"line {lineno}: {exc}") from None
            yield None


def scale_price(value: int | None, field: Field) -> float | None:
    """Convert a raw integer price to its decimal value using the field's own scale.

    CM and FAO are scale 2 (paise); CD is scale 4. A single global paise_to_rupees() helper
    is wrong for CD by a factor of 100.
    """
    if value is None:
        return None
    return value / (10 ** (field.scale or 0))
