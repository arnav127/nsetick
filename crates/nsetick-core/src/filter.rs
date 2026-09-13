//! Predicates evaluated directly against raw fixed-width record bytes.
//!
//! The point of filtering here rather than after decoding is that a rejected record costs
//! almost nothing: no UTF-8 validation, no allocation, no Arrow builder append. A
//! `series == 'EQ'` test is a two-byte comparison at a known offset, and on a session file
//! where 99% of records are rejected the parse effectively runs at memory bandwidth.
//!
//! Predicates are compiled against a specific [`Version`], so every field reference is
//! resolved to an offset once, up front, and a typo in a field name is an error before any
//! data is read rather than a silently empty result.

use std::fmt;

use anyhow::{anyhow, bail, Context, Result};
use chrono::NaiveDate;

use crate::layout::{is_pad, unpad_left, Field, FieldType, Pad, Version};
use crate::value::{parse_u64, time_of_day_to_jiffies};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    fn matches(self, ordering: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::*;
        match self {
            CmpOp::Eq => ordering == Equal,
            CmpOp::Ne => ordering != Equal,
            CmpOp::Lt => ordering == Less,
            CmpOp::Le => ordering != Greater,
            CmpOp::Gt => ordering == Greater,
            CmpOp::Ge => ordering != Less,
        }
    }
}

impl fmt::Display for CmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CmpOp::Eq => "==",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        })
    }
}

/// A set of byte-string needles, indexed for rejection rather than scanned.
///
/// The obvious implementation is a linear scan, and for a handful of needles it is the right
/// one: a few short `memcmp`s beat hashing. That was the original choice here, with a comment
/// asserting needle counts are small. Filtering a session to an index universe violates the
/// assumption badly - 1,129 symbols against 704 million records is up to 800 billion slice
/// comparisons, and because most records miss, nearly every one pays the full scan.
///
/// Needles are bucketed by their first byte and length instead. Both are available before any
/// comparison, the pair is close to unique across an equity universe, and a miss usually
/// lands in an empty bucket and returns without a single `memcmp`. There is no hashing on the
/// hot path and no allocation.
///
/// Below `LINEAR_MAX` the flat scan is kept, because for a two-element series filter the
/// indexing costs more than it saves.
#[derive(Debug, Clone, PartialEq)]
pub struct TextSet {
    /// Flat scan, used when the set is tiny.
    small: Vec<Vec<u8>>,
    /// Buckets indexed by `first_byte * stride + len`, empty when `small` is in use.
    buckets: Vec<Vec<Vec<u8>>>,
    stride: usize,
    /// Needles of zero length, which have no first byte to index on.
    has_empty: bool,
}

/// Sets no larger than this keep the flat scan.
const LINEAR_MAX: usize = 8;

impl TextSet {
    pub fn new(needles: Vec<Vec<u8>>) -> Self {
        let has_empty = needles.iter().any(|n| n.is_empty());
        if needles.len() <= LINEAR_MAX {
            return Self { small: needles, buckets: Vec::new(), stride: 0, has_empty };
        }
        let max_len = needles.iter().map(|n| n.len()).max().unwrap_or(0);
        let stride = max_len + 1;
        let mut buckets = vec![Vec::new(); 256 * stride];
        for n in &needles {
            if n.is_empty() {
                continue;
            }
            buckets[n[0] as usize * stride + n.len()].push(n.clone());
        }
        Self { small: Vec::new(), buckets, stride, has_empty }
    }

    pub fn len(&self) -> usize {
        if self.buckets.is_empty() {
            self.small.len()
        } else {
            self.buckets.iter().map(|b| b.len()).sum::<usize>() + usize::from(self.has_empty)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every needle, for diagnostics and round-tripping a filter back to text.
    pub fn needles(&self) -> Vec<&[u8]> {
        if self.buckets.is_empty() {
            self.small.iter().map(|n| n.as_slice()).collect()
        } else {
            self.buckets.iter().flatten().map(|n| n.as_slice()).collect()
        }
    }

    #[inline]
    pub fn contains(&self, value: &[u8]) -> bool {
        if self.buckets.is_empty() {
            return self.small.iter().any(|n| n.as_slice() == value);
        }
        if value.is_empty() {
            return self.has_empty;
        }
        if value.len() >= self.stride {
            // Longer than any needle, so it cannot match.
            return false;
        }
        let bucket = &self.buckets[value[0] as usize * self.stride + value.len()];
        // The common case for a filtered session: nothing shares this first byte and length,
        // so the record is rejected without comparing any bytes.
        !bucket.is_empty() && bucket.iter().any(|n| n.as_slice() == value)
    }
}

/// A compiled predicate. Every variant carries resolved byte offsets.
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Matches everything. The no-filter case, so the hot loop has no `Option` branch.
    True,
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),

    /// Set membership on a text field, compared after removing declared padding.
    ///
    /// Needles are stored unpadded and compared against the unpadded slice, so a caller
    /// writes `RELIANCE` and never has to know the field is 10 bytes wide or that NSE pads
    /// it with the letter `b`.
    TextIn {
        offset: usize,
        len: usize,
        pad: Option<Pad>,
        set: TextSet,
        negated: bool,
    },

    /// Comparison against a zero-padded integer field, parsed in place.
    NumCmp {
        offset: usize,
        len: usize,
        op: CmpOp,
        value: u64,
        /// A field that is entirely padding decodes to null, which no comparison matches.
        field: String,
    },

    /// A single-byte Y/N flag.
    FlagIs { offset: usize, want: bool },

    /// Comparison between two numeric fields of the same record.
    ///
    /// This is what lets a derived condition be pushed down rather than computed after
    /// decoding: `volume_original > volume_disclosed` identifies iceberg orders without ever
    /// building a column for the rows that fail it.
    FieldCmp {
        left_offset: usize,
        left_len: usize,
        right_offset: usize,
        right_len: usize,
        op: CmpOp,
    },
}

impl Predicate {
    /// Evaluate against one record, excluding its delimiter.
    #[inline]
    pub fn eval(&self, record: &[u8]) -> bool {
        match self {
            Predicate::True => true,
            Predicate::And(ps) => ps.iter().all(|p| p.eval(record)),
            Predicate::Or(ps) => ps.iter().any(|p| p.eval(record)),
            Predicate::Not(p) => !p.eval(record),

            Predicate::TextIn {
                offset,
                len,
                pad,
                set,
                negated,
            } => {
                let raw = &record[*offset..*offset + *len];
                let value = match pad {
                    None => raw,
                    Some(Pad::Left) => unpad_left(raw),
                    Some(Pad::Right) => {
                        let i = raw.iter().rposition(|b| !is_pad(*b)).map_or(0, |i| i + 1);
                        &raw[..i]
                    }
                    Some(Pad::Both) => {
                        let s = raw.iter().position(|b| !is_pad(*b)).unwrap_or(raw.len());
                        let e = raw.iter().rposition(|b| !is_pad(*b)).map_or(s, |i| i + 1);
                        &raw[s..e]
                    }
                };
                set.contains(value) != *negated
            }

            Predicate::NumCmp {
                offset,
                len,
                op,
                value,
                ..
            } => {
                let raw = &record[*offset..*offset + *len];
                match parse_u64(raw) {
                    // Null compares false against everything, as in SQL.
                    None => false,
                    Some(n) => op.matches(n.cmp(value)),
                }
            }

            Predicate::FlagIs { offset, want } => {
                let b = record[*offset];
                (b == b'Y') == *want && (b == b'Y' || b == b'N')
            }

            Predicate::FieldCmp {
                left_offset,
                left_len,
                right_offset,
                right_len,
                op,
            } => {
                let l = parse_u64(&record[*left_offset..*left_offset + *left_len]);
                let r = parse_u64(&record[*right_offset..*right_offset + *right_len]);
                match (l, r) {
                    // A null on either side compares false, as in SQL.
                    (Some(a), Some(b)) => op.matches(a.cmp(&b)),
                    _ => false,
                }
            }
        }
    }

    pub fn is_trivially_true(&self) -> bool {
        matches!(self, Predicate::True)
    }
}

// ---------------------------------------------------------------------------
// Expression language
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(u64),
    Op(CmpOp),
    LParen,
    RParen,
    Comma,
    And,
    Or,
    Not,
    In,
    True,
    False,
}

fn tokenize(src: &str) -> Result<Vec<Tok>> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;

    while i < b.len() {
        let c = b[i];
        match c {
            _ if c.is_ascii_whitespace() => i += 1,
            b'(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            b')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            b',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            b'\'' | b'"' => {
                let quote = c;
                let start = i + 1;
                let mut j = start;
                while j < b.len() && b[j] != quote {
                    j += 1;
                }
                if j >= b.len() {
                    bail!("unterminated string literal starting at byte {i}");
                }
                out.push(Tok::Str(src[start..j].to_string()));
                i = j + 1;
            }
            b'=' | b'!' | b'<' | b'>' => {
                let two = if i + 1 < b.len() { &src[i..i + 2] } else { "" };
                let (op, width) = match two {
                    "==" => (CmpOp::Eq, 2),
                    "!=" => (CmpOp::Ne, 2),
                    "<=" => (CmpOp::Le, 2),
                    ">=" => (CmpOp::Ge, 2),
                    _ => match c {
                        b'=' => (CmpOp::Eq, 1),
                        b'<' => (CmpOp::Lt, 1),
                        b'>' => (CmpOp::Gt, 1),
                        _ => bail!("expected != at byte {i}"),
                    },
                };
                out.push(Tok::Op(op));
                i += width;
            }
            _ if c.is_ascii_digit() => {
                let start = i;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                // A bare 09:15:00 style time is only valid as a quoted string; digits
                // followed by ':' is a common mistake worth naming precisely.
                if i < b.len() && b[i] == b':' {
                    bail!(
                        "time literals must be quoted, e.g. txn_time >= '09:15:00' \
                         (at byte {start})"
                    );
                }
                out.push(Tok::Num(src[start..i].parse().with_context(|| {
                    format!("integer literal at byte {start} does not fit in u64")
                })?));
            }
            _ if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                let word = &src[start..i];
                out.push(match word.to_ascii_lowercase().as_str() {
                    "and" => Tok::And,
                    "or" => Tok::Or,
                    "not" => Tok::Not,
                    "in" => Tok::In,
                    "true" => Tok::True,
                    "false" => Tok::False,
                    _ => Tok::Ident(word.to_string()),
                });
            }
            _ => bail!("unexpected character {:?} at byte {i}", c as char),
        }
    }
    Ok(out)
}

struct Parser<'a> {
    toks: Vec<Tok>,
    pos: usize,
    version: &'a Version,
    session_date: Option<NaiveDate>,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn eat(&mut self, want: &Tok) -> bool {
        if self.peek() == Some(want) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expr(&mut self) -> Result<Predicate> {
        let mut parts = vec![self.and_expr()?];
        while self.eat(&Tok::Or) {
            parts.push(self.and_expr()?);
        }
        Ok(if parts.len() == 1 {
            parts.pop().unwrap()
        } else {
            Predicate::Or(parts)
        })
    }

    fn and_expr(&mut self) -> Result<Predicate> {
        let mut parts = vec![self.unary()?];
        while self.eat(&Tok::And) {
            parts.push(self.unary()?);
        }
        Ok(if parts.len() == 1 {
            parts.pop().unwrap()
        } else {
            Predicate::And(parts)
        })
    }

    fn unary(&mut self) -> Result<Predicate> {
        if self.eat(&Tok::Not) {
            return Ok(Predicate::Not(Box::new(self.unary()?)));
        }
        if self.eat(&Tok::LParen) {
            let inner = self.expr()?;
            if !self.eat(&Tok::RParen) {
                bail!("expected ')'");
            }
            return Ok(inner);
        }
        self.comparison()
    }

    fn field(&self, name: &str) -> Result<&'a Field> {
        self.version.field(name).ok_or_else(|| {
            let mut names: Vec<&str> = self.version.fields.iter().map(|f| f.name.as_str()).collect();
            names.sort_unstable();
            anyhow!("unknown field {name:?}; this layout has: {}", names.join(", "))
        })
    }

    fn comparison(&mut self) -> Result<Predicate> {
        let name = match self.next() {
            Some(Tok::Ident(n)) => n,
            other => bail!("expected a field name, found {other:?}"),
        };
        let field = self.field(&name)?;

        // IN / NOT IN
        let negated_in = if self.eat(&Tok::Not) {
            if !self.eat(&Tok::In) {
                bail!("expected 'in' after 'not' in a membership test on {name}");
            }
            true
        } else if self.eat(&Tok::In) {
            false
        } else {
            return self.binary_comparison(field);
        };

        if !self.eat(&Tok::LParen) {
            bail!("expected '(' after 'in'");
        }
        let mut needles: Vec<Vec<u8>> = Vec::new();
        loop {
            match self.next() {
                Some(Tok::Str(s)) => needles.push(s.into_bytes()),
                Some(Tok::Num(n)) => needles.push(n.to_string().into_bytes()),
                other => bail!("expected a literal inside 'in (...)', found {other:?}"),
            }
            if self.eat(&Tok::Comma) {
                continue;
            }
            if self.eat(&Tok::RParen) {
                break;
            }
            bail!("expected ',' or ')' in 'in (...)'");
        }
        self.text_in(field, needles, negated_in)
    }

    fn text_in(&self, field: &Field, needles: Vec<Vec<u8>>, negated: bool) -> Result<Predicate> {
        for n in &needles {
            if n.len() > field.len {
                bail!(
                    "{:?} is {} bytes but {} is only {} bytes wide",
                    String::from_utf8_lossy(n),
                    n.len(),
                    field.name,
                    field.len
                );
            }
        }
        Ok(Predicate::TextIn {
            offset: field.offset,
            len: field.len,
            pad: field.pad,
            set: TextSet::new(needles),
            negated,
        })
    }

    /// Compare two numeric fields of the same record.
    fn field_comparison(&self, left: &Field, right: &Field, op: CmpOp) -> Result<Predicate> {
        let numeric = |f: &Field| {
            matches!(
                f.ty,
                FieldType::U8 | FieldType::U64 | FieldType::Price | FieldType::Jiffies
            )
        };
        for f in [left, right] {
            if !numeric(f) {
                bail!(
                    "field-to-field comparison needs numeric fields, but {} is {:?}",
                    f.name,
                    f.ty
                );
            }
        }
        // Comparing paise against a share count, or two different price scales, is almost
        // certainly a mistake rather than an intention.
        if left.scale != right.scale {
            bail!(
                "cannot compare {} and {}: they have different scales ({:?} and {:?}), so the comparison would be between different units",
                left.name,
                right.name,
                left.scale,
                right.scale
            );
        }
        Ok(Predicate::FieldCmp {
            left_offset: left.offset,
            left_len: left.len,
            right_offset: right.offset,
            right_len: right.len,
            op,
        })
    }

    fn binary_comparison(&mut self, field: &Field) -> Result<Predicate> {
        let op = match self.next() {
            Some(Tok::Op(o)) => o,
            other => bail!("expected a comparison operator after {}, found {other:?}", field.name),
        };

        // A bare identifier on the right means a field-to-field comparison.
        if let Some(Tok::Ident(name)) = self.peek().cloned() {
            self.pos += 1;
            let right = self.field(&name)?;
            return self.field_comparison(field, right, op);
        }

        match self.next() {
            Some(Tok::Str(s)) => match field.ty {
                FieldType::Jiffies => {
                    let date = self.session_date.ok_or_else(|| {
                        anyhow!(
                            "comparing {} against a time literal needs the session date; \
                             pass it when compiling the filter",
                            field.name
                        )
                    })?;
                    let j = time_of_day_to_jiffies(&s, date).with_context(|| {
                        format!("parsing time literal {s:?} for field {}", field.name)
                    })?;
                    Ok(Predicate::NumCmp {
                        offset: field.offset,
                        len: field.len,
                        op,
                        value: j,
                        field: field.name.clone(),
                    })
                }
                _ if op == CmpOp::Eq || op == CmpOp::Ne => {
                    self.text_in(field, vec![s.into_bytes()], op == CmpOp::Ne)
                }
                _ => bail!(
                    "operator {op} is not supported on text field {}; use == , != or in (...)",
                    field.name
                ),
            },

            Some(Tok::Num(n)) => match field.ty {
                FieldType::U8 | FieldType::U64 | FieldType::Price | FieldType::Jiffies => {
                    Ok(Predicate::NumCmp {
                        offset: field.offset,
                        len: field.len,
                        op,
                        value: n,
                        field: field.name.clone(),
                    })
                }
                _ if op == CmpOp::Eq || op == CmpOp::Ne => {
                    self.text_in(field, vec![n.to_string().into_bytes()], op == CmpOp::Ne)
                }
                _ => bail!("operator {op} is not supported on field {}", field.name),
            },

            Some(t @ (Tok::True | Tok::False)) => {
                if field.ty != FieldType::BoolYn {
                    bail!("{} is not a Y/N flag, so it cannot be compared to a boolean", field.name);
                }
                if !matches!(op, CmpOp::Eq | CmpOp::Ne) {
                    bail!("only == and != are supported on flag field {}", field.name);
                }
                let want = (t == Tok::True) == (op == CmpOp::Eq);
                Ok(Predicate::FlagIs {
                    offset: field.offset,
                    want,
                })
            }

            other => bail!("expected a literal after {op}, found {other:?}"),
        }
    }
}

/// Compile a filter expression against a layout version.
///
/// Grammar:
/// ```text
///   expr       := or_expr
///   or_expr    := and_expr ('or' and_expr)*
///   and_expr   := unary ('and' unary)*
///   unary      := 'not' unary | '(' expr ')' | comparison
///   comparison := field op literal
///               | field ['not'] 'in' '(' literal (',' literal)* ')'
///   op         := '==' | '=' | '!=' | '<' | '<=' | '>' | '>='
/// ```
///
/// Prices compare in their raw integer units, so `limit_price > 250000` means more than
/// 2500.00 rupees in CM. `session_date` is required only for time literals such as
/// `txn_time >= '09:15:00'`.
///
/// # Examples
///
/// ```text
///   series == 'EQ'
///   series == 'EQ' and symbol in ('RELIANCE', 'TCS', 'M&M')
///   activity_type == 1 and volume_original > volume_disclosed
///   not (mkt_order_flag == true) and txn_time >= '09:15:00' and txn_time < '15:30:00'
/// ```
///
/// Two numeric fields of the same record can be compared directly, which is how a derived
/// condition gets pushed down instead of being computed after decoding:
///
/// ```text
///   volume_original > volume_disclosed and volume_disclosed > 0    -- iceberg orders
/// ```
///
/// Both sides must be numeric and share a scale, so comparing a price against a share count
/// is rejected rather than silently comparing different units.
pub fn compile(expr: &str, version: &Version, session_date: Option<NaiveDate>) -> Result<Predicate> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Ok(Predicate::True);
    }
    let toks = tokenize(trimmed).with_context(|| format!("tokenising filter {trimmed:?}"))?;
    let mut p = Parser {
        toks,
        pos: 0,
        version,
        session_date,
    };
    let pred = p.expr().with_context(|| format!("parsing filter {trimmed:?}"))?;
    if p.pos != p.toks.len() {
        bail!(
            "trailing input in filter {trimmed:?} at token {}: {:?}",
            p.pos,
            &p.toks[p.pos..]
        );
    }
    Ok(pred)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::load;

    fn cm_orders() -> Version {
        load("cm_orders")
            .unwrap()
            .for_date(NaiveDate::from_ymd_opt(2022, 1, 27).unwrap())
            .unwrap()
            .clone()
    }

    /// A real pre-open record from CASH_Orders_27012022.DAT.gz, minus the LF.
    const BALKRISIND: &[u8] =
        b"POCASH100000000000208487014847291575S1BALKRISINDEQ00000000000000760023000000000000NNN13";
    /// A record whose symbol carries the literal 'b' padding NSE emits.
    const BCG: &[u8] =
        b"POCASH100000000000209487014847291579S1bbbbbbbBCGBE00000000000000750001650000000000NNN13";

    fn check(expr: &str, record: &[u8]) -> bool {
        let v = cm_orders();
        let d = NaiveDate::from_ymd_opt(2022, 1, 27).unwrap();
        compile(expr, &v, Some(d)).unwrap().eval(record)
    }

    #[test]
    fn records_are_the_length_the_layout_expects() {
        assert_eq!(BALKRISIND.len(), 87);
        assert_eq!(BCG.len(), 87);
    }

    #[test]
    fn text_equality_ignores_nse_b_padding() {
        assert!(check("symbol == 'BCG'", BCG));
        assert!(check("symbol == 'BALKRISIND'", BALKRISIND));
        assert!(!check("symbol == 'BCG'", BALKRISIND));
    }

    #[test]
    fn membership_and_negation() {
        assert!(check("symbol in ('RELIANCE', 'BCG', 'TCS')", BCG));
        assert!(!check("symbol in ('RELIANCE', 'TCS')", BCG));
        assert!(check("symbol not in ('RELIANCE', 'TCS')", BCG));
        assert!(check("series == 'EQ'", BALKRISIND));
        assert!(check("series != 'EQ'", BCG));
    }

    #[test]
    fn numeric_comparison_on_zero_padded_fields() {
        // volume_original is 76 for BALKRISIND, 75 for BCG.
        assert!(check("volume_original == 76", BALKRISIND));
        assert!(check("volume_original > 75", BALKRISIND));
        assert!(!check("volume_original > 76", BALKRISIND));
        assert!(check("volume_original >= 75 and volume_original <= 76", BCG));
        // limit_price is raw paise: 230000 = 2300.00 rupees.
        assert!(check("limit_price == 230000", BALKRISIND));
        assert!(check("limit_price > 100000", BALKRISIND));
        assert!(!check("limit_price > 100000", BCG));
    }

    #[test]
    fn flag_fields_compare_against_booleans() {
        assert!(check("mkt_order_flag == false", BALKRISIND));
        assert!(!check("mkt_order_flag == true", BALKRISIND));
        assert!(check("ioc_flag != true", BALKRISIND));
    }

    #[test]
    fn time_literals_resolve_through_the_session_date() {
        // The record is at 09:00:00.127 IST, in the pre-open window.
        assert!(check("txn_time >= '09:00:00'", BALKRISIND));
        assert!(check("txn_time < '09:15:00'", BALKRISIND));
        assert!(!check("txn_time >= '09:15:00'", BALKRISIND));
    }

    #[test]
    fn boolean_composition_and_precedence() {
        // 'and' binds tighter than 'or'.
        assert!(check("series == 'BE' or series == 'EQ' and volume_original == 76", BALKRISIND));
        assert!(check("not (series == 'BE')", BALKRISIND));
        assert!(check(
            "(symbol == 'BCG' or symbol == 'BALKRISIND') and activity_type == 1",
            BCG
        ));
    }

    #[test]
    fn two_numeric_fields_can_be_compared() {
        // The iceberg condition, pushed down instead of computed after decoding.
        // BALKRISIND: volume_disclosed 0, volume_original 76. BCG: 0 and 75.
        assert!(check("volume_original > volume_disclosed", BALKRISIND));
        assert!(!check("volume_disclosed > volume_original", BALKRISIND));
        assert!(check("volume_original >= volume_original", BALKRISIND));
        assert!(check("limit_price > trigger_price", BALKRISIND)); // 230000 vs 0, both paise
        // Neither sample is an iceberg: disclosed quantity is 0 for both.
        assert!(!check(
            "volume_original > volume_disclosed and volume_disclosed > 0",
            BALKRISIND
        ));
        assert!(!check(
            "volume_original > volume_disclosed and volume_disclosed > 0",
            BCG
        ));
    }

    #[test]
    fn comparing_fields_of_different_scales_is_refused() {
        let v = cm_orders();
        // limit_price is scale 2 (paise); volume_original is a share count.
        let err = format!(
            "{:#}",
            compile("limit_price > volume_original", &v, None).unwrap_err()
        );
        assert!(err.contains("different scales"), "{err}");
    }

    #[test]
    fn comparing_a_text_field_to_a_field_is_refused() {
        let v = cm_orders();
        let err = format!(
            "{:#}",
            compile("symbol > series", &v, None).unwrap_err()
        );
        assert!(err.contains("numeric"), "{err}");
    }

    #[test]
    fn an_unknown_field_on_the_right_is_caught() {
        let v = cm_orders();
        let err = format!(
            "{:#}",
            compile("volume_original > volume_disclsoed", &v, None).unwrap_err()
        );
        assert!(err.contains("unknown field"), "{err}");
    }

    #[test]
    fn empty_filter_is_trivially_true() {
        let v = cm_orders();
        assert!(compile("", &v, None).unwrap().is_trivially_true());
        assert!(compile("   ", &v, None).unwrap().is_trivially_true());
    }

    #[test]
    fn unknown_field_is_rejected_at_compile_time_with_suggestions() {
        let v = cm_orders();
        let err = format!("{:#}", compile("symobl == 'TCS'", &v, None).unwrap_err());
        // The whole point: fail loudly before reading 8 GB, not return an empty result.
        assert!(err.contains("unknown field"), "{err}");
    }

    #[test]
    fn needle_wider_than_the_field_is_rejected() {
        let v = cm_orders();
        let err = format!("{:#}", compile("series == 'TOOLONG'", &v, None).unwrap_err());
        assert!(err.contains("bytes wide"), "{err}");
    }

    #[test]
    fn unquoted_time_literal_gets_a_useful_message() {
        let v = cm_orders();
        let err = format!("{:#}", compile("txn_time >= 09:15:00", &v, None).unwrap_err());
        assert!(err.contains("must be quoted"), "{err}");
    }
}

#[cfg(test)]
mod textset_tests {
    use super::TextSet;

    fn set(items: &[&str]) -> TextSet {
        TextSet::new(items.iter().map(|s| s.as_bytes().to_vec()).collect())
    }

    #[test]
    fn small_sets_use_the_flat_scan_and_still_match() {
        let s = set(&["EQ", "BE"]);
        assert!(s.contains(b"EQ"));
        assert!(s.contains(b"BE"));
        assert!(!s.contains(b"SM"));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn large_sets_match_exactly_what_a_linear_scan_would() {
        // Enough needles to cross into the bucketed representation, with the first-byte and
        // length collisions real tickers have.
        let names: Vec<String> = (0..500)
            .map(|i| format!("SYM{i}"))
            .chain(["RELIANCE", "TCS", "M&M", "BAJAJ-AUTO"].iter().map(|s| s.to_string()))
            .collect();
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let s = set(&refs);

        for n in &names {
            assert!(s.contains(n.as_bytes()), "{n} should match");
        }
        for miss in ["SYM500", "RELIANC", "RELIANCEX", "", "M&", "ZZZZ"] {
            assert!(!s.contains(miss.as_bytes()), "{miss:?} should not match");
        }
        assert_eq!(s.len(), names.len());
    }

    #[test]
    fn a_value_longer_than_every_needle_is_rejected_without_scanning() {
        let names: Vec<String> = (0..100).map(|i| format!("A{i}")).collect();
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let s = set(&refs);
        assert!(!s.contains(b"AAAAAAAAAAAAAAAAAAAAAAAA"));
    }

    #[test]
    fn the_empty_needle_is_handled_in_both_representations() {
        assert!(set(&["", "EQ"]).contains(b""));
        let mut many: Vec<String> = (0..50).map(|i| format!("S{i}")).collect();
        many.push(String::new());
        let refs: Vec<&str> = many.iter().map(|s| s.as_str()).collect();
        let s = set(&refs);
        assert!(s.contains(b""));
        assert!(s.contains(b"S7"));
        assert!(!s.contains(b"S99"));
    }

    #[test]
    fn needles_round_trip_regardless_of_representation() {
        for n in [3usize, 40] {
            let names: Vec<String> = (0..n).map(|i| format!("T{i}")).collect();
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let s = set(&refs);
            let mut got: Vec<String> = s
                .needles()
                .iter()
                .map(|b| String::from_utf8(b.to_vec()).unwrap())
                .collect();
            got.sort();
            let mut want = names.clone();
            want.sort();
            assert_eq!(got, want);
        }
    }
}
