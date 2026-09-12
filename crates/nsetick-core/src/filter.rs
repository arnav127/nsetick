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
        needles: Vec<Vec<u8>>,
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
                needles,
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
                // Needle counts are small (a handful of symbols, or a few series codes), so
                // a linear scan of short slices beats hashing.
                let hit = needles.iter().any(|n| n.as_slice() == value);
                hit != *negated
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
            needles,
            negated,
        })
    }

    fn binary_comparison(&mut self, field: &Field) -> Result<Predicate> {
        let op = match self.next() {
            Some(Tok::Op(o)) => o,
            other => bail!("expected a comparison operator after {}, found {other:?}", field.name),
        };

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
///   activity_type == 1 and volume_original > volume_disclosed  -- not supported: see below
///   not (mkt_order_flag == true) and txn_time >= '09:15:00' and txn_time < '15:30:00'
/// ```
///
/// Field-to-field comparison is deliberately absent; it would not be expressible as a
/// constant-folded byte test. Derive such columns after decoding instead.
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
