//! Wire-format decoders for PostgreSQL values sqlx cannot decode in this build.
//!
//! - `NUMERIC` / `DECIMAL`: no sqlx decimal codec is enabled, and sqlx's
//!   `String` and `f64` decoders both refuse the NUMERIC OID. Decoding the wire
//!   format keeps every digit and the declared scale (`12.50` stays `12.50`),
//!   so a money column never passes through a float and never reads back as
//!   NULL.
//! - Arrays of user enums: sqlx names them `_<type>` (no `[]` suffix) and its
//!   String decoder refuses them; they decode into their labels.

/// Renders one NUMERIC value from the PostgreSQL binary wire format:
/// `ndigits i16, weight i16, sign u16, dscale u16`, then `ndigits` base-10000
/// digit groups. NaN and the infinities (PostgreSQL 14+) render as `NaN`,
/// `Infinity` and `-Infinity`.
pub(crate) fn pg_numeric_binary_to_string(buf: &[u8]) -> Result<String, String> {
    if buf.len() < 8 {
        return Err(format!("NUMERIC header is {} bytes, expected 8", buf.len()));
    }
    let read_i16 = |at: usize| i16::from_be_bytes([buf[at], buf[at + 1]]);
    let ndigits = read_i16(0);
    let weight = i32::from(read_i16(2));
    let sign = u16::from_be_bytes([buf[4], buf[5]]);
    let dscale = usize::from(u16::from_be_bytes([buf[6], buf[7]]));
    match sign {
        0xC000 => return Ok("NaN".to_string()),
        0xD000 => return Ok("Infinity".to_string()),
        0xF000 => return Ok("-Infinity".to_string()),
        0x0000 | 0x4000 => {}
        other => return Err(format!("NUMERIC sign {other:#06x} is not valid")),
    }
    if ndigits < 0 {
        return Err(format!("NUMERIC digit count {ndigits} is negative"));
    }
    let ndigits = ndigits as usize;
    if buf.len() != 8 + ndigits * 2 {
        return Err(format!(
            "NUMERIC body is {} bytes, expected {} for {ndigits} digit groups",
            buf.len() - 8,
            ndigits * 2
        ));
    }
    let digits: Vec<i16> = (0..ndigits).map(|i| read_i16(8 + i * 2)).collect();
    if let Some(bad) = digits.iter().find(|d| !(0..10_000).contains(*d)) {
        return Err(format!("NUMERIC digit group {bad} is out of range"));
    }
    // Digit group k carries weight (weight - k); a group outside the stored
    // range is zero.
    let group = |k: i32| -> i16 {
        if k < 0 {
            0
        } else {
            digits.get(k as usize).copied().unwrap_or(0)
        }
    };

    let mut out = String::new();
    if weight < 0 {
        out.push('0');
    } else {
        out.push_str(&group(0).to_string());
        for k in 1..=weight {
            out.push_str(&format!("{:04}", group(k)));
        }
    }
    if dscale > 0 {
        let mut fraction = String::with_capacity(dscale + 4);
        let mut k = weight + 1;
        while fraction.len() < dscale {
            fraction.push_str(&format!("{:04}", group(k)));
            k += 1;
        }
        fraction.truncate(dscale);
        out.push('.');
        out.push_str(&fraction);
    }
    if sign == 0x4000 && out.bytes().any(|b| (b'1'..=b'9').contains(&b)) {
        out.insert(0, '-');
    }
    Ok(out)
}

/// Renders a one-dimensional NUMERIC[] from the PostgreSQL binary array wire
/// format into its elements (`None` for a NULL element). An empty array has
/// zero dimensions.
pub(crate) fn pg_numeric_array_binary_to_strings(
    buf: &[u8],
) -> Result<Vec<Option<String>>, String> {
    let read_i32 = |at: usize| -> Result<i32, String> {
        buf.get(at..at + 4)
            .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
            .ok_or_else(|| "NUMERIC[] is truncated".to_string())
    };
    let ndim = read_i32(0)?;
    if ndim == 0 {
        return Ok(Vec::new());
    }
    if ndim != 1 {
        return Err(format!(
            "NUMERIC[] with {ndim} dimensions is not supported; only one-dimensional arrays are"
        ));
    }
    // ndim, has-null flag, element OID, then (length, lower bound) per dimension.
    let len = read_i32(12)?;
    if len < 0 {
        return Err(format!("NUMERIC[] length {len} is negative"));
    }
    let mut at = 20usize;
    let mut out = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let elem_len = read_i32(at)?;
        at += 4;
        if elem_len < 0 {
            out.push(None);
            continue;
        }
        let end = at + elem_len as usize;
        let elem = buf
            .get(at..end)
            .ok_or_else(|| "NUMERIC[] element is truncated".to_string())?;
        out.push(Some(pg_numeric_binary_to_string(elem)?));
        at = end;
    }
    Ok(out)
}

/// Splits the text form of a one-dimensional NUMERIC[] (`{1.50,NULL,-2}`).
/// NUMERIC text never contains quotes, commas or braces.
pub(crate) fn pg_numeric_array_text_to_strings(text: &str) -> Result<Vec<Option<String>>, String> {
    let inner = text
        .trim()
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(|| format!("NUMERIC[] text {text:?} is not an array literal"))?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    if inner.contains('{') {
        return Err(
            "NUMERIC[] with more than one dimension is not supported; only one-dimensional arrays are"
                .to_string(),
        );
    }
    Ok(inner
        .split(',')
        .map(|item| {
            let item = item.trim();
            (!item.eq_ignore_ascii_case("NULL")).then(|| item.to_string())
        })
        .collect())
}

/// Renders a one-dimensional array whose elements travel as text in the binary
/// format (a user enum array, which sqlx names `_<type>`) into its labels.
pub(crate) fn pg_text_element_array_binary_to_strings(
    buf: &[u8],
) -> Result<Vec<Option<String>>, String> {
    let read_i32 = |at: usize| -> Result<i32, String> {
        buf.get(at..at + 4)
            .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
            .ok_or_else(|| "array value is truncated".to_string())
    };
    let ndim = read_i32(0)?;
    if ndim == 0 {
        return Ok(Vec::new());
    }
    if ndim != 1 {
        return Err(format!(
            "an array with {ndim} dimensions is not supported; only one-dimensional arrays are"
        ));
    }
    let len = read_i32(12)?;
    if len < 0 {
        return Err(format!("array length {len} is negative"));
    }
    let mut at = 20usize;
    let mut out = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let elem_len = read_i32(at)?;
        at += 4;
        if elem_len < 0 {
            out.push(None);
            continue;
        }
        let end = at + elem_len as usize;
        let elem = buf
            .get(at..end)
            .ok_or_else(|| "array element is truncated".to_string())?;
        out.push(Some(
            std::str::from_utf8(elem)
                .map_err(|err| format!("array element is not UTF-8: {err}"))?
                .to_string(),
        ));
        at = end;
    }
    Ok(out)
}

/// Splits the text form of a one-dimensional PostgreSQL array literal such as
/// `{happy,"very sad",NULL}`. A quoted `"NULL"` is the string NULL; an unquoted
/// `NULL` is a NULL element; `\` escapes the next character inside quotes.
pub(crate) fn pg_text_array_elements(text: &str) -> Result<Vec<Option<String>>, String> {
    let inner = text
        .trim()
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(|| format!("{text:?} is not an array literal"))?;
    let mut out = Vec::new();
    if inner.trim().is_empty() {
        return Ok(out);
    }
    let mut chars = inner.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        if chars.peek() == Some(&'"') {
            chars.next();
            let mut value = String::new();
            loop {
                match chars.next() {
                    Some('\\') => value.push(
                        chars
                            .next()
                            .ok_or_else(|| format!("{text:?} ends inside an escape"))?,
                    ),
                    Some('"') => break,
                    Some(c) => value.push(c),
                    None => return Err(format!("{text:?} has an unterminated quote")),
                }
            }
            out.push(Some(value));
        } else {
            let mut value = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                if c == '{' || c == '}' {
                    return Err("an array with more than one dimension is not supported; \
                                only one-dimensional arrays are"
                        .to_string());
                }
                value.push(c);
                chars.next();
            }
            let value = value.trim().to_string();
            out.push((!value.eq_ignore_ascii_case("NULL")).then_some(value));
        }
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        match chars.next() {
            Some(',') => continue,
            None => break,
            Some(c) => return Err(format!("{text:?} has {c:?} after an element")),
        }
    }
    Ok(out)
}

/// An exact decimal bound as a PostgreSQL `NUMERIC` parameter.
///
/// The value travels in NUMERIC's own binary format, typed NUMERIC, so it is
/// exact in every context (an INSERT value, `amount = $1`, `= ANY($1)`) with no
/// cast in the SQL and no float in between. Build it with [`PgDecimal::parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgDecimal {
    sign: u16,
    weight: i16,
    dscale: u16,
    digits: Vec<i16>,
}

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;

impl PgDecimal {
    /// Parses a decimal in plain or exponent notation (`12.50`, `-0.5`, `.5`,
    /// `1e3`, `1.5E-2`) or `NaN` / `Infinity` / `-Infinity`. The scale of the
    /// text is kept (`12.50` has scale 2).
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let raw = text.trim();
        let special = |sign| {
            Ok(PgDecimal {
                sign,
                weight: 0,
                dscale: 0,
                digits: Vec::new(),
            })
        };
        match raw.to_ascii_lowercase().as_str() {
            "nan" => return special(NUMERIC_NAN),
            "infinity" | "+infinity" | "inf" | "+inf" => return special(NUMERIC_PINF),
            "-infinity" | "-inf" => return special(NUMERIC_NINF),
            _ => {}
        }
        let invalid = || format!("{text:?} is not a decimal number");
        let (negative, unsigned) = match raw.as_bytes().first() {
            Some(b'-') => (true, &raw[1..]),
            Some(b'+') => (false, &raw[1..]),
            _ => (false, raw),
        };
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(at) => {
                let exp: i64 = unsigned[at + 1..].parse().map_err(|_| invalid())?;
                (&unsigned[..at], exp)
            }
            None => (unsigned, 0),
        };
        let (int_part, frac_part) = match mantissa.split_once('.') {
            Some((int_part, frac_part)) => (int_part, frac_part),
            None => (mantissa, ""),
        };
        if (int_part.is_empty() && frac_part.is_empty())
            || !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(invalid());
        }
        // Move the decimal point by the exponent over the digit string.
        let all_digits: String = format!("{int_part}{frac_part}");
        let point = int_part.len() as i64 + exponent;
        if !(-20_000..=20_000).contains(&point) || all_digits.len() > 20_000 {
            return Err(format!("{text:?} is outside the NUMERIC range"));
        }
        let (int_digits, frac_digits) = if point <= 0 {
            (
                String::new(),
                format!("{}{all_digits}", "0".repeat((-point) as usize)),
            )
        } else if point as usize >= all_digits.len() {
            (
                format!(
                    "{all_digits}{}",
                    "0".repeat(point as usize - all_digits.len())
                ),
                String::new(),
            )
        } else {
            (
                all_digits[..point as usize].to_string(),
                all_digits[point as usize..].to_string(),
            )
        };
        let dscale = u16::try_from(frac_digits.len())
            .ok()
            .filter(|scale| *scale <= 0x3FFF)
            .ok_or_else(|| format!("{text:?} has more fraction digits than NUMERIC allows"))?;
        let int_digits = int_digits.trim_start_matches('0');

        // Base-10000 groups: the integer part grouped from the right, the
        // fraction grouped from the left.
        let mut groups: Vec<i16> = Vec::new();
        let int_pad = (4 - int_digits.len() % 4) % 4;
        let padded_int = format!("{}{int_digits}", "0".repeat(int_pad));
        for chunk in padded_int.as_bytes().chunks(4) {
            groups.push(std::str::from_utf8(chunk).unwrap().parse().unwrap());
        }
        let int_groups = groups.len() as i64;
        let frac_pad = (4 - frac_digits.len() % 4) % 4;
        let padded_frac = format!("{frac_digits}{}", "0".repeat(frac_pad));
        for chunk in padded_frac.as_bytes().chunks(4) {
            groups.push(std::str::from_utf8(chunk).unwrap().parse().unwrap());
        }
        let mut weight = int_groups - 1;
        let leading_zeros = groups.iter().take_while(|g| **g == 0).count();
        groups.drain(..leading_zeros);
        weight -= leading_zeros as i64;
        while groups.last() == Some(&0) {
            groups.pop();
        }
        if groups.is_empty() {
            return Ok(PgDecimal {
                sign: NUMERIC_POS,
                weight: 0,
                dscale,
                digits: Vec::new(),
            });
        }
        let weight =
            i16::try_from(weight).map_err(|_| format!("{text:?} is outside the NUMERIC range"))?;
        if groups.len() > i16::MAX as usize {
            return Err(format!("{text:?} is outside the NUMERIC range"));
        }
        Ok(PgDecimal {
            sign: if negative { NUMERIC_NEG } else { NUMERIC_POS },
            weight,
            dscale,
            digits: groups,
        })
    }

    /// Numeric equality, ignoring scale: `12.5` equals `12.50`. NaN equals NaN
    /// (PostgreSQL's own NUMERIC ordering treats NaN as equal to itself).
    pub(crate) fn same_value(&self, other: &Self) -> bool {
        self.sign == other.sign && self.weight == other.weight && self.digits == other.digits
    }

    /// The NUMERIC binary wire form of this value.
    pub(crate) fn to_wire(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.digits.len() * 2);
        buf.extend_from_slice(&(self.digits.len() as i16).to_be_bytes());
        buf.extend_from_slice(&self.weight.to_be_bytes());
        buf.extend_from_slice(&self.sign.to_be_bytes());
        buf.extend_from_slice(&self.dscale.to_be_bytes());
        for digit in &self.digits {
            buf.extend_from_slice(&digit.to_be_bytes());
        }
        buf
    }

    /// Reads a JSON number or numeric string as a decimal. A JSON number keeps
    /// the digits serde_json parsed it with.
    pub(crate) fn from_json(value: &serde_json::Value) -> Result<Self, String> {
        match value {
            serde_json::Value::Number(number) => Self::parse(&number.to_string()),
            serde_json::Value::String(text) => Self::parse(text),
            other => Err(format!("{other} is not a number or numeric string")),
        }
    }
}

const NUMERIC_OID: u32 = 1700;
const NUMERIC_ARRAY_OID: u32 = 1231;

impl sqlx::Type<sqlx::Postgres> for PgDecimal {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_oid(sqlx::postgres::types::Oid(NUMERIC_OID))
    }
}

impl sqlx::postgres::PgHasArrayType for PgDecimal {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_oid(sqlx::postgres::types::Oid(NUMERIC_ARRAY_OID))
    }
}

impl sqlx::Encode<'_, sqlx::Postgres> for PgDecimal {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        buf.extend_from_slice(&self.to_wire());
        Ok(sqlx::encode::IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(ndigits: &[i16], weight: i16, sign: u16, dscale: u16) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(ndigits.len() as i16).to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&dscale.to_be_bytes());
        for d in ndigits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        buf
    }

    #[test]
    fn renders_scale_sign_and_groups_exactly() {
        // 12.50 = groups [12, 5000], weight 0, dscale 2
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[12, 5000], 0, 0, 2)).unwrap(),
            "12.50"
        );
        // 0 with scale 2
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[], 0, 0, 2)).unwrap(),
            "0.00"
        );
        // -1000000000000000.01 = [1000, 0, 0, 0, 100], weight 3, dscale 2
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[1000, 0, 0, 0, 100], 3, 0x4000, 2)).unwrap(),
            "-1000000000000000.01"
        );
        // 0.0005 = [5], weight -1, dscale 4
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[5], -1, 0, 4)).unwrap(),
            "0.0005"
        );
        // 0.00000123 = [123... ] -> 0.0000 0123 -> groups: weight -2, digit 123 (0.0000|0123)
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[123], -2, 0, 8)).unwrap(),
            "0.00000123"
        );
        // 10000 = [1], weight 1, dscale 0
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[1], 1, 0, 0)).unwrap(),
            "10000"
        );
        // negative zero never renders a sign
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[], 0, 0x4000, 1)).unwrap(),
            "0.0"
        );
    }

    #[test]
    fn renders_special_values() {
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[], 0, 0xC000, 0)).unwrap(),
            "NaN"
        );
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[], 0, 0xD000, 0)).unwrap(),
            "Infinity"
        );
        assert_eq!(
            pg_numeric_binary_to_string(&encode(&[], 0, 0xF000, 0)).unwrap(),
            "-Infinity"
        );
    }

    #[test]
    fn refuses_malformed_input() {
        assert!(pg_numeric_binary_to_string(&[0, 1]).is_err());
        assert!(pg_numeric_binary_to_string(&encode(&[10_000], 0, 0, 0)).is_err());
        let mut short = encode(&[1, 2], 1, 0, 0);
        short.pop();
        assert!(pg_numeric_binary_to_string(&short).is_err());
    }

    #[test]
    fn decodes_binary_arrays_with_nulls() {
        let one = encode(&[1, 5000], 0, 0, 2); // 1.50
        let mut buf = Vec::new();
        buf.extend_from_slice(&1i32.to_be_bytes()); // ndim
        buf.extend_from_slice(&1i32.to_be_bytes()); // has nulls
        buf.extend_from_slice(&1700u32.to_be_bytes()); // NUMERIC OID
        buf.extend_from_slice(&2i32.to_be_bytes()); // length
        buf.extend_from_slice(&1i32.to_be_bytes()); // lower bound
        buf.extend_from_slice(&(one.len() as i32).to_be_bytes());
        buf.extend_from_slice(&one);
        buf.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(
            pg_numeric_array_binary_to_strings(&buf).unwrap(),
            vec![Some("1.50".to_string()), None]
        );
        let mut empty = Vec::new();
        empty.extend_from_slice(&0i32.to_be_bytes());
        empty.extend_from_slice(&0i32.to_be_bytes());
        empty.extend_from_slice(&1700u32.to_be_bytes());
        assert!(
            pg_numeric_array_binary_to_strings(&empty)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn decodes_text_arrays() {
        assert_eq!(
            pg_numeric_array_text_to_strings("{1.50,NULL,-2}").unwrap(),
            vec![Some("1.50".to_string()), None, Some("-2".to_string())]
        );
        assert!(pg_numeric_array_text_to_strings("{}").unwrap().is_empty());
        assert!(pg_numeric_array_text_to_strings("{{1},{2}}").is_err());
    }

    #[test]
    fn decimal_encoding_round_trips_through_the_decoder() {
        for (input, rendered) in [
            ("12.50", "12.50"),
            ("0", "0"),
            ("0.00", "0.00"),
            ("-3.25", "-3.25"),
            ("+7", "7"),
            (".5", "0.5"),
            ("1e3", "1000"),
            ("1.5E-2", "0.015"),
            ("-0.0000000001", "-0.0000000001"),
            ("10000", "10000"),
            ("0.0005", "0.0005"),
            (
                "123456789012345678901234567890.0123456789",
                "123456789012345678901234567890.0123456789",
            ),
            ("-1000000000000000.01", "-1000000000000000.01"),
            ("NaN", "NaN"),
            ("Infinity", "Infinity"),
            ("-Infinity", "-Infinity"),
        ] {
            let decimal = PgDecimal::parse(input).unwrap_or_else(|err| panic!("{input}: {err}"));
            assert_eq!(
                pg_numeric_binary_to_string(&decimal.to_wire()).unwrap(),
                rendered,
                "{input}"
            );
        }
    }

    #[test]
    fn decimal_parse_refuses_non_numbers() {
        for bad in ["", "abc", "1.2.3", "1e", "--1", "1,5", "."] {
            assert!(PgDecimal::parse(bad).is_err(), "{bad:?} must be refused");
        }
        assert!(PgDecimal::from_json(&serde_json::json!(true)).is_err());
        assert_eq!(
            PgDecimal::from_json(&serde_json::json!(25)).unwrap(),
            PgDecimal::parse("25").unwrap()
        );
    }

    #[test]
    fn splits_quoted_text_array_literals() {
        assert_eq!(
            pg_text_array_elements(r#"{happy,"very sad","NULL",NULL,"say \"hi\"","a\\b"}"#)
                .unwrap(),
            vec![
                Some("happy".to_string()),
                Some("very sad".to_string()),
                Some("NULL".to_string()),
                None,
                Some(r#"say "hi""#.to_string()),
                Some(r"a\b".to_string()),
            ]
        );
        assert!(pg_text_array_elements("{}").unwrap().is_empty());
        assert!(pg_text_array_elements(r#"{"open}"#).is_err());
        assert!(pg_text_array_elements("{{a},{b}}").is_err());
    }

    #[test]
    fn decodes_binary_text_element_arrays() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1i32.to_be_bytes()); // ndim
        buf.extend_from_slice(&1i32.to_be_bytes()); // has nulls
        buf.extend_from_slice(&16_385u32.to_be_bytes()); // a user enum OID
        buf.extend_from_slice(&3i32.to_be_bytes()); // length
        buf.extend_from_slice(&1i32.to_be_bytes()); // lower bound
        for label in ["happy", "sad"] {
            buf.extend_from_slice(&(label.len() as i32).to_be_bytes());
            buf.extend_from_slice(label.as_bytes());
        }
        buf.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(
            pg_text_element_array_binary_to_strings(&buf).unwrap(),
            vec![Some("happy".to_string()), Some("sad".to_string()), None]
        );
    }
}
