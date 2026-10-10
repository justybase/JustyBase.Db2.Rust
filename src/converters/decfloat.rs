use std::fmt::Write as _;

/// Decode IEEE 754 decimal floating point in the DPD format used by Db2.
pub fn decode_dfp(data: &[u8]) -> Result<String, String> {
    if data.len() != 8 && data.len() != 16 {
        return Err(format!(
            "invalid DFP byte length: {} (must be 8 or 16)",
            data.len()
        ));
    }
    let small = data.len() == 8;
    let bias: i32 = if small { 398 } else { 6176 };
    let groups: usize = if small { 5 } else { 11 };
    let exponent_bits: usize = if small { 8 } else { 12 };
    let coefficient_bits = groups * 10;
    let total_bits = data.len() * 8;
    let word = data
        .iter()
        .fold(0u128, |acc, byte| (acc << 8) | u128::from(*byte));
    let negative = ((word >> (total_bits - 1)) & 1) != 0;
    let combination = ((word >> (total_bits - 6)) & 0x1f) as u32;
    let exponent_mask = (1u128 << exponent_bits) - 1;
    let exponent_continuation = ((word >> coefficient_bits) & exponent_mask) as i32;
    let coefficient_mask = (1u128 << coefficient_bits) - 1;
    let coefficient = word & coefficient_mask;

    if combination >= 0x1e {
        if combination & 1 != 0 {
            return Ok("NaN".into());
        }
        return Ok(if negative { "-Infinity" } else { "Infinity" }.into());
    }

    let biased_exponent = if combination >= 0x18 {
        ((((combination >> 1) & 0x03) as i32) << exponent_bits) | exponent_continuation
    } else {
        (((combination >> 3) as i32) << exponent_bits) | exponent_continuation
    };
    let mut digits = [0u8; 34];
    let digit_count = 1 + groups * 3;
    digits[0] = if combination >= 0x18 {
        8 + (combination & 1) as u8
    } else {
        (combination & 0x07) as u8
    };
    let mut digit_offset = 1;
    for index in (0..groups).rev() {
        let encoded = ((coefficient >> (index * 10)) & 0x3ff) as u16;
        let decoded = decode_dpd(encoded);
        digits[digit_offset..digit_offset + decoded.len()].copy_from_slice(&decoded);
        digit_offset += decoded.len();
    }
    debug_assert_eq!(digit_offset, digit_count);
    Ok(format_dfp(
        negative,
        &digits[..digit_count],
        biased_exponent - bias,
    ))
}

/// Encode a decimal string as IEEE 754 DPD bytes for Db2 DECFLOAT(16) or DECFLOAT(34).
pub fn encode_dfp(value: &str, byte_length: usize) -> Result<Vec<u8>, String> {
    if byte_length != 8 && byte_length != 16 {
        return Err(format!(
            "invalid DFP byte length: {byte_length} (must be 8 or 16)"
        ));
    }
    let small = byte_length == 8;
    let bias = if small { 398 } else { 6176 };
    let groups = if small { 5 } else { 11 };
    let exponent_bits = if small { 8 } else { 12 };
    let max_digits = if small { 16 } else { 34 };
    let coefficient_bits = groups * 10;
    let total_bits = byte_length * 8;
    let mut text = value.trim();
    if text.is_empty() || text == "<nil>" {
        text = "0";
    }
    let mut negative = false;
    if let Some(rest) = text.strip_prefix('-') {
        negative = true;
        text = rest;
    } else if let Some(rest) = text.strip_prefix('+') {
        text = rest;
    }
    if text.eq_ignore_ascii_case("infinity") || text.eq_ignore_ascii_case("inf") {
        return Ok(encode_special(byte_length, 0x1e, negative));
    }
    if text.eq_ignore_ascii_case("nan") {
        return Ok(encode_special(byte_length, 0x1f, false));
    }
    if text.is_empty() {
        return Err(format!("invalid decimal floating-point value '{value}'"));
    }

    let (mantissa, explicit_exponent) = match text.find(['e', 'E']) {
        Some(index) => {
            if text[index + 1..].contains(['e', 'E']) {
                return Err(format!("invalid decimal floating-point value '{value}'"));
            }
            let exponent = text[index + 1..]
                .parse::<i32>()
                .map_err(|_| format!("invalid decimal floating-point exponent in '{value}'"))?;
            (&text[..index], exponent)
        }
        None => (text, 0),
    };
    let mut digits = String::with_capacity(mantissa.len());
    let mut point_seen = false;
    let mut fractional_digits = 0i32;
    for byte in mantissa.bytes() {
        if byte == b'.' {
            if point_seen {
                return Err(format!("invalid decimal floating-point value '{value}'"));
            }
            point_seen = true;
        } else if byte.is_ascii_digit() {
            digits.push(byte as char);
            if point_seen {
                fractional_digits = fractional_digits.checked_add(1).ok_or_else(|| {
                    format!("decimal floating-point exponent is out of range in '{value}'")
                })?;
            }
        } else {
            return Err(format!("invalid decimal floating-point value '{value}'"));
        }
    }
    if digits.is_empty() {
        return Err(format!("invalid decimal floating-point value '{value}'"));
    }
    let mut exponent = explicit_exponent
        .checked_sub(fractional_digits)
        .ok_or_else(|| format!("decimal floating-point exponent is out of range in '{value}'"))?;
    let max_biased = 3 * (1i32 << exponent_bits) - 1;
    let min_exponent = -bias;
    let max_exponent = max_biased - bias;
    let mut coefficient_digits = digits.bytes().map(|byte| byte - b'0').collect::<Vec<_>>();
    let first_nonzero = coefficient_digits
        .iter()
        .position(|digit| *digit != 0)
        .unwrap_or(coefficient_digits.len().saturating_sub(1));
    coefficient_digits.drain(..first_nonzero);

    if coefficient_digits.iter().all(|digit| *digit == 0) {
        coefficient_digits.clear();
        coefficient_digits.push(0);
        exponent = exponent.clamp(min_exponent, max_exponent);
    } else {
        let precision_drop = coefficient_digits.len().saturating_sub(max_digits);
        let precision_exponent = checked_add_digits(exponent, precision_drop, value)?;
        let target_exponent = precision_exponent.max(min_exponent);
        let exponent_delta = target_exponent.checked_sub(exponent).ok_or_else(|| {
            format!("decimal floating-point exponent is out of range in '{value}'")
        })?;
        let dropped = usize::try_from(exponent_delta)
            .map_err(|_| format!("decimal floating-point exponent is out of range in '{value}'"))?;
        coefficient_digits = round_to_exponent(&coefficient_digits, dropped);
        exponent = target_exponent;

        // A carry during precision rounding creates one extra zero digit. Move
        // that zero into the exponent while keeping the coefficient in range.
        if coefficient_digits.len() > max_digits {
            if coefficient_digits.len() != max_digits + 1
                || coefficient_digits[0] != 1
                || coefficient_digits[1..].iter().any(|digit| *digit != 0)
            {
                return Err(format!(
                    "decimal floating-point coefficient is out of range in '{value}'"
                ));
            }
            coefficient_digits.pop();
            exponent = exponent.checked_add(1).ok_or_else(|| {
                format!("decimal floating-point exponent is out of range in '{value}'")
            })?;
        }

        if coefficient_digits.iter().all(|digit| *digit == 0) {
            coefficient_digits.clear();
            coefficient_digits.push(0);
        } else if exponent > max_exponent {
            let appended = usize::try_from(exponent - max_exponent).map_err(|_| {
                format!("decimal floating-point exponent is out of range in '{value}'")
            })?;
            let new_length = coefficient_digits
                .len()
                .checked_add(appended)
                .ok_or_else(|| {
                    format!("decimal floating-point exponent is out of range in '{value}'")
                })?;
            if new_length > max_digits {
                return Err(format!(
                    "decimal floating-point exponent is out of range in '{value}'"
                ));
            }
            coefficient_digits.extend(std::iter::repeat_n(0, appended));
            exponent = max_exponent;
        }
    }

    if !(min_exponent..=max_exponent).contains(&exponent) {
        return Err(format!(
            "decimal floating-point exponent is out of range in '{value}'"
        ));
    }
    let biased_exponent = exponent + bias;
    let mut digits = vec![0; max_digits];
    let first_digit = max_digits - coefficient_digits.len();
    digits[first_digit..].copy_from_slice(&coefficient_digits);

    let leading = i32::from(digits[0]);
    let high_exponent = biased_exponent >> exponent_bits;
    let combination = if leading >= 8 {
        0x18 | (((high_exponent & 0x03) << 1) | (leading - 8))
    } else {
        (high_exponent << 3) | leading
    };
    let exponent_continuation = biased_exponent & ((1 << exponent_bits) - 1);
    let mut coefficient = 0u128;
    for index in 0..groups {
        let encoded = encode_dpd(
            digits[index * 3 + 1],
            digits[index * 3 + 2],
            digits[index * 3 + 3],
        );
        coefficient = (coefficient << 10) | u128::from(encoded);
    }
    let mut word = (u128::from(combination as u32) << (total_bits - 6))
        | (u128::from(exponent_continuation as u32) << coefficient_bits)
        | coefficient;
    if negative {
        word |= 1u128 << (total_bits - 1);
    }
    let bytes = word.to_be_bytes();
    Ok(bytes[16 - byte_length..].to_vec())
}

fn should_round_half_even(digits: &[u8], kept_digits: usize) -> bool {
    let Some(&first_discarded) = digits.get(kept_digits) else {
        return false;
    };
    first_discarded > 5
        || (first_discarded == 5
            && (digits[kept_digits + 1..].iter().any(|digit| *digit != 0)
                || (kept_digits > 0 && digits[kept_digits - 1] % 2 == 1)))
}

fn checked_add_digits(exponent: i32, digits: usize, value: &str) -> Result<i32, String> {
    let digits = i32::try_from(digits)
        .map_err(|_| format!("decimal floating-point exponent is out of range in '{value}'"))?;
    exponent
        .checked_add(digits)
        .ok_or_else(|| format!("decimal floating-point exponent is out of range in '{value}'"))
}

/// Increment decimal digits in place. Returns true when the value grows by
/// one digit (for example, 999 -> 1000).
fn increment_decimal(digits: &mut [u8]) -> bool {
    for digit in digits.iter_mut().rev() {
        if *digit < 9 {
            *digit += 1;
            return false;
        }
        *digit = 0;
    }
    true
}

fn round_to_exponent(digits: &[u8], dropped_digits: usize) -> Vec<u8> {
    if dropped_digits == 0 {
        return digits.to_vec();
    }
    let kept_length = digits.len().saturating_sub(dropped_digits);
    let round_up = if dropped_digits > digits.len() {
        false
    } else {
        should_round_half_even(digits, kept_length)
    };
    let mut rounded = digits[..kept_length].to_vec();
    if round_up && increment_decimal(&mut rounded) {
        rounded.insert(0, 1);
    }
    if rounded.is_empty() {
        rounded.push(0);
    }
    rounded
}

fn encode_special(byte_length: usize, combination: u32, negative: bool) -> Vec<u8> {
    let total_bits = byte_length * 8;
    let mut word = u128::from(combination) << (total_bits - 6);
    if negative {
        word |= 1u128 << (total_bits - 1);
    }
    let bytes = word.to_be_bytes();
    bytes[16 - byte_length..].to_vec()
}

fn encode_dpd(d2: u8, d1: u8, d0: u8) -> u16 {
    let h2 = d2 >= 8;
    let h1 = d1 >= 8;
    let h0 = d0 >= 8;
    let v2 = i32::from(d2 & 7);
    let v1 = i32::from(d1 & 7);
    let v0 = i32::from(d0 & 7);
    let encoded = if !h2 && !h1 && !h0 {
        (v2 << 7) | (v1 << 4) | v0
    } else if !h2 && !h1 && h0 {
        (v2 << 7) | (v1 << 4) | 8 | (v0 & 1)
    } else if !h2 && h1 && !h0 {
        (v2 << 7)
            | (((v0 >> 2) & 1) << 6)
            | (((v0 >> 1) & 1) << 5)
            | ((v1 & 1) << 4)
            | (1 << 3)
            | (1 << 1)
            | (v0 & 1)
    } else if h2 && !h1 && !h0 {
        (((v0 >> 2) & 1) << 9)
            | (((v0 >> 1) & 1) << 8)
            | ((v2 & 1) << 7)
            | (v1 << 4)
            | (1 << 3)
            | (1 << 2)
            | (v0 & 1)
    } else if h2 && h1 && !h0 {
        (((v0 >> 2) & 1) << 9)
            | (((v0 >> 1) & 1) << 8)
            | ((v2 & 1) << 7)
            | ((v1 & 1) << 4)
            | (1 << 3)
            | (1 << 2)
            | (1 << 1)
            | (v0 & 1)
    } else if h2 && !h1 && h0 {
        (((v1 >> 2) & 1) << 9)
            | (((v1 >> 1) & 1) << 8)
            | ((v2 & 1) << 7)
            | (1 << 5)
            | ((v1 & 1) << 4)
            | (1 << 3)
            | (1 << 2)
            | (1 << 1)
            | (v0 & 1)
    } else if !h2 && h1 && h0 {
        (v2 << 7) | (1 << 6) | ((v1 & 1) << 4) | (1 << 3) | (1 << 2) | (1 << 1) | (v0 & 1)
    } else {
        ((v2 & 1) << 7)
            | (1 << 6)
            | (1 << 5)
            | ((v1 & 1) << 4)
            | (1 << 3)
            | (1 << 2)
            | (1 << 1)
            | (v0 & 1)
    };
    encoded as u16
}

fn decode_dpd(encoded: u16) -> [u8; 3] {
    let b9 = (encoded >> 9) & 1;
    let b8 = (encoded >> 8) & 1;
    let b7 = (encoded >> 7) & 1;
    let b6 = (encoded >> 6) & 1;
    let b5 = (encoded >> 5) & 1;
    let b4 = (encoded >> 4) & 1;
    let b3 = (encoded >> 3) & 1;
    let b2 = (encoded >> 2) & 1;
    let b1 = (encoded >> 1) & 1;
    let b0 = encoded & 1;
    if b3 == 0 {
        return [
            (b9 * 4 + b8 * 2 + b7) as u8,
            (b6 * 4 + b5 * 2 + b4) as u8,
            (b2 * 4 + b1 * 2 + b0) as u8,
        ];
    }
    if b2 == 0 && b1 == 0 {
        return [
            (b9 * 4 + b8 * 2 + b7) as u8,
            (b6 * 4 + b5 * 2 + b4) as u8,
            (8 + b0) as u8,
        ];
    }
    if b2 == 0 && b1 == 1 {
        return [
            (b9 * 4 + b8 * 2 + b7) as u8,
            (8 + b4) as u8,
            (b6 * 4 + b5 * 2 + b0) as u8,
        ];
    }
    if b2 == 1 && b1 == 0 {
        return [
            (8 + b7) as u8,
            (b6 * 4 + b5 * 2 + b4) as u8,
            (b9 * 4 + b8 * 2 + b0) as u8,
        ];
    }
    if b6 == 0 && b5 == 0 {
        return [(8 + b7) as u8, (8 + b4) as u8, (b9 * 4 + b8 * 2 + b0) as u8];
    }
    if b6 == 0 && b5 == 1 {
        return [(8 + b7) as u8, (b9 * 4 + b8 * 2 + b4) as u8, (8 + b0) as u8];
    }
    if b6 == 1 && b5 == 0 {
        return [(b9 * 4 + b8 * 2 + b7) as u8, (8 + b4) as u8, (8 + b0) as u8];
    }
    [(8 + b7) as u8, (8 + b4) as u8, (8 + b0) as u8]
}

fn format_dfp(negative: bool, digits: &[u8], exponent: i32) -> String {
    let mut first = 0;
    while first < digits.len() - 1 && digits[first] == 0 {
        first += 1;
    }
    let significant = &digits[first..];
    if significant.len() == 1 && significant[0] == 0 {
        return if negative { "-0" } else { "0" }.into();
    }
    let decimal_position = significant.len() as i32 + exponent;
    let mut output = String::with_capacity(40);
    if negative {
        output.push('-');
    }
    if decimal_position > 0 && decimal_position <= significant.len() as i32 {
        let position = decimal_position as usize;
        append_digits(&mut output, &significant[..position]);
        if position < significant.len() {
            output.push('.');
            append_digits(&mut output, &significant[position..]);
        }
        return output;
    }
    if decimal_position > significant.len() as i32
        && decimal_position <= significant.len() as i32 + 10
    {
        append_digits(&mut output, significant);
        output.extend(std::iter::repeat_n(
            '0',
            (decimal_position - significant.len() as i32) as usize,
        ));
        return output;
    }
    if (-6..=0).contains(&decimal_position) {
        output.push_str("0.");
        output.extend(std::iter::repeat_n('0', (-decimal_position) as usize));
        append_digits(&mut output, significant);
        return output;
    }
    output.push(char::from(b'0' + significant[0]));
    if significant.len() > 1 {
        output.push('.');
        append_digits(&mut output, &significant[1..]);
    }
    let scientific_exponent = exponent + significant.len() as i32 - 1;
    output.push('E');
    if scientific_exponent >= 0 {
        output.push('+');
    }
    write!(&mut output, "{scientific_exponent}").expect("writing to String cannot fail");
    output
}

fn append_digits(output: &mut String, digits: &[u8]) {
    for digit in digits {
        output.push(char::from(b'0' + *digit));
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_dfp, encode_dfp};

    #[test]
    fn encoding_matches_go_dpd_vectors_and_decodes_canonically() {
        let vectors = [
            (8, "0", "2238000000000000", "0"),
            (8, "123.456", "222c000000028e56", "123.456"),
            (8, "-987.654", "a22c0000000e3f54", "-987.654"),
            (8, "1000", "2238000000000400", "1000"),
            (8, "0.000123", "22200000000000a3", "0.000123"),
            (8, "1.23E+2", "22380000000000a3", "123"),
            (8, "-Infinity", "f800000000000000", "-Infinity"),
            (8, "NaN", "7c00000000000000", "NaN"),
            (16, "0", "22080000000000000000000000000000", "0"),
            (
                16,
                "12345678901234567890.12345678901234",
                "2604934b9c1e28e56f3c127177823534",
                "12345678901234567890.12345678901234",
            ),
            (
                16,
                "-98765432109876543210.98765432109876",
                "ee04b7cb0d10e3f546845ef96322277c",
                "-98765432109876543210.98765432109876",
            ),
            (
                16,
                "9.87654321E+20",
                "220b0000000000000000000038fd51a1",
                "9.87654321E+20",
            ),
            (
                16,
                "Infinity",
                "78000000000000000000000000000000",
                "Infinity",
            ),
            (16, "NaN", "7c000000000000000000000000000000", "NaN"),
        ];
        for (length, input, expected_hex, expected_decoded) in vectors {
            let encoded = encode_dfp(input, length).unwrap();
            let actual_hex: String = encoded.iter().map(|byte| format!("{byte:02x}")).collect();
            assert_eq!(actual_hex, expected_hex, "{input} ({length} bytes)");
            assert_eq!(
                decode_dfp(&encoded).unwrap(),
                expected_decoded,
                "{input} ({length} bytes)"
            );
        }
    }

    #[test]
    fn encoding_rejects_malformed_values_invalid_widths_and_out_of_range_exponents() {
        assert!(
            encode_dfp("12x", 8)
                .unwrap_err()
                .contains("invalid decimal floating-point value")
        );
        assert!(
            encode_dfp("1E+1000", 8)
                .unwrap_err()
                .contains("exponent is out of range")
        );
        assert!(
            encode_dfp("1", 12)
                .unwrap_err()
                .contains("invalid DFP byte length")
        );
    }

    #[test]
    fn encoding_rounds_to_precision_using_half_even_and_adjusts_the_exponent() {
        let cases = [
            (8, "12345678901234565", "12345678901234560"),
            (8, "12345678901234575", "12345678901234580"),
            (8, "99999999999999995", "100000000000000000"),
            (8, "123456789012345600", "123456789012345600"),
            (
                16,
                "12345678901234567890123456789012345",
                "12345678901234567890123456789012340",
            ),
            (
                16,
                "12345678901234567890123456789012355",
                "12345678901234567890123456789012360",
            ),
            (
                16,
                "99999999999999999999999999999999995",
                "100000000000000000000000000000000000",
            ),
        ];
        for (width, input, expected) in cases {
            let encoded = encode_dfp(input, width).unwrap();
            assert_eq!(decode_dfp(&encoded).unwrap(), expected, "{input}");
        }
    }

    #[test]
    fn encoding_handles_zero_special_values_and_subnormal_exponent_rounding() {
        for width in [8, 16] {
            for zero in ["0", "-0", "+0", "0E+999999", "-0E-999999"] {
                let encoded = encode_dfp(zero, width).unwrap();
                let expected = if zero.starts_with('-') { "-0" } else { "0" };
                assert_eq!(
                    decode_dfp(&encoded).unwrap(),
                    expected,
                    "{zero}, width={width}"
                );
            }
            assert_eq!(
                decode_dfp(&encode_dfp("Infinity", width).unwrap()).unwrap(),
                "Infinity"
            );
            assert_eq!(
                decode_dfp(&encode_dfp("-Infinity", width).unwrap()).unwrap(),
                "-Infinity"
            );
            assert_eq!(
                decode_dfp(&encode_dfp("NaN", width).unwrap()).unwrap(),
                "NaN"
            );
        }

        assert_eq!(
            decode_dfp(&encode_dfp("1E-398", 8).unwrap()).unwrap(),
            "1E-398"
        );
        assert_eq!(decode_dfp(&encode_dfp("5E-399", 8).unwrap()).unwrap(), "0");
        assert_eq!(
            decode_dfp(&encode_dfp("6E-399", 8).unwrap()).unwrap(),
            "1E-398"
        );
        assert_eq!(
            decode_dfp(&encode_dfp("1E+370", 8).unwrap()).unwrap(),
            "1.0E+370"
        );
        assert!(encode_dfp("1E+385", 8).is_err());
        assert!(encode_dfp("1E-500", 8).is_ok());
    }

    #[test]
    fn underflow_and_precision_are_rounded_once_to_avoid_double_rounding() {
        let encoded = encode_dfp("12345678901234149E-400", 8).unwrap();
        assert_eq!(decode_dfp(&encoded).unwrap(), "1.23456789012341E-384");
    }

    #[test]
    fn decimal64_rounding_matches_an_independent_decimal_context_at_boundaries() {
        // Expected values were checked with Python's libmpdec decimal64 context:
        // precision=16, Emin=-383, Emax=384, clamp=1, ROUND_HALF_EVEN.
        for (input, expected) in [
            ("00000123.4500", "123.4500"),
            ("1E+384", "1.000000000000000E+384"),
            ("9.9999999999999994E+384", "9.999999999999999E+384"),
            ("12345678901234149E-400", "1.23456789012341E-384"),
        ] {
            let encoded = encode_dfp(input, 8).unwrap();
            assert_eq!(decode_dfp(&encoded).unwrap(), expected, "{input}");
        }
        assert!(encode_dfp("9.9999999999999995E+384", 8).is_err());
        assert!(encode_dfp("1E+385", 8).is_err());
    }
}
