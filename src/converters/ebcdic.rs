/// IBM EBCDIC CP500 conversion table from the Go reference driver.
const CP500_TO_UNICODE: [u32; 256] = [
    0, 1, 2, 3, 156, 9, 134, 127, 151, 141, 142, 11, 12, 13, 14, 15, 16, 17, 18, 19, 157, 133, 8,
    135, 24, 25, 146, 143, 28, 29, 30, 31, 128, 129, 130, 131, 132, 10, 23, 27, 136, 137, 138, 139,
    140, 5, 6, 7, 144, 145, 22, 147, 148, 149, 150, 4, 152, 153, 154, 155, 20, 21, 158, 26, 32,
    160, 226, 228, 224, 225, 227, 229, 231, 241, 91, 46, 60, 40, 43, 33, 38, 233, 234, 235, 232,
    237, 238, 239, 236, 223, 93, 36, 42, 41, 59, 94, 45, 47, 194, 196, 192, 193, 195, 197, 199,
    209, 166, 44, 37, 95, 62, 63, 248, 201, 202, 203, 200, 205, 206, 207, 204, 96, 58, 35, 64, 39,
    61, 34, 216, 97, 98, 99, 100, 101, 102, 103, 104, 105, 171, 187, 240, 253, 254, 177, 176, 106,
    107, 108, 109, 110, 111, 112, 113, 114, 170, 186, 230, 184, 198, 164, 181, 126, 115, 116, 117,
    118, 119, 120, 121, 122, 161, 191, 208, 221, 222, 174, 172, 163, 165, 183, 169, 167, 182, 188,
    189, 190, 253, 168, 180, 215, 184, 175, 123, 65, 66, 67, 68, 69, 70, 71, 72, 73, 173, 244, 246,
    242, 243, 245, 125, 74, 75, 76, 77, 78, 79, 80, 81, 82, 185, 251, 252, 249, 250, 255, 92, 247,
    83, 84, 85, 86, 87, 88, 89, 90, 178, 212, 214, 210, 211, 213, 48, 49, 50, 51, 52, 53, 54, 55,
    56, 57, 179, 219, 220, 217, 218, 159,
];

const fn make_unicode_to_cp500() -> [u16; 256] {
    let mut reverse = [u16::MAX; 256];
    let mut byte = 0;
    while byte < CP500_TO_UNICODE.len() {
        let codepoint = CP500_TO_UNICODE[byte];
        if codepoint < reverse.len() as u32 {
            // Assign in byte order so duplicate Unicode mappings keep the
            // same last-byte canonical choice as the former reverse scan.
            reverse[codepoint as usize] = byte as u16;
        }
        byte += 1;
    }
    reverse
}

const UNICODE_TO_CP500: [u16; 256] = make_unicode_to_cp500();

/// Encode Unicode text using the IBM CP500 single-byte code page.
pub fn encode_cp500(value: &str) -> Result<Vec<u8>, String> {
    let mut output = Vec::with_capacity(value.len());
    for character in value.chars() {
        let codepoint = character as usize;
        let byte = UNICODE_TO_CP500.get(codepoint).copied().unwrap_or(u16::MAX);
        if byte == u16::MAX {
            return Err("character cannot be encoded to CP500 EBCDIC".into());
        }
        output.push(byte as u8);
    }
    Ok(output)
}

/// Decode IBM CP500 bytes into Unicode text.
pub fn decode_cp500(value: &[u8]) -> String {
    value
        .iter()
        .map(|&byte| {
            char::from_u32(CP500_TO_UNICODE[byte as usize]).unwrap_or(char::REPLACEMENT_CHARACTER)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{CP500_TO_UNICODE, decode_cp500, encode_cp500};

    #[test]
    fn cp500_reverse_mapping_is_canonical_for_all_bytes() {
        for byte in 0u8..=u8::MAX {
            let decoded = decode_cp500(&[byte]);
            let encoded = encode_cp500(&decoded).unwrap();
            let canonical = CP500_TO_UNICODE
                .iter()
                .rposition(|codepoint| *codepoint == decoded.chars().next().unwrap() as u32)
                .unwrap() as u8;
            assert_eq!(encoded, [canonical], "input CP500 byte 0x{byte:02X}");
            assert_eq!(decode_cp500(&encoded), decoded);
        }
    }

    #[test]
    fn cp500_encodes_supported_mixed_text_and_rejects_unrepresentable_unicode() {
        let encoded = encode_cp500("AéB").unwrap();
        assert_eq!(decode_cp500(&encoded), "AéB");
        assert!(encode_cp500("A😀B").is_err());
    }
}
