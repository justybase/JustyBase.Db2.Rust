//! Golden DSS/DDM vectors independent of the replay generator.
use justybase_db2::network::{codepoint as cp, ddm, dss};

#[test]
fn golden_request_frame_boundaries_match_materialized_encoder() {
    // Hand-authored small payloads guard the production codec used to frame
    // generated replay responses.
    let payload = ddm::pack_ddm_object(cp::EXCSAT, &[0xde, 0xad, 0xbe, 0xef]);
    let (bytes, next) = dss::encode_request_dss(&payload, 1, false, true).unwrap();
    assert_eq!(next, 2);
    assert_eq!(bytes[0..2], [0, 6 + payload.len() as u8]);
    assert_eq!(bytes[2], 0xd0);
}

#[test]
fn golden_qrydsc_single_int_column() {
    let dsc = justybase_db2::replay::qrydsc_integer(1);
    assert_eq!(dsc, vec![6, 0x76, 0xd0, 0x02, 0, 4]);
    let fields = ddm::parse_query_descriptor(&dsc).unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].field_type, 0x02);
}

#[test]
fn golden_qrydsc_two_columns_parse_to_two_fields() {
    let dsc = justybase_db2::replay::qrydsc_integer(2);
    assert_eq!(dsc[0], 9);
    let fields = ddm::parse_query_descriptor(&dsc).unwrap();
    assert_eq!(fields.len(), 2);
}

#[test]
fn golden_int_rows_cover_null_indicator_and_le_encoding() {
    let bytes = justybase_db2::replay::encode_int_rows(1, 42, 42);
    assert_eq!(bytes, vec![0xff, 0, 42, 0, 0, 0]);
}

#[test]
fn golden_checksum_closed_form() {
    assert_eq!(justybase_db2::replay::checksum(1_000_000), 500000500000);
}
