use std::borrow::Cow;

use crate::converters::decfloat::encode_dfp;
use crate::network::codepoint as cp;
use crate::network::ddm::{self, ColumnDescription};
use crate::network::session::{Db2InputParameter, Db2Value};

/// Builds a complete SQLDTA object using server-described positional parameter types.
pub fn build_sql_dta(
    columns: &[ColumnDescription],
    values: &[Db2Value],
) -> Result<Vec<u8>, String> {
    Ok(build_sql_dta_with_external_data(columns, values)?.sql_dta)
}

pub struct EncodedSqlDta {
    pub sql_dta: Vec<u8>,
    pub external_data: Vec<EncodedExternalData>,
}

pub enum EncodedExternalData {
    DdmObject(Vec<u8>),
    LobStream { parameter_index: usize, length: u64 },
}

enum ExternalLobValue<'a> {
    Bytes(Cow<'a, [u8]>),
    Stream(u64),
}

struct DrdaWriter {
    buf: Vec<u8>,
}

impl DrdaWriter {
    fn with_capacity(capacity: usize) -> Result<Self, String> {
        let mut buf = Vec::new();
        buf.try_reserve_exact(capacity)
            .map_err(|error| format!("unable to reserve SQLDTA request buffer: {error}"))?;
        Ok(Self { buf })
    }

    fn begin_ddm(
        &mut self,
        codepoint: cp::CodePoint,
        body_length: usize,
    ) -> Result<(usize, usize), String> {
        ddm::append_ddm_header(&mut self.buf, codepoint, body_length)?;
        Ok((self.buf.len(), body_length))
    }

    fn put_slice(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    fn put_ddm(&mut self, codepoint: cp::CodePoint, payload: &[u8]) -> Result<(), String> {
        let marker = self.begin_ddm(codepoint, payload.len())?;
        self.put_slice(payload);
        self.end_ddm(marker)
    }

    fn end_ddm(&self, (body_start, expected_length): (usize, usize)) -> Result<(), String> {
        let actual_length = self.buf.len().saturating_sub(body_start);
        if actual_length == expected_length {
            Ok(())
        } else {
            Err("SQLDTA DDM length changed while writing the request".into())
        }
    }

    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

fn encoded_ddm_length(body_length: usize) -> Result<usize, String> {
    ddm::ddm_header_length(body_length)?
        .checked_add(body_length)
        .ok_or_else(|| "SQLDTA request length overflow".to_owned())
}

pub fn build_sql_dta_with_external_data(
    columns: &[ColumnDescription],
    values: &[Db2Value],
) -> Result<EncodedSqlDta, String> {
    let inputs = values
        .iter()
        .map(Db2InputParameter::Value)
        .collect::<Vec<_>>();
    build_sql_dta_with_inputs(columns, &inputs)
}

pub fn build_sql_dta_with_inputs(
    columns: &[ColumnDescription],
    inputs: &[Db2InputParameter<'_>],
) -> Result<EncodedSqlDta, String> {
    if columns.len() != inputs.len() {
        return Err(format!(
            "DB2 expected {} parameters, got {}",
            columns.len(),
            inputs.len()
        ));
    }
    let mut external: Vec<Option<ExternalLobValue<'_>>> = Vec::with_capacity(columns.len());
    let mut has_non_empty_external = false;
    for (column, input) in columns.iter().zip(inputs) {
        let value = match input {
            Db2InputParameter::Value(value) => {
                validate_parameter_source(column, value)?;
                external_lob(column, value)?.map(ExternalLobValue::Bytes)
            }
            Db2InputParameter::BlobStream { length, .. } => {
                validate_stream_parameter(column, *length, 404, "BLOB")?;
                Some(ExternalLobValue::Stream(*length))
            }
            Db2InputParameter::ClobStream { length, .. } => {
                validate_stream_parameter(column, *length, 408, "CLOB")?;
                (*length > 0).then_some(ExternalLobValue::Stream(*length))
            }
            Db2InputParameter::DbClobStream { length, .. } => {
                validate_stream_parameter(column, *length, 412, "DBCLOB")?;
                (*length > 0).then_some(ExternalLobValue::Stream(*length))
            }
            Db2InputParameter::XmlStream { length, .. } => {
                validate_stream_parameter(column, *length, 988, "XML")?;
                Some(ExternalLobValue::Stream(*length))
            }
        };
        has_non_empty_external |= match &value {
            Some(ExternalLobValue::Bytes(bytes)) => !bytes.is_empty(),
            Some(ExternalLobValue::Stream(length)) => *length > 0,
            None => false,
        };
        external.push(value);
    }
    let group_count = columns.len().div_ceil(84);
    let descriptor_capacity = columns
        .len()
        .saturating_mul(3)
        .saturating_add(group_count.saturating_mul(3))
        .saturating_add(6);
    let mut fdodsc = Vec::with_capacity(descriptor_capacity);
    for group_start in (0..columns.len()).step_by(84) {
        let count = (columns.len() - group_start).min(84);
        // SQLDTAGRP starts with NGDA (0x76 / LID 0xD0); continuation groups
        // use CPT (0x7F / NULL LID), rather than starting another NGDA group.
        // See Apache Derby's DRDA encoder and IBM's FDOCA continuation notes:
        // https://raw.githubusercontent.com/apache/derby/trunk/java/org.apache.derby.client/org/apache/derby/client/net/NetStatementRequest.java
        // https://www.ibm.com/docs/en/db2-for-zos/12.0.0?topic=procedures-interpreting-ifcid-0191-records
        let (triplet_type, lid) = if group_start == 0 {
            (0x76, 0xd0)
        } else {
            (0x7f, 0x00)
        };
        fdodsc.extend_from_slice(&[((count + 1) * 3) as u8, triplet_type, lid]);
        for (offset, column) in columns[group_start..group_start + count].iter().enumerate() {
            let input = &inputs[group_start + offset];
            append_field_descriptor(
                &mut fdodsc,
                column,
                input,
                external[group_start + offset].is_some(),
            );
        }
    }
    fdodsc.extend_from_slice(&[0x06, 0x71, 0xe4, 0xd0, 0x00, 0x01]);

    let mut fdodta = Vec::with_capacity(estimated_fdodta_capacity(columns, inputs, &external));
    // Db2 requires the FDODTA alignment byte before the first nullable
    // indicator even when the complete parameter payload has even length.
    if !columns.is_empty() {
        fdodta.push(0);
    }
    for ((column, input), external_blob) in columns.iter().zip(inputs).zip(&external) {
        append_parameter(&mut fdodta, column, input, external_blob.as_ref())?;
    }
    let body_length = encoded_ddm_length(fdodsc.len())?
        .checked_add(encoded_ddm_length(fdodta.len())?)
        .ok_or_else(|| "SQLDTA body length overflow".to_owned())?;
    let mut writer = DrdaWriter::with_capacity(encoded_ddm_length(body_length)?)?;
    let body = writer.begin_ddm(cp::SQLDTA, body_length)?;
    writer.put_ddm(cp::FDODSC, &fdodsc)?;
    writer.put_ddm(cp::FDODTA, &fdodta)?;
    writer.end_ddm(body)?;
    let external_count = external.iter().filter(|value| value.is_some()).count();
    let mut external_data = Vec::with_capacity(external_count);
    for (parameter_index, value) in external.into_iter().enumerate() {
        match value {
            None => (),
            Some(ExternalLobValue::Bytes(bytes)) if bytes.is_empty() && has_non_empty_external => {}
            Some(ExternalLobValue::Bytes(bytes)) => external_data.push(
                EncodedExternalData::DdmObject(pack_external_lob_object(bytes.as_ref())?),
            ),
            Some(ExternalLobValue::Stream(length)) => {
                external_data.push(EncodedExternalData::LobStream {
                    parameter_index,
                    length,
                });
            }
        }
    }
    Ok(EncodedSqlDta {
        sql_dta: writer.finish(),
        external_data,
    })
}

fn base_type(sql_type: u16) -> u16 {
    if sql_type % 2 == 1 {
        sql_type - 1
    } else {
        sql_type
    }
}

fn estimated_fdodta_capacity(
    columns: &[ColumnDescription],
    inputs: &[Db2InputParameter<'_>],
    external: &[Option<ExternalLobValue<'_>>],
) -> usize {
    let mut size = usize::from(!columns.is_empty());
    for ((column, input), lob) in columns.iter().zip(inputs).zip(external) {
        let estimate = match input {
            Db2InputParameter::BlobStream { length, .. }
            | Db2InputParameter::ClobStream { length, .. }
            | Db2InputParameter::DbClobStream { length, .. }
            | Db2InputParameter::XmlStream { length, .. } => {
                if *length == 0 && base_type(column.sql_type) != 404 {
                    2
                } else {
                    9
                }
            }
            Db2InputParameter::Value(Db2Value::Null) => 1,
            Db2InputParameter::Value(_) if lob.is_some() => 9,
            Db2InputParameter::Value(Db2Value::Bytes(bytes))
                if is_binary_to_character_target(base_type(column.sql_type)) =>
            {
                bytes.len().saturating_add(3)
            }
            Db2InputParameter::Value(Db2Value::Bytes(bytes)) => match base_type(column.sql_type) {
                412 => bytes.len().saturating_add(2),
                912 => usize::try_from(column.length)
                    .unwrap_or(usize::MAX)
                    .saturating_add(1),
                908 | 404 => bytes.len().saturating_add(3),
                _ => bytes.len().saturating_mul(2).saturating_add(3),
            },
            Db2InputParameter::Value(Db2Value::Text(text) | Db2Value::Decimal(text)) => {
                let value_size = text.len().saturating_mul(2).saturating_add(3);
                if base_type(column.sql_type) == 468 {
                    value_size.max((column.length as usize).saturating_mul(2).saturating_add(1))
                } else {
                    value_size
                }
            }
            Db2InputParameter::Value(Db2Value::Integer(_))
            | Db2InputParameter::Value(Db2Value::Float(_))
            | Db2InputParameter::Value(Db2Value::Boolean(_)) => 40,
            Db2InputParameter::Value(Db2Value::ProgressiveLobReference { .. })
            | Db2InputParameter::Value(Db2Value::Lob(_)) => 16,
        };
        size = size.saturating_add(estimate);
    }
    size
}

fn is_binary_to_character_target(sql_type: u16) -> bool {
    matches!(sql_type, 408 | 448 | 452 | 456 | 988)
}

fn validate_parameter_source(column: &ColumnDescription, value: &Db2Value) -> Result<(), String> {
    if matches!(value, Db2Value::Lob(_)) {
        return Err("cannot bind a row-stream Db2 LOB locator".into());
    }
    if matches!(
        value,
        Db2Value::Null | Db2Value::ProgressiveLobReference { .. }
    ) {
        return Ok(());
    }
    let sql_type = base_type(column.sql_type);
    let binary = matches!(value, Db2Value::Bytes(_));
    if matches!(sql_type, 500 | 496 | 492 | 480 | 484 | 504 | 996 | 2436) {
        if binary {
            return Err(format!(
                "a binary value cannot be bound to Db2 SQL type {sql_type}"
            ));
        }
        return Ok(());
    }
    if matches!(sql_type, 912 | 908 | 404) {
        match value {
            Db2Value::Bytes(_) => {}
            Db2Value::Text(text) => {
                let _ = decode_hex_binary(column, text)?;
            }
            _ => {
                return Err(format!(
                    "Db2 binary SQL type {sql_type} requires binary or hexadecimal text input"
                ));
            }
        }
        return Ok(());
    }
    if matches!(sql_type, 408 | 412 | 988) {
        if !matches!(value, Db2Value::Text(_) | Db2Value::Bytes(_)) {
            return Err(format!(
                "Db2 text LOB/XML SQL type {sql_type} requires text or binary input"
            ));
        }
        return Ok(());
    }
    if matches!(sql_type, 464 | 468 | 472 | 476) && binary {
        return Err(format!(
            "a binary value cannot be bound to Db2 graphic SQL type {sql_type}"
        ));
    }
    Ok(())
}

fn append_field_descriptor(
    output: &mut Vec<u8>,
    column: &ColumnDescription,
    input: &Db2InputParameter<'_>,
    external_lob: bool,
) {
    let descriptor: [u8; 3] = if matches!(input, Db2InputParameter::Value(Db2Value::Bytes(_)))
        && is_binary_to_character_target(base_type(column.sql_type))
    {
        [0x3f, 0x7f, 0xff]
    } else {
        match base_type(column.sql_type) {
            408 | 412 if external_lob => [0xcf, 0x80, 9],
            988 if external_lob => [0xc7, 0x80, 9],
            448 | 452 | 408 | 412 | 464 | 472 | 988 => [0x39, 0x3f, 0xff],
            500 => [0x05, 0, 2],
            496 => [0x03, 0, 4],
            492 => [0x17, 0, 8],
            480 if column.length == 4 => [0x0d, 0, 4],
            480 => [0x0b, 0, 8],
            384 => [0x21, 0, 10],
            388 => [0x23, 0, 8],
            392 => [0x25, 0, 32],
            2436 if column.length == 2 => [0x05, 0, 2],
            2436 => [0xbf, 0, 1],
            404 if external_lob => [0xc9, 0x80, 9],
            404 => [0xc9, 0x80, 2],
            468 => [0x37, (column.length >> 8) as u8, column.length as u8],
            908 => [0xc3, (column.length >> 8) as u8, column.length as u8],
            912 => [0x27, (column.length >> 8) as u8, column.length as u8],
            484 | 504 => [0x0f, column.precision as u8, column.scale as u8],
            996 if column.length == 16 => [0xbb, 0, 16],
            996 => [0xbb, 0, 8],
            _ => [0x39, 0x3f, 0xff],
        }
    };
    output.extend_from_slice(&descriptor);
}

fn append_parameter(
    output: &mut Vec<u8>,
    column: &ColumnDescription,
    input: &Db2InputParameter<'_>,
    external_lob: Option<&ExternalLobValue<'_>>,
) -> Result<(), String> {
    let stream_length = match input {
        Db2InputParameter::BlobStream { length, .. }
        | Db2InputParameter::ClobStream { length, .. }
        | Db2InputParameter::DbClobStream { length, .. }
        | Db2InputParameter::XmlStream { length, .. } => Some(length),
        Db2InputParameter::Value(_) => None,
    };
    if let Some(length) = stream_length {
        if *length == 0 {
            output.push(0);
            match input {
                Db2InputParameter::BlobStream { .. } => {
                    output.push(0x02);
                    output.extend_from_slice(&0u64.to_be_bytes());
                }
                Db2InputParameter::ClobStream { .. } | Db2InputParameter::DbClobStream { .. } => {
                    append_text(output, "")?
                }
                Db2InputParameter::XmlStream { .. } => {
                    return Err("an XML stream must contain a non-empty XML document".into());
                }
                Db2InputParameter::Value(_) => unreachable!("stream length belongs to a stream"),
            }
            return Ok(());
        }
        output.push(0);
        output.push(0x02);
        output.extend_from_slice(&length.to_be_bytes());
        return Ok(());
    }
    let Db2InputParameter::Value(value) = input else {
        unreachable!("stream parameters are handled above");
    };
    if matches!(value, Db2Value::Null) {
        output.push(0xff);
        return Ok(());
    }
    output.push(0);
    if let Db2Value::Bytes(bytes) = value
        && is_binary_to_character_target(base_type(column.sql_type))
    {
        return append_length_prefixed(output, bytes);
    }
    if let Some(ExternalLobValue::Bytes(bytes)) = external_lob {
        output.push(0x02);
        output.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        return Ok(());
    }
    match base_type(column.sql_type) {
        500 => {
            let number = i16::try_from(to_i64(value)?)
                .map_err(|_| "Db2 integer value is outside the signed 16-bit range".to_string())?;
            output.extend_from_slice(&number.to_le_bytes());
        }
        496 => {
            let number = i32::try_from(to_i64(value)?)
                .map_err(|_| "Db2 integer value is outside the signed 32-bit range".to_string())?;
            output.extend_from_slice(&number.to_le_bytes());
        }
        492 => output.extend_from_slice(&to_i64(value)?.to_le_bytes()),
        480 if column.length == 4 => {
            output.extend_from_slice(&(to_f64(value)? as f32).to_le_bytes())
        }
        480 => output.extend_from_slice(&to_f64(value)?.to_le_bytes()),
        2436 if column.length == 2 => {
            output.extend_from_slice(&(to_bool(value)? as u16).to_le_bytes())
        }
        2436 => output.push(to_bool(value)? as u8),
        448 | 452 | 456 | 460 | 408 | 464 | 472 => append_string(output, value)?,
        412 => match value {
            Db2Value::Bytes(bytes) => append_utf16_be_text(output, bytes)?,
            _ => append_string(output, value)?,
        },
        468 => {
            let text = text_value(value)?;
            let target_chars = if column.length > 0 {
                column.length as usize
            } else {
                text.encode_utf16().count()
            };
            output
                .try_reserve(target_chars.saturating_mul(2))
                .map_err(|error| format!("unable to reserve Db2 GRAPHIC parameter: {error}"))?;
            let mut written = 0;
            for unit in text.encode_utf16().take(target_chars) {
                output.extend_from_slice(&unit.to_be_bytes());
                written += 1;
            }
            for _ in written..target_chars {
                output.extend_from_slice(&0x20u16.to_be_bytes());
            }
        }
        384 => output.extend_from_slice(format_date(&to_text(value)?, "date")?.as_bytes()),
        388 => output.extend_from_slice(format_date(&to_text(value)?, "time")?.as_bytes()),
        392 => {
            let timestamp = format_date(&to_text(value)?, "timestamp")?;
            output.extend_from_slice(timestamp.as_bytes());
            output.resize(output.len() + 32 - timestamp.len(), b' ');
        }
        988 => append_string(output, value)?,
        912 => {
            let bytes = binary_parameter_bytes(column, value)?;
            let bytes = bytes.as_ref();
            let target = if column.length > 0 {
                column.length as usize
            } else {
                bytes.len()
            };
            output.resize(output.len() + target, 0);
            let end = output.len();
            let start = end - target;
            output[start..start + target.min(bytes.len())]
                .copy_from_slice(&bytes[..target.min(bytes.len())]);
        }
        908 | 404 => {
            let bytes = binary_parameter_bytes(column, value)?;
            append_length_prefixed(output, bytes.as_ref())?;
        }
        484 | 504 => append_packed_decimal(
            output,
            value,
            column.precision as usize,
            column.scale as usize,
        )?,
        996 => output.extend_from_slice(&encode_dfp(
            &to_text(value)?,
            if column.length == 16 { 16 } else { 8 },
        )?),
        _ => append_string(output, value)?,
    }
    Ok(())
}

fn external_lob<'a>(
    column: &ColumnDescription,
    value: &'a Db2Value,
) -> Result<Option<Cow<'a, [u8]>>, String> {
    // Rust strings already contain UTF-8: borrow large text LOB payloads rather
    // than allocating an additional encoded copy.
    if matches!(base_type(column.sql_type), 408 | 412 | 988) {
        if let Db2Value::Text(text) = value
            && text.len() > u16::MAX as usize
            && text.encode_utf16().count() > u16::MAX as usize
        {
            return Ok(Some(Cow::Borrowed(text.as_bytes())));
        }
        return Ok(None);
    }
    if base_type(column.sql_type) != 404 {
        return Ok(None);
    }
    match value {
        Db2Value::Null => Ok(None),
        Db2Value::Bytes(bytes) => Ok(Some(Cow::Borrowed(bytes))),
        Db2Value::Text(_) => Ok(Some(Cow::Owned(
            binary_parameter_bytes(column, value)?.into_owned(),
        ))),
        _ => Err("Db2 BLOB parameter requires binary or hexadecimal text input".to_string()),
    }
}

fn pack_external_lob_object(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let body_length = bytes
        .len()
        .checked_add(1)
        .ok_or_else(|| "DB2 EXTDTA object length overflow".to_owned())?;
    let short_length = body_length
        .checked_add(4)
        .ok_or_else(|| "DB2 EXTDTA object length overflow".to_owned())?;
    let header_length: usize = if short_length <= 0x7fff {
        4
    } else if body_length <= 0x7fff_ffff {
        8
    } else {
        12
    };
    let total_length = header_length
        .checked_add(body_length)
        .ok_or_else(|| "DB2 EXTDTA object length overflow".to_owned())?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(total_length)
        .map_err(|error| format!("unable to reserve a DB2 EXTDTA object: {error}"))?;
    if header_length == 4 {
        output.extend_from_slice(&(short_length as u16).to_be_bytes());
    } else {
        output.extend_from_slice(&(0x8000u16 | header_length as u16).to_be_bytes());
    }
    output.extend_from_slice(&cp::EXTDTA.0.to_be_bytes());
    match header_length {
        8 => output.extend_from_slice(&(body_length as u32).to_be_bytes()),
        12 => output.extend_from_slice(
            &u64::try_from(body_length)
                .map_err(|_| "DB2 EXTDTA object length exceeds u64".to_owned())?
                .to_be_bytes(),
        ),
        _ => (),
    }
    output.push(0); // EXTDTA null indicator.
    output.extend_from_slice(bytes);
    Ok(output)
}

fn binary_parameter_bytes<'a>(
    column: &ColumnDescription,
    value: &'a Db2Value,
) -> Result<Cow<'a, [u8]>, String> {
    match value {
        Db2Value::Bytes(bytes) => Ok(Cow::Borrowed(bytes)),
        Db2Value::Text(text) => decode_hex_binary(column, text).map(Cow::Owned),
        _ => Err(format!(
            "Db2 binary SQL type {} requires binary or hexadecimal text input",
            base_type(column.sql_type)
        )),
    }
}

fn decode_hex_binary(column: &ColumnDescription, text: &str) -> Result<Vec<u8>, String> {
    let source = text.as_bytes();
    if !source.len().is_multiple_of(2) {
        return Err(format!(
            "Db2 binary SQL type {} requires an even-length hexadecimal string",
            base_type(column.sql_type)
        ));
    }
    let mut bytes = Vec::with_capacity(source.len() / 2);
    for pair in source.as_chunks::<2>().0 {
        let high = hex_nibble(pair[0]);
        let low = hex_nibble(pair[1]);
        if high < 0 || low < 0 {
            return Err(format!(
                "Db2 binary SQL type {} requires an even-length hexadecimal string",
                base_type(column.sql_type)
            ));
        }
        bytes.push(((high << 4) | low) as u8);
    }
    if column.length > 0 && bytes.len() as u64 > column.length {
        return Err(format!(
            "Db2 binary string input exceeds its {}-byte target length",
            column.length
        ));
    }
    Ok(bytes)
}

fn hex_nibble(value: u8) -> i16 {
    match value {
        b'0'..=b'9' => (value - b'0') as i16,
        b'A'..=b'F' => (value - b'A' + 10) as i16,
        b'a'..=b'f' => (value - b'a' + 10) as i16,
        _ => -1,
    }
}

fn validate_stream_parameter(
    column: &ColumnDescription,
    length: u64,
    expected_type: u16,
    name: &str,
) -> Result<(), String> {
    if base_type(column.sql_type) != expected_type {
        return Err(format!(
            "streamed {name} can only be bound to a {name} parameter, not {}",
            column.name
        ));
    }
    if length == u64::MAX {
        return Err(format!(
            "streamed {name} length overflows the DRDA extended length field"
        ));
    }
    if expected_type == 988 && length == 0 {
        return Err("an XML stream must contain a non-empty XML document".into());
    }
    Ok(())
}

fn append_string(output: &mut Vec<u8>, value: &Db2Value) -> Result<(), String> {
    append_text(output, &text_value(value)?)
}

fn append_text(output: &mut Vec<u8>, text: &str) -> Result<(), String> {
    let unit_count = text.encode_utf16().count();
    if unit_count > u16::MAX as usize {
        return Err("DRDA character parameter exceeds 65535 UTF-16 code units".into());
    }
    output
        .try_reserve(2 + unit_count * 2)
        .map_err(|error| format!("unable to reserve Db2 character parameter: {error}"))?;
    output.extend_from_slice(&(unit_count as u16).to_be_bytes());
    for unit in text.encode_utf16() {
        output.extend_from_slice(&unit.to_be_bytes());
    }
    Ok(())
}

fn append_utf16_be_text(output: &mut Vec<u8>, data: &[u8]) -> Result<(), String> {
    let decoded = || {
        std::char::decode_utf16(
            data.as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u16::from_be_bytes([pair[0], pair[1]])),
        )
        .map(|character| character.unwrap_or(char::REPLACEMENT_CHARACTER))
    };
    let unit_count: usize = decoded().map(char::len_utf16).sum();
    if unit_count > u16::MAX as usize {
        return Err("DRDA character parameter exceeds 65535 UTF-16 code units".into());
    }
    output
        .try_reserve(2 + unit_count * 2)
        .map_err(|error| format!("unable to reserve Db2 DBCLOB parameter: {error}"))?;
    output.extend_from_slice(&(unit_count as u16).to_be_bytes());
    for character in decoded() {
        let mut units = [0u16; 2];
        for unit in character.encode_utf16(&mut units).iter() {
            output.extend_from_slice(&unit.to_be_bytes());
        }
    }
    Ok(())
}

fn text_value(value: &Db2Value) -> Result<Cow<'_, str>, String> {
    match value {
        Db2Value::Text(text) | Db2Value::Decimal(text) => Ok(Cow::Borrowed(text)),
        Db2Value::Bytes(bytes) => std::str::from_utf8(bytes)
            .map(Cow::Borrowed)
            .map_err(|error| {
                format!("binary input is not valid UTF-8 for a Db2 character target: {error}")
            }),
        _ => to_text(value).map(Cow::Owned),
    }
}

fn append_length_prefixed(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > u16::MAX as usize {
        return Err("DRDA binary parameter exceeds 65535 bytes".into());
    }
    output.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn append_packed_decimal(
    output: &mut Vec<u8>,
    value: &Db2Value,
    precision: usize,
    scale: usize,
) -> Result<(), String> {
    if precision == 0 || precision > 31 || scale > precision {
        return Err(format!(
            "invalid Db2 decimal precision ({precision}) or scale ({scale})"
        ));
    }
    let text = match value {
        Db2Value::Float(number) => Cow::Owned(expand_scientific_decimal(&number.to_string())?),
        Db2Value::Integer(number) => Cow::Owned(number.to_string()),
        Db2Value::Decimal(number) | Db2Value::Text(number) => Cow::Borrowed(number.trim()),
        Db2Value::Boolean(value) => Cow::Owned((*value as u8).to_string()),
        Db2Value::Bytes(_) => return Err("a binary value cannot be bound to Db2 DECIMAL".into()),
        Db2Value::Null => unreachable!(),
        Db2Value::ProgressiveLobReference { .. } => {
            return Err("cannot encode an unresolved Db2 progressive LOB reference".into());
        }
        Db2Value::Lob(_) => return Err("cannot bind a row-stream Db2 LOB locator".into()),
    };
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix(['-', '+']).unwrap_or(&text);
    let mut parts = unsigned.split('.');
    let whole = parts.next().unwrap_or("");
    let fraction = parts.next().unwrap_or("");
    if parts.next().is_some()
        || (whole.is_empty() && fraction.is_empty())
        || (!whole.is_empty() && !whole.bytes().all(|byte| byte.is_ascii_digit()))
        || (!fraction.is_empty() && !fraction.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err(format!("invalid Db2 decimal value '{text}'"));
    }
    let integer_digits = precision - scale;
    let whole = if whole.is_empty() { "0" } else { whole };
    if whole.trim_start_matches('0').len() > integer_digits {
        return Err(format!(
            "Db2 decimal value '{text}' exceeds DECIMAL({precision},{scale})"
        ));
    }
    let byte_count = (precision + 2) / 2;
    let mut packed = [0u8; 16];
    let packed = &mut packed[..byte_count];
    let first_nibble = byte_count * 2 - 1 - precision;
    for index in 0..precision {
        let digit = if index < integer_digits {
            let source = whole.len() as isize - integer_digits as isize + index as isize;
            if source >= 0 {
                whole
                    .as_bytes()
                    .get(source as usize)
                    .map_or(0, |byte| byte - b'0')
            } else {
                0
            }
        } else {
            fraction
                .as_bytes()
                .get(index - integer_digits)
                .map_or(0, |byte| byte - b'0')
        };
        set_nibble(packed, first_nibble + index, digit);
    }
    set_nibble(
        packed,
        byte_count * 2 - 1,
        if negative { 0x0d } else { 0x0c },
    );
    output.extend_from_slice(packed);
    Ok(())
}

fn expand_scientific_decimal(text: &str) -> Result<String, String> {
    let Some((mantissa, exponent)) = text.split_once(['e', 'E']) else {
        return Ok(text.to_string());
    };
    let exponent: i32 = exponent
        .parse()
        .map_err(|error| format!("invalid Db2 decimal exponent in '{text}': {error}"))?;
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = format!("{whole}{fraction}");
    let point = whole.len() as i32 + exponent;
    let expanded = if point <= 0 {
        format!("0.{}{digits}", "0".repeat((-point) as usize))
    } else if point as usize >= digits.len() {
        format!("{digits}{}", "0".repeat(point as usize - digits.len()))
    } else {
        let point = point as usize;
        format!("{}.{}", &digits[..point], &digits[point..])
    };
    Ok(if negative {
        format!("-{expanded}")
    } else {
        expanded
    })
}

fn set_nibble(output: &mut [u8], position: usize, value: u8) {
    let target = &mut output[position / 2];
    if position.is_multiple_of(2) {
        *target |= value << 4;
    } else {
        *target |= value;
    }
}

fn to_i64(value: &Db2Value) -> Result<i64, String> {
    match value {
        Db2Value::Integer(number) => Ok(*number),
        Db2Value::Boolean(boolean) => Ok(*boolean as i64),
        Db2Value::Float(number) => {
            const I64_UPPER_EXCLUSIVE: f64 = 9_223_372_036_854_775_808.0;
            if !number.is_finite() || *number < i64::MIN as f64 || *number >= I64_UPPER_EXCLUSIVE {
                return Err(format!(
                    "value {number} cannot be represented as a Db2 BIGINT"
                ));
            }
            Ok(*number as i64)
        }
        Db2Value::Decimal(text) | Db2Value::Text(text) => parse_integer_text(text)
            .map_err(|error| format!("cannot encode '{text}' as a Db2 integer: {error}")),
        Db2Value::Bytes(_) => Err("a binary value cannot be bound to a Db2 integer".into()),
        Db2Value::Null => Err("cannot encode NULL as a non-null Db2 integer".into()),
        Db2Value::ProgressiveLobReference { .. } => {
            Err("cannot encode an unresolved Db2 progressive LOB reference".into())
        }
        Db2Value::Lob(_) => Err("cannot bind a row-stream Db2 LOB locator".into()),
    }
}

fn parse_integer_text(value: &str) -> Result<i64, String> {
    let text = value.trim();
    let (mantissa, exponent) = match text.find(['e', 'E']) {
        Some(index) => {
            let exponent_text = &text[index + 1..];
            if exponent_text.contains(['e', 'E']) {
                return Err("invalid numeric exponent".into());
            }
            let exponent = exponent_text
                .parse::<i64>()
                .map_err(|_| "invalid numeric exponent".to_owned())?;
            (&text[..index], exponent)
        }
        None => (text, 0),
    };

    let (negative, unsigned) = if let Some(unsigned) = mantissa.strip_prefix('-') {
        (true, unsigned)
    } else if let Some(unsigned) = mantissa.strip_prefix('+') {
        (false, unsigned)
    } else {
        (false, mantissa)
    };
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) if !fraction.contains('.') => (whole, fraction),
        Some(_) => return Err("invalid numeric decimal point".into()),
        None => (unsigned, ""),
    };
    if whole.is_empty() && fraction.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("invalid numeric digits".into());
    }

    let digits = format!("{whole}{fraction}");
    let significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return Ok(0);
    }
    let fraction_len = fraction.len() as i64;
    if exponent > fraction_len + 19 {
        return Err("value is outside the signed BIGINT range".into());
    }
    if exponent < fraction_len - significant.len() as i64 {
        return Ok(0);
    }
    let integer_len = significant.len() as i64 - fraction_len + exponent;
    if integer_len <= 0 {
        return Ok(0);
    }
    if integer_len > 19 {
        return Err("value is outside the signed BIGINT range".into());
    }

    let integer_len = integer_len as usize;
    let integer_digits = if integer_len <= significant.len() {
        significant[..integer_len].to_owned()
    } else {
        format!(
            "{significant}{}",
            "0".repeat(integer_len - significant.len())
        )
    };
    let signed = if negative {
        format!("-{integer_digits}")
    } else {
        integer_digits
    };
    signed
        .parse::<i64>()
        .map_err(|_| "value is outside the signed BIGINT range".into())
}

fn to_f64(value: &Db2Value) -> Result<f64, String> {
    match value {
        Db2Value::Float(number) => Ok(*number),
        Db2Value::Integer(number) => Ok(*number as f64),
        Db2Value::Boolean(boolean) => Ok(*boolean as u8 as f64),
        Db2Value::Decimal(text) | Db2Value::Text(text) => text
            .parse()
            .map_err(|error| format!("cannot encode '{text}' as a Db2 float: {error}")),
        Db2Value::Bytes(_) => {
            Err("a binary value cannot be bound to a Db2 floating-point value".into())
        }
        Db2Value::Null => Err("cannot encode NULL as a non-null Db2 float".into()),
        Db2Value::ProgressiveLobReference { .. } => {
            Err("cannot encode an unresolved Db2 progressive LOB reference".into())
        }
        Db2Value::Lob(_) => Err("cannot bind a row-stream Db2 LOB locator".into()),
    }
}

fn to_bool(value: &Db2Value) -> Result<bool, String> {
    match value {
        Db2Value::Boolean(value) => Ok(*value),
        Db2Value::Integer(value) => Ok(*value != 0),
        Db2Value::Text(value) => parse_boolean_text(value),
        _ => Err("cannot encode this value as a Db2 boolean".into()),
    }
}

fn parse_boolean_text(value: &str) -> Result<bool, String> {
    let text = value.trim();
    if ["true", "yes", "y", "on"]
        .iter()
        .any(|accepted| text.eq_ignore_ascii_case(accepted))
    {
        return Ok(true);
    }
    if ["false", "no", "n", "off"]
        .iter()
        .any(|accepted| text.eq_ignore_ascii_case(accepted))
    {
        return Ok(false);
    }
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("cannot encode '{value}' as a Db2 boolean"));
    }
    Ok(digits.bytes().any(|byte| byte != b'0'))
}

fn to_text(value: &Db2Value) -> Result<String, String> {
    match value {
        Db2Value::Text(text) | Db2Value::Decimal(text) => Ok(text.clone()),
        Db2Value::Integer(number) => Ok(number.to_string()),
        Db2Value::Float(number) => Ok(number.to_string()),
        Db2Value::Boolean(value) => Ok(if *value { "1" } else { "0" }.into()),
        Db2Value::Bytes(bytes) => {
            String::from_utf8(bytes.clone()).map_err(|error| error.to_string())
        }
        Db2Value::Null => Err("cannot encode NULL as a non-null Db2 text value".into()),
        Db2Value::ProgressiveLobReference { .. } => {
            Err("cannot encode an unresolved Db2 progressive LOB reference".into())
        }
        Db2Value::Lob(_) => Err("cannot bind a row-stream Db2 LOB locator".into()),
    }
}

fn format_date(value: &str, kind: &str) -> Result<String, String> {
    let value = value.trim();
    match kind {
        "date" => {
            if let Some(date) = parse_db2_date(value) {
                return Ok(date);
            }
            if let Some(timestamp) = parse_db2_timestamp(value) {
                return Ok(timestamp[..10].to_owned());
            }
        }
        "time" => {
            if let Some(time) = parse_db2_time(value) {
                return Ok(time);
            }
            if let Some(timestamp) = parse_db2_timestamp(value) {
                return Ok(timestamp[11..19].replace('.', ":"));
            }
        }
        "timestamp" => {
            if let Some(timestamp) = parse_db2_timestamp(value) {
                return Ok(timestamp);
            }
        }
        _ => {}
    }
    Err(format!("cannot convert '{value}' to a Db2 {kind}"))
}

fn parse_db2_date(value: &str) -> Option<String> {
    let (year_text, month_text, day_text) = if let Some((month, rest)) = value.split_once('/') {
        let (day, year) = rest.split_once('/')?;
        (year, month, day)
    } else {
        let mut parts = value.split('-');
        (parts.next()?, parts.next()?, parts.next()?)
    };
    if value.contains('/') && value.matches('/').count() != 2
        || value.contains('-') && !value.contains('/') && value.matches('-').count() != 2
    {
        return None;
    }
    let year = parse_digits(year_text, 4, 4)?;
    let month = parse_digits(month_text, 1, 2)?;
    let day = parse_digits(day_text, 1, 2)?;
    if year == 0 || month == 0 || month > 12 || day == 0 {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if day > days_in_month {
        return None;
    }
    Some(format!("{year:04}-{month:02}-{day:02}"))
}

fn parse_digits(value: &str, minimum: usize, maximum: usize) -> Option<u32> {
    if !(minimum..=maximum).contains(&value.len())
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn parse_db2_time(value: &str) -> Option<String> {
    let parts = value.split([':', '.']).collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return None;
    }
    let hour = parse_digits(parts[0], 1, 2)?;
    let minute = parse_digits(parts[1], 2, 2)?;
    let second = if parts.len() == 3 {
        parse_digits(parts[2], 2, 2)?
    } else {
        0
    };
    if minute > 59 || second > 59 || hour > 24 || hour == 24 && (minute != 0 || second != 0) {
        return None;
    }
    Some(format!("{hour:02}:{minute:02}:{second:02}"))
}

fn parse_db2_timestamp(value: &str) -> Option<String> {
    if let Some(date) = parse_db2_date(value) {
        return Some(format!("{date}-00.00.00.000000"));
    }
    for (index, separator) in value.char_indices() {
        if !matches!(separator, 'T' | ' ' | '-') {
            continue;
        }
        let Some(date) = parse_db2_date(&value[..index]) else {
            continue;
        };
        let time = &value[index + separator.len_utf8()..];
        if let Some((hour, minute, second, fraction)) = parse_db2_timestamp_time(time) {
            return Some(format!(
                "{date}-{hour:02}.{minute:02}.{second:02}.{fraction}"
            ));
        }
    }
    None
}

fn parse_db2_timestamp_time(value: &str) -> Option<(u32, u32, u32, String)> {
    let parts = value.split([':', '.']).collect::<Vec<_>>();
    if !(3..=4).contains(&parts.len()) {
        return None;
    }
    let hour = parse_digits(parts[0], 1, 2)?;
    let minute = parse_digits(parts[1], 2, 2)?;
    let second = parse_digits(parts[2], 2, 2)?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let fraction = if parts.len() == 4 {
        let value = parts[3];
        if !(1..=12).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        value.to_owned()
    } else {
        String::new()
    };
    Some((hour, minute, second, format!("{fraction:0<6}")))
}
#[cfg(test)]
mod tests {
    use super::*;

    fn column(sql_type: u16, length: u64, precision: u16, scale: u16) -> ColumnDescription {
        ColumnDescription {
            name: String::new(),
            sql_type,
            length,
            precision,
            scale,
            nullable: true,
        }
    }

    #[test]
    fn mixed_empty_blobs_omit_redundant_data_objects_and_keep_order() {
        let columns = [
            column(405, 100, 0, 0),
            column(405, 100, 0, 0),
            column(405, 100, 0, 0),
        ];
        for empty in 0..3 {
            let mut values = [
                Db2Value::Bytes(vec![1]),
                Db2Value::Bytes(vec![2]),
                Db2Value::Bytes(vec![3]),
            ];
            values[empty] = Db2Value::Bytes(Vec::new());
            let encoded = build_sql_dta_with_external_data(&columns, &values).unwrap();
            assert_eq!(encoded.external_data.len(), 2);
            let mut next = 0;
            for (index, value) in values.iter().enumerate() {
                if index == empty {
                    continue;
                }
                let Db2Value::Bytes(bytes) = value else {
                    unreachable!()
                };
                let EncodedExternalData::DdmObject(actual) = &encoded.external_data[next] else {
                    panic!("expected materialized object")
                };
                assert_eq!(*actual, pack_external_lob_object(bytes).unwrap());
                next += 1;
            }
        }
        let encoded =
            build_sql_dta_with_external_data(&columns[..1], &[Db2Value::Bytes(Vec::new())])
                .unwrap();
        assert_eq!(encoded.external_data.len(), 1);
    }

    #[test]
    fn sql_dta_matches_go_reference_vector_byte_for_byte() {
        let columns = [
            column(497, 4, 0, 0),
            column(449, 100, 0, 0),
            column(481, 8, 0, 0),
            column(2437, 2, 0, 0),
            column(385, 10, 0, 0),
        ];
        let values = [
            Db2Value::Integer(1),
            Db2Value::Text("Teclado Mecânico".into()),
            Db2Value::Float(250.50),
            Db2Value::Boolean(true),
            Db2Value::Text("2026-08-26".into()),
        ];
        let actual = build_sql_dta(&columns, &values).unwrap();
        let expected = "00642412001c00101276d0030004393fff0b000805000221000a0671e4d000010044147a000001000000000010005400650063006c00610064006f0020004d0065006300e2006e00690063006f000000000000506f4000010000323032362d30382d3236";
        assert_eq!(
            actual
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            expected
        );
    }

    #[test]
    fn sql_dta_uses_cpt_continuation_groups_and_preserves_wide_parameter_values() {
        for (count, expected_groups) in [(83usize, 1usize), (84, 1), (85, 2), (148, 2)] {
            let columns = (0..count).map(|_| column(501, 2, 0, 0)).collect::<Vec<_>>();
            let values = (0..count)
                .map(|index| {
                    if index % 11 == 0 {
                        Db2Value::Null
                    } else {
                        Db2Value::Integer(index as i64)
                    }
                })
                .collect::<Vec<_>>();
            let encoded = build_sql_dta(&columns, &values).unwrap();
            let objects = crate::network::ddm::parse_ddm_parameters(&encoded[4..]).unwrap();
            let descriptor = &objects[&cp::FDODSC];
            let data = &objects[&cp::FDODTA];

            let mut offset = 0;
            let mut groups = 0;
            let mut fields = 0;
            while offset < descriptor.len() - 6 {
                let group_length = descriptor[offset] as usize;
                assert!((6..=255).contains(&group_length));
                assert_eq!((group_length - 3) % 3, 0);
                assert_eq!(
                    descriptor[offset + 1],
                    if groups == 0 { 0x76 } else { 0x7f }
                );
                assert_eq!(
                    descriptor[offset + 2],
                    if groups == 0 { 0xd0 } else { 0x00 }
                );
                let group_fields = (group_length - 3) / 3;
                for field in 0..group_fields {
                    assert_eq!(
                        &descriptor[offset + 3 + field * 3..offset + 6 + field * 3],
                        &[0x05, 0, 2]
                    );
                }
                fields += group_fields;
                groups += 1;
                offset += group_length;
            }
            assert_eq!(groups, expected_groups);
            assert_eq!(fields, count);
            assert_eq!(&descriptor[offset..], &[0x06, 0x71, 0xe4, 0xd0, 0, 1]);

            assert_eq!(data[0], 0); // FDODTA leading alignment byte.
            offset = 1;
            for index in 0..count {
                if index % 11 == 0 {
                    assert_eq!(data[offset], 0xff);
                    offset += 1;
                    continue;
                }
                assert_eq!(data[offset], 0);
                offset += 1;
                assert_eq!(
                    i16::from_le_bytes([data[offset], data[offset + 1]]),
                    index as i16
                );
                offset += 2;
            }
            assert_eq!(offset, data.len());
        }
    }

    #[test]
    fn sql_dta_normalizes_db2_date_and_time_text_without_changing_its_meaning() {
        fn assert_text(sql_type: u16, length: u64, value: &str, expected: &str) {
            let encoded = build_sql_dta(
                &[column(sql_type, length, 0, 0)],
                &[Db2Value::Text(value.into())],
            )
            .unwrap();
            let suffix = [vec![0], expected.as_bytes().to_vec()].concat();
            assert!(
                encoded.ends_with(&suffix),
                "type {sql_type}, input {value:?}, expected {expected:?}"
            );
        }

        assert_text(385, 10, "2024-2-9", "2024-02-09");
        assert_text(385, 10, "02/29/2024", "2024-02-29");
        assert_text(389, 8, "12:34", "12:34:00");
        assert_text(389, 8, "23.59.58", "23:59:58");
        assert_text(
            393,
            32,
            "2024-02-29T23:59:58.123456",
            "2024-02-29-23.59.58.123456      ",
        );
        assert_text(
            393,
            32,
            "2024-02-29T23:59:58.123456789012",
            "2024-02-29-23.59.58.123456789012",
        );
        assert_text(393, 32, "2024-02-29", "2024-02-29-00.00.00.000000      ");

        for (sql_type, length, value) in [
            (385, 10, "2024-02-30"),
            (389, 8, "23:59:58.123456"),
            (389, 8, "24:00:01"),
            (393, 32, "2024-02-30 23:59:58"),
            (393, 32, "2024-02-29 23:59:58+02:00"),
        ] {
            assert!(
                build_sql_dta(
                    &[column(sql_type, length, 0, 0)],
                    &[Db2Value::Text(value.into())]
                )
                .is_err()
            );
        }
    }

    #[test]
    fn sql_dta_includes_alignment_byte_for_even_decimal_payloads() {
        let actual =
            build_sql_dta(&[column(485, 0, 4, 2)], &[Db2Value::Decimal("1.25".into())]).unwrap();
        let encoded = actual
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert!(encoded.ends_with("0009147a000000125c"));
    }

    #[test]
    fn sql_dta_describes_nullable_varbinary_with_its_type_and_length() {
        let encoded = build_sql_dta(&[column(909, 10, 0, 0)], &[Db2Value::Null]).unwrap();
        assert!(encoded.windows(3).any(|bytes| bytes == [0xc3, 0, 10]));
    }

    #[test]
    fn sql_dta_rejects_integer_overflow_instead_of_wrapping() {
        assert!(build_sql_dta(&[column(501, 2, 0, 0)], &[Db2Value::Integer(32768)]).is_err());
        assert!(build_sql_dta(&[column(501, 2, 0, 0)], &[Db2Value::Integer(-32768)]).is_ok());
        assert_eq!(
            build_sql_dta(
                &[column(493, 8, 0, 0)],
                &[Db2Value::Text("-9223372036854775808.0".into())],
            )
            .unwrap()
            .last(),
            Some(&0x80)
        );
        for value in ["-9223372036854775809.0", "-1e19"] {
            assert!(
                build_sql_dta(&[column(493, 8, 0, 0)], &[Db2Value::Text(value.into())]).is_err()
            );
        }
    }

    #[test]
    fn sql_dta_truncates_fractional_float_values_toward_zero_for_integer_parameters() {
        for (value, expected) in [
            (-3.75, -3),
            (-1.5, -1),
            (-0.5, 0),
            (0.5, 0),
            (1.5, 1),
            (3.75, 3),
        ] {
            let encoded =
                build_sql_dta(&[column(501, 2, 0, 0)], &[Db2Value::Float(value)]).unwrap();
            let actual = i16::from_le_bytes(
                encoded[encoded.len() - 2..]
                    .try_into()
                    .expect("encoded SMALLINT payload has two bytes"),
            );
            assert_eq!(actual, expected, "value {value}");
        }
    }

    #[test]
    fn sql_dta_integer_parameters_truncate_db2_numeric_text_and_accept_booleans() {
        let accepted = [
            (Db2Value::Text("-3.75".into()), -3),
            (Db2Value::Text("1.5".into()), 1),
            (Db2Value::Text(" 1.5 ".into()), 1),
            (Db2Value::Text("1e0".into()), 1),
            (Db2Value::Text(".5".into()), 0),
            (Db2Value::Text("1.".into()), 1),
            (Db2Value::Text("0.000".into()), 0),
            (Db2Value::Text("+0001.50".into()), 1),
            (Db2Value::Boolean(true), 1),
            (Db2Value::Boolean(false), 0),
        ];

        for (value, expected) in accepted {
            let encoded = build_sql_dta(&[column(501, 2, 0, 0)], &[value]).unwrap();
            let actual = i16::from_le_bytes(
                encoded[encoded.len() - 2..]
                    .try_into()
                    .expect("encoded SMALLINT payload has two bytes"),
            );
            assert_eq!(actual, expected);
        }

        for invalid in ["1,5", "abc", ""] {
            assert!(
                build_sql_dta(&[column(501, 2, 0, 0)], &[Db2Value::Text(invalid.into())]).is_err()
            );
        }
        assert!(
            build_sql_dta(&[column(501, 2, 0, 0)], &[Db2Value::Text("32768.0".into())]).is_err()
        );
    }

    #[test]
    fn sql_dta_boolean_parameters_match_db2_text_and_integer_conversions() {
        let boolean = column(2437, 1, 0, 0);
        let accepted = [
            (Db2Value::Boolean(true), true),
            (Db2Value::Boolean(false), false),
            (Db2Value::Text("true".into()), true),
            (Db2Value::Text(" FALSE ".into()), false),
            (Db2Value::Text("YES".into()), true),
            (Db2Value::Text("no".into()), false),
            (Db2Value::Text("y".into()), true),
            (Db2Value::Text("N".into()), false),
            (Db2Value::Text("On".into()), true),
            (Db2Value::Text("OFF".into()), false),
            (Db2Value::Text("0".into()), false),
            (Db2Value::Text("2".into()), true),
            (Db2Value::Text("-0".into()), false),
            (Db2Value::Text(" +2 ".into()), true),
            (
                Db2Value::Text("99999999999999999999999999999999".into()),
                true,
            ),
            (Db2Value::Integer(0), false),
            (Db2Value::Integer(2), true),
            (Db2Value::Integer(-1), true),
            (Db2Value::Integer(i64::MAX), true),
        ];

        for (value, expected) in accepted {
            let encoded = build_sql_dta(std::slice::from_ref(&boolean), &[value]).unwrap();
            assert_eq!(encoded.last(), Some(&(expected as u8)));
        }

        for invalid in [
            Db2Value::Text("".into()),
            Db2Value::Text(" ".into()),
            Db2Value::Text("1.0".into()),
            Db2Value::Text("maybe".into()),
            Db2Value::Float(1.5),
            Db2Value::Decimal("1.5".into()),
        ] {
            assert!(build_sql_dta(std::slice::from_ref(&boolean), &[invalid]).is_err());
        }
    }

    #[test]
    fn sql_dta_matches_ibm_cli_source_type_rules_for_numeric_text_and_lob_targets() {
        let varchar = column(449, 20, 0, 0);
        for (value, expected) in [
            (Db2Value::Boolean(true), b'1'),
            (Db2Value::Boolean(false), b'0'),
        ] {
            let encoded = build_sql_dta(std::slice::from_ref(&varchar), &[value]).unwrap();
            assert_eq!(&encoded[encoded.len() - 2..], &[0, expected]);
        }
        let utf8_text = build_sql_dta(
            std::slice::from_ref(&varchar),
            &[Db2Value::Bytes(vec![0xe6, 0x9d, 0xb1])],
        )
        .unwrap();
        assert!(
            utf8_text
                .windows(3)
                .any(|window| window == [0x3f, 0x7f, 0xff])
        );
        assert_eq!(
            &utf8_text[utf8_text.len() - 6..],
            &[0, 0, 3, 0xe6, 0x9d, 0xb1]
        );
        let invalid_utf8_text = build_sql_dta(
            std::slice::from_ref(&varchar),
            &[Db2Value::Bytes(vec![0xe9])],
        )
        .unwrap();
        assert!(
            invalid_utf8_text
                .windows(3)
                .any(|window| window == [0x3f, 0x7f, 0xff])
        );
        assert_eq!(
            &invalid_utf8_text[invalid_utf8_text.len() - 4..],
            &[0, 0, 1, 0xe9]
        );

        let numeric_targets = [
            column(501, 2, 0, 0),
            column(497, 4, 0, 0),
            column(493, 8, 0, 0),
            column(481, 4, 0, 0),
            column(481, 8, 0, 0),
            column(485, 0, 9, 3),
            column(997, 16, 0, 0),
        ];
        for target in &numeric_targets {
            for value in [Db2Value::Boolean(false), Db2Value::Boolean(true)] {
                assert!(
                    build_sql_dta(std::slice::from_ref(target), &[value]).is_ok(),
                    "{}",
                    target.sql_type
                );
            }
            assert!(
                build_sql_dta(
                    std::slice::from_ref(target),
                    &[Db2Value::Bytes(b"123".to_vec())]
                )
                .is_err(),
                "{}",
                target.sql_type
            );
        }

        for target in [
            column(913, 4, 0, 0),
            column(909, 10, 0, 0),
            column(405, 10, 0, 0),
        ] {
            for value in [
                Db2Value::Boolean(true),
                Db2Value::Integer(123),
                Db2Value::Decimal("12.50".into()),
            ] {
                assert!(
                    build_sql_dta(std::slice::from_ref(&target), &[value]).is_err(),
                    "{}",
                    target.sql_type
                );
            }
            assert!(
                build_sql_dta(
                    std::slice::from_ref(&target),
                    &[Db2Value::Text("313233".into())]
                )
                .is_ok()
            );
            assert!(build_sql_dta(&[target], &[Db2Value::Bytes(b"123".to_vec())]).is_ok());
        }
        let fixed_binary_text =
            build_sql_dta(&[column(913, 4, 0, 0)], &[Db2Value::Text("313233".into())]).unwrap();
        assert_eq!(
            &fixed_binary_text[fixed_binary_text.len() - 5..],
            &[0, 0x31, 0x32, 0x33, 0]
        );
        let variable_binary_text =
            build_sql_dta(&[column(909, 4, 0, 0)], &[Db2Value::Text("aBcD".into())]).unwrap();
        assert_eq!(
            &variable_binary_text[variable_binary_text.len() - 5..],
            &[0, 0, 2, 0xab, 0xcd]
        );
        let empty_binary_text =
            build_sql_dta(&[column(909, 4, 0, 0)], &[Db2Value::Text("".into())]).unwrap();
        assert_eq!(
            &empty_binary_text[empty_binary_text.len() - 3..],
            &[0, 0, 0]
        );
        for invalid in ["123", "12G4"] {
            assert!(
                build_sql_dta(&[column(909, 10, 0, 0)], &[Db2Value::Text(invalid.into())]).is_err()
            );
        }
        assert!(
            build_sql_dta(&[column(909, 2, 0, 0)], &[Db2Value::Text("313233".into())]).is_err()
        );
        let blob_text = build_sql_dta_with_external_data(
            &[column(405, 4, 0, 0)],
            &[Db2Value::Text("313233".into())],
        )
        .unwrap();
        match &blob_text.external_data[0] {
            EncodedExternalData::DdmObject(object) => {
                assert!(object.windows(4).any(|w| w == [0, 0x31, 0x32, 0x33]))
            }
            EncodedExternalData::LobStream { .. } => {
                panic!("text BLOB parameter must use EXTDTA bytes")
            }
        }

        for target in [
            column(409, 20, 0, 0),
            column(413, 20, 0, 0),
            column(988, 20, 0, 0),
        ] {
            for value in [
                Db2Value::Boolean(true),
                Db2Value::Integer(123),
                Db2Value::Float(1.5),
                Db2Value::Decimal("12.50".into()),
            ] {
                assert!(
                    build_sql_dta(std::slice::from_ref(&target), &[value]).is_err(),
                    "{}",
                    target.sql_type
                );
            }
            assert!(
                build_sql_dta(
                    std::slice::from_ref(&target),
                    &[Db2Value::Text("text".into())]
                )
                .is_ok()
            );
            assert!(build_sql_dta(&[target], &[Db2Value::Bytes(b"text".to_vec())]).is_ok());
        }
        let clob_text = build_sql_dta(
            &[column(409, 20, 0, 0)],
            &[Db2Value::Bytes(b"text".to_vec())],
        )
        .unwrap();
        assert_eq!(
            &clob_text[clob_text.len() - 7..],
            &[0, 0, 4, b't', b'e', b'x', b't']
        );
        let dbclob_text = build_sql_dta(
            &[column(413, 20, 0, 0)],
            &[Db2Value::Bytes(vec![0x31, 0x32, 0x33])],
        )
        .unwrap();
        assert_eq!(
            &dbclob_text[dbclob_text.len() - 5..],
            &[0, 0, 1, 0x31, 0x32]
        );
        let xml_text = build_sql_dta(
            &[column(988, 20, 0, 0)],
            &[Db2Value::Bytes(b"<r/>".to_vec())],
        )
        .unwrap();
        assert_eq!(
            &xml_text[xml_text.len() - 7..],
            &[0, 0, 4, b'<', b'r', b'/', b'>']
        );
    }

    #[test]
    fn packed_decimal_truncates_fraction_and_rejects_integer_overflow() {
        let encoded = build_sql_dta(
            &[column(485, 0, 5, 2)],
            &[Db2Value::Decimal("1.236".into())],
        )
        .unwrap();
        let hex = encoded
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert!(hex.ends_with("000000123c"));
        assert!(
            build_sql_dta(&[column(485, 0, 2, 2)], &[Db2Value::Decimal("1.25".into())],).is_err()
        );
    }

    #[test]
    fn xml_parameter_length_uses_utf16_code_units() {
        let xml = "<root><message>XML café</message></root>";
        let actual = build_sql_dta(&[column(988, 0, 0, 0)], &[Db2Value::Text(xml.into())]).unwrap();
        let units: Vec<u16> = xml.encode_utf16().collect();
        let mut expected = vec![0, (units.len() >> 8) as u8, units.len() as u8];
        for unit in units {
            expected.extend_from_slice(&unit.to_be_bytes());
        }
        assert!(actual.ends_with(&expected));
    }

    #[test]
    fn large_blob_parameters_use_ordered_extdta_objects_and_bounded_sqldta() {
        let first = vec![0xa5; 70_000];
        let second = vec![0x5a; 65_537];
        let columns = [
            column(405, 0, 0, 0),
            column(497, 4, 0, 0),
            column(405, 0, 0, 0),
        ];
        let values = [
            Db2Value::Bytes(first.clone()),
            Db2Value::Integer(7),
            Db2Value::Bytes(second.clone()),
        ];
        let encoded = build_sql_dta_with_external_data(&columns, &values).unwrap();
        assert!(encoded.sql_dta.len() < 256);
        assert_eq!(
            encoded
                .sql_dta
                .windows(3)
                .filter(|part| *part == [0xc9, 0x80, 9])
                .count(),
            2
        );
        assert_eq!(encoded.external_data.len(), 2);
        assert!(
            encoded
                .sql_dta
                .windows(11)
                .any(|part| part == [0x00, 0x00, 0x02, 0, 0, 0, 0, 0, 0x01, 0x11, 0x70])
        );
        assert!(
            encoded
                .sql_dta
                .windows(11)
                .any(|part| part == [0x00, 0x00, 0x02, 0, 0, 0, 0, 0, 0x01, 0x00, 0x01])
        );
        for (object, bytes) in encoded.external_data.iter().zip([first, second]) {
            let EncodedExternalData::DdmObject(object) = object else {
                panic!("materialized BLOB must use a packed EXTDTA object");
            };
            assert_eq!(&object[..4], &[0x80, 0x08, 0x14, 0x6c]);
            assert_eq!(
                u32::from_be_bytes(object[4..8].try_into().unwrap()),
                bytes.len() as u32 + 1
            );
            assert_eq!(object[8], 0);
            assert_eq!(&object[9..], bytes);
        }
    }

    #[test]
    fn graphic_and_dbclob_encoder_preserves_utf16_padding_and_replacement() {
        let graphic_value = Db2Value::Text("A😀".into());
        let mut graphic = Vec::new();
        append_parameter(
            &mut graphic,
            &column(468, 4, 0, 0),
            &Db2InputParameter::Value(&graphic_value),
            None,
        )
        .unwrap();
        assert_eq!(graphic, [0, 0, 0x41, 0xd8, 0x3d, 0xde, 0x00, 0x00, 0x20]);

        let dbclob_value = Db2Value::Bytes(vec![0xd8, 0x00, 0x00, 0x41]);
        let mut dbclob = Vec::new();
        append_parameter(
            &mut dbclob,
            &column(412, 0, 0, 0),
            &Db2InputParameter::Value(&dbclob_value),
            None,
        )
        .unwrap();
        assert_eq!(dbclob, [0, 0, 2, 0xff, 0xfd, 0, 0x41]);
    }

    #[test]
    fn external_lob_writer_encodes_short_and_extended_headers_without_payload_staging() {
        assert_eq!(
            pack_external_lob_object(&[0xaa]).unwrap(),
            [0, 6, 0x14, 0x6c, 0, 0xaa]
        );
        let large = pack_external_lob_object(&[0x5a; 32_768]).unwrap();
        assert_eq!(&large[..8], &[0x80, 0x08, 0x14, 0x6c, 0, 0, 0x80, 1]);
        assert_eq!(large[8], 0);
        assert_eq!(large.len(), 8 + 1 + 32_768);
    }

    #[test]
    fn small_and_null_blob_parameters_use_extdta_only_for_non_null_values() {
        let columns = [column(405, 0, 0, 0), column(405, 0, 0, 0)];
        let values = [Db2Value::Bytes(vec![0, 0xff, 0x2a]), Db2Value::Null];
        let encoded = build_sql_dta_with_external_data(&columns, &values).unwrap();
        assert_eq!(encoded.external_data.len(), 1);
        assert!(
            encoded
                .sql_dta
                .windows(3)
                .any(|part| part == [0xc9, 0x80, 9])
        );
    }

    #[tokio::test]
    async fn streamed_blob_inputs_encode_the_declared_length_without_reading_source() {
        let value = Db2Value::Integer(7);
        let mut source = tokio::io::BufReader::new(std::io::Cursor::new(vec![0x5a; 80_000]));
        let inputs = [
            Db2InputParameter::Value(&value),
            Db2InputParameter::BlobStream {
                source: &mut source,
                length: 80_000,
            },
        ];
        let columns = [column(497, 4, 0, 0), column(405, 1_048_576, 0, 0)];
        let encoded = build_sql_dta_with_inputs(&columns, &inputs).unwrap();
        assert!(encoded.sql_dta.len() < 256);
        assert_eq!(encoded.external_data.len(), 1);
        assert!(matches!(
            encoded.external_data[0],
            EncodedExternalData::LobStream {
                parameter_index: 1,
                length: 80_000
            }
        ));
        assert!(
            encoded
                .sql_dta
                .windows(3)
                .any(|part| part == [0xc9, 0x80, 9])
        );
        assert!(
            encoded
                .sql_dta
                .windows(11)
                .any(|part| part == [0, 0, 2, 0, 0, 0, 0, 0, 1, 0x38, 0x80])
        );
        assert_eq!(source.get_ref().position(), 0);
    }

    #[tokio::test]
    async fn streamed_clob_and_xml_inputs_use_typed_extdta_descriptors_without_reading_sources() {
        let mut clob_source = tokio::io::BufReader::new(std::io::Cursor::new(vec![0x61; 80_000]));
        let mut dbclob_source = tokio::io::BufReader::new(std::io::Cursor::new(vec![0x62; 85_000]));
        let mut xml_source = tokio::io::BufReader::new(std::io::Cursor::new(vec![0x78; 90_001]));
        let inputs = [
            Db2InputParameter::ClobStream {
                source: &mut clob_source,
                length: 80_000,
            },
            Db2InputParameter::DbClobStream {
                source: &mut dbclob_source,
                length: 85_000,
            },
            Db2InputParameter::XmlStream {
                source: &mut xml_source,
                length: 90_001,
            },
        ];
        let columns = [
            column(409, 2_097_152, 0, 0),
            column(413, 2_097_152, 0, 0),
            column(989, 2_097_152, 0, 0),
        ];
        let encoded = build_sql_dta_with_inputs(&columns, &inputs).unwrap();
        assert!(encoded.sql_dta.len() < 256);
        assert!(
            encoded
                .sql_dta
                .windows(3)
                .any(|part| part == [0xcf, 0x80, 9])
        );
        assert_eq!(
            encoded
                .sql_dta
                .windows(3)
                .filter(|part| *part == [0xcf, 0x80, 9])
                .count(),
            2
        );
        assert!(
            encoded
                .sql_dta
                .windows(3)
                .any(|part| part == [0xc7, 0x80, 9])
        );
        assert!(matches!(
            encoded.external_data[0],
            EncodedExternalData::LobStream {
                parameter_index: 0,
                length: 80_000
            }
        ));
        assert!(matches!(
            encoded.external_data[1],
            EncodedExternalData::LobStream {
                parameter_index: 1,
                length: 85_000
            }
        ));
        assert!(matches!(
            encoded.external_data[2],
            EncodedExternalData::LobStream {
                parameter_index: 2,
                length: 90_001
            }
        ));
        assert_eq!(clob_source.get_ref().position(), 0);
        assert_eq!(dbclob_source.get_ref().position(), 0);
        assert_eq!(xml_source.get_ref().position(), 0);
    }

    #[tokio::test]
    async fn zero_length_text_lob_streams_are_inline_and_empty_xml_is_rejected() {
        let mut clob_source = tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        let mut dbclob_source = tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        let xml_bytes = b"<empty/>";
        let mut xml_source = tokio::io::BufReader::new(std::io::Cursor::new(xml_bytes));
        let inputs = [
            Db2InputParameter::ClobStream {
                source: &mut clob_source,
                length: 0,
            },
            Db2InputParameter::DbClobStream {
                source: &mut dbclob_source,
                length: 0,
            },
            Db2InputParameter::XmlStream {
                source: &mut xml_source,
                length: xml_bytes.len() as u64,
            },
        ];
        let columns = [
            column(409, 2_097_152, 0, 0),
            column(413, 2_097_152, 0, 0),
            column(989, 2_097_152, 0, 0),
        ];
        let encoded = build_sql_dta_with_inputs(&columns, &inputs).unwrap();
        assert_eq!(encoded.external_data.len(), 1);
        assert!(matches!(
            encoded.external_data[0],
            EncodedExternalData::LobStream {
                parameter_index: 2,
                length: 8
            }
        ));
        assert!(
            !encoded
                .sql_dta
                .windows(3)
                .any(|part| part == [0xcf, 0x80, 9])
        );
        assert!(
            encoded
                .sql_dta
                .windows(3)
                .any(|part| part == [0xc7, 0x80, 9])
        );
        assert_eq!(clob_source.get_ref().position(), 0);
        assert_eq!(dbclob_source.get_ref().position(), 0);

        let mut empty_blob = tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        let blob = build_sql_dta_with_inputs(
            &[column(405, 1_048_576, 0, 0)],
            &[Db2InputParameter::BlobStream {
                source: &mut empty_blob,
                length: 0,
            }],
        )
        .unwrap();
        assert!(matches!(
            blob.external_data.as_slice(),
            [EncodedExternalData::LobStream {
                parameter_index: 0,
                length: 0
            }]
        ));
        assert!(blob.sql_dta.windows(3).any(|part| part == [0xc9, 0x80, 9]));

        let mut empty_xml = tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        let error = build_sql_dta_with_inputs(
            &[column(989, 2_097_152, 0, 0)],
            &[Db2InputParameter::XmlStream {
                source: &mut empty_xml,
                length: 0,
            }],
        )
        .err()
        .unwrap();
        assert!(error.contains("XML stream must contain a non-empty XML document"));
    }

    #[tokio::test]
    async fn rejects_every_mismatched_typed_lob_stream_before_reading_its_source() {
        let targets = [(405, "BLOB"), (409, "CLOB"), (413, "DBCLOB"), (989, "XML")];
        let streams = [(404, "BLOB"), (408, "CLOB"), (412, "DBCLOB"), (988, "XML")];

        for (target_type, target_name) in targets {
            for (stream_type, stream_name) in streams {
                if target_type == stream_type + 1 {
                    continue;
                }

                let mut source = tokio::io::BufReader::new(std::io::Cursor::new(vec![0x61; 3]));
                let input = match stream_type {
                    404 => Db2InputParameter::BlobStream {
                        source: &mut source,
                        length: 3,
                    },
                    408 => Db2InputParameter::ClobStream {
                        source: &mut source,
                        length: 3,
                    },
                    412 => Db2InputParameter::DbClobStream {
                        source: &mut source,
                        length: 3,
                    },
                    988 => Db2InputParameter::XmlStream {
                        source: &mut source,
                        length: 3,
                    },
                    _ => unreachable!(),
                };
                let inputs = [input];
                let error = match build_sql_dta_with_inputs(
                    &[column(target_type, 1_048_576, 0, 0)],
                    &inputs,
                ) {
                    Ok(_) => panic!("{stream_name} stream must be rejected for {target_name}"),
                    Err(error) => error,
                };

                assert!(
                    error.contains(stream_name),
                    "{stream_name} stream for {target_name} should identify the rejected stream: {error}"
                );
                let _ = inputs;
                assert_eq!(source.get_ref().position(), 0);
            }
        }
    }

    #[test]
    fn sql_dta_rejects_mismatched_value_count_and_decimal_metadata() {
        assert!(
            build_sql_dta(&[column(497, 4, 0, 0)], &[])
                .unwrap_err()
                .contains("expected 1 parameters, got 0")
        );
        assert!(
            build_sql_dta(
                &[column(485, 5, 32, 2)],
                &[Db2Value::Decimal("1.00".into())]
            )
            .unwrap_err()
            .contains("invalid Db2 decimal precision")
        );
    }
    #[test]
    fn large_materialized_text_lobs_borrow_utf8_and_preserve_inline_boundary() {
        for sql_type in [409, 413, 989] {
            let col = column(sql_type, 2_000_000, 0, 0);
            for text in [
                String::new(),
                "x".repeat(65535),
                format!("東京🚀{}", "x".repeat(65536)),
                "small".into(),
            ] {
                let value = Db2Value::Text(text.clone());
                let external = external_lob(&col, &value).unwrap();
                let large = text.encode_utf16().count() > 65535;
                assert_eq!(external.is_some(), large);
                if let Some(bytes) = external {
                    assert!(matches!(bytes, Cow::Borrowed(_)));
                    assert_eq!(bytes.as_ref(), text.as_bytes());
                }
                let encoded =
                    build_sql_dta_with_external_data(std::slice::from_ref(&col), &[value]).unwrap();
                assert_eq!(encoded.external_data.len(), usize::from(large));
                if large {
                    assert!(encoded.sql_dta.len() < 256);
                }
            }
            assert!(external_lob(&col, &Db2Value::Null).unwrap().is_none());
        }
        // UTF-8 byte length alone must not promote a valid inline UTF-16 value.
        let value = Db2Value::Text("東京".repeat(20000));
        assert!(
            external_lob(&column(409, 2_000_000, 0, 0), &value)
                .unwrap()
                .is_none()
        );
    }
}
