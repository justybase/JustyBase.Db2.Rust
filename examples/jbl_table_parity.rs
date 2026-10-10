use justybase_db2::{Db2LobEncoding, Db2Session, Db2Value};
use ring::digest::{SHA256, digest};
use std::fmt::Write as _;

const QUERIES: [(&str, &str); 8] = [
    ("JBL_CUSTOMERS", "SELECT * FROM JBL_LIVE.JBL_CUSTOMERS"),
    ("JBL_PRODUCTS", "SELECT * FROM JBL_LIVE.JBL_PRODUCTS"),
    ("JBL_DEPARTMENTS", "SELECT * FROM JBL_LIVE.JBL_DEPARTMENTS"),
    ("JBL_EMPLOYEES", "SELECT * FROM JBL_LIVE.JBL_EMPLOYEES"),
    ("JBL_ORDER_LINES", "SELECT * FROM JBL_LIVE.JBL_ORDER_LINES"),
    ("JBL_ORDERS", "SELECT * FROM JBL_LIVE.JBL_ORDERS"),
    ("JBL_PRODUCTS", "SELECT * FROM JBL_LIVE.JBL_PRODUCTS"),
    ("JBL_SALES_FACT", "SELECT * FROM JBL_LIVE.JBL_SALES_FACT"),
];

fn type_name(sql_type: u16) -> Result<&'static str, String> {
    let base = if sql_type % 2 == 1 {
        sql_type - 1
    } else {
        sql_type
    };
    match base {
        384 => Ok("DATE"),
        388 => Ok("TIME"),
        392 => Ok("TIMESTAMP"),
        404 => Ok("BLOB"),
        408 => Ok("CLOB"),
        412 => Ok("DBCLOB"),
        448 => Ok("VARCHAR"),
        452 => Ok("CHAR"),
        464 => Ok("VARGRAPHIC"),
        468 => Ok("GRAPHIC"),
        480 => Ok("FLOAT"),
        484 | 504 => Ok("DECIMAL"),
        492 => Ok("BIGINT"),
        496 => Ok("INTEGER"),
        500 => Ok("SMALLINT"),
        908 => Ok("VARBINARY"),
        912 => Ok("BINARY"),
        988 => Ok("XML"),
        996 => Ok("DECFLOAT"),
        2436 => Ok("BOOLEAN"),
        _ => Err(format!(
            "unmapped SQL type {sql_type} in requested table result"
        )),
    }
}

fn decimal_text(value: &str) -> Result<String, String> {
    let input = value.trim();
    if [
        "INFINITY",
        "INF",
        "NAN",
        "SNAN",
        "-INFINITY",
        "-INF",
        "-NAN",
        "-SNAN",
    ]
    .iter()
    .any(|value| input.eq_ignore_ascii_case(value))
    {
        return Ok(input.trim_start_matches('+').to_ascii_uppercase());
    }
    let sign = if input.starts_with('-') { "-" } else { "" };
    let unsigned = input
        .strip_prefix('-')
        .or_else(|| input.strip_prefix('+'))
        .unwrap_or(input);
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => (
            &unsigned[..index],
            unsigned[index + 1..]
                .parse::<i32>()
                .map_err(|_| format!("invalid decimal exponent {input}"))?,
        ),
        None => (unsigned, 0),
    };
    if !(-1024..=1024).contains(&exponent) {
        return Err(format!("decimal exponent out of range {input}"));
    }
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((whole, fraction)) => (whole.to_owned(), fraction.to_owned()),
        None => (mantissa.to_owned(), String::new()),
    };
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!("invalid decimal result {input}"));
    }
    let digits = format!("{whole}{fraction}");
    let point = whole.len() as i32 + exponent;
    let mut normalized_whole;
    let mut normalized_fraction;
    if point <= 0 {
        normalized_whole = "0".to_owned();
        normalized_fraction = format!("{}{}", "0".repeat((-point) as usize), digits);
    } else if point as usize >= digits.len() {
        normalized_whole = format!("{}{}", digits, "0".repeat(point as usize - digits.len()));
        normalized_fraction = String::new();
    } else {
        normalized_whole = digits[..point as usize].to_owned();
        normalized_fraction = digits[point as usize..].to_owned();
    }
    normalized_whole = normalized_whole.trim_start_matches('0').to_owned();
    if normalized_whole.is_empty() {
        normalized_whole.push('0');
    }
    while normalized_fraction.ends_with('0') {
        normalized_fraction.pop();
    }
    if normalized_whole == "0" && normalized_fraction.is_empty() {
        Ok("0".to_owned())
    } else if normalized_fraction.is_empty() {
        Ok(format!("{sign}{normalized_whole}"))
    } else {
        Ok(format!("{sign}{normalized_whole}.{normalized_fraction}"))
    }
}

fn canonical_value(value: &Db2Value, kind: &str) -> Result<Option<String>, String> {
    match value {
        Db2Value::Null => Ok(None),
        Db2Value::Integer(value) if matches!(kind, "SMALLINT" | "INTEGER" | "BIGINT") => {
            Ok(Some(value.to_string()))
        }
        Db2Value::Decimal(value) if matches!(kind, "DECIMAL" | "DECFLOAT") => {
            Ok(Some(decimal_text(value)?))
        }
        Db2Value::Float(value) if kind == "FLOAT" => {
            if value.is_nan() {
                Ok(Some("NAN".to_owned()))
            } else if *value == f64::INFINITY {
                Ok(Some("INFINITY".to_owned()))
            } else if *value == f64::NEG_INFINITY {
                Ok(Some("-INFINITY".to_owned()))
            } else if *value == 0.0 && value.is_sign_negative() {
                Ok(Some("-0".to_owned()))
            } else {
                Ok(Some(format!("{:016x}", value.to_bits())))
            }
        }
        Db2Value::Boolean(value) if kind == "BOOLEAN" => Ok(Some(value.to_string())),
        Db2Value::Text(value)
            if matches!(
                kind,
                "VARCHAR" | "CHAR" | "GRAPHIC" | "VARGRAPHIC" | "CLOB" | "DBCLOB" | "XML"
            ) =>
        {
            Ok(Some(value.clone()))
        }
        Db2Value::Text(value) if kind == "DATE" => {
            let date = value
                .get(..10)
                .ok_or_else(|| format!("invalid DATE result {value}"))?;
            if date.as_bytes().get(4) != Some(&b'-') || date.as_bytes().get(7) != Some(&b'-') {
                return Err(format!("invalid DATE result {value}"));
            }
            Ok(Some(date.to_owned()))
        }
        Db2Value::Text(value) if kind == "TIME" => Ok(Some(normalize_time(value)?)),
        Db2Value::Text(value) if kind == "TIMESTAMP" => Ok(Some(normalize_timestamp(value)?)),
        Db2Value::Bytes(value) if matches!(kind, "BINARY" | "VARBINARY" | "BLOB") => {
            let mut hex = String::with_capacity(value.len() * 2);
            for byte in value {
                write!(&mut hex, "{byte:02x}").map_err(|error| error.to_string())?;
            }
            Ok(Some(hex))
        }
        other => Err(format!("value {other:?} does not match DB2 type {kind}")),
    }
}

fn normalize_time(value: &str) -> Result<String, String> {
    let pieces: Vec<_> = value.split([':', '.']).collect();
    let (components, fraction) = match pieces.len() {
        3 => (&pieces[..], ""),
        4 => (&pieces[..3], pieces[3]),
        _ => return Err(format!("invalid TIME result {value}")),
    };
    if components.len() != 3
        || components
            .iter()
            .any(|part| part.len() != 2 || !part.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(format!("invalid TIME result {value}"));
    }
    if !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("invalid TIME result {value}"));
    }
    if fraction.len() > 12 {
        return Err(format!(
            "TIME/TIMESTAMP precision exceeds Db2 maximum: {value}"
        ));
    }
    let fraction = if fraction.len() < 6 {
        format!("{fraction:0<6}")
    } else {
        fraction.to_owned()
    };
    Ok(format!(
        "{}:{}:{}.{}",
        components[0], components[1], components[2], fraction
    ))
}

fn normalize_timestamp(value: &str) -> Result<String, String> {
    let (date, time) = value
        .split_once(' ')
        .or_else(|| {
            let bytes = value.as_bytes();
            if bytes.len() > 10 && bytes.get(10) == Some(&b'-') {
                Some((&value[..10], &value[11..]))
            } else {
                None
            }
        })
        .ok_or_else(|| format!("invalid TIMESTAMP result {value}"))?;
    let time = normalize_time(time)?;
    if date.len() != 10
        || date.as_bytes().get(4) != Some(&b'-')
        || date.as_bytes().get(7) != Some(&b'-')
    {
        return Err(format!("invalid TIMESTAMP result {value}"));
    }
    Ok(format!("{date} {time}"))
}

fn add_part(output: &mut Vec<u8>, value: &[u8]) -> Result<(), String> {
    let length =
        u32::try_from(value.len()).map_err(|_| "parity value exceeds u32 length".to_owned())?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn hex_text(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(value.len() * 2);
    for byte in value.bytes() {
        result.push(char::from(HEX[(byte >> 4) as usize]));
        result.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    result
}

fn row_hash(
    row: &[Db2Value],
    columns: &[justybase_db2::network::ddm::ColumnDescription],
) -> Result<String, String> {
    if row.len() != columns.len() {
        return Err("row and column counts differ".to_owned());
    }
    let mut bytes = Vec::new();
    for (value, column) in row.iter().zip(columns) {
        let kind = type_name(column.sql_type)?;
        let canonical = canonical_value(value, kind)?;
        bytes.push(u8::from(canonical.is_none()));
        add_part(&mut bytes, kind.as_bytes())?;
        add_part(&mut bytes, canonical.as_deref().unwrap_or("").as_bytes())?;
    }
    let digest = digest(&SHA256, &bytes);
    let mut result = String::with_capacity(64);
    for byte in digest.as_ref() {
        write!(&mut result, "{byte:02x}").map_err(|error| error.to_string())?;
    }
    Ok(result)
}

fn record(
    table: &str,
    columns: &[justybase_db2::network::ddm::ColumnDescription],
    rows: &[Vec<Db2Value>],
) -> Result<String, String> {
    let mut schema = Vec::with_capacity(columns.len());
    for column in columns {
        let kind = type_name(column.sql_type)?;
        let scale = column.scale;
        schema.push(format!("{}:{}:{}", column.name, kind, scale));
    }
    let mut hashes = rows
        .iter()
        .map(|row| row_hash(row, columns))
        .collect::<Result<Vec<_>, _>>()?;
    hashes.sort_unstable();
    let mut column_hashes = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let mut hashes = rows
            .iter()
            .map(|row| {
                row_hash(
                    std::slice::from_ref(&row[index]),
                    std::slice::from_ref(column),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        hashes.sort_unstable();
        column_hashes.push(hashes.join(","));
    }
    let upper_table = table.to_ascii_uppercase();
    let diagnostic_values = if [".DB2_PARITY_", ".DB2PN_", ".DB2PT_", ".DB2PX_"]
        .iter()
        .any(|marker| upper_table.contains(marker))
    {
        let mut columns_with_values = Vec::with_capacity(columns.len());
        for (index, column) in columns.iter().enumerate() {
            let kind = type_name(column.sql_type)?;
            let mut values = rows
                .iter()
                .map(|row| match canonical_value(&row[index], kind)? {
                    Some(value) => Ok(hex_text(&value)),
                    None => Ok("NULL".to_owned()),
                })
                .collect::<Result<Vec<_>, String>>()?;
            values.sort_unstable();
            columns_with_values.push(values.join(","));
        }
        columns_with_values.join("|")
    } else {
        String::new()
    };
    Ok(format!(
        "{table}\t{}\t{}\t{}\t{}\t{}",
        rows.len(),
        schema.join(","),
        hashes.join(","),
        column_hashes.join("|"),
        diagnostic_values
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = std::env::var("DB2_TEST_DSN").map_err(|_| "Set DB2_TEST_DSN")?;
    let queries = match std::env::var("DB2_PARITY_QUERIES") {
        Ok(value) => value
            .lines()
            .map(|line| {
                let mut fields = line.splitn(3, '\t');
                let table = fields
                    .next()
                    .ok_or_else(|| "malformed DB2_PARITY_QUERIES entry".to_owned())?;
                let sql = fields
                    .next()
                    .ok_or_else(|| "malformed DB2_PARITY_QUERIES entry".to_owned())?;
                let parameter_field = fields.next().unwrap_or_default();
                let parameters = if parameter_field.is_empty() {
                    Vec::new()
                } else {
                    parameter_field
                        .split(',')
                        .map(parse_parameter_token)
                        .collect::<Result<Vec<_>, String>>()?
                };
                Ok((decode_base64(table)?, decode_base64(sql)?, parameters))
            })
            .collect::<Result<Vec<_>, String>>()?,
        Err(_) => QUERIES
            .iter()
            .map(|(table, sql)| ((*table).to_owned(), (*sql).to_owned(), Vec::new()))
            .collect(),
    };
    let mut db = Db2Session::connect_dsn(&dsn).await?;
    for (table, sql, parameters) in queries {
        let result = if parameters.is_empty() {
            db.query(&sql).await?
        } else {
            db.query_with_params(&sql, &parameters).await?
        };
        let baseline = record(&table, &result.columns, &result.rows)?;
        let prepared = db.prepare(&sql).await?;
        let rebound = db.query_prepared(&prepared, &parameters).await?;
        if record(&table, &rebound.columns, &rebound.rows)? != baseline {
            return Err(format!("prepared read parity failed for {table}").into());
        }
        for prepared_stream in [false, true] {
            let mut reader = if prepared_stream {
                db.query_prepared_stream(&prepared, &parameters).await?
            } else if parameters.is_empty() {
                db.query_stream(&sql).await?
            } else {
                db.query_stream_with_params(&sql, &parameters).await?
            };
            let columns = reader.columns().to_vec();
            let mut rows = Vec::new();
            while let Some(mut row) = reader.next().await? {
                for value in &mut row {
                    if let Db2Value::Lob(locator) = value {
                        let encoding = locator.encoding;
                        let mut lob = reader.open_lob(locator).await?;
                        let mut bytes = Vec::new();
                        while let Some(chunk) = lob.next_chunk().await? {
                            bytes.extend_from_slice(&chunk);
                        }
                        *value = match encoding {
                            Db2LobEncoding::Binary => Db2Value::Bytes(bytes),
                            Db2LobEncoding::Utf8 => Db2Value::Text(String::from_utf8(bytes)?),
                            Db2LobEncoding::Utf16Be => {
                                if bytes.len() % 2 != 0 {
                                    return Err("odd UTF-16BE LOB length".into());
                                }
                                let units = bytes
                                    .as_chunks::<2>()
                                    .0
                                    .iter()
                                    .map(|b| u16::from_be_bytes([b[0], b[1]]))
                                    .collect::<Vec<_>>();
                                Db2Value::Text(String::from_utf16(&units)?)
                            }
                        };
                    }
                }
                rows.push(row);
            }
            if record(&table, &columns, &rows)? != baseline {
                return Err(format!(
                    "stream read parity failed for {table}, prepared={prepared_stream}"
                )
                .into());
            }
        }
        println!("{baseline}");
    }
    Ok(())
}

fn parse_parameter_token(token: &str) -> Result<Db2Value, String> {
    match token {
        "N" => return Ok(Db2Value::Null),
        "BT" => return Ok(Db2Value::Boolean(true)),
        "BF" => return Ok(Db2Value::Boolean(false)),
        _ => {}
    }
    let (kind, encoded) = token
        .split_once(':')
        .ok_or_else(|| format!("malformed parity parameter token {token}"))?;
    if kind == "X" {
        return decode_base64_bytes(encoded).map(Db2Value::Bytes);
    }
    let value = decode_base64(encoded)?;
    match kind {
        "I" => value
            .parse::<i64>()
            .map(Db2Value::Integer)
            .map_err(|error| format!("invalid integer parity parameter: {error}")),
        "D" => Ok(Db2Value::Decimal(value)),
        "F" => value
            .parse::<f64>()
            .map(Db2Value::Float)
            .map_err(|error| format!("invalid float parity parameter: {error}")),
        "S" => Ok(Db2Value::Text(value)),
        _ => Err(format!("unsupported parity parameter kind {kind}")),
    }
}

fn decode_base64(input: &str) -> Result<String, String> {
    String::from_utf8(decode_base64_bytes(input)?)
        .map_err(|error| format!("parity query manifest is not UTF-8: {error}"))
}

fn decode_base64_bytes(input: &str) -> Result<Vec<u8>, String> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    let mut accumulator = 0u32;
    let mut bits = 0u8;
    for byte in input.bytes() {
        if byte == b'=' {
            break;
        }
        let value = ALPHABET
            .iter()
            .position(|candidate| *candidate == byte)
            .ok_or_else(|| format!("invalid base64 byte in parity query manifest: {byte}"))?
            as u32;
        accumulator = (accumulator << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push(((accumulator >> bits) & 0xff) as u8);
        }
    }
    Ok(output)
}
