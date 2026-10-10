use crate::converters::ebcdic::encode_cp500;
use crate::error::Db2Warning;
use crate::network::codepoint::{self as cp, CodePoint, SecurityMechanism};
use crate::network::dss::DssPacket;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StringEncoding {
    Cp500,
    Utf8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManagerLevel {
    pub manager: CodePoint,
    pub level: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDescription {
    pub name: String,
    pub sql_type: u16,
    pub length: u64,
    pub precision: u16,
    pub scale: u16,
    pub nullable: bool,
}

fn normalize_scale(sql_type: u16, length: u64, scale: u16) -> u16 {
    // SQLDA encodes TIMESTAMP(p) in SQLLENGTH as 19 for p=0, otherwise 20+p.
    // Some Db2 LUW DRDA replies leave SQLSCALE at zero, unlike CLI/ODBC.
    if sql_type & 0xfffe != 392 {
        return scale;
    }
    if length == 19 {
        return 0;
    }
    if (21..=32).contains(&length) {
        return (length - 20) as u16;
    }
    scale
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultSetPackageSection {
    pub database: Vec<u8>,
    pub schema: Vec<u8>,
    pub package_name: Vec<u8>,
    pub consistency_token: [u8; 8],
    pub section_number: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldDescriptor {
    pub field_type: u8,
    pub precision_scale: [u8; 2],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlCard {
    pub sql_code: i32,
    pub sql_state: String,
    pub message: String,
    pub rows_affected: i64,
    pub warning_flags: String,
}

impl SqlCard {
    pub fn warning(&self) -> Option<Db2Warning> {
        let is_warning = (self.sql_code > 0 && self.sql_code != 100)
            || self.sql_state.starts_with("01")
            || self.warning_flags.starts_with('W');
        if !is_warning {
            return None;
        }
        Some(Db2Warning {
            sql_code: self.sql_code,
            sql_state: self.sql_state.clone(),
            message: if self.message.trim().is_empty() {
                format!(
                    "DB2 returned a warning (SQLCODE={}, SQLSTATE={}).",
                    self.sql_code, self.sql_state
                )
            } else {
                self.message.clone()
            },
            warning_flags: self.warning_flags.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgressiveChunkReply {
    pub data: Vec<u8>,
    pub end_of_reference: bool,
    pub is_last_chunk: bool,
}

const DEFAULT_CRTTKN: &[u8] = &[
    0xd5, 0xc6, 0xf0, 0xf0, 0xf0, 0xf0, 0xf0, 0xf1, 0x2e, 0xc3, 0xf0, 0xc1, 0xf5, 0x01, 0x55, 0x63,
    0x0d, 0x5a, 0x11,
];
const DEFAULT_TYPDEFOVR: &[u8] = &[
    0x00, 0x06, 0x11, 0x9c, 0x04, 0xb8, 0x00, 0x06, 0x11, 0x9d, 0x04, 0xb0, 0x00, 0x06, 0x11, 0x9e,
    0x04, 0xb8,
];
const DEFAULT_CONSISTENCY_TOKEN: [u8; 8] = [1; 8];
const PARAM_RDBCMTOK: &[u8] = &[0x00, 0x05, 0x21, 0x05, 0xf1];
const PARAM_RTNSQLDA: &[u8] = &[0x00, 0x05, 0x21, 0x16, 0xf1];
const PARAM_TYPSQLDA: &[u8] = &[0x00, 0x05, 0x21, 0x46, 0x01];
const PARAM_QRYCLSIMP: &[u8] = &[0x00, 0x05, 0x21, 0x5d, 0x01];

pub fn pack_ddm_object(codepoint: CodePoint, body: &[u8]) -> Vec<u8> {
    let short_len = body.len().saturating_add(4);
    let extension_len = if short_len <= 0x7fff {
        0
    } else if body.len() <= 0x7fff_ffff {
        4
    } else {
        8
    };
    let header_len = 4 + extension_len;
    let mut out = vec![0; header_len + body.len()];
    let length_field = if extension_len == 0 {
        short_len as u16
    } else {
        0x8000 | header_len as u16
    };
    out[0..2].copy_from_slice(&length_field.to_be_bytes());
    out[2..4].copy_from_slice(&codepoint.0.to_be_bytes());
    match extension_len {
        4 => out[4..8].copy_from_slice(&(body.len() as u32).to_be_bytes()),
        8 => out[4..12].copy_from_slice(&(body.len() as u64).to_be_bytes()),
        _ => (),
    }
    out[header_len..].copy_from_slice(body);
    out
}

pub(crate) fn ddm_header_length(body_length: usize) -> Result<usize, String> {
    let short_length = body_length
        .checked_add(4)
        .ok_or_else(|| "DDM object length overflow".to_owned())?;
    Ok(if short_length <= 0x7fff {
        4
    } else if body_length <= 0x7fff_ffff {
        8
    } else {
        12
    })
}

pub(crate) fn append_ddm_header(
    output: &mut Vec<u8>,
    codepoint: CodePoint,
    body_length: usize,
) -> Result<(), String> {
    let header_length = ddm_header_length(body_length)?;
    output
        .try_reserve(header_length)
        .map_err(|error| format!("unable to reserve a DDM header: {error}"))?;
    if header_length == 4 {
        let total_length = body_length + 4;
        output.extend_from_slice(&(total_length as u16).to_be_bytes());
    } else {
        output.extend_from_slice(&(0x8000u16 | header_length as u16).to_be_bytes());
    }
    output.extend_from_slice(&codepoint.0.to_be_bytes());
    match header_length {
        8 => output.extend_from_slice(&(body_length as u32).to_be_bytes()),
        12 => output.extend_from_slice(
            &u64::try_from(body_length)
                .map_err(|_| "DDM object length exceeds u64".to_owned())?
                .to_be_bytes(),
        ),
        _ => (),
    }
    Ok(())
}

pub fn pack_bytes(codepoint: CodePoint, value: &[u8]) -> Vec<u8> {
    pack_ddm_object(codepoint, value)
}

pub fn pack_u16(codepoint: CodePoint, value: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(6);
    out.extend_from_slice(&6u16.to_be_bytes());
    out.extend_from_slice(&codepoint.0.to_be_bytes());
    out.extend_from_slice(&value.to_be_bytes());
    out
}

pub fn pack_u32(codepoint: CodePoint, value: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(8);
    out.extend_from_slice(&8u16.to_be_bytes());
    out.extend_from_slice(&codepoint.0.to_be_bytes());
    out.extend_from_slice(&value.to_be_bytes());
    out
}

pub fn pack_u64(codepoint: CodePoint, value: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.extend_from_slice(&12u16.to_be_bytes());
    out.extend_from_slice(&codepoint.0.to_be_bytes());
    out.extend_from_slice(&value.to_be_bytes());
    out
}

pub fn pack_string(
    codepoint: CodePoint,
    value: &str,
    encoding: StringEncoding,
) -> Result<Vec<u8>, String> {
    let bytes = match encoding {
        StringEncoding::Cp500 => encode_cp500(value)?,
        StringEncoding::Utf8 => value.as_bytes().to_vec(),
    };
    Ok(pack_ddm_object(codepoint, &bytes))
}

pub fn pack_null_string(value: Option<&str>, encoding: StringEncoding) -> Result<Vec<u8>, String> {
    let Some(value) = value else {
        return Ok(vec![0xff]);
    };
    let bytes = match encoding {
        StringEncoding::Cp500 => encode_cp500(value)?,
        StringEncoding::Utf8 => value.as_bytes().to_vec(),
    };
    let mut out = Vec::with_capacity(5 + bytes.len());
    out.push(0);
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&bytes);
    Ok(out)
}

pub fn default_manager_levels() -> Vec<ManagerLevel> {
    vec![
        ManagerLevel {
            manager: cp::AGENT,
            level: 10,
        },
        ManagerLevel {
            manager: cp::SQLAM,
            level: 11,
        },
        ManagerLevel {
            manager: cp::CMNTCPIP,
            level: 5,
        },
        ManagerLevel {
            manager: cp::RDB,
            level: 12,
        },
        ManagerLevel {
            manager: cp::SECMGR,
            level: 9,
        },
        ManagerLevel {
            manager: cp::UNICODEMGR,
            level: 1208,
        },
    ]
}

fn concat(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(parts.iter().map(|part| part.len()).sum());
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}

fn pack_manager_levels(levels: &[ManagerLevel]) -> Vec<u8> {
    let mut out = Vec::with_capacity(levels.len() * 4);
    for item in levels {
        out.extend_from_slice(&item.manager.0.to_be_bytes());
        out.extend_from_slice(&item.level.to_be_bytes());
    }
    out
}

pub fn pack_excsat(
    ext_name: &str,
    server_name: &str,
    release: &str,
    server_class: &str,
    levels: &[ManagerLevel],
) -> Result<Vec<u8>, String> {
    let manager_data = pack_manager_levels(levels);
    let ext = pack_string(cp::EXTNAM, ext_name, StringEncoding::Cp500)?;
    let server = pack_string(cp::SRVNAM, server_name, StringEncoding::Cp500)?;
    let release = pack_string(cp::SRVRLSLV, release, StringEncoding::Cp500)?;
    let managers = pack_bytes(cp::MGRLVLLS, &manager_data);
    let class = pack_string(cp::SRVCLSNM, server_class, StringEncoding::Cp500)?;
    Ok(pack_ddm_object(
        cp::EXCSAT,
        &concat(&[&ext, &server, &release, &managers, &class]),
    ))
}

pub fn pack_excsat_manager_levels(levels: &[ManagerLevel]) -> Vec<u8> {
    let packed = pack_bytes(cp::MGRLVLLS, &pack_manager_levels(levels));
    pack_ddm_object(cp::EXCSAT, &packed)
}

pub fn pack_accsec(
    database: &str,
    security: SecurityMechanism,
    token: &[u8],
) -> Result<Vec<u8>, String> {
    let secmec = pack_u16(cp::SECMEC, security as u16);
    let db = pack_string(cp::RDBNAM, database, StringEncoding::Cp500)?;
    let token = if token.is_empty() {
        Vec::new()
    } else {
        pack_bytes(cp::SECTKN, token)
    };
    Ok(pack_ddm_object(
        cp::ACCSEC,
        &concat(&[&secmec, &db, &token]),
    ))
}

pub fn pack_secchk(
    security: u16,
    token: &[u8],
    database: &str,
    user: &str,
    password: &[u8],
    encrypted: bool,
    encoding: StringEncoding,
) -> Result<Vec<u8>, String> {
    let mut body = pack_u16(cp::SECMEC, security);
    if !token.is_empty() {
        body.extend_from_slice(&pack_bytes(cp::SECTKN, token));
    }
    body.extend_from_slice(&pack_string(cp::RDBNAM, database, encoding)?);
    body.extend_from_slice(&pack_string(cp::USRID, user, encoding)?);
    if security == 3 {
        if !password.is_empty() {
            if encrypted {
                body.extend_from_slice(&pack_bytes(cp::PASSWORD, password));
            } else {
                let value = std::str::from_utf8(password)
                    .map_err(|_| "non-encrypted password is not valid UTF-8".to_string())?;
                body.extend_from_slice(&pack_string(cp::PASSWORD, value, encoding)?);
            }
        }
    } else if security != 4 {
        return Err(format!(
            "only USRIDPWD (3) and USRIDONL (4) SECCHK requests are supported; got {security}"
        ));
    }
    Ok(pack_ddm_object(cp::SECCHK, &body))
}

/// Builds the EUSRIDPWD (SECMEC 9) SECCHK as Db2 LUW expects it: two SECTKN
/// fields — the DES-CBC-encrypted userid followed by the DES-CBC-encrypted
/// password — with no cleartext USRID/PASSWORD. Verified against IBM CLI wire.
pub fn pack_secchk_eusridpwd(
    encrypted_userid: &[u8],
    encrypted_password: &[u8],
    database: &str,
) -> Result<Vec<u8>, String> {
    let db = pack_string(cp::RDBNAM, database, StringEncoding::Cp500)?;
    Ok(pack_ddm_object(
        cp::SECCHK,
        &concat(&[
            &pack_u16(cp::SECMEC, 9),
            &db,
            &pack_bytes(cp::SECTKN, encrypted_userid),
            &pack_bytes(cp::SECTKN, encrypted_password),
        ]),
    ))
}

/// Builds the Kerberos/GSSAPI (SECMEC 11) SECCHK with a caller-supplied GSSAPI token.
pub fn pack_secchk_kerberos(token: &[u8], database: &str, user: &str) -> Result<Vec<u8>, String> {
    let db = pack_string(cp::RDBNAM, database, StringEncoding::Cp500)?;
    let user = pack_string(cp::USRID, user, StringEncoding::Cp500)?;
    Ok(pack_ddm_object(
        cp::SECCHK,
        &concat(&[
            &pack_u16(cp::SECMEC, 11),
            &pack_bytes(cp::SECTKN, token),
            &db,
            &user,
        ]),
    ))
}

pub fn pack_accrdb(
    product_id: &str,
    database: &str,
    encoding: StringEncoding,
) -> Result<Vec<u8>, String> {
    let db = pack_string(cp::RDBNAM, database, encoding)?;
    let class = pack_u16(cp::RDBACCCL, cp::SQLAM.0);
    let product = pack_string(cp::PRDID, product_id, encoding)?;
    let types = pack_string(cp::TYPDEFNAM, "QTDSQLX86", encoding)?;
    let token = pack_bytes(cp::CRRTKN, DEFAULT_CRTTKN);
    let overrides = pack_bytes(cp::TYPDEFOVR, DEFAULT_TYPDEFOVR);
    Ok(pack_ddm_object(
        cp::ACCRDB,
        &concat(&[&db, &class, &product, &types, &token, &overrides]),
    ))
}

pub fn pack_commit() -> Vec<u8> {
    pack_ddm_object(cp::RDBCMM, &[])
}
pub fn pack_rollback() -> Vec<u8> {
    pack_ddm_object(cp::RDBRLLBCK, &[])
}

fn package_len(database: &str, package: &str, token: &str) -> usize {
    4 + database.len().max(18) + 18 + package.len().max(18) + token.len().max(8) + 2
}

pub fn pack_package_name_csn(database: &str, package: &str, token: &str, section: u16) -> Vec<u8> {
    let db_len = database.len().max(18);
    let pkg_len = package.len().max(18);
    let total = package_len(database, package, token);
    let mut out = vec![0; total];
    out[0..2].copy_from_slice(&(total as u16).to_be_bytes());
    out[2..4].copy_from_slice(&cp::PKGNAMCSN.0.to_be_bytes());
    let mut offset = 4;
    out[offset..offset + database.len()].copy_from_slice(database.as_bytes());
    out[offset + database.len()..offset + db_len].fill(b' ');
    offset += db_len;
    out[offset..offset + 18].copy_from_slice(b"NULLID            ");
    offset += 18;
    out[offset..offset + package.len()].copy_from_slice(package.as_bytes());
    out[offset + package.len()..offset + pkg_len].fill(b' ');
    offset += pkg_len;
    if token.is_empty() {
        out[offset..offset + 8].copy_from_slice(&DEFAULT_CONSISTENCY_TOKEN);
        offset += 8;
    } else if token.len() < 8 {
        let pad = 8 - token.len();
        out[offset..offset + pad].fill(b' ');
        out[offset + pad..offset + 8].copy_from_slice(token.as_bytes());
        offset += 8;
    } else {
        out[offset..offset + token.len()].copy_from_slice(token.as_bytes());
        offset += token.len();
    }
    out[offset..offset + 2].copy_from_slice(&section.to_be_bytes());
    debug_assert_eq!(offset + 2, total);
    out
}

fn pack_command(
    codepoint: CodePoint,
    database: &str,
    package: &str,
    token: &str,
    section: u16,
    suffix: &[u8],
) -> Vec<u8> {
    let pkg = pack_package_name_csn(database, package, token, section);
    let body = concat(&[&pkg, suffix]);
    pack_ddm_object(codepoint, &body)
}

pub fn pack_exec_sql_immediate(
    package: &str,
    token: &str,
    section: u16,
    database: &str,
) -> Vec<u8> {
    pack_command(
        cp::EXCSQLIMM,
        database,
        package,
        token,
        section,
        PARAM_RDBCMTOK,
    )
}
pub fn pack_prepare_sql_statement(
    package: &str,
    token: &str,
    section: u16,
    database: &str,
) -> Vec<u8> {
    pack_command(
        cp::PRPSQLSTT,
        database,
        package,
        token,
        section,
        PARAM_RTNSQLDA,
    )
}
pub fn pack_describe_sql_statement(
    package: &str,
    token: &str,
    section: u16,
    database: &str,
) -> Vec<u8> {
    pack_command(
        cp::DSCSQLSTT,
        database,
        package,
        token,
        section,
        PARAM_TYPSQLDA,
    )
}
pub fn pack_execute_sql_statement(
    package: &str,
    token: &str,
    section: u16,
    database: &str,
) -> Vec<u8> {
    pack_command(
        cp::EXCSQLSTT,
        database,
        package,
        token,
        section,
        PARAM_RDBCMTOK,
    )
}
#[allow(clippy::too_many_arguments)]
pub fn pack_execute_sql_statement_for_result_sets(
    package: &str,
    token: &str,
    section: u16,
    database: &str,
    maximum_result_sets: u16,
    query_block_size: u32,
    query_rowset_size: Option<u32>,
    output_expected: bool,
) -> Vec<u8> {
    let mut options = PARAM_RDBCMTOK.to_vec();
    options.extend_from_slice(&pack_bytes(
        cp::OUTEXP,
        &[if output_expected { 0xf1 } else { 0xf0 }],
    ));
    options.extend_from_slice(&pack_u32(cp::QRYBLKSZ, query_block_size));
    if query_rowset_size.is_some() {
        options.extend_from_slice(&pack_u16(cp::MAXBLKEXT, u16::MAX));
    }
    options.extend_from_slice(&pack_u16(cp::MAXRSLCNT, maximum_result_sets));
    options.extend_from_slice(&pack_bytes(cp::RSLSETFLG, &[0x04]));
    if let Some(rowset_size) = query_rowset_size {
        options.extend_from_slice(&pack_u32(cp::QRYROWSET, rowset_size));
    }
    pack_command(cp::EXCSQLSTT, database, package, token, section, &options)
}

fn pack_result_set_package_name_csn(section: &ResultSetPackageSection) -> Result<Vec<u8>, String> {
    let mut output = vec![0; 68];
    output[0..2].copy_from_slice(&68u16.to_be_bytes());
    output[2..4].copy_from_slice(&cp::PKGNAMCSN.0.to_be_bytes());
    for (name, offset, description) in [
        (&section.database, 4, "database"),
        (&section.schema, 22, "schema"),
        (&section.package_name, 40, "package name"),
    ] {
        if name.len() > 18 {
            return Err(format!(
                "result-set {description} exceeds its DRDA field width"
            ));
        }
        output[offset..offset + 18].fill(b' ');
        output[offset..offset + name.len()].copy_from_slice(name);
    }
    output[58..66].copy_from_slice(&section.consistency_token);
    output[66..68].copy_from_slice(&section.section_number.to_be_bytes());
    Ok(output)
}

pub fn pack_continue_result_set_query(
    section: &ResultSetPackageSection,
    block_size: u32,
    query_id: u64,
    rowset_size: Option<u32>,
    return_all_data: bool,
) -> Result<Vec<u8>, String> {
    let mut body = pack_result_set_package_name_csn(section)?;
    body.extend_from_slice(&pack_u32(cp::QRYBLKSZ, block_size));
    if rowset_size.is_some() {
        body.extend_from_slice(&pack_u16(cp::MAXBLKEXT, u16::MAX));
    }
    let mut query_instance = Vec::with_capacity(12);
    query_instance.extend_from_slice(&12u16.to_be_bytes());
    query_instance.extend_from_slice(&cp::QRYINSID.0.to_be_bytes());
    query_instance.extend_from_slice(&query_id.to_be_bytes());
    body.extend_from_slice(&query_instance);
    if let Some(rowset_size) = rowset_size {
        body.extend_from_slice(&pack_u32(cp::QRYROWSET, rowset_size));
    }
    if return_all_data {
        body.extend_from_slice(&pack_bytes(cp::RTNEXTDTA, &[0x02]));
    }
    Ok(pack_ddm_object(cp::CNTQRY, &body))
}

pub fn pack_continue_procedure_result_set(
    section: &ResultSetPackageSection,
    block_size: u32,
    query_id: u64,
) -> Result<Vec<u8>, String> {
    let mut body = pack_result_set_package_name_csn(section)?;
    body.extend_from_slice(&pack_u32(cp::QRYBLKSZ, block_size));
    body.extend_from_slice(&12u16.to_be_bytes());
    body.extend_from_slice(&cp::QRYINSID.0.to_be_bytes());
    body.extend_from_slice(&query_id.to_be_bytes());
    body.extend_from_slice(&pack_bytes(cp::RTNEXTDTA, &[0x02]));
    body.extend_from_slice(&pack_bytes(cp::FREPRVREF, &[0xf0]));
    Ok(pack_ddm_object(cp::CNTQRY, &body))
}

pub fn pack_close_procedure_result_set(
    section: &ResultSetPackageSection,
    query_id: u64,
) -> Result<Vec<u8>, String> {
    let mut body = pack_result_set_package_name_csn(section)?;
    body.extend_from_slice(&12u16.to_be_bytes());
    body.extend_from_slice(&cp::QRYINSID.0.to_be_bytes());
    body.extend_from_slice(&query_id.to_be_bytes());
    Ok(pack_ddm_object(cp::CLSQRY, &body))
}
pub fn pack_execute_sql_set(package: &str, token: &str, section: u16, database: &str) -> Vec<u8> {
    pack_command(cp::EXCSQLSET, database, package, token, section, &[])
}
pub fn pack_interrupt_rdb_request(token: &[u8]) -> Vec<u8> {
    pack_ddm_object(cp::INTRDBRQS, &pack_bytes(cp::RDBINTTKN, token))
}

/// Builds a locator-based GETNXTCHK request observed from the IBM Db2 driver.
pub fn pack_get_next_chunk(
    package: &str,
    token: &str,
    section: u16,
    database: &str,
    query_id: u64,
    locator: u64,
    requested_length: u64,
) -> Vec<u8> {
    let mut body = pack_package_name_csn(database, package, token, section);
    body.extend_from_slice(&pack_u64(cp::QRYINSID, query_id));
    body.extend_from_slice(&pack_u64(cp::GETNXTREF, locator));
    body.extend_from_slice(&pack_u64(cp::GETNXTLEN, requested_length));
    pack_ddm_object(cp::GETNXTCHK, &body)
}

/// Parses one GETNXTCHK reply, removing the status byte from each EXTDTA object.
pub fn parse_progressive_chunk_reply(
    replies: &[DssPacket],
) -> Result<ProgressiveChunkReply, String> {
    let mut data = Vec::new();
    let mut end_of_reference = false;
    let mut is_last_chunk = false;
    let mut saw_end = false;
    for reply in replies {
        if reply.codepoint == cp::EXTDTA {
            if saw_end {
                return Err("GETNXTCHK reply contained EXTDTA after ENDDTARM".into());
            }
            match reply.payload.split_first() {
                Some((0, bytes)) => data.extend_from_slice(bytes),
                _ => return Err("GETNXTCHK reply contained an invalid EXTDTA status byte".into()),
            }
        } else if reply.codepoint == cp::ENDDTARM {
            if saw_end {
                return Err("GETNXTCHK reply contained multiple ENDDTARM messages".into());
            }
            saw_end = true;
            let mut server_code = None;
            let mut end_reference = None;
            let mut last_chunk = None;
            visit_ddm_parameters(&reply.payload, |object| match object.codepoint {
                value if value == cp::SVRCOD => server_code = Some(object.payload),
                value if value == cp::ENDREF => end_reference = Some(object.payload),
                value if value == cp::LSTCHK => last_chunk = Some(object.payload),
                _ => (),
            })?;
            let server_code = server_code
                .filter(|value| value.len() == 2)
                .ok_or("GETNXTCHK ENDDTARM omitted its two-byte SVRCOD")?;
            let server_code = u16::from_be_bytes([server_code[0], server_code[1]]);
            if server_code != 0 {
                return Err(format!("GETNXTCHK ENDDTARM returned SVRCOD {server_code}"));
            }
            end_of_reference = parse_optional_ddm_boolean(end_reference, cp::ENDREF)?;
            is_last_chunk = parse_optional_ddm_boolean(last_chunk, cp::LSTCHK)?;
        }
    }
    if !saw_end {
        return Err("GETNXTCHK reply did not contain ENDDTARM".into());
    }
    Ok(ProgressiveChunkReply {
        data,
        end_of_reference,
        is_last_chunk,
    })
}

fn parse_optional_ddm_boolean(value: Option<&[u8]>, codepoint: CodePoint) -> Result<bool, String> {
    let Some(value) = value else {
        return Ok(false);
    };
    match value {
        [0xf0] => Ok(false),
        [0xf1] => Ok(true),
        _ => Err(format!(
            "ENDDTARM contained an invalid {} Boolean value",
            codepoint.name()
        )),
    }
}

fn pack_query(
    codepoint: CodePoint,
    package: &str,
    token: &str,
    section: u16,
    database: &str,
    block_size: u32,
    query_id: Option<u64>,
) -> Vec<u8> {
    let mut body = pack_package_name_csn(database, package, token, section);
    body.extend_from_slice(&8u16.to_be_bytes());
    body.extend_from_slice(&cp::QRYBLKSZ.0.to_be_bytes());
    body.extend_from_slice(&block_size.to_be_bytes());
    if let Some(query_id) = query_id {
        body.extend_from_slice(&12u16.to_be_bytes());
        body.extend_from_slice(&cp::QRYINSID.0.to_be_bytes());
        body.extend_from_slice(&query_id.to_be_bytes());
        body.extend_from_slice(&[0, 5, 0x21, 0x48, 0x02]);
    } else {
        body.extend_from_slice(&6u16.to_be_bytes());
        body.extend_from_slice(&cp::MAXBLKEXT.0.to_be_bytes());
        body.extend_from_slice(&(block_size as u16).to_be_bytes());
        body.extend_from_slice(PARAM_QRYCLSIMP);
        if codepoint == cp::OPNQRY {
            body.extend_from_slice(&pack_bytes(cp::DYNDTAFMT, &[0xf1]));
        }
    }
    pack_ddm_object(codepoint, &body)
}

pub fn pack_open_query(package: &str, token: &str, section: u16, db: &str, block: u32) -> Vec<u8> {
    pack_query(cp::OPNQRY, package, token, section, db, block, None)
}
pub fn pack_open_query_with_params(
    package: &str,
    token: &str,
    section: u16,
    db: &str,
    block: u32,
) -> Vec<u8> {
    pack_query(cp::OPNQRY, package, token, section, db, block, None)
}
pub fn pack_continue_query(
    package: &str,
    token: &str,
    section: u16,
    db: &str,
    block: u32,
    id: u64,
) -> Vec<u8> {
    pack_query(cp::CNTQRY, package, token, section, db, block, Some(id))
}

pub fn pack_sql_statement(sql: &str) -> Vec<u8> {
    let length = 10 + sql.len();
    if length <= 0x7fff {
        let mut out = Vec::with_capacity(length);
        out.extend_from_slice(&(length as u16).to_be_bytes());
        out.extend_from_slice(&cp::SQLSTT.0.to_be_bytes());
        out.push(0);
        out.extend_from_slice(&(sql.len() as u32).to_be_bytes());
        out.extend_from_slice(sql.as_bytes());
        out.push(0xff);
        out
    } else {
        let body = concat(&[
            &pack_null_string(Some(sql), StringEncoding::Utf8)
                .expect("UTF-8 string encoding cannot fail"),
            &pack_null_string(None, StringEncoding::Utf8)
                .expect("null string encoding cannot fail"),
        ]);
        pack_ddm_object(cp::SQLSTT, &body)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DdmObjectRef<'a> {
    pub codepoint: CodePoint,
    pub payload: &'a [u8],
}

/// Allocation-free iterator over DDM objects contained in one payload.
pub struct DdmObjects<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> DdmObjects<'a> {
    pub const fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }
}

impl<'a> Iterator for DdmObjects<'a> {
    type Item = Result<DdmObjectRef<'a>, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.data.len() {
            return None;
        }
        match parse_ddm_object_ref(self.data, self.offset) {
            Ok((object, next_offset)) => {
                self.offset = next_offset;
                Some(Ok(object))
            }
            Err(error) => {
                self.offset = self.data.len();
                Some(Err(error))
            }
        }
    }
}

fn parse_ddm_object_ref(data: &[u8], offset: usize) -> Result<(DdmObjectRef<'_>, usize), String> {
    if data.len().saturating_sub(offset) < 4 {
        return Err("incomplete DDM parameter header in reply".into());
    }
    let encoded_length = u16::from_be_bytes([data[offset], data[offset + 1]]);
    let codepoint = CodePoint(u16::from_be_bytes([data[offset + 2], data[offset + 3]]));
    let (header_length, body_length) = if encoded_length & 0x8000 != 0 {
        let header_length = (encoded_length & 0x7fff) as usize;
        if header_length != 8 && header_length != 12 {
            return Err(format!(
                "invalid extended DDM header length {header_length}"
            ));
        }
        let header_end = offset
            .checked_add(header_length)
            .ok_or("DDM header length overflow")?;
        if header_end > data.len() {
            return Err("truncated extended DDM parameter header".into());
        }
        let body_length = if header_length == 8 {
            u32::from_be_bytes(data[offset + 4..offset + 8].try_into().unwrap()) as u64
        } else {
            u64::from_be_bytes(data[offset + 4..offset + 12].try_into().unwrap())
        };
        let body_length = usize::try_from(body_length)
            .map_err(|_| "extended DDM parameter exceeds the supported buffer size")?;
        (header_length, body_length)
    } else {
        let length = encoded_length as usize;
        if length < 4 {
            return Err(format!(
                "invalid DDM parameter length {length} at offset {offset}"
            ));
        }
        (4, length - 4)
    };
    let header_end = offset
        .checked_add(header_length)
        .ok_or("DDM header length overflow")?;
    let end = header_end
        .checked_add(body_length)
        .ok_or("DDM length overflow")?;
    if header_end > data.len() || end > data.len() {
        return Err(format!(
            "DDM parameter at offset {offset} exceeds the remaining {} bytes",
            data.len() - offset
        ));
    }
    Ok((
        DdmObjectRef {
            codepoint,
            payload: &data[header_end..end],
        },
        end,
    ))
}

/// Visits each validated DDM object without allocating or copying its payload.
pub fn visit_ddm_parameters<'a>(
    data: &'a [u8],
    mut visitor: impl FnMut(DdmObjectRef<'a>),
) -> Result<(), String> {
    for object in DdmObjects::new(data) {
        visitor(object?);
    }
    Ok(())
}

/// Finds a codepoint while validating the complete DDM parameter list.
pub fn find_ddm_parameter(data: &[u8], wanted: CodePoint) -> Result<Option<&[u8]>, String> {
    let mut found = None;
    visit_ddm_parameters(data, |object| {
        if object.codepoint == wanted {
            found = Some(object.payload);
        }
    })?;
    Ok(found)
}

pub fn parse_ddm_parameters(data: &[u8]) -> Result<HashMap<CodePoint, Vec<u8>>, String> {
    let mut results = HashMap::new();
    for object in DdmObjects::new(data) {
        let object = object?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(object.payload.len())
            .map_err(|error| format!("unable to reserve DDM payload: {error}"))?;
        payload.extend_from_slice(object.payload);
        results
            .try_reserve(1)
            .map_err(|error| format!("unable to reserve DDM parameter map: {error}"))?;
        results.insert(object.codepoint, payload);
    }
    Ok(results)
}

pub fn parse_sql_result_set_count(data: &[u8]) -> Result<usize, String> {
    if data.len() < 2 {
        return Err("SQLRSLRD is missing its result-set count".into());
    }
    let count = u16::from_le_bytes([data[0], data[1]]) as usize;
    if data.len() - 2 < count * 12 {
        return Err(format!(
            "SQLRSLRD declares {count} result sets but its row summaries are truncated"
        ));
    }
    Ok(count)
}

pub fn parse_result_set_sections(data: &[u8]) -> Result<Vec<ResultSetPackageSection>, String> {
    let package_list = find_ddm_parameter(data, cp::PKGSNLST)?
        .ok_or("RSLSETRM does not contain the required PKGSNLST object")?;
    let mut sections = Vec::new();
    for object in DdmObjects::new(package_list) {
        let object = object?;
        if object.codepoint != cp::PKGNAMCSN {
            continue;
        }
        if object.payload.len() < 64 {
            return Err("RSLSETRM contains a truncated PKGNAMCSN object".into());
        }
        sections
            .try_reserve(1)
            .map_err(|error| format!("unable to reserve result-set sections: {error}"))?;
        sections.push(ResultSetPackageSection {
            database: object.payload[..18].to_vec(),
            schema: object.payload[18..36].to_vec(),
            package_name: object.payload[36..54].to_vec(),
            consistency_token: object.payload[54..62].try_into().unwrap(),
            section_number: u16::from_be_bytes(object.payload[62..64].try_into().unwrap()),
        });
    }
    Ok(sections)
}

fn get_u16(data: &[u8], offset: usize, little: bool) -> u16 {
    let b = [data[offset], data[offset + 1]];
    if little {
        u16::from_le_bytes(b)
    } else {
        u16::from_be_bytes(b)
    }
}
fn get_u32(data: &[u8], offset: usize, little: bool) -> u32 {
    let b = [
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ];
    if little {
        u32::from_le_bytes(b)
    } else {
        u32::from_be_bytes(b)
    }
}
fn get_u64(data: &[u8], offset: usize, little: bool) -> u64 {
    let b = [
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ];
    if little {
        u64::from_le_bytes(b)
    } else {
        u64::from_be_bytes(b)
    }
}

pub fn parse_sql_card(data: &[u8], little_endian: bool) -> Result<SqlCard, String> {
    if data.is_empty() {
        return Err("empty SQLCARD payload".into());
    }
    if data[0] == 0xff {
        return Ok(SqlCard {
            sql_code: 0,
            sql_state: String::new(),
            message: String::new(),
            rows_affected: 0,
            warning_flags: String::new(),
        });
    }
    if data.len() < 18 {
        return Err(format!("SQLCARD payload too short: {} bytes", data.len()));
    }
    let sql_code = get_u32(data, 1, little_endian) as i32;
    let sql_state = String::from_utf8_lossy(&data[5..10]).to_string();
    let warning_flags = if data.len() >= 54 && data[18] == 0 {
        String::from_utf8_lossy(&data[43..54]).to_string()
    } else {
        String::new()
    };
    // Nullable SQLCAXGRP at 18, six I4 fields at 19: SQLERRD(3) is at 27.
    let has_extension = data.len() > 18 && data[18] != 0xff;
    if has_extension && data.len() < 54 {
        return Err("SQLCARD SQLCAXGRP is truncated".into());
    }
    let rows_affected = if has_extension {
        get_u32(data, 27, little_endian) as i32 as i64
    } else {
        0
    };
    let mut message = String::new();
    if has_extension && data.len() > 54 {
        let mut rest = &data[54..];
        if rest.len() >= 2 {
            let len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            if rest.len() >= 2 + len {
                rest = &rest[2 + len..];
            }
        }
        if rest.len() >= 2 {
            let len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            if rest.len() >= 2 + len {
                message = String::from_utf8_lossy(&rest[2..2 + len]).replace('\u{fffd}', ", ");
            }
        }
    }
    Ok(SqlCard {
        sql_code,
        sql_state,
        message,
        rows_affected,
        warning_flags,
    })
}

pub fn parse_query_descriptor(data: &[u8]) -> Result<Vec<FieldDescriptor>, String> {
    if data.is_empty() {
        return Err("QRYDSC payload is empty".into());
    }
    if data.len() < 3 {
        return Err("QRYDSC payload too short".into());
    }
    let mut fields = Vec::new();
    let mut pos = 0;
    let mut has_descriptor_base = false;
    while pos < data.len() {
        if data.len() - pos < 3 {
            return Err(format!("QRYDSC has {} trailing bytes", data.len() - pos));
        }
        let group_len = data[pos] as usize;
        if group_len < 3 {
            return Err(format!(
                "invalid QRYDSC triplet length {group_len} at offset {pos}"
            ));
        }
        let group_end = pos
            .checked_add(group_len)
            .ok_or("QRYDSC triplet length overflow")?;
        if group_end > data.len() {
            return Err(format!(
                "QRYDSC triplet length {group_len} at offset {pos} exceeds the remaining {} bytes",
                data.len() - pos
            ));
        }
        let triplet_type = data[pos + 1];
        let triplet_id = data[pos + 2];
        if triplet_type == 0x76 && triplet_id == 0xd0 {
            has_descriptor_base = true;
        } else if triplet_type == 0x7f && triplet_id == 0 && !has_descriptor_base {
            return Err("QRYDSC CPT continuation has no preceding NGDA descriptor group".into());
        }
        if (triplet_type == 0x76 && triplet_id == 0xd0) || (triplet_type == 0x7f && triplet_id == 0)
        {
            if !(group_len - 3).is_multiple_of(3) {
                return Err("QRYDSC descriptor group has an invalid length".into());
            }
            let count = group_len / 3 - 1;
            let mut field_pos = pos + 3;
            for _ in 0..count {
                fields.push(FieldDescriptor {
                    field_type: data[field_pos],
                    precision_scale: [data[field_pos + 1], data[field_pos + 2]],
                });
                field_pos += 3;
            }
        }
        pos = group_end;
    }
    Ok(fields)
}

pub fn parse_sql_dta_reply(data: &[u8]) -> Result<(Vec<FieldDescriptor>, Vec<u8>), String> {
    let mut descriptor = None;
    let mut values = None;
    visit_ddm_parameters(data, |object| match object.codepoint {
        value if value == cp::FDODSC => descriptor = Some(object.payload),
        value if value == cp::FDODTA => values = Some(object.payload),
        _ => (),
    })?;
    let fields = descriptor
        .map(parse_query_descriptor)
        .transpose()?
        .unwrap_or_default();
    let values = if let Some(value) = values {
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(value.len())
            .map_err(|error| format!("unable to reserve SQLDTA values: {error}"))?;
        owned.extend_from_slice(value);
        owned
    } else {
        Vec::new()
    };
    Ok((fields, values))
}

fn parse_string(data: &[u8]) -> Result<(String, &[u8]), String> {
    if data.len() < 2 {
        return Err("truncated length prefix for DDM string".into());
    }
    let len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let end = 2usize
        .checked_add(len)
        .ok_or("DDM string length overflow")?;
    if end > data.len() {
        return Err(format!(
            "DDM string declares {len} bytes but only {} remain",
            data.len() - 2
        ));
    }
    Ok((
        String::from_utf8_lossy(&data[2..2 + len]).to_string(),
        &data[end..],
    ))
}

fn parse_name(data: &[u8]) -> Result<(String, &[u8]), String> {
    let (first, rest) = parse_string(data)?;
    let (second, rest) = parse_string(rest)?;
    Ok(if first.is_empty() {
        (second, rest)
    } else {
        (first, rest)
    })
}
fn printable_utf8(data: &[u8]) -> bool {
    std::str::from_utf8(data).is_ok_and(|s| !s.chars().any(char::is_control))
}

pub fn parse_sql_dard(
    data: &[u8],
    little_endian: bool,
) -> Result<Vec<ColumnDescription>, crate::Db2Error> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let has_name = data[0] == 0;
    let mut rest: &[u8];
    if data[0] == 0xff {
        rest = &data[1..];
    } else {
        let card = parse_sql_card(data, little_endian).map_err(crate::Db2Error::from)?;
        if card.sql_code < 0 {
            return Err(crate::Db2Error::sql(
                card.sql_code,
                &card.sql_state,
                &card.message,
            ));
        }
        if data.len() > 54 {
            rest = &data[54..];
            for _ in 0..3 {
                if rest.len() < 2 {
                    break;
                }
                let len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
                if rest.len() >= 2 + len {
                    rest = &rest[2 + len..];
                }
            }
            if rest.first() == Some(&0xff) {
                rest = &rest[1..];
            }
        } else {
            rest = data;
        }
    }
    if rest.len() < 2 {
        return Err(crate::Db2Error::from(
            "SQLDARD is missing its SQLNUMROW field".to_string(),
        ));
    }
    if rest[0] == 0 {
        if rest.len() < 14 {
            return Err(crate::Db2Error::from(
                "truncated SQLDARD header group".to_string(),
            ));
        }
        rest = &rest[13..];
        let (_, r) = parse_string(rest)?;
        rest = r;
        let (_, r) = parse_name(rest)?;
        rest = r;
    } else {
        rest = &rest[1..];
    }
    if rest.len() < 2 {
        return Err(crate::Db2Error::from(
            "SQLDARD is missing its SQLNUMROW field".to_string(),
        ));
    }
    let count = get_u16(rest, 0, little_endian) as usize;
    rest = &rest[2..];
    let mut cols = Vec::with_capacity(count);
    for index in 0..count {
        if rest.len() < 16 {
            return Err(crate::Db2Error::from(format!(
                "SQLDARD column {} is missing its fixed descriptor",
                index + 1
            )));
        }
        let precision = get_u16(rest, 0, little_endian);
        let scale = get_u16(rest, 2, little_endian);
        let length = get_u64(rest, 4, little_endian);
        let sql_type = get_u16(rest, 12, little_endian);
        rest = &rest[16..];
        let mut name = String::new();
        if let Some(delim) = rest.windows(3).position(|w| w == [0xff, 0xff, 0xff]) {
            let chunk = &rest[..delim];
            rest = &rest[delim + 3..];
            for offset in 0..chunk.len().saturating_sub(1) {
                let n = chunk[offset] as usize;
                if n > 0
                    && offset + 1 + n <= chunk.len()
                    && printable_utf8(&chunk[offset + 1..offset + 1 + n])
                {
                    name = String::from_utf8_lossy(&chunk[offset + 1..offset + 1 + n]).to_string();
                    break;
                }
            }
        } else if has_name {
            if rest.len() >= 9 {
                rest = &rest[9..];
                let (candidate, r) = parse_name(rest)?;
                rest = r;
                let (label, r) = parse_name(rest)?;
                rest = r;
                let (_, r) = parse_name(rest)?;
                rest = r;
                if rest.len() >= 7 {
                    rest = &rest[7..];
                }
                name = if label.is_empty() { candidate } else { label };
            }
        } else if rest.len() >= 29 {
            rest = &rest[29..];
        }
        if name.is_empty() {
            name = format!("PARAM{}", index + 1);
        }
        cols.push(ColumnDescription {
            name,
            sql_type,
            length,
            precision,
            scale: normalize_scale(sql_type, length, scale),
            nullable: !sql_type.is_multiple_of(2),
        });
    }
    Ok(cols)
}

pub fn parse_sql_cursor_info(
    data: &[u8],
    little_endian: bool,
) -> Result<Vec<ColumnDescription>, String> {
    if data.is_empty() {
        return Err("empty SQLCINRD payload".into());
    }
    for level in [10, 9, 8, 7] {
        for width in [4, 2] {
            if let Ok(columns) = parse_sql_cursor_info_modern(data, little_endian, level, width) {
                return Ok(columns);
            }
        }
    }
    match parse_sql_cursor_info_strict(data) {
        Ok(columns) => Ok(columns),
        Err(_) => parse_sql_cursor_info_legacy(data, little_endian),
    }
}

fn parse_sql_cursor_info_modern(
    data: &[u8],
    little_endian: bool,
    sqlam_level: u16,
    header_string_width: usize,
) -> Result<Vec<ColumnDescription>, String> {
    let mut offset = 0;
    if read_nullable_group(data, &mut offset)? {
        require_fdoca(data, offset, 12)?;
        offset += 12;
        let _ = read_fdoca_string_width(data, &mut offset, header_string_width)?;
        let _ = read_fdoca_string_pair_width(data, &mut offset, header_string_width)?;
    }
    require_fdoca(data, offset, 2)?;
    let count = get_u16(data, offset, little_endian) as usize;
    offset += 2;
    let mut columns = Vec::with_capacity(count);
    for index in 0..count {
        require_fdoca(data, offset, 16)?;
        let precision = get_u16(data, offset, little_endian);
        let scale = get_u16(data, offset + 2, little_endian);
        let length = get_u64(data, offset + 4, little_endian);
        let sql_type = get_u16(data, offset + 12, little_endian);
        offset += 16;
        if sqlam_level >= 9 {
            require_fdoca(data, offset, 8)?;
            offset += 8; // SQLARREXTENT.
            if sqlam_level >= 10 {
                require_fdoca(data, offset, 2)?;
                offset += 2; // SQLAM 10 descriptor extension.
            }
        }
        let mut name = String::new();
        if read_nullable_group(data, &mut offset)? {
            require_fdoca(data, offset, 2)?;
            offset += 2; // SQLUNNAMED.
            let sql_name = read_fdoca_string_pair(data, &mut offset)?;
            let sql_label = read_fdoca_string_pair(data, &mut offset)?;
            let _ = read_fdoca_string_pair(data, &mut offset)?; // SQLCOMMENTS.
            let mut sql_x_name = String::new();
            if read_nullable_group(data, &mut offset)? && sql_type & 0xfffe == 2000 {
                require_fdoca(data, offset, 4)?;
                offset += 4;
                let _ = read_fdoca_string(data, &mut offset)?;
                let _ = read_fdoca_string_pair(data, &mut offset)?;
                let _ = read_fdoca_string_pair(data, &mut offset)?;
                if sqlam_level >= 10 {
                    let _ = read_fdoca_string_pair(data, &mut offset)?;
                }
            }
            if read_nullable_group(data, &mut offset)? {
                let flags_length = if sqlam_level >= 9 { 12 } else { 8 };
                require_fdoca(data, offset, flags_length)?;
                offset += flags_length;
                let _ = read_fdoca_string(data, &mut offset)?; // SQLXRDBNAM.
                let _ = read_fdoca_string_pair(data, &mut offset)?; // SQLXCORNAME.
                let _ = read_fdoca_string_pair(data, &mut offset)?; // SQLXBASENAME.
                let _ = read_fdoca_string_pair(data, &mut offset)?; // SQLXSCHEMA.
                sql_x_name = read_fdoca_string_pair(data, &mut offset)?;
                if sqlam_level >= 10 {
                    let _ = read_fdoca_string_pair(data, &mut offset)?; // SQLXMODULE.
                }
            }
            if sqlam_level >= 10 {
                require_fdoca(data, offset, 1)?;
                offset += 1;
            }
            name = if !sql_label.is_empty() {
                sql_label
            } else if !sql_x_name.is_empty() {
                sql_x_name
            } else {
                sql_name
            };
        }
        if name.is_empty() {
            name = format!("COLUMN{}", index + 1);
        }
        columns.push(ColumnDescription {
            name,
            sql_type,
            length,
            precision,
            scale: normalize_scale(sql_type, length, scale),
            nullable: sql_type & 1 != 0,
        });
    }
    if offset != data.len() {
        return Err(format!(
            "SQLCINRD has {} trailing bytes",
            data.len() - offset
        ));
    }
    Ok(columns)
}

fn parse_sql_cursor_info_strict(data: &[u8]) -> Result<Vec<ColumnDescription>, String> {
    let mut offset = 0;
    if read_nullable_group(data, &mut offset)? {
        require_fdoca(data, offset, 12)?;
        offset += 12;
        let _ = read_fdoca_string(data, &mut offset)?;
        let _ = read_fdoca_string_pair(data, &mut offset)?;
    }
    require_fdoca(data, offset, 2)?;
    let count = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
    offset += 2;
    let mut columns = Vec::with_capacity(count);
    for index in 0..count {
        require_fdoca(data, offset, 16)?;
        let precision = u16::from_le_bytes([data[offset], data[offset + 1]]);
        let scale = u16::from_le_bytes([data[offset + 2], data[offset + 3]]);
        let length = u64::from_le_bytes(data[offset + 4..offset + 12].try_into().unwrap());
        let sql_type = u16::from_le_bytes([data[offset + 12], data[offset + 13]]);
        offset += 16;
        let mut name = String::new();
        if read_nullable_group(data, &mut offset)? {
            require_fdoca(data, offset, 2)?;
            offset += 2;
            name = read_fdoca_string_pair(data, &mut offset)?;
            let _ = read_fdoca_string_pair(data, &mut offset)?;
            let _ = read_fdoca_string_pair(data, &mut offset)?;
            if read_nullable_group(data, &mut offset)? {
                let _ = read_fdoca_string_pair(data, &mut offset)?;
                let _ = read_fdoca_string_pair(data, &mut offset)?;
            }
            if read_nullable_group(data, &mut offset)? {
                require_fdoca(data, offset, 8)?;
                offset += 8;
                let _ = read_fdoca_string(data, &mut offset)?;
                for _ in 0..4 {
                    let _ = read_fdoca_string_pair(data, &mut offset)?;
                }
            }
        }
        if name.is_empty() {
            name = format!("COLUMN{}", index + 1);
        }
        columns.push(ColumnDescription {
            name,
            sql_type,
            length,
            precision,
            scale: normalize_scale(sql_type, length, scale),
            nullable: sql_type & 1 != 0,
        });
    }
    if offset != data.len() {
        return Err("SQLCINRD contains optional descriptor extensions".into());
    }
    Ok(columns)
}

fn parse_sql_cursor_info_legacy(
    data: &[u8],
    little_endian: bool,
) -> Result<Vec<ColumnDescription>, String> {
    let has_name = data[0] == 0;
    let mut rest = if data[0] == 0xff {
        &data[1..]
    } else {
        if data.len() < 13 {
            return Err("truncated SQLCINRD header group".into());
        }
        let mut rest = &data[13..];
        let (_, remaining) = parse_string(rest)?;
        rest = remaining;
        let (_, remaining) = parse_name(rest)?;
        remaining
    };
    if rest.len() < 2 {
        return Err("SQLCINRD is missing SQLNUMROW".into());
    }
    let count = get_u16(rest, 0, little_endian) as usize;
    rest = &rest[2..];
    let mut columns = Vec::with_capacity(count);
    for index in 0..count {
        if rest.len() < 16 {
            return Err(format!(
                "SQLCINRD column {} is missing its fixed descriptor",
                index + 1
            ));
        }
        let precision = get_u16(rest, 0, little_endian);
        let scale = get_u16(rest, 2, little_endian);
        let length = get_u64(rest, 4, little_endian);
        let sql_type = get_u16(rest, 12, little_endian);
        rest = &rest[16..];
        let mut name = String::new();
        if let Some(delimiter) = rest
            .windows(3)
            .position(|window| window == [0xff, 0xff, 0xff])
        {
            let chunk = &rest[..delimiter];
            rest = &rest[delimiter + 3..];
            for position in 0..chunk.len().saturating_sub(1) {
                let length = chunk[position] as usize;
                if length > 0
                    && position + 1 + length <= chunk.len()
                    && printable_utf8(&chunk[position + 1..position + 1 + length])
                {
                    name = String::from_utf8_lossy(&chunk[position + 1..position + 1 + length])
                        .into_owned();
                    break;
                }
            }
        } else if has_name && rest.len() >= 9 {
            rest = &rest[9..];
            let (candidate, remaining) = parse_name(rest)?;
            rest = remaining;
            let (label, remaining) = parse_name(rest)?;
            rest = remaining;
            let (_, remaining) = parse_name(rest)?;
            rest = remaining;
            if rest.len() >= 7 {
                rest = &rest[7..];
            }
            name = if label.is_empty() { candidate } else { label };
        } else if !has_name && rest.len() >= 29 {
            rest = &rest[29..];
        }
        if name.is_empty() {
            name = format!("COLUMN{}", index + 1);
        }
        columns.push(ColumnDescription {
            name,
            sql_type,
            length,
            precision,
            scale: normalize_scale(sql_type, length, scale),
            nullable: sql_type & 1 != 0,
        });
    }
    Ok(columns)
}

fn require_fdoca(data: &[u8], offset: usize, length: usize) -> Result<(), String> {
    if offset > data.len() || length > data.len() - offset {
        return Err(format!("truncated SQLCINRD FDOCA data at offset {offset}"));
    }
    Ok(())
}

fn read_nullable_group(data: &[u8], offset: &mut usize) -> Result<bool, String> {
    require_fdoca(data, *offset, 1)?;
    let present = data[*offset] != 0xff;
    *offset += 1;
    Ok(present)
}

fn read_fdoca_string(data: &[u8], offset: &mut usize) -> Result<String, String> {
    require_fdoca(data, *offset, 2)?;
    let length = u16::from_be_bytes([data[*offset], data[*offset + 1]]) as usize;
    *offset += 2;
    require_fdoca(data, *offset, length)?;
    let value = String::from_utf8_lossy(&data[*offset..*offset + length]).into_owned();
    *offset += length;
    Ok(value)
}

fn read_fdoca_string_width(
    data: &[u8],
    offset: &mut usize,
    width: usize,
) -> Result<String, String> {
    require_fdoca(data, *offset, width)?;
    let length = if width == 4 {
        u32::from_be_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize
    } else {
        u16::from_be_bytes(data[*offset..*offset + 2].try_into().unwrap()) as usize
    };
    *offset += width;
    require_fdoca(data, *offset, length)?;
    let value = String::from_utf8_lossy(&data[*offset..*offset + length]).into_owned();
    *offset += length;
    Ok(value)
}

fn read_fdoca_string_pair_width(
    data: &[u8],
    offset: &mut usize,
    width: usize,
) -> Result<String, String> {
    let mbcs = read_fdoca_string_width(data, offset, width)?;
    let sbcs = read_fdoca_string_width(data, offset, width)?;
    if !mbcs.is_empty() && !sbcs.is_empty() {
        return Err("SQLCINRD VCM/VCS string has both encodings".into());
    }
    Ok(if mbcs.is_empty() { sbcs } else { mbcs })
}

fn read_fdoca_string_pair(data: &[u8], offset: &mut usize) -> Result<String, String> {
    let mbcs = read_fdoca_string(data, offset)?;
    let sbcs = read_fdoca_string(data, offset)?;
    if !mbcs.is_empty() && !sbcs.is_empty() {
        return Err("SQLCINRD VCM/VCS string has both encodings".into());
    }
    Ok(if mbcs.is_empty() { sbcs } else { mbcs })
}

#[cfg(test)]
mod tests {
    #[test]
    fn interrupt_request_uses_intrdbrqs_and_original_token() {
        assert_eq!(cp::INTRDBRQS.0, 0x2003);
        assert_eq!(cp::DRPPKG.0, 0x2007);
        assert_eq!(
            pack_interrupt_rdb_request(&[0x04, 0x1c]),
            vec![0, 10, 0x20, 0x03, 0, 6, 0x21, 0x03, 0x04, 0x1c]
        );
    }

    #[test]
    fn progressive_lob_chunks_match_observed_drda_fields_and_reply_markers() {
        assert_eq!(cp::GETNXTCHK.0, 0x2015);
        assert_eq!(cp::GETNXTLEN.0, 0x214e);
        assert_eq!(cp::GETNXTREF.0, 0x214f);
        assert_eq!(cp::ENDDTARM.0, 0x2214);
        assert_eq!(cp::ENDREF.0, 0x2163);
        assert_eq!(cp::LSTCHK.0, 0x2164);

        let request = pack_get_next_chunk(
            "SQLC0X0",
            "",
            2,
            "TESTDB",
            0x0102_0304_0506_0708,
            0x1122_3344_5566_7788,
            i32::MAX as u64,
        );
        assert_eq!(&request[2..4], &cp::GETNXTCHK.0.to_be_bytes());
        let parameters = parse_ddm_parameters(&request[4..]).unwrap();
        assert_eq!(parameters[&cp::PKGNAMCSN].len(), 64);
        assert_eq!(
            u64::from_be_bytes(parameters[&cp::QRYINSID][..8].try_into().unwrap()),
            0x0102_0304_0506_0708
        );
        assert_eq!(
            u64::from_be_bytes(parameters[&cp::GETNXTREF][..8].try_into().unwrap()),
            0x1122_3344_5566_7788
        );
        assert_eq!(
            u64::from_be_bytes(parameters[&cp::GETNXTLEN][..8].try_into().unwrap()),
            i32::MAX as u64
        );

        let header = crate::network::dss::DssHeader {
            length: 0,
            kind: crate::network::dss::DSS_TYPE_OBJECT,
            chained: false,
            same_id: false,
            has_error: false,
            correlation_id: 1,
        };
        let intermediate = parse_progressive_chunk_reply(&[
            DssPacket::new(header, cp::EXTDTA, vec![0, 1, 2, 3], false),
            DssPacket::new(
                header,
                cp::ENDDTARM,
                [pack_u16(cp::SVRCOD, 0), pack_bytes(cp::RDBNAM, b"TESTDB")].concat(),
                false,
            ),
        ])
        .unwrap();
        assert_eq!(intermediate.data, [1, 2, 3]);
        assert!(!intermediate.end_of_reference);
        assert!(!intermediate.is_last_chunk);

        let final_chunk = parse_progressive_chunk_reply(&[
            DssPacket::new(header, cp::EXTDTA, vec![0, 4, 5], false),
            DssPacket::new(
                header,
                cp::ENDDTARM,
                [
                    pack_u16(cp::SVRCOD, 0),
                    pack_bytes(cp::ENDREF, &[0xf1]),
                    pack_bytes(cp::LSTCHK, &[0xf1]),
                ]
                .concat(),
                false,
            ),
        ])
        .unwrap();
        assert_eq!(final_chunk.data, [4, 5]);
        assert!(final_chunk.end_of_reference);
        assert!(final_chunk.is_last_chunk);
        assert!(parse_progressive_chunk_reply(&[]).is_err());
    }

    #[test]
    fn progressive_lob_reply_parser_rejects_malformed_boundaries_and_allows_empty_final_chunk() {
        let header = crate::network::dss::DssHeader {
            length: 0,
            kind: crate::network::dss::DSS_TYPE_OBJECT,
            chained: false,
            same_id: false,
            has_error: false,
            correlation_id: 1,
        };
        let packet = |codepoint, payload| DssPacket::new(header, codepoint, payload, false);
        let success = pack_u16(cp::SVRCOD, 0);
        let end = |fields: Vec<u8>| packet(cp::ENDDTARM, fields);
        let reject = |expected: &str, replies: Vec<DssPacket>| {
            let error = parse_progressive_chunk_reply(&replies).unwrap_err();
            assert!(
                error.contains(expected),
                "expected {expected:?}, got {error:?}"
            );
        };

        reject("did not contain ENDDTARM", vec![]);
        reject(
            "invalid EXTDTA status byte",
            vec![packet(cp::EXTDTA, vec![]), end(success.clone())],
        );
        reject(
            "invalid EXTDTA status byte",
            vec![packet(cp::EXTDTA, vec![1, 0xaa]), end(success.clone())],
        );
        reject("omitted its two-byte SVRCOD", vec![end(vec![])]);
        reject(
            "omitted its two-byte SVRCOD",
            vec![end(pack_bytes(cp::SVRCOD, &[0]))],
        );
        reject("returned SVRCOD 8", vec![end(pack_u16(cp::SVRCOD, 8))]);
        reject(
            "multiple ENDDTARM",
            vec![end(success.clone()), end(success.clone())],
        );
        reject(
            "EXTDTA after ENDDTARM",
            vec![end(success.clone()), packet(cp::EXTDTA, vec![0, 0x11])],
        );
        reject(
            "ENDREF Boolean value",
            vec![end([success.clone(), pack_bytes(cp::ENDREF, &[1])].concat())],
        );
        reject(
            "LSTCHK Boolean value",
            vec![end(
                [success.clone(), pack_bytes(cp::LSTCHK, &[0xf2])].concat()
            )],
        );

        let empty_intermediate =
            parse_progressive_chunk_reply(&[packet(cp::EXTDTA, vec![0]), end(success.clone())])
                .unwrap();
        assert!(empty_intermediate.data.is_empty());
        assert!(!empty_intermediate.end_of_reference);
        assert!(!empty_intermediate.is_last_chunk);

        let empty_final = parse_progressive_chunk_reply(&[
            packet(cp::EXTDTA, vec![0]),
            end([
                success,
                pack_bytes(cp::ENDREF, &[0xf1]),
                pack_bytes(cp::LSTCHK, &[0xf1]),
            ]
            .concat()),
        ])
        .unwrap();
        assert!(empty_final.data.is_empty());
        assert!(empty_final.end_of_reference);
        assert!(empty_final.is_last_chunk);
    }

    #[test]
    fn open_query_requests_progressive_lob_data_format() {
        let request = pack_open_query_with_params("SQLC0X0", "SYSLVL01", 65, "TESTDB", 65_535);
        let parameters = parse_ddm_parameters(&request[4..]).unwrap();

        assert_eq!(&request[2..4], &cp::OPNQRY.0.to_be_bytes());
        assert_eq!(parameters[&cp::DYNDTAFMT], [0xf1]);
        assert_eq!(parameters[&cp::QRYBLKSZ], 65_535_u32.to_be_bytes());
        assert_eq!(parameters[&cp::MAXBLKEXT], u16::MAX.to_be_bytes());
    }

    use super::*;
    use crate::converters::ebcdic::decode_cp500;

    #[test]
    fn timestamp_scale_is_recovered_from_sqllength_for_query_and_procedure_metadata() {
        for (length, expected_scale) in [(19_u64, 0_u16), (23, 3), (26, 6), (32, 12)] {
            let mut sqlvar = [0_u8; 16];
            sqlvar[4..12].copy_from_slice(&length.to_le_bytes());
            sqlvar[12..14].copy_from_slice(&393_u16.to_le_bytes()); // Nullable TIMESTAMP.

            let mut dard = vec![0xff, 0xff, 1, 0];
            dard.extend_from_slice(&sqlvar);
            let query_column = parse_sql_dard(&dard, true).unwrap().remove(0);
            assert_eq!(query_column.scale, expected_scale);

            let mut cursor_info = vec![0xff, 1, 0];
            cursor_info.extend_from_slice(&sqlvar);
            cursor_info.push(0xff); // No SQLDOPTGRP.
            let procedure_column = parse_sql_cursor_info(&cursor_info, true).unwrap().remove(0);
            assert_eq!(procedure_column.scale, expected_scale);
        }
    }

    #[test]
    fn cp500_and_handshake_objects_match_reference_shapes() {
        let data = encode_cp500("SELECT 1 FROM SYSIBM.SYSDUMMY1").unwrap();
        assert_eq!(decode_cp500(&data), "SELECT 1 FROM SYSIBM.SYSDUMMY1");
        let excsat = pack_excsat(
            "justybase-db2",
            "myhost",
            "justybase-db2-v0.1",
            "justybase-db2",
            &default_manager_levels(),
        )
        .unwrap();
        assert_eq!(
            u16::from_be_bytes([excsat[0], excsat[1]]) as usize,
            excsat.len()
        );
        assert_eq!(
            CodePoint(u16::from_be_bytes([excsat[2], excsat[3]])).name(),
            "EXCSAT"
        );
        let params = parse_ddm_parameters(&excsat[4..]).unwrap();
        assert_eq!(decode_cp500(&params[&cp::EXTNAM]), "justybase-db2");
        assert_eq!(
            params[&cp::MGRLVLLS].len(),
            default_manager_levels().len() * 4
        );
    }

    #[test]
    fn packs_security_package_and_sql_objects() {
        let accsec = pack_accsec("TESTDB", SecurityMechanism::USRIDPWD, &[1, 2]).unwrap();
        let params = parse_ddm_parameters(&accsec[4..]).unwrap();
        assert_eq!(
            u16::from_be_bytes([params[&cp::SECMEC][0], params[&cp::SECMEC][1]]),
            3
        );
        assert_eq!(decode_cp500(&params[&cp::RDBNAM]), "TESTDB");
        assert_eq!(params[&cp::SECTKN], vec![1, 2]);

        let user_only = pack_secchk(
            4,
            &[],
            "TESTDB",
            "db2inst1",
            b"must-not-be-sent",
            false,
            StringEncoding::Cp500,
        )
        .unwrap();
        let user_only = parse_ddm_parameters(&user_only[4..]).unwrap();
        assert_eq!(
            u16::from_be_bytes([user_only[&cp::SECMEC][0], user_only[&cp::SECMEC][1]]),
            4
        );
        assert_eq!(decode_cp500(&user_only[&cp::USRID]), "db2inst1");
        assert!(!user_only.contains_key(&cp::PASSWORD));
        assert!(
            pack_secchk(
                9,
                &[],
                "TESTDB",
                "db2inst1",
                b"secret",
                false,
                StringEncoding::Cp500
            )
            .is_err()
        );

        let pkg = pack_package_name_csn("TESTDB", "SQLC0X0", "", 0);
        assert_eq!(u16::from_be_bytes([pkg[0], pkg[1]]) as usize, pkg.len());
        assert_eq!(
            pack_sql_statement("SELECT 1")[2..4],
            cp::SQLSTT.0.to_be_bytes()
        );
        assert_eq!(pack_commit()[2..4], cp::RDBCMM.0.to_be_bytes());
    }

    #[test]
    fn procedure_result_set_execute_and_continue_requests_match_drda_order() {
        fn child_codepoints(data: &[u8]) -> Vec<u16> {
            let mut result = Vec::new();
            let mut offset = 4;
            while offset < data.len() {
                let length = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
                assert!(length >= 4 && offset + length <= data.len());
                result.push(u16::from_be_bytes([data[offset + 2], data[offset + 3]]));
                offset += length;
            }
            result
        }

        let execute = pack_execute_sql_statement_for_result_sets(
            "SQLC0X0",
            "",
            0,
            "TESTDB",
            u16::MAX,
            32767,
            Some(64),
            false,
        );
        let execute_params = parse_ddm_parameters(&execute[4 + 68..]).unwrap();
        assert_eq!(execute[2..4], cp::EXCSQLSTT.0.to_be_bytes());
        assert_eq!(
            child_codepoints(&execute),
            vec![
                cp::PKGNAMCSN.0,
                cp::RDBCMTOK.0,
                cp::OUTEXP.0,
                cp::QRYBLKSZ.0,
                cp::MAXBLKEXT.0,
                cp::MAXRSLCNT.0,
                cp::RSLSETFLG.0,
                cp::QRYROWSET.0,
            ]
        );
        assert_eq!(execute_params[&cp::OUTEXP], [0xf0]);
        assert_eq!(
            u32::from_be_bytes(execute_params[&cp::QRYBLKSZ][..4].try_into().unwrap()),
            32767
        );
        assert_eq!(
            u16::from_be_bytes(execute_params[&cp::MAXBLKEXT][..2].try_into().unwrap()),
            u16::MAX
        );
        assert_eq!(
            u16::from_be_bytes(execute_params[&cp::MAXRSLCNT][..2].try_into().unwrap()),
            u16::MAX
        );
        assert_eq!(execute_params[&cp::RSLSETFLG], [0x04]);
        assert_eq!(
            u32::from_be_bytes(execute_params[&cp::QRYROWSET][..4].try_into().unwrap()),
            64
        );

        let section = ResultSetPackageSection {
            database: b"TESTDB".to_vec(),
            schema: b"NULLID".to_vec(),
            package_name: b"SQLC0X0".to_vec(),
            consistency_token: [1; 8],
            section_number: 2,
        };
        let continue_query =
            pack_continue_result_set_query(&section, 32767, 19, Some(64), false).unwrap();
        let continue_params = parse_ddm_parameters(&continue_query[4..]).unwrap();
        assert_eq!(continue_query[2..4], cp::CNTQRY.0.to_be_bytes());
        assert_eq!(
            child_codepoints(&continue_query),
            vec![
                cp::PKGNAMCSN.0,
                cp::QRYBLKSZ.0,
                cp::MAXBLKEXT.0,
                cp::QRYINSID.0,
                cp::QRYROWSET.0,
            ]
        );
        assert_eq!(
            u32::from_be_bytes(continue_params[&cp::QRYBLKSZ][..4].try_into().unwrap()),
            32767
        );
        assert_eq!(
            u16::from_be_bytes(continue_params[&cp::MAXBLKEXT][..2].try_into().unwrap()),
            u16::MAX
        );
        assert_eq!(
            u64::from_be_bytes(continue_params[&cp::QRYINSID][..8].try_into().unwrap()),
            19
        );
        assert_eq!(
            u32::from_be_bytes(continue_params[&cp::QRYROWSET][..4].try_into().unwrap()),
            64
        );
        assert!(!continue_params.contains_key(&cp::RTNEXTDTA));

        let all_data = pack_continue_result_set_query(&section, 32767, 19, Some(64), true).unwrap();
        let all_data_params = parse_ddm_parameters(&all_data[4..]).unwrap();
        assert_eq!(all_data_params[&cp::RTNEXTDTA], [0x02]);
    }

    #[test]
    fn procedure_cursor_metadata_and_fetch_close_requests_parse() {
        fn append_fdoca_pair(output: &mut Vec<u8>, value: &str) {
            output.extend_from_slice(&[0, 0]);
            output.extend_from_slice(&(value.len() as u16).to_be_bytes());
            output.extend_from_slice(value.as_bytes());
        }

        let mut cursor_info = vec![0xff, 1, 0, 10, 0, 0, 0, 4];
        cursor_info.extend_from_slice(&[0; 7]);
        cursor_info.extend_from_slice(&[0xf1, 1, 0, 0, 0, 0, 0]);
        append_fdoca_pair(&mut cursor_info, "VALUE");
        append_fdoca_pair(&mut cursor_info, "VALUE");
        append_fdoca_pair(&mut cursor_info, "");
        cursor_info.extend_from_slice(&[0xff, 0xff]);
        let column = parse_sql_cursor_info(&cursor_info, true).unwrap().remove(0);
        assert_eq!(column.name, "VALUE");
        assert_eq!(column.sql_type, 0x01f1);
        assert!(column.nullable);
        assert_eq!(column.length, 4);
        assert_eq!(column.precision, 10);

        let mut summary = vec![2, 0];
        summary.extend_from_slice(&[0; 24]);
        assert_eq!(parse_sql_result_set_count(&summary).unwrap(), 2);
        assert!(parse_sql_result_set_count(&[2, 0]).is_err());

        let first = pack_package_name_csn("TESTDB", "SQLC0X0", "", 1);
        let second = pack_package_name_csn("TESTDB", "SQLC0X0", "", 2);
        let package_list = pack_ddm_object(cp::PKGSNLST, &concat(&[&first, &second]));
        let mut sections = parse_result_set_sections(&package_list).unwrap();
        assert_eq!(
            sections
                .iter()
                .map(|item| item.section_number)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        let section = sections.remove(0);

        let fetch = pack_continue_procedure_result_set(&section, 32_767, 19).unwrap();
        assert_eq!(
            child_codepoints(&fetch),
            vec![
                cp::PKGNAMCSN.0,
                cp::QRYBLKSZ.0,
                cp::QRYINSID.0,
                cp::RTNEXTDTA.0,
                cp::FREPRVREF.0
            ]
        );
        let fetch_values = parse_ddm_parameters(&fetch[4..]).unwrap();
        assert_eq!(fetch_values[&cp::RTNEXTDTA], [0x02]);
        assert_eq!(fetch_values[&cp::FREPRVREF], [0xf0]);

        let close = pack_close_procedure_result_set(&section, 19).unwrap();
        assert_eq!(close[2..4], cp::CLSQRY.0.to_be_bytes());
        assert_eq!(
            child_codepoints(&close),
            vec![cp::PKGNAMCSN.0, cp::QRYINSID.0]
        );

        fn child_codepoints(data: &[u8]) -> Vec<u16> {
            let mut values = Vec::new();
            let mut offset = 4;
            while offset < data.len() {
                let length = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
                values.push(u16::from_be_bytes([data[offset + 2], data[offset + 3]]));
                offset += length;
            }
            values
        }
    }

    #[test]
    fn sqlam10_cursor_metadata_prefers_the_extended_expression_alias() {
        fn append_fdoca_pair(output: &mut Vec<u8>, value: &str) {
            output.extend_from_slice(&[0, 0]);
            output.extend_from_slice(&(value.len() as u16).to_be_bytes());
            output.extend_from_slice(value.as_bytes());
        }
        fn append_fdoca_string(output: &mut Vec<u8>, value: &str) {
            output.extend_from_slice(&(value.len() as u16).to_be_bytes());
            output.extend_from_slice(value.as_bytes());
        }

        let mut cursor_info = vec![0xff, 1, 0]; // No SQLDHGRP; one SQLDAROW.
        let mut fixed = [0_u8; 16];
        fixed[12..14].copy_from_slice(&0x01f1_u16.to_le_bytes());
        cursor_info.extend_from_slice(&fixed);
        cursor_info.extend_from_slice(&[0; 10]); // SQLARREXTENT plus SQLAM 10 bytes.
        cursor_info.push(0); // SQLDOPTGRP.
        cursor_info.extend_from_slice(&[0, 0]); // SQLUNNAMED.
        append_fdoca_pair(&mut cursor_info, "V"); // SQLNAME.
        append_fdoca_pair(&mut cursor_info, ""); // SQLLABEL.
        append_fdoca_pair(&mut cursor_info, ""); // SQLCOMMENTS.
        cursor_info.push(0xff); // No SQLUDTGRP.
        cursor_info.push(0); // SQLDXGRP.
        cursor_info.extend_from_slice(&[0; 12]); // SQLAM 9+ SQLDX flags.
        append_fdoca_string(&mut cursor_info, "DB"); // SQLXRDBNAM.
        append_fdoca_pair(&mut cursor_info, "COR");
        append_fdoca_pair(&mut cursor_info, "BASE");
        append_fdoca_pair(&mut cursor_info, "APP");
        append_fdoca_pair(&mut cursor_info, "PAYLOAD"); // SQLXNAME.
        append_fdoca_pair(&mut cursor_info, ""); // SQLXMODULE.
        cursor_info.push(0xff); // SQLAM 10 trailing extension marker.

        let column = parse_sql_cursor_info(&cursor_info, true).unwrap().remove(0);
        assert_eq!(column.name, "PAYLOAD");
        assert_eq!(column.sql_type, 0x01f1);
    }

    #[test]
    fn parses_sqlcard_and_multiple_qrydsc_groups() {
        let mut card = vec![0; 54];
        card[1..5].copy_from_slice(&0i32.to_le_bytes());
        card[5..10].copy_from_slice(b"00000");
        card[27..31].copy_from_slice(&12i32.to_le_bytes());
        let parsed_card = parse_sql_card(&card, true).unwrap();
        assert_eq!(parsed_card.rows_affected, 12);
        assert!(parsed_card.warning().is_none());

        let mut warning_card = vec![0; 54];
        warning_card[1..5].copy_from_slice(&0i32.to_le_bytes());
        warning_card[5..10].copy_from_slice(b"01003");
        warning_card[18] = 0; // SQLCAXGRP is present.
        warning_card[43..54].copy_from_slice(b"W W        ");
        let parsed_warning = parse_sql_card(&warning_card, true).unwrap();
        assert_eq!(parsed_warning.warning_flags, "W W        ");
        let warning = parsed_warning.warning().unwrap();
        assert_eq!(warning.sql_state, "01003");
        assert!(warning.message.contains("01003"));
        let data = [
            9, 0x76, 0xd0, 0x80, 1, 2, 0x81, 3, 4, 6, 0x76, 0xd0, 0x82, 5, 6,
        ];
        let parsed = parse_query_descriptor(&data).unwrap();
        assert_eq!(
            parsed.iter().map(|f| f.field_type).collect::<Vec<_>>(),
            vec![0x80, 0x81, 0x82]
        );
    }

    #[test]
    fn cp500_null_string_rejects_unrepresentable_text_without_utf8_fallback() {
        let encoded = pack_null_string(Some("AéB"), StringEncoding::Cp500).unwrap();
        let length = u32::from_be_bytes(encoded[1..5].try_into().unwrap()) as usize;
        assert_eq!(length, 3);
        assert_eq!(decode_cp500(&encoded[5..]), "AéB");

        assert!(pack_null_string(Some("A😀B"), StringEncoding::Cp500).is_err());
        let utf8 = pack_null_string(Some("A😀B"), StringEncoding::Utf8).unwrap();
        assert_eq!(&utf8[5..], "A😀B".as_bytes());
    }

    #[test]
    fn qrydsc_rejects_invalid_group_lengths_truncation_and_trailing_bytes() {
        for malformed in [
            vec![0],
            vec![1, 0, 0],
            vec![2, 0, 0],
            vec![9, 0x76, 0xd0, 0x02, 0, 4],
            vec![6, 0x7f, 0, 0x02, 0, 4],
            vec![6, 0x76, 0xd0, 0x02, 0, 4, 0],
            vec![6, 0x76, 0xd0, 0x02, 0, 4, 0, 0],
        ] {
            assert!(
                parse_query_descriptor(&malformed).is_err(),
                "malformed descriptor was accepted: {malformed:02x?}"
            );
        }

        for missing in 1..=2 {
            let mut truncated = vec![9, 0x76, 0xd0, 0x02, 0, 4, 0x03, 0, 4];
            truncated.truncate(truncated.len() - missing);
            assert!(parse_query_descriptor(&truncated).is_err());
        }

        let valid_empty_name = [0, 0, 0, 0];
        assert_eq!(parse_name(&valid_empty_name).unwrap().0, "");
        assert!(parse_string(&[]).is_err());
        assert!(parse_string(&[0]).is_err());
        assert!(parse_string(&[0, 2, b'x']).is_err());
        assert_eq!(parse_string(&[0, 0]).unwrap().0, "");
        assert!(parse_name(&[0, 0, 0]).is_err());
    }

    #[test]
    fn ddm_parameter_parser_rejects_malformed_and_overflowing_lengths() {
        let mut overflow = vec![0x80, 0x0c, 0x24, 0x13];
        overflow.extend_from_slice(&u64::MAX.to_be_bytes());
        for malformed in [
            vec![0],
            vec![0, 3, 0x24, 0x13],
            vec![0, 8, 0x24, 0x13],
            vec![0x80, 7, 0x24, 0x13, 0, 0, 0],
            vec![0x80, 8, 0x24, 0x13, 0, 0, 0],
            overflow,
        ] {
            assert!(
                parse_ddm_parameters(&malformed).is_err(),
                "malformed DDM list was accepted: {malformed:02x?}"
            );
        }
    }

    #[test]
    fn borrowed_ddm_objects_expose_packet_slices_and_fail_closed() {
        let mut input = pack_u16(cp::SVRCOD, 0);
        input.extend_from_slice(&pack_u32(cp::QRYBLKSZ, 65_535));
        let first_payload = &input[4..6];
        let mut objects = DdmObjects::new(&input);
        let first = objects.next().unwrap().unwrap();
        assert_eq!(first.codepoint, cp::SVRCOD);
        assert_eq!(first.payload, [0, 0]);
        assert_eq!(first.payload.as_ptr(), first_payload.as_ptr());
        let second = objects.next().unwrap().unwrap();
        assert_eq!(second.codepoint, cp::QRYBLKSZ);
        assert_eq!(second.payload, 65_535_u32.to_be_bytes());
        assert!(objects.next().is_none());

        let found = find_ddm_parameter(&input, cp::SVRCOD).unwrap().unwrap();
        assert_eq!(found.as_ptr(), first_payload.as_ptr());
        let mut malformed_tail = input.clone();
        malformed_tail.push(0);
        assert!(find_ddm_parameter(&malformed_tail, cp::SVRCOD).is_err());

        let extended = pack_ddm_object(cp::SQLDTA, &[0x5a; 32_768]);
        let mut objects = DdmObjects::new(&extended);
        let object = objects.next().unwrap().unwrap();
        assert_eq!(object.codepoint, cp::SQLDTA);
        assert_eq!(object.payload, [0x5a; 32_768]);
        assert!(objects.next().is_none());
    }

    #[test]
    fn sqlcard_reads_sqlerrd_three_after_nullable_extension_indicator() {
        for little_endian in [true, false] {
            for count in [0_i32, 1, 257, -1] {
                let mut card = vec![0; 54];
                card[5..10].copy_from_slice(b"00000");
                for i in 0..6 {
                    let value = if i == 2 { count } else { 0x123456 + i as i32 };
                    let bytes = if little_endian {
                        value.to_le_bytes()
                    } else {
                        value.to_be_bytes()
                    };
                    card[19 + i * 4..23 + i * 4].copy_from_slice(&bytes);
                }
                assert_eq!(
                    parse_sql_card(&card, little_endian).unwrap().rows_affected,
                    i64::from(count)
                );
                card[18] = 0xff;
                assert_eq!(
                    parse_sql_card(&card, little_endian).unwrap().rows_affected,
                    0
                );
            }
        }
        for length in [19, 26, 31, 53] {
            assert!(
                parse_sql_card(&vec![0; length], true)
                    .unwrap_err()
                    .contains("truncated")
            );
        }
    }

    #[test]
    fn sqlcard_zero_row_count_is_not_rewritten_as_one() {
        for sql_code in [0_i32, 100] {
            let mut card = vec![0; 54];
            card[1..5].copy_from_slice(&sql_code.to_le_bytes());
            card[5..10].copy_from_slice(if sql_code == 100 { b"02000" } else { b"00000" });
            card[27..31].copy_from_slice(&0i32.to_le_bytes());
            assert_eq!(parse_sql_card(&card, true).unwrap().rows_affected, 0);
        }

        let card_without_count = vec![0; 18];
        assert_eq!(
            parse_sql_card(&card_without_count, true)
                .unwrap()
                .rows_affected,
            0
        );
    }

    #[test]
    fn sqldard_rejects_missing_count_and_truncated_column_descriptors() {
        assert!(parse_sql_dard(&[0xff, 0xff], true).is_err());
        assert!(parse_sql_dard(&[0xff, 0xff, 1, 0, 0, 0, 0], true).is_err());

        let mut almost_complete = vec![0xff, 0xff, 1, 0];
        almost_complete.extend_from_slice(&[0; 15]);
        assert!(parse_sql_dard(&almost_complete, true).is_err());
    }

    #[test]
    fn parses_qrydsc_cpt_continuations_without_losing_metadata() {
        for count in [83usize, 84, 85, 169, 170] {
            let mut data = Vec::new();
            for start in (0..count).step_by(84) {
                let group_count = (count - start).min(84);
                data.push(((group_count + 1) * 3) as u8);
                data.push(if start == 0 { 0x76 } else { 0x7f });
                data.push(if start == 0 { 0xd0 } else { 0x00 });
                for index in start..start + group_count {
                    data.push(index as u8);
                    data.push((index >> 8) as u8);
                    data.push(255 - (index % 256) as u8);
                }
            }
            let parsed = parse_query_descriptor(&data).unwrap();
            assert_eq!(parsed.len(), count);
            for (index, field) in parsed.iter().enumerate() {
                assert_eq!(field.field_type, index as u8);
                assert_eq!(
                    field.precision_scale,
                    [(index >> 8) as u8, 255 - (index % 256) as u8]
                );
            }
        }
        assert!(
            parse_query_descriptor(&[6, 0x7f, 0, 1, 2, 3])
                .unwrap_err()
                .contains("no preceding NGDA")
        );
    }

    #[test]
    fn query_descriptor_rejects_malformed_groups() {
        let cases: &[(&[u8], &str)] = &[
            (&[1, 2], "too short"),
            (&[2, 0x76, 0xd0], "triplet length 2"),
            (&[9, 0x76, 0xd0, 1, 2, 3], "exceeds the remaining"),
            (&[7, 0x76, 0xd0, 1, 2, 3, 4], "invalid length"),
            (&[3, 0x76, 0xd0, 0x00], "trailing bytes"),
        ];
        for (data, expected) in cases {
            let error = parse_query_descriptor(data).unwrap_err();
            assert!(error.contains(expected), "{data:?}: {error}");
        }
    }

    #[test]
    fn sqlcard_inside_sql_dard_preserves_structured_server_error() {
        let mut card = vec![0; 54];
        card[1..5].copy_from_slice(&(-204_i32).to_le_bytes());
        card[5..10].copy_from_slice(b"42704");

        let error = parse_sql_dard(&card, true).unwrap_err();
        assert_eq!(error.kind, crate::Db2ErrorKind::SqlServer);
        assert_eq!(error.sql_code, Some(-204));
        assert_eq!(error.sql_state.as_deref(), Some("42704"));
        assert_eq!(error.to_string(), "DB2 SQLCODE=-204 SQLSTATE=42704");
    }

    #[test]
    fn parses_sql_dtard_output_parameter_descriptor_and_data() {
        let descriptor = [9, 0x76, 0xd0, 0x04, 0, 2, 0x03, 0, 4];
        let values = [0xff, 0, 0x2a, 0, 0, 0, 0x64, 0, 0, 0];
        let mut payload = pack_ddm_object(cp::FDODSC, &descriptor);
        payload.extend_from_slice(&pack_ddm_object(cp::FDODTA, &values));
        let (fields, actual_values) = parse_sql_dta_reply(&payload).unwrap();
        assert_eq!(
            fields
                .iter()
                .map(|field| field.field_type)
                .collect::<Vec<_>>(),
            vec![4, 3]
        );
        assert_eq!(actual_values, values);
        assert!(parse_sql_dta_reply(&[0, 2, 0x24, 0x13]).is_err());
    }
}
