use std::collections::{HashMap, HashSet};
use std::fmt;

/// Per-connection caps for lengths and materialized server responses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolLimits {
    pub max_dss_size: usize,
    pub max_ddm_object_size: usize,
    pub max_reply_chain_bytes: usize,
    pub max_reply_chain_packets: usize,
    pub max_materialized_lob_size: usize,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_dss_size: 64 * 1024 * 1024,
            max_ddm_object_size: 64 * 1024 * 1024,
            max_reply_chain_bytes: 256 * 1024 * 1024,
            max_reply_chain_packets: 4_096,
            max_materialized_lob_size: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub use_ssl: bool,
    pub ssl_root_ca_path: String,
    pub ssl_client_cert_path: String,
    pub ssl_client_key_path: String,
    /// Timeout in milliseconds; signed to preserve Go's accepted negative durations.
    pub timeout_ms: i64,
    pub block_size: usize,
    pub protocol_limits: ProtocolLimits,
    pub security_mechanism: u16,
    pub kerberos_spn: String,
    pub kerberos_realm: String,
    pub krb5_config_file: String,
    pub krb5_keytab_file: String,
    pub krb5_ccache_file: String,
    pub gssapi_token_file: String,
    pub client_appl_name: String,
    pub client_wrkstn_name: String,
    pub client_userid: String,
    pub client_acctng: String,
    pub client_corr_token: String,
    pub params: HashMap<String, String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 50000,
            database: String::new(),
            user: String::new(),
            password: String::new(),
            use_ssl: false,
            ssl_root_ca_path: String::new(),
            ssl_client_cert_path: String::new(),
            ssl_client_key_path: String::new(),
            timeout_ms: 30_000,
            block_size: 65_535,
            protocol_limits: ProtocolLimits::default(),
            security_mechanism: 0,
            kerberos_spn: String::new(),
            kerberos_realm: String::new(),
            krb5_config_file: String::new(),
            krb5_keytab_file: String::new(),
            krb5_ccache_file: String::new(),
            gssapi_token_file: String::new(),
            client_appl_name: String::new(),
            client_wrkstn_name: String::new(),
            client_userid: String::new(),
            client_acctng: String::new(),
            client_corr_token: String::new(),
            params: HashMap::new(),
        }
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"******")
            .field("use_ssl", &self.use_ssl)
            .field("timeout_ms", &self.timeout_ms)
            .field("block_size", &self.block_size)
            .field("protocol_limits", &self.protocol_limits)
            .field("security_mechanism", &self.security_mechanism)
            .field("params", &self.params)
            .finish()
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let timeout = if self.timeout_ms % 1_000 == 0 {
            format!("{}s", self.timeout_ms / 1_000)
        } else {
            format!("{}ms", self.timeout_ms)
        };
        write!(
            f,
            "Config{{Host:{}, Port:{}, Database:{}, User:{}, Password:\"******\", UseSSL:{}, Timeout:{}}}",
            self.host, self.port, self.database, self.user, self.use_ssl, timeout
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl ConfigError {
    /// Returns the stable driver category for connection-string parsing failures.
    pub fn kind(&self) -> crate::error::Db2ErrorKind {
        crate::error::Db2ErrorKind::InvalidConfiguration
    }
}

impl std::error::Error for ConfigError {}

pub fn parse_dsn(input: &str) -> Result<Config, ConfigError> {
    let dsn = input.trim();
    if dsn.is_empty() {
        return Err(ConfigError("invalid connection string: empty DSN".into()));
    }
    if dsn.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("db2") || scheme.eq_ignore_ascii_case("db2s")
    }) {
        parse_url(dsn)
    } else {
        parse_key_value(dsn)
    }
}

fn parse_url(dsn: &str) -> Result<Config, ConfigError> {
    let (scheme, remainder) = dsn.split_once("://").ok_or_else(invalid_url)?;
    if !scheme.eq_ignore_ascii_case("db2") && !scheme.eq_ignore_ascii_case("db2s") {
        return Err(ConfigError(
            "invalid connection string: expected scheme 'db2' or 'db2s'".into(),
        ));
    }
    let path_start = remainder.find('/').unwrap_or(remainder.len());
    let authority = &remainder[..path_start];
    let path_and_query = &remainder[path_start..];
    let query_start = path_and_query.find('?').unwrap_or(path_and_query.len());
    let raw_path = &path_and_query[..query_start];
    let raw_query = if query_start < path_and_query.len() {
        &path_and_query[query_start + 1..]
    } else {
        ""
    };

    let mut config = Config {
        use_ssl: scheme.eq_ignore_ascii_case("db2s"),
        ..Default::default()
    };
    let (host_port, user_info) = match authority.rsplit_once('@') {
        Some((userinfo, host)) => (host, Some(userinfo)),
        None => (authority, None),
    };
    if let Some(user_info) = user_info {
        let (user, password) = user_info.split_once(':').unwrap_or((user_info, ""));
        config.user = percent_decode(user, false)?;
        config.password = percent_decode(password, false)?;
    }
    if !host_port.is_empty() {
        let (host, port) = parse_host_port(host_port)?;
        if !host.is_empty() {
            config.host = host;
        }
        if let Some(port) = port {
            config.port = port;
        }
    }
    let raw_db = raw_path.strip_prefix('/').unwrap_or(raw_path);
    if raw_db.is_empty() {
        return Err(ConfigError(
            "invalid connection string: missing database name".into(),
        ));
    }
    config.database = percent_decode(raw_db, false)?;

    let mut seen = HashSet::new();
    for pair in raw_query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key, true)?;
        if seen.insert(key.clone()) {
            apply_param(&mut config, &key, &percent_decode(value, true)?)?;
        }
    }
    Ok(config)
}

fn invalid_url() -> ConfigError {
    ConfigError(format!("invalid connection string: {}", "malformed URL"))
}

fn parse_host_port(authority: &str) -> Result<(String, Option<u16>), ConfigError> {
    if authority.starts_with('[') {
        let close = authority
            .find(']')
            .ok_or_else(|| ConfigError("invalid connection string: malformed IPv6 host".into()))?;
        let host = authority[1..close].to_string();
        let suffix = &authority[close + 1..];
        if suffix.is_empty() {
            return Ok((host, None));
        }
        let raw_port = suffix
            .strip_prefix(':')
            .ok_or_else(|| ConfigError("invalid connection string: malformed host".into()))?;
        return parse_port(raw_port).map(|port| (host, Some(port)));
    }
    if let Some((host, raw_port)) = authority.rsplit_once(':') {
        if raw_port.is_empty() {
            return Ok((authority.to_string(), None));
        }
        // An unbracketed IPv6 literal has multiple colons; only treat the last as a port if numeric.
        if raw_port.bytes().all(|b| b.is_ascii_digit()) {
            return parse_port(raw_port).map(|port| (host.to_string(), Some(port)));
        }
    }
    Ok((authority.to_string(), None))
}

fn parse_port(value: &str) -> Result<u16, ConfigError> {
    let port = value
        .parse::<u32>()
        .ok()
        .filter(|p| *p > 0 && *p <= 65_535)
        .ok_or_else(|| ConfigError(format!("invalid connection string: invalid port {value}")))?;
    Ok(port as u16)
}

fn parse_key_value(dsn: &str) -> Result<Config, ConfigError> {
    let mut config = Config::default();
    for pair in dsn.split(';') {
        let pair = pair.trim();
        if let Some((key, value)) = pair.split_once('=') {
            apply_param(&mut config, key.trim(), value.trim())?;
        }
    }
    Ok(config)
}

fn apply_param(config: &mut Config, key: &str, value: &str) -> Result<(), ConfigError> {
    let key_lower = key.to_ascii_lowercase();
    match key_lower.as_str() {
        "host" | "hostname" => config.host = value.into(),
        "server" => {
            let (host, port) = parse_host_port(value)?;
            config.host = host;
            if let Some(port) = port {
                config.port = port;
            }
        }
        "port" => {
            if let Ok(port) = value.parse::<u32>()
                && (1..=65_535).contains(&port)
            {
                config.port = port as u16;
            }
        }
        "database" | "dbname" | "db" => config.database = value.into(),
        "user" | "uid" | "username" => config.user = value.into(),
        "password" | "pwd" => config.password = value.into(),
        "authentication" => {
            let mode = value.trim().to_ascii_lowercase();
            config.security_mechanism = match mode.as_str() {
                "server" => 3,
                "server_encrypt" => 9,
                "kerberos" | "gssplugin" => 11,
                _ => return Err(unsupported_authentication(value)),
            };
        }
        "security" => {
            if !value.eq_ignore_ascii_case("ssl") {
                return Err(ConfigError("only Security=SSL is supported; use a TLS connection or omit the Security keyword".into()));
            }
            config.use_ssl = true;
        }
        "ssl" | "use_ssl" | "usessl" => {
            config.use_ssl = is_true(value);
            if config.use_ssl && config.port == 50_000 {
                config.port = 50_001;
            }
        }
        "ssl_root_ca"
        | "ssl_ca"
        | "sslrootcapath"
        | "ssl_ca_path"
        | "sslservercertificate"
        | "ssl_server_certificate" => {
            config.ssl_root_ca_path = value.into();
            config.use_ssl = true;
        }
        "ssl_client_cert_path" | "ssl_client_cert" | "sslcert" | "cert" => {
            config.ssl_client_cert_path = value.into();
            config.use_ssl = true;
        }
        "ssl_client_key_path" | "ssl_client_key" | "sslkey" | "key" => {
            config.ssl_client_key_path = value.into();
            config.use_ssl = true;
        }
        "timeout" | "connect_timeout" => {
            if let Some(ms) = parse_duration_ms(value) {
                config.timeout_ms = ms;
            }
        }
        "block_size" | "blocksize" | "qryblksz" => {
            if let Ok(size) = value.parse::<usize>()
                && (1_024..=1_048_576).contains(&size)
            {
                config.block_size = size;
            }
        }
        "max_dss_size" | "maxdsssize" => {
            set_positive_limit(&mut config.protocol_limits.max_dss_size, value)
        }
        "max_ddm_object_size" | "maxddmobjectsize" => {
            set_positive_limit(&mut config.protocol_limits.max_ddm_object_size, value)
        }
        "max_reply_chain_bytes" | "maxreplychainbytes" => {
            set_positive_limit(&mut config.protocol_limits.max_reply_chain_bytes, value)
        }
        "max_reply_chain_packets" | "maxreplychainpackets" => {
            set_positive_limit(&mut config.protocol_limits.max_reply_chain_packets, value)
        }
        "max_materialized_lob_size" | "maxmaterializedlobsize" => {
            set_positive_limit(&mut config.protocol_limits.max_materialized_lob_size, value)
        }
        "security_mechanism" | "secmec" | "auth_mechanism" | "auth" => {
            config.security_mechanism = parse_security_mechanism(value)?;
        }
        "spn" | "service_principal" | "server_principal" => config.kerberos_spn = value.into(),
        "realm" | "krb5_realm" => config.kerberos_realm = value.into(),
        "krb5_config" | "krb5_conf" => config.krb5_config_file = value.into(),
        "krb5_keytab" | "keytab" => config.krb5_keytab_file = value.into(),
        "krb5_ccache" | "ccache" => config.krb5_ccache_file = value.into(),
        "gssapi_token_file" | "gssapi_token" => config.gssapi_token_file = value.into(),
        "client_applname" | "applname" | "application_name" | "appname" => {
            config.client_appl_name = value.into()
        }
        "client_wrkstnname" | "wrkstnname" | "workstation_name" | "workstation" => {
            config.client_wrkstn_name = value.into()
        }
        "client_userid" | "client_user" | "end_user" => config.client_userid = value.into(),
        "client_acctng" | "acctng" | "accounting" => config.client_acctng = value.into(),
        "client_corr_token" | "corr_token" | "correlation_token" => {
            config.client_corr_token = value.into()
        }
        _ => {
            config.params.insert(key.into(), value.into());
        }
    }
    Ok(())
}

fn set_positive_limit(target: &mut usize, value: &str) {
    if let Ok(limit) = value.parse::<usize>()
        && limit > 0
    {
        *target = limit;
    }
}

fn is_true(value: &str) -> bool {
    matches!(value.to_ascii_lowercase().as_str(), "true" | "1" | "yes")
}

fn parse_security_mechanism(value: &str) -> Result<u16, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "server" | "usridpwd" | "plaintext" | "secmec3" | "3" => Ok(3),
        "server_encrypt" | "eusridpwd" | "dh_encrypted_password" | "secmec9" | "9" => Ok(9),
        "kerberos" | "gssapi" | "gssplugin" | "secmec11" | "11" => Ok(11),
        "0" => Ok(0),
        _ => match value.trim().parse::<u16>() {
            Ok(0) => Ok(0),
            Ok(3) => Ok(3),
            Ok(9) => Ok(9),
            Ok(11) => Ok(11),
            _ => Err(unsupported_authentication(value)),
        },
    }
}

fn unsupported_authentication(value: &str) -> ConfigError {
    ConfigError(format!(
        "unsupported DB2 authentication mode '{value}'; supported modes are Authentication=SERVER (3), Authentication=SERVER_ENCRYPT / security_mechanism=9 (EUSRIDPWD), and security_mechanism=11 (Kerberos/GSSAPI with gssapi_token_file)"
    ))
}

fn parse_duration_ms(input: &str) -> Option<i64> {
    let input = input.trim();
    if input.bytes().all(|b| b.is_ascii_digit()) {
        return input.parse::<i64>().ok()?.checked_mul(1_000);
    }
    if input.is_empty() {
        return None;
    }
    let (sign, body) = if let Some(rest) = input.strip_prefix('-') {
        (-1.0, rest)
    } else if let Some(rest) = input.strip_prefix('+') {
        (1.0, rest)
    } else {
        (1.0, input)
    };
    let mut index = 0;
    let bytes = body.as_bytes();
    let mut total_ms = 0.0;
    while index < bytes.len() {
        let number_start = index;
        let mut dot = false;
        while index < bytes.len()
            && (bytes[index].is_ascii_digit() || (bytes[index] == b'.' && !dot))
        {
            if bytes[index] == b'.' {
                dot = true;
            }
            index += 1;
        }
        if index == number_start || &body[number_start..index] == "." {
            return None;
        }
        let amount = body[number_start..index].parse::<f64>().ok()?;
        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        if index < bytes.len() && (bytes[index] == 0xB5 || body[index..].starts_with('μ')) {
            let rest = &body[index..];
            if rest.starts_with('µ') {
                index += 'µ'.len_utf8();
            } else if rest.starts_with('μ') {
                index += 'μ'.len_utf8();
            }
        }
        let unit = &body[unit_start..index];
        let multiplier = match unit {
            "h" => 3_600_000.0,
            "m" => 60_000.0,
            "s" => 1_000.0,
            "ms" => 1.0,
            "us" | "µs" | "μs" => 0.001,
            "ns" => 0.000_001,
            _ => return None,
        };
        total_ms += amount * multiplier;
    }
    let value = (total_ms * sign).trunc();
    if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
        None
    } else {
        Some(value as i64)
    }
}

fn percent_decode(input: &str, plus_as_space: bool) -> Result<String, ConfigError> {
    let mut bytes = Vec::with_capacity(input.len());
    let mut iter = input.as_bytes().iter().copied();
    while let Some(byte) = iter.next() {
        match byte {
            b'%' => {
                let hi = iter.next().and_then(hex).ok_or_else(invalid_escape)?;
                let lo = iter.next().and_then(hex).ok_or_else(invalid_escape)?;
                bytes.push((hi << 4) | lo);
            }
            b'+' if plus_as_space => bytes.push(b' '),
            other => bytes.push(other),
        }
    }
    String::from_utf8(bytes).map_err(|_| invalid_escape())
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
fn invalid_escape() -> ConfigError {
    ConfigError("invalid connection string: malformed URL escape".into())
}

pub fn redact_dsn(input: &str) -> String {
    let mut result = input.to_string();
    if let Some(scheme) = result.find("://") {
        let authority_end = result[scheme + 3..]
            .find(['/', '?', '#'])
            .map(|i| scheme + 3 + i)
            .unwrap_or(result.len());
        let authority_start = scheme + 3;
        if let Some(at_rel) = result[authority_start..authority_end].rfind('@') {
            let at = authority_start + at_rel;
            if let Some(colon_rel) = result[authority_start..at].rfind(':') {
                let colon = authority_start + colon_rel;
                result.replace_range(colon + 1..at, "******");
            }
        }
    }
    redact_kv_passwords(&result)
}

fn redact_kv_passwords(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for (index, segment) in input.split_inclusive(';').enumerate() {
        let content = segment.strip_suffix(';').unwrap_or(segment);
        let Some((key, _)) = content.split_once('=') else {
            output.push_str(segment);
            continue;
        };
        if matches!(key.trim().to_ascii_lowercase().as_str(), "password" | "pwd") {
            output.push_str(key);
            output.push_str("=******");
            if segment.ends_with(';') {
                output.push(';');
            }
        } else {
            output.push_str(segment);
        }
        let _ = index;
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_materialized_lob_budget_is_sixteen_mibibytes() {
        assert_eq!(
            ProtocolLimits::default().max_materialized_lob_size,
            16 * 1024 * 1024
        );
    }

    #[test]
    fn parses_url_and_ssl_dsn() {
        let cfg = parse_dsn("db2://db2inst1:password@localhost:50000/SAMPLE").unwrap();
        assert_eq!(
            (
                &*cfg.host,
                cfg.port,
                &*cfg.database,
                &*cfg.user,
                &*cfg.password
            ),
            ("localhost", 50000, "SAMPLE", "db2inst1", "password")
        );
        assert_eq!(cfg.timeout_ms, 30_000);
        let secure = parse_dsn(
            "db2s://myuser:sec%40ret@db2server:50001/PRODDB?timeout=15s&block_size=131072",
        )
        .unwrap();
        assert_eq!(
            (
                &*secure.user,
                &*secure.password,
                secure.port,
                secure.timeout_ms,
                secure.block_size
            ),
            ("myuser", "sec@ret", 50001, 15_000, 131_072)
        );
        assert!(secure.use_ssl);
    }

    #[test]
    fn dsn_parity_corpus_accepts_and_rejects_consistently() {
        let full = parse_dsn("db2://user:pass@host:50000/TESTDB").unwrap();
        assert_eq!(
            (
                &*full.host,
                full.port,
                &*full.database,
                &*full.user,
                &*full.password,
                full.use_ssl
            ),
            ("host", 50000, "TESTDB", "user", "pass", false)
        );

        let ssl = parse_dsn("db2s://user:pass@host/TESTDB").unwrap();
        assert!(ssl.use_ssl);
        assert_eq!(ssl.port, 50000);

        let upper = parse_dsn("DB2://user:pass@host:50000/TESTDB").unwrap();
        assert_eq!((&*upper.host, upper.port), ("host", 50000));

        let escaped = parse_dsn("db2://us%40er:pa%40ss@host/TESTDB").unwrap();
        assert_eq!((&*escaped.user, &*escaped.password), ("us@er", "pa@ss"));

        let ipv6 = parse_dsn("db2://user:pass@[::1]:50000/TESTDB").unwrap();
        assert_eq!((&*ipv6.host, ipv6.port), ("::1", 50000));

        let user_only = parse_dsn("db2://user@host/TESTDB").unwrap();
        assert_eq!((&*user_only.user, &*user_only.password), ("user", ""));
        let no_user = parse_dsn("db2://host/TESTDB").unwrap();
        assert_eq!((&*no_user.user, &*no_user.password), ("", ""));

        assert_eq!(
            parse_dsn("db2://user:pass@host/TESTDB?timeout=5s")
                .unwrap()
                .timeout_ms,
            5_000
        );
        assert_eq!(
            parse_dsn("db2://user:pass@host/TESTDB?timeout=7")
                .unwrap()
                .timeout_ms,
            7_000
        );
        assert_eq!(
            parse_dsn("db2://user:pass@host/TESTDB?security_mechanism=9")
                .unwrap()
                .security_mechanism,
            9
        );
        assert_eq!(
            parse_dsn("db2://user:pass@host/TESTDB?block_size=131072")
                .unwrap()
                .block_size,
            131_072
        );

        // Not a db2 scheme: the key/value parser finds no pairs and keeps defaults.
        let not_db2 = parse_dsn("http://user:pass@host/TESTDB").unwrap();
        assert_eq!(not_db2.database, "");

        assert_eq!(
            parse_dsn("db2://u:p@host/TESTDB?timeout=abc")
                .unwrap()
                .timeout_ms,
            30_000
        );
        assert_eq!(
            parse_dsn("db2://u:p@host/TESTDB?port=abc").unwrap().port,
            50000
        );
        assert_eq!(
            parse_dsn("db2://u:p@host/TESTDB?port=70000").unwrap().port,
            50000
        );
        assert_eq!(
            parse_dsn("db2://u:p@host/TESTDB?block_size=512")
                .unwrap()
                .block_size,
            65_535
        );

        let kv = parse_dsn("Database=TESTDB;Host=h;Port=50001;UID=u;PWD=p").unwrap();
        assert_eq!(
            (&*kv.host, kv.port, &*kv.user, &*kv.password),
            ("h", 50001, "u", "p")
        );
        assert_eq!(parse_dsn("DATABASE=TESTDB;HOST=h").unwrap().host, "h");
        assert_eq!(parse_dsn("Database=TESTDB;UID=a;UID=b").unwrap().user, "b");
        assert_eq!(
            parse_dsn("Database=TESTDB;Unknown=1")
                .unwrap()
                .params
                .get("Unknown")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            parse_dsn("Database=TESTDB;PWD=a=b").unwrap().password,
            "a=b"
        );
        assert_eq!(parse_dsn("Database=TESTDB;PWD=a;b").unwrap().password, "a");
        let server = parse_dsn("Server=host:50001;Database=TESTDB").unwrap();
        assert_eq!((&*server.host, server.port), ("host", 50001));
        let server6 = parse_dsn("Server=[::1]:50001;Database=TESTDB").unwrap();
        assert_eq!((&*server6.host, server6.port), ("::1", 50001));
        assert_eq!(
            parse_dsn("Database=TESTDB;Authentication=SERVER_ENCRYPT")
                .unwrap()
                .security_mechanism,
            9
        );
        assert_eq!(
            parse_dsn("Database=TESTDB;Timeout=5s").unwrap().timeout_ms,
            5_000
        );
        let defaults = parse_dsn("Database=TESTDB").unwrap();
        assert_eq!((&*defaults.host, defaults.port), ("localhost", 50000));

        for invalid in [
            "",
            "   ",
            "db2://host",
            "db2://user:pass@host:0/TESTDB",
            "db2://user:pass@host:70000/TESTDB",
            "db2://user:pa%ZZss@host/TESTDB",
            "db2://u:p@host/TESTDB?Authentication=BOGUS",
            "Database=TESTDB;Authentication=TOKEN",
        ] {
            assert!(parse_dsn(invalid).is_err(), "expected reject: {invalid}");
        }
    }

    #[test]
    fn parses_url_scheme_case_insensitively() {
        // RFC 3986: the scheme is case-insensitive.
        let upper = parse_dsn("DB2://db2admin:pw@host:50000/TESTDB?timeout=5s").unwrap();
        assert_eq!(
            (
                &*upper.host,
                upper.port,
                &*upper.database,
                &*upper.user,
                &*upper.password,
                upper.use_ssl,
                upper.timeout_ms
            ),
            ("host", 50000, "TESTDB", "db2admin", "pw", false, 5_000)
        );
        assert!(parse_dsn("Db2S://host/TESTDB").unwrap().use_ssl);
    }

    #[test]
    fn parses_key_value_aliases_and_unknown_options() {
        let cfg = parse_dsn("server=mydb2.local;port=50000;dbname=TESTDB;uid=alice;pwd=secret123;ssl=no;timeout=45s;blocksize=65536;custom_key=x;").unwrap();
        assert_eq!(
            (
                &*cfg.host,
                cfg.port,
                &*cfg.database,
                &*cfg.user,
                &*cfg.password
            ),
            ("mydb2.local", 50000, "TESTDB", "alice", "secret123")
        );
        assert_eq!(
            (
                cfg.timeout_ms,
                cfg.block_size,
                cfg.params["custom_key"].as_str()
            ),
            (45_000, 65_536, "x")
        );
        assert_eq!(parse_dsn("database=TESTDB;ssl=yes").unwrap().port, 50_001);
        assert_eq!(
            parse_dsn("database=TESTDB;ssl=yes;port=50000")
                .unwrap()
                .port,
            50_000
        );
        assert_eq!(
            parse_dsn("db2://host/TESTDB?timeout=2m30s")
                .unwrap()
                .timeout_ms,
            150_000
        );
    }

    #[test]
    fn parses_ibm_cli_tls_keywords_and_rejects_unsupported_security_modes() {
        let config = parse_dsn("Server=db.example.test:50001;Database=TEST;UID=user;PWD=secret;Security=SSL;SSLServerCertificate=/tmp/db2-ca.pem").unwrap();
        assert_eq!(
            (
                config.host.as_str(),
                config.port,
                config.database.as_str(),
                config.user.as_str(),
                config.password.as_str(),
                config.use_ssl,
                config.ssl_root_ca_path.as_str()
            ),
            (
                "db.example.test",
                50001,
                "TEST",
                "user",
                "secret",
                true,
                "/tmp/db2-ca.pem"
            )
        );
        assert!(
            parse_dsn("db2://host/TEST?Security=SSL&SSLServerCertificate=%2Ftmp%2Fca.pem")
                .unwrap()
                .use_ssl
        );
        assert!(
            parse_dsn("Hostname=host;Port=50001;Database=TEST;Security=SERVER_ENCRYPT").is_err()
        );
    }

    #[test]
    fn parses_protocol_resource_limits_from_url_and_key_value_dsns() {
        let url = parse_dsn(
            "db2://localhost/TESTDB?max_dss_size=1024&max_ddm_object_size=2048&max_reply_chain_bytes=4096&max_reply_chain_packets=7&max_materialized_lob_size=8192",
        )
        .unwrap();
        assert_eq!(url.protocol_limits.max_dss_size, 1024);
        assert_eq!(url.protocol_limits.max_ddm_object_size, 2048);
        assert_eq!(url.protocol_limits.max_reply_chain_bytes, 4096);
        assert_eq!(url.protocol_limits.max_reply_chain_packets, 7);
        assert_eq!(url.protocol_limits.max_materialized_lob_size, 8192);

        let key_value = parse_dsn("database=TESTDB;maxdsssize=512;maxreplychainpackets=3").unwrap();
        assert_eq!(key_value.protocol_limits.max_dss_size, 512);
        assert_eq!(key_value.protocol_limits.max_reply_chain_packets, 3);

        let ignored = parse_dsn("database=TESTDB;max_dss_size=0").unwrap();
        assert_eq!(
            ignored.protocol_limits.max_dss_size,
            ProtocolLimits::default().max_dss_size
        );
    }

    #[test]
    fn parses_supported_server_auth_and_client_fields_and_redacts_secrets() {
        let cfg = parse_dsn("db2://localhost:50000/TESTDB?Authentication=SERVER&security_mechanism=3&spn=db2%2Fserver@CORP.LOCAL&krb5_config=%2Fetc%2Fkrb5.conf&krb5_keytab=%2Fetc%2Fdb2.keytab&krb5_ccache=%2Ftmp%2Fcc&client_applname=orders&client_corr_token=trace").unwrap();
        assert_eq!(
            (
                cfg.security_mechanism,
                &*cfg.kerberos_spn,
                &*cfg.krb5_config_file,
                &*cfg.krb5_keytab_file,
                &*cfg.krb5_ccache_file
            ),
            (
                3,
                "db2/server@CORP.LOCAL",
                "/etc/krb5.conf",
                "/etc/db2.keytab",
                "/tmp/cc"
            )
        );
        assert_eq!(
            (&*cfg.client_appl_name, &*cfg.client_corr_token),
            ("orders", "trace")
        );
        let secret = parse_dsn("db2://db2admin:super_secret_password@db2.example.com:50000/TESTDB?ssl=true&timeout=10s").unwrap();
        assert_eq!(
            secret.to_string(),
            "Config{Host:db2.example.com, Port:50001, Database:TESTDB, User:db2admin, Password:\"******\", UseSSL:true, Timeout:10s}"
        );
        assert!(!format!("{secret:?}").contains("super_secret_password"));
        assert_eq!(
            redact_dsn("db2://u:secret@host/DB;pwd=other"),
            "db2://u:******@host/DB;pwd=******"
        );
        // A password that itself contains '@' must be masked completely, not only up to the first one.
        assert_eq!(redact_dsn("db2://u:p@ss@host/DB"), "db2://u:******@host/DB");
        assert_eq!(
            redact_dsn("db2://u:p@ss@host/DB;pwd=other"),
            "db2://u:******@host/DB;pwd=******"
        );
    }

    #[test]
    fn rejects_unsupported_authentication_modes_for_url_and_key_value_dsns() {
        for option in [
            "Authentication=CLIENT",
            "Authentication=TOKEN",
            "Authentication=CERTIFICATE",
            "security_mechanism=4",
            "security_mechanism=6",
            "security_mechanism=7",
            "security_mechanism=10",
            "security_mechanism=12",
            "security_mechanism=garbage",
        ] {
            assert!(
                parse_dsn(&format!("Database=TESTDB;{option}")).is_err(),
                "{option}"
            );
            let (key, value) = option.split_once('=').unwrap();
            assert!(
                parse_dsn(&format!("db2://localhost/TESTDB?{key}={value}")).is_err(),
                "{option}"
            );
        }
        assert_eq!(
            parse_dsn("Database=TESTDB;Authentication=SERVER")
                .unwrap()
                .security_mechanism,
            3
        );
        assert_eq!(
            parse_dsn("db2://localhost/TESTDB?security_mechanism=0")
                .unwrap()
                .security_mechanism,
            0
        );
    }

    #[test]
    fn parses_encrypted_and_kerberos_authentication_modes() {
        for (option, expected) in [
            ("Authentication=SERVER_ENCRYPT", 9),
            ("Authentication=KERBEROS", 11),
            ("Authentication=GSSPLUGIN", 11),
            ("security_mechanism=9", 9),
            ("security_mechanism=dh_encrypted_password", 9),
            ("security_mechanism=11", 11),
            ("security_mechanism=gssapi", 11),
        ] {
            assert_eq!(
                parse_dsn(&format!("Database=TESTDB;{option}"))
                    .unwrap()
                    .security_mechanism,
                expected,
                "{option}"
            );
        }
        assert_eq!(
            parse_dsn("Database=TESTDB;security_mechanism=11;gssapi_token_file=/tmp/token.bin")
                .unwrap()
                .gssapi_token_file,
            "/tmp/token.bin"
        );
    }

    #[test]
    fn rejects_empty_and_missing_url_database() {
        assert!(parse_dsn("  ").is_err());
        assert!(parse_dsn("db2://u:secret@host/").is_err());
        assert!(parse_dsn("db2://host:70000/DB").is_err());
    }
}
