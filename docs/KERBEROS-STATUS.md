# Kerberos / GSSAPI status

The current Rust alpha accepts DRDA security mechanism 11 configuration, but it does not synthesize Kerberos tickets from keytabs or credential caches.

| Source | Status |
| --- | --- |
| `gssapi_token_file` | Supported: caller-supplied GSSAPI token bytes are sent as the SECMEC 11 token. |
| `krb5_keytab` / `krb5_ccache` / default cache | Parsed, then rejected at connection time because ticket acquisition is not implemented. |
| `spn` / `realm` | Parsed for forward compatibility; currently not used for ticket acquisition. |

The mechanism 11 path never falls back to password authentication after a Kerberos failure, and no password is sent on the token path. Mutual-authentication and native platform ticket acquisition are not yet provided by this Rust crate.

Treat Kerberos/GSSAPI support as experimental and validate it explicitly in the target environment.
