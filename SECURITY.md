# Security policy

## Supported versions

The project is currently in alpha development. Security fixes are applied to the latest revision of the default branch; older alpha snapshots are not maintained as separate supported lines.

## Reporting a vulnerability

Please **do not open a public GitHub issue** for vulnerabilities that could expose credentials, allow protocol/session confusion, weaken TLS/authentication, cause unsafe query replay, bypass resource limits, or otherwise create a realistic security impact.

Use GitHub's private security advisory / vulnerability-reporting mechanism for this repository when available. If that mechanism is unavailable, contact the repository owner privately through GitHub before disclosing technical details publicly.

Include, when possible:

- affected revision/version,
- minimal reproduction steps,
- expected and observed behavior,
- whether a live Db2 server is required,
- relevant Db2 LUW version and connection mode (TCP/TLS),
- impact assessment,
- whether credentials or sensitive data may have been exposed.

Never include real production passwords, access tokens, private keys or confidential database contents in a report.

## Security-sensitive areas

Reports are especially valuable for:

- DSN/credential redaction,
- TLS certificate and hostname validation,
- authentication/security-mechanism negotiation,
- SQL interrupt/cancellation cleanup,
- transaction outcome ambiguity,
- pool session-state leakage between callers,
- malformed DRDA/DDM frame handling,
- memory/spool/resource-limit bypasses,
- LOB length or boundary validation,
- unsafe reuse after transport/protocol errors.

The driver intentionally fails closed and discards sessions when safe protocol synchronization cannot be established. A change that weakens that property should be treated as security-sensitive.
