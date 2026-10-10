pub(crate) const ADMIN_COMMAND_CALL_SQL: &str = "CALL SYSPROC.ADMIN_CMD(?)";

pub(crate) fn normalize_admin_command(command: &str) -> Result<&str, &'static str> {
    if command.contains('\0') {
        return Err("Db2 admin command cannot contain a NUL character");
    }
    let normalized = command.trim();
    if normalized.is_empty() {
        return Err("Db2 admin command cannot be empty");
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::{ADMIN_COMMAND_CALL_SQL, normalize_admin_command};

    #[test]
    fn normalizes_outer_whitespace_and_preserves_command_content() {
        assert_eq!(ADMIN_COMMAND_CALL_SQL, "CALL SYSPROC.ADMIN_CMD(?)");
        assert_eq!(
            normalize_admin_command("  RUNSTATS ON TABLE S.T WITH DISTRIBUTION  ").unwrap(),
            "RUNSTATS ON TABLE S.T WITH DISTRIBUTION"
        );
        assert_eq!(
            normalize_admin_command("COMMENT ON TABLE S.T IS 'O''Reilly'").unwrap(),
            "COMMENT ON TABLE S.T IS 'O''Reilly'"
        );
    }

    #[test]
    fn rejects_blank_and_nul_containing_commands() {
        assert!(
            normalize_admin_command(" \t\r\n")
                .unwrap_err()
                .contains("cannot be empty")
        );
        assert!(
            normalize_admin_command("RUNSTATS\0 ON TABLE S.T")
                .unwrap_err()
                .contains("NUL")
        );
    }
}
