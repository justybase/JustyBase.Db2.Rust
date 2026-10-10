pub mod sqltype;

#[cfg(test)]
mod tests {
    use super::sqltype::{NDECIMAL, SqlType, VARCHAR};

    #[test]
    fn nullable_sql_types_map_to_their_base_type_and_source_name() {
        assert!(NDECIMAL.is_nullable());
        assert_eq!(NDECIMAL.base_type(), SqlType(484));
        assert_eq!(NDECIMAL.name(), "DECIMAL (NULLABLE)");
        assert_eq!(VARCHAR.name(), "VARCHAR");
    }
}
