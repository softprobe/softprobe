use uuid::Uuid;

pub fn generate_transaction_id() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::generate_transaction_id;

    #[test]
    fn transaction_id_is_uuid_hex() {
        let id = generate_transaction_id();
        assert_eq!(id.len(), 36);
        assert!(uuid::Uuid::parse_str(&id).is_ok());
    }
}
