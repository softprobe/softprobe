use uuid::Uuid;

pub fn generate_transaction_id() -> String {
    Uuid::new_v4().to_string()
}

