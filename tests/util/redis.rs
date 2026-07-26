//! Host Redis port for integration-e2e (published by `make setup-local`).
//!
//! Default **6380** matches Makefile/`docker-compose.yml` and avoids workspace
//! demo `sp-dev-redis` on **6379**. Override with `REDIS_PORT` to match compose.

/// Host port for `runtime-redis` as published by thelake compose (`REDIS_PORT`, default 6380).
pub fn test_redis_port() -> u16 {
    match std::env::var("REDIS_PORT") {
        Ok(s) => s.parse().unwrap_or_else(|_| {
            panic!("REDIS_PORT must be a u16 (got {s:?}); unset to use default 6380")
        }),
        Err(_) => 6380,
    }
}
