// Logging macros that enforce the "SP: " prefix consistently

#[macro_export]
macro_rules! sp_trace {
    ($fmt:expr) => {
        log::trace!(concat!("SP: ", $fmt));
    };
    ($fmt:expr, $($arg:tt)*) => {
        log::trace!(concat!("SP: ", $fmt), $($arg)*);
    };
}

#[macro_export]
macro_rules! sp_debug {
    ($fmt:expr) => {
        log::debug!(concat!("SP: ", $fmt));
    };
    ($fmt:expr, $($arg:tt)*) => {
        log::debug!(concat!("SP: ", $fmt), $($arg)*);
    };
}

#[macro_export]
macro_rules! sp_info {
    ($fmt:expr) => {
        log::info!(concat!("SP: ", $fmt));
    };
    ($fmt:expr, $($arg:tt)*) => {
        log::info!(concat!("SP: ", $fmt), $($arg)*);
    };
}

#[macro_export]
macro_rules! sp_warn {
    ($fmt:expr) => {
        log::warn!(concat!("SP: ", $fmt));
    };
    ($fmt:expr, $($arg:tt)*) => {
        log::warn!(concat!("SP: ", $fmt), $($arg)*);
    };
}

#[macro_export]
macro_rules! sp_error {
    ($fmt:expr) => {
        log::error!(concat!("SP: ", $fmt));
    };
    ($fmt:expr, $($arg:tt)*) => {
        log::error!(concat!("SP: ", $fmt), $($arg)*);
    };
}


