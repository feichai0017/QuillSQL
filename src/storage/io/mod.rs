#[cfg(target_os = "linux")]
pub mod io_uring;

#[cfg(not(target_os = "linux"))]
pub mod blocking;
