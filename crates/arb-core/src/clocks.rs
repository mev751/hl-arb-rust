use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// High-resolution clock tracker for arbitrage execution.
/// Separates monotonic durations from wall clock timestamps to avoid NTP slews.
#[derive(Debug, Clone)]
pub struct ClockTracker {
    start_instant: Instant,
    start_unix_ns: u64,
}

impl Default for ClockTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ClockTracker {
    pub fn new() -> Self {
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self {
            start_instant: Instant::now(),
            start_unix_ns: now_unix,
        }
    }

    /// Monotonic wall timestamp in nanoseconds since UNIX epoch, anchored at start.
    pub fn now_ns(&self) -> u64 {
        let elapsed = self.start_instant.elapsed().as_nanos() as u64;
        self.start_unix_ns + elapsed
    }

    /// Monotonic wall timestamp in microseconds since UNIX epoch.
    pub fn now_us(&self) -> u64 {
        self.now_ns() / 1_000
    }

    /// Monotonic wall timestamp in milliseconds since UNIX epoch.
    pub fn now_ms(&self) -> u64 {
        self.now_ns() / 1_000_000
    }

    /// Compute one-way or processing latency in microseconds.
    pub fn latency_us(&self, since_ns: u64) -> u64 {
        let now = self.now_ns();
        if now > since_ns {
            (now - since_ns) / 1_000
        } else {
            0
        }
    }
}

/// Helper function to get current UTC timestamp in nanoseconds directly.
pub fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Helper function to get current UTC timestamp in milliseconds directly.
pub fn unix_now_ms() -> u64 {
    unix_now_ns() / 1_000_000
}
