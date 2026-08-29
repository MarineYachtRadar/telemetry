//! A per-address cap on how often reports are accepted.
//!
//! Nothing about the sender is stored: the address lives in memory only, for
//! as long as its window lasts, and is gone when the process restarts.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Addresses tracked before expired windows are swept. A sweep is O(tracked),
/// and only runs when the map has grown past this.
const SWEEP_AT: usize = 10_000;

struct Window {
    start: Instant,
    count: u32,
}

pub(crate) struct RateLimit {
    max: u32,
    window: Duration,
    seen: Mutex<HashMap<IpAddr, Window>>,
}

impl RateLimit {
    pub(crate) fn new(max: u32, window: Duration) -> RateLimit {
        RateLimit {
            max,
            window,
            seen: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn allow(&self, address: IpAddr) -> bool {
        self.allow_at(address, Instant::now())
    }

    fn allow_at(&self, address: IpAddr, now: Instant) -> bool {
        let mut seen = self.seen.lock().expect("rate limit mutex poisoned");

        if seen.len() >= SWEEP_AT {
            seen.retain(|_, window| now.duration_since(window.start) < self.window);
        }

        let window = seen.entry(address).or_insert(Window {
            start: now,
            count: 0,
        });
        if now.duration_since(window.start) >= self.window {
            window.start = now;
            window.count = 0;
        }
        window.count += 1;
        window.count <= self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: Duration = Duration::from_secs(60);

    fn address(last: u8) -> IpAddr {
        IpAddr::from([192, 0, 2, last])
    }

    #[test]
    fn an_address_may_report_up_to_its_budget() {
        let limit = RateLimit::new(3, MINUTE);
        let now = Instant::now();

        assert!(limit.allow_at(address(1), now));
        assert!(limit.allow_at(address(1), now));
        assert!(limit.allow_at(address(1), now));
        assert!(!limit.allow_at(address(1), now));
    }

    #[test]
    fn one_noisy_address_does_not_silence_another() {
        let limit = RateLimit::new(1, MINUTE);
        let now = Instant::now();

        assert!(limit.allow_at(address(1), now));
        assert!(!limit.allow_at(address(1), now));
        assert!(limit.allow_at(address(2), now));
    }

    #[test]
    fn the_budget_returns_with_the_next_window() {
        let limit = RateLimit::new(1, MINUTE);
        let now = Instant::now();

        assert!(limit.allow_at(address(1), now));
        assert!(!limit.allow_at(address(1), now + MINUTE / 2));
        assert!(limit.allow_at(address(1), now + MINUTE));
    }
}
