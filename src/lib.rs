// Copyright (c) 2018 Nuclear Furnace
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
#[macro_use]
extern crate futures;
extern crate tokio_timer;

#[cfg(tests)]
extern crate tokio_executor;

use futures::{
    future::Fuse,
    prelude::*,
    sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_timer::{clock::now as clock_now, Delay};

/// Handle to control the count of waiters for the evacuation.
///
/// `Warden` is cloneable.
#[derive(Clone)]
pub struct Warden {
    count: Arc<AtomicUsize>,
    notifier: UnboundedSender<usize>,
}

/// A future for safely "evacuating" a resource that is used by multiple parties.
///
/// `Evacuate` tracks a tripwire, the count of concurrent users, and a evacuation timeout.  Until
/// the tripwire completes, `Evacuate` will always be `Async::NotReady`.  Once the tripwire is
/// complete, a timeout is started, based on a configurable value passed in during creation.
///
/// When the tripwire completes, if the user count is zero, we immediately return `Async::Ready`.
/// Otherwise, we wait until either the user count falls to 0 or our internal timeout fires.
///
/// This allows us to build logic which safely attempts to clear users of a resource during
/// shutdown before timing out and forcefully closing.
pub struct Evacuate<F: Future> {
    count: Arc<AtomicUsize>,
    notifications: UnboundedReceiver<usize>,
    tripwire: Fuse<F>,
    timeout_ms: u64,
    timeout: Delay,
}

impl Warden {
    pub(crate) fn new(count: Arc<AtomicUsize>, notifier: UnboundedSender<usize>) -> Warden {
        Warden { count, notifier }
    }

    pub fn increment(&self) { let _ = self.count.fetch_add(1, Ordering::SeqCst); }

    pub fn decrement(&self) { let _ = self.count.fetch_sub(1, Ordering::SeqCst); }
}

impl<F: Future> Evacuate<F> {
    pub fn new(tripwire: F, timeout_ms: u64) -> (Warden, Evacuate<F>) {
        let (tx, rx) = unbounded();
        let count = Arc::new(AtomicUsize::new(0));

        let warden = Warden::new(count.clone(), tx);
        let evacuate = Evacuate {
            count,
            notifications: rx,
            tripwire: tripwire.fuse(),
            timeout_ms,
            timeout: Delay::new(clock_now()),
        };

        (warden, evacuate)
    }
}

impl<F: Future> Future for Evacuate<F> {
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // Drain the notifications queue to make sure we keep getting notified.
        while let Ok(Async::Ready(_)) = self.notifications.poll() {}

        // We have to wait for our tripwire.
        if !self.tripwire.is_done() {
            let _ = try_ready!(self.tripwire.poll().map_err(|_| ()));

            // If we're here, reset our delay based on the timeout.
            self.timeout.reset(clock_now() + Duration::from_millis(self.timeout_ms));
        }

        // We've tripped, so let's see what we're at for count.  If we're at zero, then we're done,
        // otherwise, fall through and see if we've hit our delay yet.
        if self.count.load(Ordering::SeqCst) == 0 {
            // We've tripped and we're at count 0, so we're done.
            return Ok(Async::Ready(()));
        }

        // Our count isn't at zero, but let's see if we've timed out yet.
        self.timeout.poll().map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    #[macro_use]
    mod support;
    use self::support::*;

    use super::Evacuate;

    use futures::{
        future::{empty, ok},
        Future,
    };

    #[test]
    fn test_evacuate_stops_at_tripwire() {
        mocked(|_, _| {
            let tripwire = empty::<(), ()>();
            let (_warden, mut evacuate) = Evacuate::new(tripwire, 10000);
            assert_not_ready!(evacuate);
        });
    }

    #[test]
    fn test_evacuate_falls_through_on_tripwire() {
        mocked(|_, _| {
            let tripwire = ok::<(), ()>(());
            let (_warden, mut evacuate) = Evacuate::new(tripwire, 10000);
            assert_ready!(evacuate);
        });
    }

    #[test]
    fn test_evacuate_stops_after_tripping_with_clients() {
        mocked(|_, _| {
            let tripwire = ok::<(), ()>(());
            let (warden, mut evacuate) = Evacuate::new(tripwire, 10000);
            warden.increment();
            assert_not_ready!(evacuate);
        });
    }

    #[test]
    fn test_evacuate_completes_after_client_count_ping_pong() {
        mocked(|_, _| {
            let tripwire = ok::<(), ()>(());
            let (warden, mut evacuate) = Evacuate::new(tripwire, 10000);
            warden.increment();
            assert_not_ready!(evacuate);
            warden.increment();
            assert_not_ready!(evacuate);
            warden.decrement();
            warden.decrement();
            assert_ready!(evacuate);
        });
    }

    #[test]
    fn test_evacuate_delay_before_clients_hit_zero() {
        mocked(|timer, _| {
            let tripwire = ok::<(), ()>(());
            let (warden, mut evacuate) = Evacuate::new(tripwire, 10000);
            warden.increment();
            assert_not_ready!(evacuate);
            warden.increment();
            assert_not_ready!(evacuate);
            warden.decrement();
            assert_not_ready!(evacuate);
            advance(timer, ms(10001));
            assert_ready!(evacuate);
        });
    }

}
