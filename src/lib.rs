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
//! `tokio-evacuate` provides a way to safely "evacuate" users of a resource before forcefully
//! removing them.
//!
//! In many networked applications, there comes a time when the server must shutdown or reload, and
//! may still be actively serving traffic.  Listeners or publishers can be shut down, and remaining
//! work can be processed while no new work is allowed.. but this may take longer than the operator
//! is comfortable with.
//!
//! `Evacuate` is a middleware future, that works in conjuction with a classic "shutdown signal."
//! By combining a way to track the number of current users, as well as a way to fire a global
//! timeout, we allow applications to provide soft shutdown capabilities, giving work a chance to
//! complete, before forcefully stopping computation.
#[macro_use]
extern crate futures;
extern crate tokio_timer;

#[cfg(test)]
extern crate tokio_executor;

use futures::{
    future::Fuse,
    prelude::*,
    sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
};
use std::time::Duration;
use tokio_timer::{clock::now as clock_now, Delay};

/// Dispatcher for user count updates.
///
/// [`Warden`] is cloneable.
#[derive(Clone)]
pub struct Warden {
    pub(crate) notify_tx: UnboundedSender<bool>,
}

/// A future for safely "evacuating" a resource that is used by multiple parties.
///
/// [`Evacuate`] tracks a tripwire, the count of concurrent users, and an evacuation timeout, and
/// functions in a two-step process: we must be tripped, and then we race to the timeout.
///
/// Until the tripwire completes, [`Evacuate`] will always return `Async::NotReady`.  Once we detect
/// that the tripwire has completed, however, we immediately spawn a timeout, based on the
/// configured value, and race between the user count dropping to zero and the timeout firing.
///
/// The user count is updated by calls to [`Warden::increment`] and [`Warden::decrement`].
pub struct Evacuate<F: Future> {
    count: u64,
    notify_rx: UnboundedReceiver<bool>,
    tripwire: Fuse<F>,
    timeout_ms: u64,
    timeout: Delay,
}

impl Warden {
    /// Increments the user count.
    pub fn increment(&self) { let _ = self.notify_tx.unbounded_send(true); }

    /// Decrements the user count.
    pub fn decrement(&self) { let _ = self.notify_tx.unbounded_send(false); }
}

impl<F: Future> Evacuate<F> {
    /// Creates a new [`Evacuate`].
    ///
    /// The given `tripwire` is used, and the internal timeout is set to the value of `timeout_ms`.
    ///
    /// Returns both a [`Warden`] handle, used for incrementing and decrementing the user count, and
    /// [`Evacuate`] itself.
    pub fn new(tripwire: F, timeout_ms: u64) -> (Warden, Evacuate<F>) {
        let (notify_tx, notify_rx) = unbounded();

        let warden = Warden { notify_tx };
        let evacuate = Evacuate {
            count: 0,
            notify_rx,
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
        while let Ok(Async::Ready(Some(state))) = self.notify_rx.poll() {
            if state {
                self.count += 1;
            } else {
                self.count -= 1;
            }
        }

        // We have to wait for our tripwire.
        if !self.tripwire.is_done() {
            let _ = try_ready!(self.tripwire.poll().map_err(|_| ()));

            // If we're here, reset our delay based on the timeout.
            self.timeout.reset(clock_now() + Duration::from_millis(self.timeout_ms));
        }

        // We've tripped, so let's see what we're at for count.  If we're at zero, then we're done,
        // otherwise, fall through and see if we've hit our delay yet.
        if self.count == 0 {
            // We've tripped and we're at count 0, so we're done.
            return Ok(Async::Ready(()));
        }

        // Our count isn't at zero, but let's see if we've timed out yet.
        self.timeout.poll().map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    extern crate tokio_executor;

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
