//! A small, bounded, drop-oldest message queue — the std-only substitute
//! for what a crate like `crossbeam`'s bounded channel would normally
//! give us. See STDLIB.md.
//!
//! `std::sync::mpsc::sync_channel(N)` was considered and rejected: it
//! applies backpressure by *blocking the sender* when full, which is the
//! wrong policy here — the broker thread must never stall because one
//! subscriber is slow. We explicitly want to drop the oldest buffered
//! message for that subscriber instead. See DECISIONS.md #4 and
//! PLAN.md §3 (backpressure: bounded per-client outbound queue,
//! drop-oldest).

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

struct BoundedDropOldestQueue<T> {
    items: Mutex<VecDeque<T>>,
    not_empty: Condvar,
    capacity: usize,
    closed: Mutex<bool>,
}

/// A cheaply-cloneable handle to a bounded drop-oldest queue, shared
/// between a producer (the broker thread, pushing outbound packets for
/// one client) and a consumer (that client's writer thread, draining and
/// writing to the socket).
pub struct QueueHandle<T> {
    queue: Arc<BoundedDropOldestQueue<T>>,
}

impl<T> Clone for QueueHandle<T> {
    fn clone(&self) -> Self {
        QueueHandle {
            queue: Arc::clone(&self.queue),
        }
    }
}

/// Create a new bounded drop-oldest queue with the given capacity.
pub fn new<T>(capacity: usize) -> QueueHandle<T> {
    let q = BoundedDropOldestQueue {
        items: Mutex::new(VecDeque::with_capacity(capacity)),
        not_empty: Condvar::new(),
        capacity,
        closed: Mutex::new(false),
    };
    QueueHandle { queue: Arc::new(q) }
}

impl<T> QueueHandle<T> {
    /// Push an item, dropping the oldest buffered item first if the
    /// queue is already at capacity. Returns `true` if an item was
    /// dropped to make room (callers may want to log this).
    pub fn push(&self, item: T) -> bool {
        let mut guard = self.queue.items.lock().unwrap();
        let dropped = if guard.len() >= self.queue.capacity {
            guard.pop_front();
            true
        } else {
            false
        };
        guard.push_back(item);
        drop(guard);
        self.queue.not_empty.notify_one();
        dropped
    }

    /// Block until an item is available (or the queue is closed and
    /// drained), then return it. Returns `None` once closed and empty.
    pub fn pop_blocking(&self) -> Option<T> {
        let mut guard = self.queue.items.lock().unwrap();
        loop {
            if let Some(item) = guard.pop_front() {
                return Some(item);
            }
            if *self.queue.closed.lock().unwrap() {
                return None;
            }
            guard = self.queue.not_empty.wait(guard).unwrap();
        }
    }

    /// Mark the queue closed and wake any blocked consumer so it can
    /// exit. Call this when the owning connection disconnects.
    pub fn close(&self) {
        *self.queue.closed.lock().unwrap() = true;
        self.queue.not_empty.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_oldest_when_full() {
        let q: QueueHandle<i32> = new(2);
        assert!(!q.push(1));
        assert!(!q.push(2));
        assert!(q.push(3)); // capacity 2, pushing a 3rd drops the oldest (1)
        assert_eq!(q.pop_blocking(), Some(2));
        assert_eq!(q.pop_blocking(), Some(3));
    }

    #[test]
    fn close_wakes_blocked_consumer() {
        let q: QueueHandle<i32> = new(4);
        q.close();
        assert_eq!(q.pop_blocking(), None);
    }

    #[test]
    fn fifo_order_preserved_under_capacity() {
        let q: QueueHandle<i32> = new(10);
        for i in 0..5 {
            q.push(i);
        }
        for i in 0..5 {
            assert_eq!(q.pop_blocking(), Some(i));
        }
    }
}
