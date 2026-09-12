use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use tokio::sync::Notify;

pub(crate) mod metrics;


// Defines a synchronization primitive like Python's asyncio.Event –
// single flag that can be set at most once, which releases all waiters.
// Waiters that try to wait after the flag is set don't block at all.
#[derive(Clone)]
pub struct SyncEvent {
    inner: Arc<SyncEventInner>,
}

struct SyncEventInner {
    notify: Notify,
    flag: AtomicBool,
}

impl SyncEvent {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SyncEventInner {
                notify: Notify::new(),
                flag: AtomicBool::new(false),
            }),
        }
    }

    pub fn is_set(&self) -> bool {
      self.inner.flag.load(Ordering::Acquire)
    }

    pub async fn wait(&self) {
        loop {
            let notified = self.inner.notify.notified();
            if self.inner.flag.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    pub async fn signal(&self) {
        if !self.inner.flag.swap(true, Ordering::Release) {
            self.inner.notify.notify_waiters();
        }
    }
}
