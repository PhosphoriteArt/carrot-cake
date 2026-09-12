use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use tokio::sync::Notify;

pub(crate) mod metrics;

struct SyncEventInner {
    notify: Notify,
    flag: AtomicBool,
}

#[derive(Clone)]
pub struct SyncEvent {
    inner: Arc<SyncEventInner>,
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
