//! Shared Ctrl-C / shutdown coordinator.
//!
//! Tasks register a `oneshot::Receiver` and select on it inside their sleep
//! loop. When `trigger()` is called (via Ctrl-C), every registered receiver
//! fires and tasks exit cleanly after their current work completes.

// copied nearly verbatim from main prism package

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use log::info;
use tokio::sync::{Mutex, oneshot};

pub type SharedShutdown = Arc<Mutex<Shutdown>>;

pub struct Shutdown {
    triggered: bool,
    tx: Vec<oneshot::Sender<()>>,
    /// Shared flag readable by blocking threads without async overhead.
    cancel: Arc<AtomicBool>,
}

impl Shutdown {
    pub fn new() -> SharedShutdown {
        Arc::new(Mutex::new(Self {
            triggered: false,
            tx: Vec::new(),
            cancel: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Register a receiver that fires when shutdown is triggered.
    /// Returns `None` if shutdown has already been triggered.
    pub fn register(&mut self) -> Option<oneshot::Receiver<()>> {
        if self.triggered {
            return None;
        }
        let (tx, rx) = oneshot::channel();
        self.tx.push(tx);
        Some(rx)
    }

    /// Returns `true` if shutdown has already been triggered.
    pub fn is_triggered(&self) -> bool {
        self.triggered
    }

    /// Clone of the cancel flag, suitable for passing into blocking threads.
    /// The flag is set to `true` when `trigger()` is called.
    pub fn cancel_flag(&self) -> Arc<AtomicBool> {
        self.cancel.clone()
    }

    pub fn trigger(&mut self) {
        self.triggered = true;
        self.cancel.store(true, Ordering::Relaxed);
        for tx in self.tx.drain(..) {
            let _ = tx.send(());
        }
    }
}

/// Spawn a task that triggers shutdown on Ctrl-C.
pub fn install_ctrl_c(shutdown: SharedShutdown) {
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("[cartographer] ctrl-c received, shutting down");
        shutdown.lock().await.trigger();
    });
}
