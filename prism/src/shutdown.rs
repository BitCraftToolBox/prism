//! Shared Ctrl-C / shutdown coordinator.
//!
//! Pattern lifted from `nodeindex-example`: tasks register a `oneshot::Receiver`
//! and select on it inside their main loop. When `trigger()` is called, every
//! registered receiver fires.

use std::sync::Arc;

use log::info;
use tokio::sync::{Mutex, oneshot};

pub type SharedShutdown = Arc<Mutex<Shutdown>>;

pub struct Shutdown {
    triggered: bool,
    tx: Vec<oneshot::Sender<()>>,
}

impl Shutdown {
    pub fn new() -> SharedShutdown {
        Arc::new(Mutex::new(Self { triggered: false, tx: Vec::new() }))
    }

    /// Register a receiver that fires when shutdown is triggered. Returns
    /// `None` if shutdown has already happened (caller should exit immediately).
    pub fn register(&mut self) -> Option<oneshot::Receiver<()>> {
        if self.triggered {
            return None;
        }
        let (tx, rx) = oneshot::channel();
        self.tx.push(tx);
        Some(rx)
    }

    pub fn trigger(&mut self) {
        self.triggered = true;
        for tx in self.tx.drain(..) {
            let _ = tx.send(());
        }
    }
}

/// Spawn a task that triggers shutdown on Ctrl-C.
pub fn install_ctrl_c(shutdown: SharedShutdown) {
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("ctrl-c received, shutting down");
        shutdown.lock().await.trigger();
    });
}
