//! Runtime-owned renewal for explicitly claimed logical shards.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::watch;

use crate::storage::{Ownership, StorageBackend};

pub(super) fn supervise(
    storage: StorageBackend,
    ownership: Ownership,
    shards: Vec<usize>,
    lease: Duration,
    mut stop: watch::Receiver<bool>,
    ownership_lost: Arc<AtomicBool>,
    ownership_status: watch::Sender<bool>,
) {
    let interval = lease / 3;
    tokio::spawn(async move {
        loop {
            let renewal = async {
                tokio::time::sleep(interval).await;
                tokio::time::timeout(
                    interval,
                    storage.renew_selected(ownership.clone(), shards.clone(), lease),
                )
                .await
            };
            tokio::select! {
                biased;
                _ = stop.changed() => break,
                result = renewal => {
                    if !matches!(result, Ok(Ok(()))) {
                        ownership_lost.store(true, Ordering::Release);
                        ownership_status.send_replace(true);
                        break;
                    }
                }
            }
        }
    });
}
