//
// Copyright (c) 2025-2026 rustmailer.com (https://rustmailer.com)
//
// This file is part of the Bichon Email Archiving Project
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use crate::{
    common::signal::SIGNAL_MANAGER,
    envelope::extractor::reattach_eml_content_self_healing,
    error::{code::ErrorCode, BichonResult},
    settings::dir::DATA_DIR_MANAGER,
};
use crate::raise_error;
use bytes::Bytes;
use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, config::{BlockSizePolicy, CompressionPolicy}};

use std::{io::Cursor, sync::LazyLock};
use tokio::{
    sync::{mpsc, Mutex},
    task::{self, JoinHandle},
};

pub static BLOB_MANAGER: LazyLock<BlobManager> = LazyLock::new(BlobManager::new);

pub struct DetachedEmail {
    pub email: (String, Bytes),
    pub attachments: Option<Vec<(String, Bytes)>>,
}

pub struct BlobManager {
    sender: mpsc::Sender<DetachedEmail>,
    db: Database,
    email_keyspace: Keyspace,
    attachments_keyspace: Keyspace,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl BlobManager {
    pub async fn shutdown(&self) {
        let mut guard = self.handle.lock().await;
        if let Some(handle) = guard.take() {
            let _ = handle.await;
        }
    }

    fn process_detached_email(
        eml: DetachedEmail,
        email_ks: &Keyspace,
        attach_ks: &Keyspace,
    ) {
        let (email_hash, email_data) = eml.email;
        match email_ks.contains_key(&email_hash) {
            Ok(false) => {
                if let Err(e) = email_ks.insert(email_hash, email_data) {
                    tracing::error!("CRITICAL: Failed to insert email: {:?}",  e);
                }
            }
            Err(e) => tracing::error!("Fjall email_ks error: {:?}", e),
            Ok(true) => {
                tracing::debug!("Email blob already exists (dedup)");
            }
        }

        if let Some(attachments) = eml.attachments {
            for (a_hash, a_data) in attachments {
                match attach_ks.contains_key(&a_hash) {
                    Ok(false) => {
                        if let Err(e) = attach_ks.insert(a_hash, a_data) {
                            tracing::error!("CRITICAL: Failed to insert attachment: {:?}", e);
                        }
                    }
                    Err(e) => tracing::error!("Fjall attach_ks error: {:?}", e),
                    Ok(true) => {
                        tracing::debug!("Attachment blob already exists (dedup)");
                    }
                }
            }
        }
    }

    /// Proactively compacts a keyspace when its L0 table count is already high, as
    /// insurance against a write-halt. Used **only at startup** to digest any L0
    /// backlog left from a previous (crashed / busy) run.
    ///
    /// fjall's compaction is driven by memtable rotation and ingestion: it runs as a
    /// side effect of flush, never on an idle tree. An idle keyspace that was shut
    /// down with a large L0 backlog (e.g. 70 fragmented tables from a prior import)
    /// would never be compacted on its own, and the very next `insert` could hit
    /// `check_write_halt` (`l0_run_count >= 30` busy-wait) before any compaction ran.
    ///
    /// Steady-state L0 bounding is handled by the sized memtable/journal (fewer, larger
    /// flushes) plus fjall's own leveled compaction, so we do *not* run a full
    /// `major_compact` after every batch — that would rewrite the whole tree on a
    /// fixed L0-table cadence and scale poorly as the archive grows. This startup pass
    /// is the only place we force a compaction, and it runs on a `spawn_blocking`
    /// thread so it never blocks the async runtime.
    fn maybe_compact(ks: &Keyspace, name: &str) {
        // Only act on an already-large backlog; below this the normal leveled
        // compaction keeps up. Reading the count is cheap (in-memory version lookup).
        const COMPACT_TRIGGER: usize = 15;
        let l0 = ks.l0_table_count();
        if l0 >= COMPACT_TRIGGER {
            tracing::info!(
                "BlobManager: keyspace {} L0 table count = {} >= {}, triggering major_compact",
                name,
                l0,
                COMPACT_TRIGGER
            );
            let start = std::time::Instant::now();
            match ks.major_compact() {
                Ok(()) => {
                    tracing::info!(
                        "BlobManager: keyspace {} major_compact done in {:?}, L0 now = {}",
                        name,
                        start.elapsed(),
                        ks.l0_table_count()
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "BlobManager: keyspace {} major_compact failed: {:?}",
                        name,
                        e
                    );
                }
            }
        }
    }

    pub fn new() -> Self {
        let db = Database::builder(&DATA_DIR_MANAGER.storage_dir)
        .cache_size(64 * 1024 * 1024)
        .max_cached_files(Some(400))
        .journal_compression(CompressionType::None)
        // The journal (WAL) is rotated when it exceeds this size, and each rotation
        // forces a memtable flush => one (small, overlapping, randomly-keyed) L0 table.
        // Blob values average ~18 MB (email) / ~33 MB (attachment), so a 64 MiB journal
        // rotates after just a few inserts, flooding L0 and risking fjall's write-halt
        // (busy-wait once `l0_run_count >= 30`). Use the default-sized journal so flushes
        // are driven by memtable rotation (which we also batch up below), not the WAL.
        .max_journaling_size(512 * 1024 * 1024)
        // More compaction/flush workers than the default min(CPU, 4). Under bursty
        // blob writes (e.g. EML batch import) the default 4 workers cannot keep L0
        // compacted fast enough; once `l0_run_count >= 30` fjall's `check_write_halt`
        // busy-waits inside `Keyspace::insert`, which blocks the BlobManager's
        // spawn_blocking task, fills the blob channel, and stalls the whole import.
        // Doubling the workers lets compaction keep pace with ingest.
        .worker_threads(8)
            .open()
            .expect("Failed to initialize Fjall database: Check if the directory exists and has write permissions.");


        let email_keyspace = db
            .keyspace("email", || {
                KeyspaceCreateOptions::default()
                // kv-separation writes the value to a blob file only at *flush* time, so the
                // active memtable holds each full blob value until rotation. A 16 MiB memtable
                // rotates after roughly one average email (~18 MB), producing a tiny L0 table
                // per message — the direct cause of L0 accumulating toward the write-halt
                // threshold. Batching several blobs per memtable (here ~256 MiB) cuts the L0
                // flush rate by an order of magnitude. The global write buffer is unbounded by
                // default, so this only raises peak memory, not a hard cap.
                //
                // NOTE: `max_memtable_size` is persisted in the keyspace's config kv and
                // restored from disk on recovery, so this value takes effect only for a freshly
                // created keyspace. Existing deployments keep their persisted (16 MiB) value
                // and rely on the lightweight startup compaction below to keep L0 bounded.
                .max_memtable_size(256 * 1024 * 1024)
                .data_block_size_policy(BlockSizePolicy::all(4 * 1024))
                .data_block_compression_policy(  
                    CompressionPolicy::all(CompressionType::Lz4)  
                )  
                .with_kv_separation(Some(
                    KvSeparationOptions::default()
                        .separation_threshold(1024)
                        .compression(CompressionType::Lz4)
                        .file_target_size(512 * 1024 * 1024)
                        .staleness_threshold(0.5)
                        .age_cutoff(0.6),
                ))
            })
            .expect("Failed to open 'email' keyspace: The partition metadata might be corrupted or inaccessible.");
        
        let attachments_keyspace = db
            .keyspace("attachments", || {
                KeyspaceCreateOptions::default()
                .data_block_size_policy(BlockSizePolicy::all(4 * 1024))
                .data_block_compression_policy(  
                    CompressionPolicy::all(CompressionType::Lz4)  
                )
                .with_kv_separation(Some(
                    KvSeparationOptions::default()
                        .separation_threshold(1024)
                        .compression(CompressionType::Lz4)
                        .file_target_size(512 * 1024 * 1024)
                        .staleness_threshold(0.5)
                        .age_cutoff(0.6),
                ))
                .max_memtable_size(256 * 1024 * 1024)
            })
            .expect("Failed to open 'attachments' keyspace: Check disk space for blob storage initialization.");
        
        let (sender, mut receiver) = mpsc::channel::<DetachedEmail>(100);

        let email_ks = email_keyspace.clone();
        let attach_ks = attachments_keyspace.clone();
        let handler = task::spawn(async move {
            let mut shutdown = SIGNAL_MANAGER.subscribe();

            // Trigger an initial compaction pass to digest any L0 backlog left from a
            // previous run. fjall's compaction is write-driven: it only runs as a side
            // effect of memtable rotation/flush, which only happens on new writes. An
            // idle keyspace with a large L0 backlog (e.g. 70 fragmented tables from a
            // prior import) would never be compacted on its own, and the very next
            // `insert` would hit `check_write_halt` (`l0_run_count >= 30` busy-wait)
            // before any compaction could run — deadlocking the writer. We compact
            // proactively so writes never stall.
            {
                let email_ks = email_ks.clone();
                let attach_ks = attach_ks.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    Self::maybe_compact(&email_ks, "email");
                    Self::maybe_compact(&attach_ks, "attachments");
                })
                .await;
            }

            loop {
                tokio::select! {
                    res = receiver.recv() => {
                        match res {
                            Some(eml) => {
                                let mut batch = vec![eml];
                                while let Ok(next_eml) = receiver.try_recv() {
                                    batch.push(next_eml);
                                }
                                let email_ks = email_ks.clone();
                                let attach_ks = attach_ks.clone();
                                if let Err(e) = tokio::task::spawn_blocking(move || {
                                    for eml in batch {
                                        Self::process_detached_email(eml, &email_ks, &attach_ks);
                                    }
                                    // Steady-state L0 bounding is left to fjall's own leveled
                                    // compaction (triggered by the flushes these inserts cause) plus
                                    // the sized memtable/journal above. We deliberately do NOT call
                                    // `maybe_compact` here: a per-batch `major_compact` would rewrite
                                    // the whole tree on a fixed L0-table cadence and scale poorly as
                                    // the archive grows. `insert` may still apply fjall's built-in
                                    // write-stall/backpressure when L0 is busy — that is the intended
                                    // signal to slow ingest, and `queue` below propagates it rather
                                    // than dropping data.
                                }).await {
                                    tracing::error!("BlobManager: spawn_blocking join error: {:#?}", e);
                                }
                            }
                            None => {
                                tracing::info!("BlobManager: All senders dropped, closing storage.");
                                break;
                            }
                        }
                    }
                    _ = shutdown.recv() => {
                        receiver.close();
                        let mut remaining = Vec::new();
                        while let Some(eml) = receiver.recv().await {
                            remaining.push(eml);
                        }
                        tracing::info!(
                            "BlobManager: Shutdown signal received. Processing {} remaining tasks...",
                            remaining.len()
                        );
                        if !remaining.is_empty() {
                            let email_ks = email_ks.clone();
                            let attach_ks = attach_ks.clone();
                            if let Err(e) = tokio::task::spawn_blocking(move || {
                                for eml in remaining {
                                    Self::process_detached_email(eml, &email_ks, &attach_ks);
                                }
                            }).await {
                                tracing::error!("BlobManager: shutdown spawn_blocking join error: {:#?}", e);
                            }
                        }
                        tracing::info!("BlobManager: All remaining tasks processed. Closing Fjall.");
                        break;
                    }
                }
            }
        });

        Self {
            sender,
            db,
            email_keyspace,
            attachments_keyspace,
            handle: Mutex::new(Some(handler)),
        }
    }

    /// Queues a detached email for asynchronous blob storage.
    ///
    /// This applies **backpressure, not data loss**. The bounded channel (capacity 100)
    /// blocks the caller while the background writer is busy — which is exactly the right
    /// behaviour when fjall's compaction falls behind: ingest slows to the pace compaction
    /// can sustain, instead of outrunning it and accumulating L0 toward a write-halt. The
    /// caller (envelope extraction) simply awaits, so import throughput dips rather than
    /// silently dropping blobs.
    ///
    /// The only non-recoverable case is the channel closing — i.e. the background writer
    /// task itself panicked or was shut down. That is a real failure (not transient
    /// backpressure) and is logged at `error` level. We do not time out and drop the blob:
    /// a dropped blob is unrecoverable for imported/`NoSync` emails (no IMAP source, and
    /// imported envelopes carry `uid == 0`), so `reattach_eml_content_self_healing` cannot
    /// refetch them. Blocking indefinitely is preferable to silently losing mail content.
    pub async fn queue(&self, email: DetachedEmail) {
        if let Err(e) = self.sender.send(email).await {
            // Channel closed: the writer task is gone. This should not happen during normal
            // operation (only on a panicked writer or post-shutdown). The blob is lost; surface
            // it loudly rather than swallowing it.
            tracing::error!(
                "BlobManager channel closed, email blob could not be stored: {:#?}. \
                 The envelope is indexed but its original content is missing; for IMAP \
                 accounts it can be re-fetched on demand, for imported/NoSync mail it is lost.",
                e
            );
        }
    }

    pub fn get_email(&self, content_hash: &str) -> BichonResult<Option<Bytes>> {
        self.email_keyspace
            .get(content_hash)
            .map(|user_value| user_value.map(|s| s.into()))
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))
    }

    pub fn get_attachment(&self, content_hash: &str) -> BichonResult<Option<Bytes>> {
        self.attachments_keyspace
            .get(content_hash)
            .map(|user_value| user_value.map(|s| s.into()))
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))
    }

    pub fn delete<I1, I2>(
        &self,
        email_content_hashes: I1,
        attachment_content_hashes: I2,
    ) -> BichonResult<()>
    where
        I1: IntoIterator,
        I1::Item: AsRef<str>,
        I2: IntoIterator,
        I2::Item: AsRef<str> {
        let mut batch = self.db.batch();
        for hash in email_content_hashes {
            batch.remove(&self.email_keyspace, hash.as_ref());
        }
        for hash in attachment_content_hashes {
            batch.remove(&self.attachments_keyspace, hash.as_ref());
        }
        batch
            .commit()
            .map_err(|e| raise_error!(format!("{:#?}", e), ErrorCode::InternalError))?;
        Ok(())
    }
}

/// Returns a reader over the raw EML for an indexed message.
///
/// If the message's content blob is missing from the blob store, it is fetched
/// on demand from the IMAP server, persisted, and returned (self-healing). The
/// underlying "content not found" error is only surfaced if that on-demand
/// fetch itself fails.
pub async fn get_reader(account_id: u64, eid: String) -> BichonResult<Cursor<Bytes>> {
    let (_, data) = reattach_eml_content_self_healing(account_id, eid).await?;
    Ok(Cursor::new(data))
}
