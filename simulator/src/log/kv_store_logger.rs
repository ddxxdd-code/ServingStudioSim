//! Sparse receipt and placement log shared by the router and dedicated stores.
//! No shadow residency: entry transitions come directly from PrefixCache.
use crate::log::{
    rows::{kv_store_event_to_record_batch, KvStoreEventEntry},
    schemas::kv_store_event_schema,
    StreamingParquetWriter,
};
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread::JoinHandle;

pub(crate) type SharedKvStoreLogger = Rc<RefCell<KvStoreLogger>>;
pub(crate) struct KvStoreLogger {
    tx: Option<SyncSender<Vec<KvStoreEventEntry>>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
    buffer: Vec<KvStoreEventEntry>,
    sequence: u64,
}
impl KvStoreLogger {
    pub(crate) fn open(directory: &Path) -> SharedKvStoreLogger {
        let mut writer = StreamingParquetWriter::new(
            directory.join("raw/kv_store_event.parquet"),
            kv_store_event_schema(),
        );
        let (tx, rx) = sync_channel::<Vec<KvStoreEventEntry>>(64);
        let handle = std::thread::spawn(move || -> anyhow::Result<()> {
            for rows in rx {
                writer.write(&kv_store_event_to_record_batch(&rows)?)?;
            }
            writer.close()?;
            Ok(())
        });
        Rc::new(RefCell::new(Self {
            tx: Some(tx),
            handle: Some(handle),
            buffer: Vec::new(),
            sequence: 0,
        }))
    }
    pub(crate) fn record(&mut self, mut row: KvStoreEventEntry) {
        row.sequence = self.sequence;
        self.sequence += 1;
        self.buffer.push(row);
        if self.buffer.len() >= 1024 {
            self.flush();
        }
    }
    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let rows = std::mem::take(&mut self.buffer);
        if let Some(tx) = &self.tx {
            if let Err(error) = tx.send(rows) {
                tracing::warn!("kv_store_event writer stopped: {error}");
            }
        }
    }
}
impl Drop for KvStoreLogger {
    fn drop(&mut self) {
        self.flush();
        self.tx.take();
        if let Some(handle) = self.handle.take() {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!("kv_store_event close failed: {error:#}"),
                Err(_) => tracing::warn!("kv_store_event writer panicked"),
            }
        }
    }
}
