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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, UInt64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn batch_flush_and_drop_preserve_every_receipt_in_run_wide_sequence() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("raw")).unwrap();
        let logger = KvStoreLogger::open(directory.path());
        // Cross the 1024-row flush threshold, then leave a tail for Drop.
        for i in 0..1030_u64 {
            logger.borrow_mut().record(KvStoreEventEntry {
                time_ms: i as f64 * 0.25,
                operation: "reserve",
                reason: "incoming-push",
                cache_used_before: i,
                cache_used_after: i,
                requested_tokens: 80,
                reserved_tokens: Some(80),
                ..Default::default()
            });
        }
        drop(logger);
        let file =
            std::fs::File::open(directory.path().join("raw/kv_store_event.parquet")).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut next = 0_u64;
        for batch in reader {
            let batch = batch.unwrap();
            let integers = |name| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
            };
            let times = batch
                .column_by_name("time_ms")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(integers("sequence").value(row), next);
                assert_eq!(times.value(row), next as f64 * 0.25);
                assert_eq!(integers("cache_used_before").value(row), next);
                assert_eq!(integers("cache_used_after").value(row), next);
                assert_eq!(integers("reserved_tokens").value(row), 80);
                next += 1;
            }
        }
        assert_eq!(next, 1030);
    }
}
