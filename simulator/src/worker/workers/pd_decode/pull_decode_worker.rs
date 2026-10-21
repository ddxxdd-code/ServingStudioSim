//! Direct-link PD shell. DecodeCore owns the shared pull and compute timelines;
//! this shell owns direct-source acknowledgements and terminal KV release.
use crate::common::{RequestId, Time, WorkerId};
use crate::worker::admission::LoadBalance;
use crate::worker::execution::{IterModelExecution, UnifiedIterExecution};
use crate::worker::gpu_cluster::SharedGpuCluster;
use crate::worker::iter_worker::IterWorker;
use crate::worker::kv::{FullAttnKv, IterWorkerKv};
use crate::worker::shared::context::WorkerContext;
use crate::worker::types::{PdDecodeEvent, PdDecodeMsg, WorkerStatus};

use super::decode_core::DecodeCore;
pub(super) use super::decode_core::PULL_BUDGET_FRACTION;
pub struct PullDecodeWorker<K: IterWorkerKv, E: IterModelExecution<K>> {
    core: DecodeCore<K, E>,
}
pub type PdDecodeWorker<M> = PullDecodeWorker<FullAttnKv, UnifiedIterExecution<M>>;
impl<K: IterWorkerKv, E: IterModelExecution<K>> PullDecodeWorker<K, E> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_components(
        context: WorkerContext,
        kv_store: K,
        execution: E,
        balance: LoadBalance,
        cluster: SharedGpuCluster,
        receive_group_id: u16,
        pull_budget_tokens: u64,
        kv_bytes_per_token: u64,
    ) -> Self {
        Self {
            core: DecodeCore::from_components(
                context,
                kv_store,
                execution,
                balance,
                cluster,
                receive_group_id,
                pull_budget_tokens,
                kv_bytes_per_token,
            ),
        }
    }
    pub fn release_request(&mut self, request: RequestId, current_kv: u64) -> Option<u16> {
        self.core.release_request(request, current_kv)
    }
}
impl<K: IterWorkerKv, E: IterModelExecution<K>> IterWorker for PullDecodeWorker<K, E> {
    type Msg = PdDecodeMsg;
    type Event = PdDecodeEvent;
    fn id(&self) -> WorkerId {
        self.core.context.id
    }
    fn enqueue(&mut self, msg: Self::Msg) {
        match msg {
            PdDecodeMsg::Request(req) => self.core.on_msg_request(req),
            PdDecodeMsg::Handoff {
                req,
                send_gid,
                tokens,
                prefill_worker,
            } => self
                .core
                .on_msg_handoff(req, send_gid, tokens, prefill_worker),
        }
    }
    fn tick(&mut self, now: Time, events: &mut Vec<Self::Event>) -> Option<Time> {
        self.core.advance_pulls(now, events);
        while let Some(completed) = self.core.advance_decode(now) {
            for (req, partition) in completed {
                self.core.kv_store.release(req, partition);
                events.push(PdDecodeEvent::RequestComplete {
                    worker: self.id(),
                    req,
                });
            }
            self.core.sample_partitions(now);
        }
        self.core.next_wakeup(now)
    }
    fn status(&self) -> WorkerStatus {
        self.core.status()
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Arc;

    use super::*;
    use crate::common::{PoolId, SharedRequests};
    use crate::test_helpers::{prefilled_store, test_cluster, FakeModel};
    use crate::worker::gpu_cluster::SharedGpuCluster;
    use crate::worker::types::WorkerConfig;
    use crate::worker::workers::pd_decode::build_pd_decode_worker;
    use tempfile::tempdir;

    fn worker(store: SharedRequests) -> PdDecodeWorker<FakeModel> {
        worker_with_partitions(store, 1)
    }

    fn worker_with_partitions(
        store: SharedRequests,
        num_partitions: u16,
    ) -> PdDecodeWorker<FakeModel> {
        build_pd_decode_worker(
            WorkerId(0),
            "decode",
            Arc::new(FakeModel {
                ms: 1.0,
                dp_groups: num_partitions,
            }),
            store,
            WorkerConfig::default(),
            None,
            PoolId(0),
            "test-gpu",
            test_cluster(),
        )
    }

    #[test]
    fn handed_off_request_decodes_to_completion() {
        let store = prefilled_store(&[(0, 16, 3)]);
        let mut worker = worker(Rc::clone(&store));
        worker.enqueue(PdDecodeMsg::Request(RequestId(0)));
        let mut events = Vec::new();
        for step in 0..50u64 {
            worker.tick(Time::from_ms(step as f64), &mut events);
        }
        assert_eq!(
            events,
            vec![PdDecodeEvent::RequestComplete {
                worker: WorkerId(0),
                req: RequestId(0),
            }]
        );
        let store = store.borrow();
        let record = &store[RequestId(0)];
        assert!(record.lifecycle.completed);
        assert_eq!(record.progress.output_tokens_emitted, 3);
    }

    #[test]
    fn multiple_handed_off_requests_all_complete() {
        let store = prefilled_store(&[(0, 8, 2), (1, 8, 2), (2, 8, 2)]);
        let mut worker = worker(Rc::clone(&store));
        for request in 0..3 {
            worker.enqueue(PdDecodeMsg::Request(RequestId(request)));
        }
        let mut events = Vec::new();
        for step in 0..200u64 {
            worker.tick(Time::from_ms(step as f64), &mut events);
        }
        assert_eq!(events.len(), 3);
        let store = store.borrow();
        for request in 0..3 {
            assert!(
                store[RequestId(request)].lifecycle.completed,
                "request {request} should complete"
            );
        }
    }

    #[test]
    fn builds_one_decode_partition_per_dp_shard() {
        let worker = worker_with_partitions(prefilled_store(&[]), 2);
        assert_eq!(worker.core.kv_store.num_partitions(), 2);
    }

    #[test]
    fn decode_recipe_does_not_open_a_prefix_cache_event_stream() {
        let log_directory = tempdir().unwrap();
        let worker = build_pd_decode_worker(
            WorkerId(0),
            "decode",
            Arc::new(FakeModel::for_ms(1.0)),
            prefilled_store(&[]),
            WorkerConfig::default(),
            Some(log_directory.path().to_path_buf()),
            PoolId(0),
            "test-gpu",
            test_cluster(),
        );
        drop(worker);

        assert!(!log_directory.path().join("raw/prefix_cache_event").exists());
    }

    #[test]
    fn decode_round_robins_handoffs_across_partitions() {
        let store = prefilled_store(&[(0, 4, 50), (1, 4, 50)]);
        let mut worker = worker_with_partitions(store, 2);
        worker.enqueue(PdDecodeMsg::Request(RequestId(0)));
        worker.enqueue(PdDecodeMsg::Request(RequestId(1)));
        let mut events = Vec::new();
        for step in 0..10u64 {
            worker.tick(Time::from_ms(step as f64), &mut events);
        }
        assert_eq!(
            (
                worker.core.kv_store.live_decode_count(0),
                worker.core.kv_store.live_decode_count(1),
            ),
            (1, 1)
        );
    }

    fn register_test_sender(cluster: &SharedGpuCluster) -> u16 {
        let mut cluster = cluster.borrow_mut();
        cluster.allocate(99, 99, 1, "sender-gpu", "prefill");
        cluster.register_comm_group(0, 1, "prefill", 99)
    }

    #[test]
    fn pull_backlog_gate_holds_second_handoff_until_first_drains() {
        let store = prefilled_store(&[(0, 40, 1), (1, 20, 1)]);
        let cluster = test_cluster();
        let sender_group_id = register_test_sender(&cluster);
        let mut worker = build_pd_decode_worker(
            WorkerId(0),
            "decode",
            Arc::new(FakeModel {
                ms: 1.0,
                dp_groups: 1,
            }),
            Rc::clone(&store),
            WorkerConfig {
                attn_kv_bytes: 1000,
                ..WorkerConfig::default()
            },
            None,
            PoolId(0),
            "test-gpu",
            Rc::clone(&cluster),
        );
        for request in [0u32, 1] {
            worker.enqueue(PdDecodeMsg::Handoff {
                req: RequestId(request),
                send_gid: sender_group_id,
                tokens: if request == 0 { 40 } else { 20 },
                prefill_worker: WorkerId(99),
            });
        }
        let mut events = Vec::new();
        worker.tick(Time::ZERO, &mut events);
        assert!(worker
            .core
            .pull_pipeline
            .in_flight
            .is_some_and(|pull| pull.request == RequestId(0)));
        assert_eq!(worker.core.pull_pipeline.pending_pulls.len(), 1);
    }

    #[test]
    fn pull_backlog_allows_one_oversized_request_when_empty() {
        let store = prefilled_store(&[(0, 200, 1)]);
        let cluster = test_cluster();
        let sender_group_id = register_test_sender(&cluster);
        let mut worker = build_pd_decode_worker(
            WorkerId(0),
            "decode",
            Arc::new(FakeModel {
                ms: 1.0,
                dp_groups: 1,
            }),
            Rc::clone(&store),
            WorkerConfig {
                attn_kv_bytes: 1000,
                ..WorkerConfig::default()
            },
            None,
            PoolId(0),
            "test-gpu",
            Rc::clone(&cluster),
        );
        worker.enqueue(PdDecodeMsg::Handoff {
            req: RequestId(0),
            send_gid: sender_group_id,
            tokens: 200,
            prefill_worker: WorkerId(99),
        });
        let mut events = Vec::new();
        worker.tick(Time::ZERO, &mut events);
        assert!(worker
            .core
            .pull_pipeline
            .in_flight
            .is_some_and(|pull| pull.request == RequestId(0)));
        assert!(worker.core.pull_pipeline.pending_pulls.is_empty());
    }

    #[test]
    fn partitioned_decode_requests_all_complete() {
        let store = prefilled_store(&[(0, 8, 2), (1, 8, 2), (2, 8, 2), (3, 8, 2)]);
        let mut worker = worker_with_partitions(Rc::clone(&store), 2);
        for request in 0..4u32 {
            worker.enqueue(PdDecodeMsg::Request(RequestId(request)));
        }
        let mut events = Vec::new();
        for step in 0..200u64 {
            worker.tick(Time::from_ms(step as f64), &mut events);
        }
        assert_eq!(events.len(), 4);
        let store = store.borrow();
        for request in 0..4u32 {
            assert!(
                store[RequestId(request)].lifecycle.completed,
                "request {request} should complete"
            );
        }
    }
}
