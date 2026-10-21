//! Whole-iteration cadence composed with a separate remote-read timeline.
//! IterBatchWorker remains the sole iteration FSM. Admission owns selected read
//! states; KV owns occupancy. This shell only submits and lands transfers.
//! Read construction → IterWorker → read advancement.
use crate::common::{RequestId, Time, WorkerId};
use crate::worker::admission::RemoteKvAdmission;
use crate::worker::execution::IterModelExecution;
use crate::worker::gpu_cluster::SharedGpuCluster;
use crate::worker::iter_worker::IterWorker;
use crate::worker::kv::IterWorkerKv;
use crate::worker::types::WorkerStatus;
use crate::worker::workers::iter::IterBatchWorker;
use std::collections::VecDeque;

pub(crate) struct RemoteCacheIterWorker<
    K: IterWorkerKv,
    A: RemoteKvAdmission<K>,
    E: IterModelExecution<K>,
> {
    iteration: IterBatchWorker<K, A, E>,
    cluster: SharedGpuCluster,
    bytes_per_token: u64,
    reads: VecDeque<(Time, RequestId)>,
}

#[cfg(test)]
mod tests {
    use super::super::build_remote_cache_unified_worker;
    use super::*;
    use crate::common::{PoolId, RequestStore, SessionInput};
    use crate::test_helpers::{test_cluster, text_request, FakeModel};
    use crate::worker::types::{
        RemoteCacheWorkerEvent as Event, RemoteCacheWorkerMsg as Msg, WorkerConfig,
    };
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    #[test]
    fn selected_lookups_obey_backpressure_and_do_not_reenter_pending() {
        let requests = Rc::new(RefCell::new(RequestStore::new()));
        for id in 0..2 {
            let mut request = text_request(RequestId(id), 10, 3, Time::ZERO);
            request.definition.session = SessionInput::Session {
                session_id: id,
                session_start_time: Time::ZERO,
                declared_prefix_tokens: 20,
            };
            requests.borrow_mut().insert(request);
        }
        let mut worker = build_remote_cache_unified_worker(
            WorkerId(0),
            "main",
            Arc::new(FakeModel::for_ms(1.0)),
            requests,
            WorkerConfig {
                max_inflight_remote_reads: 1,
                ..Default::default()
            },
            None,
            PoolId(0),
            "test",
            test_cluster(),
        );
        worker.enqueue(Msg::Request(RequestId(0)));
        worker.enqueue(Msg::Request(RequestId(1)));
        let mut events = Vec::new();
        worker.tick(Time::ZERO, &mut events);
        assert!(matches!(
            events.as_slice(),
            [Event::KvLookup {
                req: RequestId(0),
                ..
            }]
        ));
        events.clear();
        worker.tick(Time::ZERO, &mut events);
        assert!(events.is_empty());
        worker.enqueue(Msg::KvLookupResult {
            req: RequestId(0),
            resident_tokens: 0,
            store_send_gid: 0,
            store_worker: WorkerId(0),
        });
        worker.tick(Time::ZERO, &mut events);
        assert!(matches!(
            events.as_slice(),
            [Event::KvLookup {
                req: RequestId(1),
                ..
            }]
        ));
        assert_eq!(worker.status().queued_requests, 1);
    }
}
impl<K: IterWorkerKv, A: RemoteKvAdmission<K>, E: IterModelExecution<K>>
    RemoteCacheIterWorker<K, A, E>
{
    pub(super) fn new(
        iteration: IterBatchWorker<K, A, E>,
        cluster: SharedGpuCluster,
        bytes_per_token: u64,
    ) -> Self {
        Self {
            iteration,
            cluster,
            bytes_per_token,
            reads: VecDeque::new(),
        }
    }
}
impl<K: IterWorkerKv, A: RemoteKvAdmission<K>, E: IterModelExecution<K>> IterWorker
    for RemoteCacheIterWorker<K, A, E>
{
    type Msg = A::Msg;
    type Event = A::Event;
    fn id(&self) -> WorkerId {
        self.iteration.id()
    }
    fn enqueue(&mut self, msg: Self::Msg) {
        self.iteration.enqueue(msg);
    }
    fn status(&self) -> WorkerStatus {
        self.iteration.status()
    }
    fn tick(&mut self, now: Time, events: &mut Vec<Self::Event>) -> Option<Time> {
        self.advance_reads(now);
        let compute = self.iteration.tick(now, events);
        for row in self.iteration.admission.drain_reuse() {
            self.cluster.borrow().record_kv_event(row);
        }
        self.iteration
            .admission
            .drain_lookups(&self.iteration.context, events);
        match (compute, self.reads.front().map(|(end, _)| *end)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}
impl<K: IterWorkerKv, A: RemoteKvAdmission<K>, E: IterModelExecution<K>>
    RemoteCacheIterWorker<K, A, E>
{
    fn advance_reads(&mut self, now: Time) {
        while let Some((req, plan)) = self.iteration.admission.take_read() {
            let end = self.cluster.borrow_mut().submit_transfer(
                now,
                plan.store_send_gid,
                plan.recv_gid,
                plan.tokens
                    .checked_mul(self.bytes_per_token)
                    .expect("KV byte overflow"),
                "kv_store_read",
                &req.0.to_string(),
            );
            self.reads.push_back((end, req));
        }
        while self.reads.front().is_some_and(|(end, _)| *end <= now) {
            let (_, req) = self.reads.pop_front().unwrap();
            self.iteration
                .admission
                .read_landed(&mut self.iteration.kv_store, req, now);
        }
    }
}
