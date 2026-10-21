//! Remote PD decode composes DecodeCore's pull/iteration cadence.
//! The shell translates landed relays to Consumed and moves completed KV into
//! held ownership until store acknowledgement. It never performs prefix lookup.
//! Read construction → message dispatch → tick → cancellation.
use crate::common::{RequestId, Time, WorkerId};
use crate::worker::execution::IterModelExecution;
use crate::worker::iter_worker::IterWorker;
use crate::worker::kv::RemotePrefixKv;
use crate::worker::types::{
    KvRetention, PdDecodeEvent, RemotePdDecodeEvent as Event, RemotePdDecodeMsg as Msg,
    WorkerStatus,
};
use crate::worker::workers::pd_decode::decode_core::DecodeCore;
use std::collections::HashMap;

pub(crate) struct RemotePullDecodeWorker<K: RemotePrefixKv, E: IterModelExecution<K>> {
    core: DecodeCore<K, E>,
    group: u16,
    relays: HashMap<RequestId, (u32, WorkerId)>,
    cancellations: Vec<RequestId>,
}

#[cfg(test)]
mod tests {
    use super::super::build_remote_cache_pd_decode_worker;
    use super::*;
    use crate::common::{PoolId, SessionInput};
    use crate::test_helpers::{prefilled_store, test_cluster, FakeModel};
    use crate::worker::types::WorkerConfig;
    use std::sync::Arc;

    #[test]
    fn cancellation_releases_an_inflight_relay_without_decoding_it() {
        let requests = prefilled_store(&[(0, 20, 3)]);
        requests.borrow_mut()[RequestId(0)]
            .request
            .definition
            .session = SessionInput::Session {
            session_id: 7,
            session_start_time: Time::ZERO,
            declared_prefix_tokens: 0,
        };
        let cluster = test_cluster();
        let host = cluster.borrow_mut().allocate_host(2, 0, "DRAM");
        let send = cluster
            .borrow_mut()
            .register_comm_group(host, 1, "kv_store", 0);
        let mut worker = build_remote_cache_pd_decode_worker(
            WorkerId(0),
            "decode",
            Arc::new(FakeModel::for_ms(1.0)),
            requests,
            WorkerConfig::default(),
            None,
            PoolId(1),
            "test",
            cluster,
        );
        worker.enqueue(Msg::Handoff {
            req: RequestId(0),
            store_send_gid: send,
            tokens: 20,
            store_worker: WorkerId(0),
        });
        let mut events = Vec::new();
        worker.tick(Time::ZERO, &mut events);
        worker.enqueue(Msg::Cancel { req: RequestId(0) });
        worker.tick(Time::ZERO, &mut events);
        assert!(matches!(
            events.as_slice(),
            [Event::KvConsumed {
                req: RequestId(0),
                session: 7,
                store_worker: WorkerId(0),
                ..
            }]
        ));
        events.clear();
        assert_eq!(worker.tick(Time::from_ms(10.0), &mut events), None);
        assert!(events.is_empty());
        assert_eq!(worker.status().active_requests, 0);
    }
}
impl<K: RemotePrefixKv, E: IterModelExecution<K>> RemotePullDecodeWorker<K, E> {
    pub(super) fn new(mut core: DecodeCore<K, E>, group: u16) -> Self {
        core.set_transfer_kind("kv_store_read");
        Self {
            core,
            group,
            relays: HashMap::new(),
            cancellations: Vec::new(),
        }
    }
}
impl<K: RemotePrefixKv, E: IterModelExecution<K>> IterWorker for RemotePullDecodeWorker<K, E> {
    type Msg = Msg;
    type Event = Event;
    fn id(&self) -> WorkerId {
        self.core.context.id
    }
    fn status(&self) -> WorkerStatus {
        self.core.status()
    }
    fn enqueue(&mut self, msg: Msg) {
        match msg {
            Msg::Request(req) => self.core.on_msg_request(req),
            Msg::Handoff {
                req,
                store_send_gid,
                tokens,
                store_worker,
            } => {
                let session = self.core.context.requests.borrow()[req]
                    .request
                    .definition
                    .session
                    .session_id()
                    .expect("remote relay requires a session");
                self.relays.insert(req, (session, store_worker));
                self.core
                    .on_msg_handoff(req, store_send_gid, tokens, store_worker);
            }
            Msg::StandaloneHandoff {
                req,
                send_gid,
                tokens,
                prefill_worker,
            } => self
                .core
                .on_msg_handoff(req, send_gid, tokens, prefill_worker),
            Msg::KvPushComplete { req, at, .. } => self.core.kv_store.complete_push(req, at),
            Msg::Cancel { req } => self.cancellations.push(req),
        }
    }
    fn tick(&mut self, now: Time, events: &mut Vec<Event>) -> Option<Time> {
        for req in std::mem::take(&mut self.cancellations) {
            if let Some((session, store_worker)) = self.relays.remove(&req) {
                events.push(Event::KvConsumed {
                    worker: self.id(),
                    req,
                    session,
                    store_worker,
                });
            }
            let mut current_tokens = 0;
            for partition in 0..self.core.kv_store.num_partitions() as u16 {
                self.core
                    .kv_store
                    .visit_decode_members(partition, |member, tokens| {
                        if member == req {
                            current_tokens = tokens;
                        }
                    });
            }
            self.core.cancel_pull(req);
            self.core.kv_store.cancel_remote(req, now);
            self.core.release_request(req, current_tokens);
        }
        let mut landed = Vec::new();
        self.core.advance_pulls(now, &mut landed);
        for event in landed {
            if let PdDecodeEvent::PullComplete {
                req,
                prefill_worker,
                ..
            } = event
            {
                if let Some((session, store_worker)) = self.relays.remove(&req) {
                    events.push(Event::KvConsumed {
                        worker: self.id(),
                        req,
                        session,
                        store_worker,
                    });
                } else {
                    events.push(Event::StandalonePullComplete {
                        worker: self.id(),
                        req,
                        prefill_worker,
                    });
                }
            }
        }
        while let Some(completed) = self.core.advance_decode(now) {
            for (req, partition) in completed {
                let session = self.core.context.requests.borrow()[req]
                    .request
                    .definition
                    .session;
                if let Some(session_id) = session.session_id() {
                    let tokens = self
                        .core
                        .kv_store
                        .hold_for_push(req, partition, session, now);
                    events.push(Event::KvPushOffer {
                        worker: self.id(),
                        req,
                        session: session_id,
                        tokens,
                        send_gid: self.group,
                        retention: KvRetention::Shared,
                    });
                } else {
                    self.core.kv_store.release(req, partition);
                }
                events.push(Event::RequestComplete {
                    worker: self.id(),
                    req,
                });
            }
            self.core.sample_partitions(now);
        }
        self.core.next_wakeup(now)
    }
}
