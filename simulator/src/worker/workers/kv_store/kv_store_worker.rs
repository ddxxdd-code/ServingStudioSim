//! Storage-only shell: queued messages → reserve → receive → insert → ack.
//! SessionStore owns capacity, copies and pins; this shell owns transfer timing.
//! No request store, model, token generation, or decoder KV is present.
//! Read construction, IterWorker dispatch, then push/landing helpers.

use super::session_store::SessionStore;
use crate::common::{RequestId, Time, WorkerId};
use crate::log::kv_store_logger::SharedKvStoreLogger;
use crate::log::rows::KvStoreEventEntry;
use crate::worker::gpu_cluster::SharedGpuCluster;
use crate::worker::iter_worker::{IterWorker, KvStoreLoad};
use crate::worker::kv::{PrefixCacheMutation, PrefixCacheMutationKind};
use crate::worker::types::{
    KvPushRejectReason, KvRetention, KvStoreEvent, KvStoreMsg, WorkerStatus,
};
use std::collections::VecDeque;

#[derive(Clone, Copy)]
struct Push {
    origin: WorkerId,
    req: RequestId,
    session: u32,
    tokens: u64,
    send_gid: u16,
    retention: KvRetention,
}

pub(crate) struct KvStoreWorker {
    pub(crate) logger: Option<SharedKvStoreLogger>,
    id: WorkerId,
    store: SessionStore,
    cluster: SharedGpuCluster,
    group: u16,
    bytes_per_token: u64,
    inbox: VecDeque<KvStoreMsg>,
    waiting: VecDeque<Push>,
    receiving: VecDeque<(Time, Push)>,
}

impl KvStoreWorker {
    pub(super) fn new(
        id: WorkerId,
        store: SessionStore,
        cluster: SharedGpuCluster,
        group: u16,
        bytes_per_token: u64,
    ) -> Self {
        Self {
            logger: None,
            id,
            store,
            cluster,
            group,
            bytes_per_token,
            inbox: VecDeque::new(),
            waiting: VecDeque::new(),
            receiving: VecDeque::new(),
        }
    }
}

impl IterWorker for KvStoreWorker {
    type Msg = KvStoreMsg;
    type Event = KvStoreEvent;
    fn id(&self) -> WorkerId {
        self.id
    }
    fn enqueue(&mut self, msg: KvStoreMsg) {
        if matches!(msg, KvStoreMsg::Cancel { .. }) {
            self.inbox.push_front(msg);
        } else {
            self.inbox.push_back(msg);
        }
    }
    fn status(&self) -> WorkerStatus {
        WorkerStatus {
            queued_requests: 0,
            active_requests: 0,
        }
    }
    fn tick(&mut self, now: Time, events: &mut Vec<KvStoreEvent>) -> Option<Time> {
        // Cancellation wins over an arrival at this tick. Other messages must
        // observe already-due landings, especially lookups of a growing entry.
        while let Some(KvStoreMsg::Cancel { session, req }) = self.inbox.front().copied() {
            self.inbox.pop_front();
            self.on_msg_cancel(session, req, now);
        }
        self.land_pushes(now, events);
        while let Some(msg) = self.inbox.pop_front() {
            match msg {
                KvStoreMsg::Lookup {
                    origin,
                    req,
                    session,
                    requested_tokens,
                } => self.on_msg_lookup(origin, req, session, requested_tokens, now, events),
                KvStoreMsg::Push {
                    origin,
                    req,
                    session,
                    tokens,
                    send_gid,
                    retention,
                } => self.on_msg_push(Push {
                    origin,
                    req,
                    session,
                    tokens,
                    send_gid,
                    retention,
                }),
                KvStoreMsg::Consumed { session, req } => self.on_msg_consumed(session, req, now),
                KvStoreMsg::Cancel { session, req } => self.on_msg_cancel(session, req, now),
            }
        }
        let waiting = self.waiting.len();
        for _ in 0..waiting {
            let push = self.waiting.pop_front().unwrap();
            if self.store.busy(push.session) {
                self.waiting.push_back(push);
            } else {
                self.start_push(push, now, events);
            }
        }
        self.land_pushes(now, events);
        if self
            .waiting
            .iter()
            .any(|push| !self.store.busy(push.session))
        {
            Some(now)
        } else {
            self.receiving.front().map(|(end, _)| *end)
        }
    }
}

impl KvStoreWorker {
    fn on_msg_cancel(&mut self, session: u32, req: RequestId, now: Time) {
        self.inbox.retain(|msg| match msg {
            KvStoreMsg::Lookup { req: r, .. }
            | KvStoreMsg::Push { req: r, .. }
            | KvStoreMsg::Consumed { req: r, .. }
            | KvStoreMsg::Cancel { req: r, .. } => *r != req,
        });
        self.waiting.retain(|p| p.req != req);
        self.receiving.retain(|(_, p)| p.req != req);
        self.on_msg_consumed(session, req, now);
        self.store.cancel(session, req);
        self.record(KvStoreEventEntry {
            time_ms: now.as_ms(),
            request_id: req.0,
            session_id: session,
            operation: "cancel",
            reason: "request",
            cache_used_before: self.store.cached(),
            cache_used_after: self.store.cached(),
            ..Default::default()
        });
    }

    fn on_msg_lookup(
        &mut self,
        origin: WorkerId,
        req: RequestId,
        session: u32,
        requested: u32,
        now: Time,
        events: &mut Vec<KvStoreEvent>,
    ) {
        let hit = self.store.copy(session, requested);
        self.record(KvStoreEventEntry {
            time_ms: now.as_ms(),
            request_id: req.0,
            session_id: session,
            operation: "lookup",
            reason: if hit == 0 { "miss" } else { "hit" },
            requested_tokens: u64::from(requested),
            hit_tokens: u64::from(hit),
            cache_used_before: self.store.cached(),
            cache_used_after: self.store.cached(),
            ..Default::default()
        });
        events.push(KvStoreEvent::LookupResult {
            store: self.id,
            origin,
            req,
            resident_tokens: hit,
            store_send_gid: self.group,
        });
    }
    fn on_msg_push(&mut self, push: Push) {
        self.waiting.push_back(push);
    }
    fn on_msg_consumed(&mut self, session: u32, req: RequestId, now: Time) {
        if !self.store.consumed(session, req) {
            return;
        }
        self.record(KvStoreEventEntry {
            time_ms: now.as_ms(),
            request_id: req.0,
            session_id: session,
            operation: "unpin",
            reason: "consumed",
            cache_used_before: self.store.cached(),
            cache_used_after: self.store.cached(),
            ..Default::default()
        });
    }
    fn start_push(&mut self, mut push: Push, now: Time, events: &mut Vec<KvStoreEvent>) {
        let Some((accepted, mutations)) =
            self.store
                .reserve(push.session, push.req, push.tokens, push.retention)
        else {
            self.record(KvStoreEventEntry {
                time_ms: now.as_ms(),
                request_id: push.req.0,
                session_id: push.session,
                operation: "push-rejected",
                reason: "capacity-exceeded",
                requested_tokens: push.tokens,
                ..Default::default()
            });
            events.push(KvStoreEvent::PushRejected {
                store: self.id,
                origin: push.origin,
                req: push.req,
                reason: KvPushRejectReason::CapacityExceeded,
            });
            return;
        };
        self.emit_mutations(mutations, push.req, now, events);
        self.record(KvStoreEventEntry {
            time_ms: now.as_ms(),
            request_id: push.req.0,
            session_id: push.session,
            operation: "reserve",
            reason: "incoming-push",
            requested_tokens: accepted,
            cache_used_before: self.store.cached(),
            cache_used_after: self.store.cached(),
            ..Default::default()
        });
        push.tokens = accepted;
        let end = if accepted == 0 {
            now
        } else {
            self.cluster.borrow_mut().submit_transfer(
                now,
                push.send_gid,
                self.group,
                accepted
                    .checked_mul(self.bytes_per_token)
                    .expect("KV transfer byte overflow"),
                "kv_store_push",
                &push.req.0.to_string(),
            )
        };
        // recv_group serializes nonempty transfers. An empty write still waits
        // behind prior receives so the ordered arrival queue stays monotonic.
        let end = end.max(self.receiving.back().map_or(now, |(end, _)| *end));
        self.receiving.push_back((end, push));
    }
    fn land_pushes(&mut self, now: Time, events: &mut Vec<KvStoreEvent>) {
        while self.receiving.front().is_some_and(|(end, _)| *end <= now) {
            let (_, push) = self.receiving.pop_front().unwrap();
            let (tokens, mutations) = self.store.land(push.session, push.req, push.retention);
            self.emit_mutations(mutations, push.req, now, events);
            if push.retention == KvRetention::PinnedUntilFetched {
                self.record(KvStoreEventEntry {
                    time_ms: now.as_ms(),
                    request_id: push.req.0,
                    session_id: push.session,
                    operation: "pin",
                    reason: "relay",
                    cache_used_before: self.store.cached(),
                    cache_used_after: self.store.cached(),
                    ..Default::default()
                });
            }
            events.push(KvStoreEvent::PushComplete {
                store: self.id,
                origin: push.origin,
                req: push.req,
                session: push.session,
                retention: push.retention,
                store_send_gid: self.group,
                tokens,
            });
        }
    }
    fn emit_mutations(
        &self,
        mutations: Vec<PrefixCacheMutation>,
        req: RequestId,
        now: Time,
        events: &mut Vec<KvStoreEvent>,
    ) {
        for mutation in mutations {
            self.record(KvStoreEventEntry {
                time_ms: now.as_ms(),
                request_id: req.0,
                session_id: mutation.session_id,
                operation: if mutation.kind == PrefixCacheMutationKind::Retain {
                    "retain"
                } else {
                    "evict"
                },
                reason: match mutation.kind {
                    PrefixCacheMutationKind::Retain => "push-landed",
                    PrefixCacheMutationKind::SameSessionReplacement => "same-session-replacement",
                    PrefixCacheMutationKind::CapacityEviction => "incoming-reservation",
                    PrefixCacheMutationKind::ReplacementPolicyEviction => "replacement-policy",
                },
                entry_tokens_before: mutation.entry_tokens_before,
                entry_tokens_after: mutation.entry_tokens_after,
                cache_used_before: mutation.cache_used_before,
                cache_used_after: mutation.cache_used_after,
                ..Default::default()
            });
            if mutation.kind != PrefixCacheMutationKind::Retain {
                events.push(KvStoreEvent::Evicted {
                    store: self.id,
                    session: mutation.session_id,
                });
            }
        }
    }
    fn record(&self, mut row: KvStoreEventEntry) {
        row.store_worker = self.id.0;
        row.reserved_tokens = Some(self.store.reserved());
        row.pinned_tokens = Some(self.store.pinned());
        if let Some(logger) = &self.logger {
            logger.borrow_mut().record(row);
        }
    }
}

impl KvStoreLoad for KvStoreWorker {
    fn used_tokens(&self) -> u64 {
        self.store.used()
    }
    fn capacity_tokens(&self) -> u64 {
        self.store.capacity()
    }
    fn available_pinned_tokens(&self) -> u64 {
        self.store.available_for_pin()
    }
    fn send_backlog(&self, now: Time) -> Time {
        self.cluster.borrow().send_backlog(self.group, now)
    }
    fn recv_backlog(&self, now: Time) -> Time {
        self.cluster.borrow().recv_backlog(self.group, now)
    }
}
