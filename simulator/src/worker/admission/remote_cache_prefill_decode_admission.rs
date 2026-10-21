//! Unified remote lifecycle: select → local/remote prefix → prefill/decode → push.
//! Completed KV moves once from resident to held; acknowledgement returns it to
//! the local evictable cache. It is never counted as both held and retained.
use super::remote_prefill_state::RemotePrefillState;
use super::{IterAdmission, LoadBalance, PendingOrderPolicy, RemoteKvAdmission};
use crate::common::{RequestId, Time, UnifiedStage};
use crate::worker::kv::RemotePrefixKv;
use crate::worker::shared::{advance_scope::AdvanceScope, context::WorkerContext};
use crate::worker::types::{
    KvRetention, RemoteCacheWorkerEvent as Event, RemoteCacheWorkerMsg as Msg, RemoteReadPlan,
};

pub(crate) struct RemoteCachePrefillDecodeAdmission<P: PendingOrderPolicy> {
    state: RemotePrefillState<P>,
    send_gid: u16,
}
impl<P: PendingOrderPolicy> RemoteCachePrefillDecodeAdmission<P> {
    pub(crate) fn new(
        policy: P,
        policy_context: P::Context,
        balance: LoadBalance,
        budget: Option<u32>,
        max_reads: usize,
        group: u16,
    ) -> Self {
        Self {
            state: RemotePrefillState::new(
                policy,
                policy_context,
                balance,
                budget,
                max_reads,
                group,
            ),
            send_gid: group,
        }
    }
}
impl<P: PendingOrderPolicy, K: RemotePrefixKv> IterAdmission<K>
    for RemoteCachePrefillDecodeAdmission<P>
{
    type Msg = Msg;
    type Event = Event;
    fn accept_message(&mut self, kv: &mut K, msg: Msg, context: &WorkerContext) {
        match msg {
            Msg::Request(req) => {
                let remaining = context.requests.borrow()[req]
                    .request
                    .definition
                    .target_output_tokens;
                self.state
                    .enqueue(kv, req, context, UnifiedStage::Pending as u16, remaining);
            }
            Msg::KvLookupResult {
                req,
                resident_tokens,
                store_send_gid,
                store_worker,
            } => self
                .state
                .lookup_result(req, resident_tokens, store_send_gid, store_worker),
            Msg::KvPushComplete { req, at, .. } => kv.complete_handoff(req, at),
            Msg::KvPushRejected { req, reason } => {
                panic!("remote KV push rejected for {req:?}: {reason:?}")
            }
        }
    }
    fn form_batch(&mut self, kv: &mut K, context: &WorkerContext, now: Time) -> bool {
        self.state
            .form_batch(kv, context, now, UnifiedStage::Prefill as u16, usize::MAX)
    }
    fn complete_iteration(
        &mut self,
        kv: &mut K,
        context: &WorkerContext,
        events: &mut Vec<Event>,
        now: Time,
    ) {
        for partition in 0..kv.num_partitions() as u16 {
            let mut completed = Vec::new();
            {
                let mut requests = context.requests.borrow_mut();
                kv.visit_decode_members(partition, |req, _| {
                    let record = &mut requests[req];
                    record.record_token(now, context.log_tokens());
                    if record.is_complete() {
                        completed.push(req);
                    }
                });
            }
            kv.advance(AdvanceScope::WholePartition(partition), 1);
            let mut to_decode = Vec::new();
            {
                let mut requests = context.requests.borrow_mut();
                kv.visit_prefill_admits(partition, |req| {
                    let record = &mut requests[req];
                    record.progress.prefill_tokens_processed = kv.prefill_tokens_to_compute(req);
                    record.record_first_token(now, context.log_tokens());
                    if record.is_complete() {
                        completed.push(req);
                    } else {
                        context.stamp_stage(record, now, UnifiedStage::Decode as u16);
                        to_decode.push((
                            req,
                            kv.post_prefill_context_tokens(req),
                            record.request.definition.target_output_tokens
                                - record.progress.output_tokens_emitted,
                        ));
                    }
                });
            }
            for (req, tokens, remaining) in to_decode {
                kv.commit_resident(req, partition, tokens, remaining);
            }
            kv.clear_prefill_admits(partition);
            for req in completed {
                let session = {
                    let mut requests = context.requests.borrow_mut();
                    let record = &mut requests[req];
                    context.stamp_stage(record, now, UnifiedStage::Done as u16);
                    record.request.definition.session
                };
                if let Some(session_id) = session.session_id() {
                    let tokens = kv.hold_for_push(req, partition, session);
                    events.push(Event::KvPushOffer {
                        worker: context.id,
                        req,
                        session: session_id,
                        tokens,
                        send_gid: self.send_gid,
                        retention: KvRetention::Shared,
                    });
                } else {
                    kv.release_retaining_prefix(req, partition, now);
                }
                events.push(Event::RequestComplete {
                    worker: context.id,
                    req,
                });
            }
            kv.sample_submit(partition, now);
        }
    }
    fn queued_requests(&self) -> u32 {
        self.state.queued()
    }
    fn cancel_pending(&mut self, req: RequestId) -> bool {
        self.state.cancel(req)
    }
}
impl<P: PendingOrderPolicy, K: RemotePrefixKv> RemoteKvAdmission<K>
    for RemoteCachePrefillDecodeAdmission<P>
{
    fn drain_lookups(&mut self, context: &WorkerContext, events: &mut Vec<Event>) {
        for (req, session, requested_tokens) in self.state.lookups.drain(..) {
            events.push(Event::KvLookup {
                worker: context.id,
                req,
                session,
                requested_tokens,
            });
        }
    }
    fn take_read(&mut self) -> Option<(RequestId, RemoteReadPlan)> {
        self.state.take_read()
    }
    fn read_landed(&mut self, kv: &mut K, req: RequestId, now: Time) {
        self.state.read_landed(kv, req, now);
    }
}
