//! Remote PD prefill: shared-prefix admission followed by a pinned store relay.
use super::remote_prefill_state::RemotePrefillState;
use super::{IterAdmission, LoadBalance, PendingOrderPolicy, RemoteKvAdmission};
use crate::common::{PdStage, RequestId, Time};
use crate::worker::kv::RemotePrefixKv;
use crate::worker::shared::context::WorkerContext;
use crate::worker::types::{
    KvRetention, RemotePdPrefillEvent as Event, RemotePdPrefillMsg as Msg, RemoteReadPlan,
};

pub(crate) struct RemoteCachePrefillHandoffAdmission<P: PendingOrderPolicy> {
    state: RemotePrefillState<P>,
    send_gid: u16,
}
impl<P: PendingOrderPolicy> RemoteCachePrefillHandoffAdmission<P> {
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
    for RemoteCachePrefillHandoffAdmission<P>
{
    type Msg = Msg;
    type Event = Event;
    fn accept_message(&mut self, kv: &mut K, msg: Msg, context: &WorkerContext) {
        match msg {
            Msg::Request(req) => {
                self.state
                    .enqueue(kv, req, context, PdStage::PendingPrefill as u16, 0)
            }
            Msg::KvLookupResult {
                req,
                resident_tokens,
                store_send_gid,
                store_worker,
            } => self
                .state
                .lookup_result(req, resident_tokens, store_send_gid, store_worker),
            Msg::KvPushComplete { req, at, .. } => kv.complete_push(req, at),
            Msg::Cancel { req, at } => {
                self.state.cancel(req);
                kv.cancel_remote(req, at);
            }
        }
    }
    fn form_batch(&mut self, kv: &mut K, context: &WorkerContext, now: Time) -> bool {
        self.state
            .form_batch(kv, context, now, PdStage::Prefill as u16, 1)
    }
    fn complete_iteration(
        &mut self,
        kv: &mut K,
        context: &WorkerContext,
        events: &mut Vec<Event>,
        now: Time,
    ) {
        for partition in 0..kv.num_partitions() as u16 {
            let mut transitions = Vec::new();
            {
                let mut requests = context.requests.borrow_mut();
                kv.visit_prefill_admits(partition, |req| {
                    let record = &mut requests[req];
                    record.progress.prefill_tokens_processed = kv.prefill_tokens_to_compute(req);
                    record.record_first_token(now, context.log_tokens());
                    let complete = record.is_complete();
                    context.stamp_stage(
                        record,
                        now,
                        if complete {
                            PdStage::Done
                        } else {
                            PdStage::PrefillDoneAwaitPull
                        } as u16,
                    );
                    transitions.push((req, record.request.definition.session, complete));
                });
            }
            for (req, session, complete) in transitions {
                if let Some(session_id) = session.session_id() {
                    let tokens = kv.hold_for_push(req, partition, session, now);
                    events.push(Event::KvPushOffer {
                        worker: context.id,
                        req,
                        session: session_id,
                        tokens,
                        send_gid: self.send_gid,
                        retention: if complete {
                            KvRetention::Shared
                        } else {
                            KvRetention::PinnedUntilFetched
                        },
                    });
                } else if complete {
                    kv.release_retaining_prefix(req, partition, now);
                } else {
                    let tokens = kv.post_prefill_context_tokens(req);
                    kv.hold(partition, req, tokens);
                    events.push(Event::StandaloneHandoff {
                        worker: context.id,
                        req,
                        send_gid: self.send_gid,
                        tokens,
                    });
                }
                if complete {
                    events.push(Event::RequestComplete {
                        worker: context.id,
                        req,
                    });
                }
            }
            kv.clear_prefill_admits(partition);
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
    for RemoteCachePrefillHandoffAdmission<P>
{
    fn drain_reuse(&mut self) -> Vec<crate::log::rows::KvStoreEventEntry> {
        std::mem::take(&mut self.state.reuse)
    }
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
