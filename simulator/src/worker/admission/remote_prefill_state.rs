//! Shared fresh-prefill gate mechanics for the two remote lifecycles.
//! Policy owns fresh pending membership; selected requests live in lookup/read/
//! ready states and never re-enter the selection policy. KV owns all byte facts.

use super::{
    prefill_fits_budget, AdmissionCandidate, EnqueueSequence, LoadBalance, PendingOrderPolicy,
};
use crate::common::{RequestId, Time};
use crate::worker::kv::{PrefixKv, RemotePrefixKv};
use crate::worker::shared::context::WorkerContext;
use crate::worker::types::RemoteReadPlan;
use std::collections::{HashMap, VecDeque};

#[derive(Clone, Copy)]
struct Selected {
    candidate: AdmissionCandidate,
    partition: u16,
    read_tokens: u32,
}

pub(super) struct RemotePrefillState<P: PendingOrderPolicy> {
    policy: P,
    policy_context: P::Context,
    sequence: EnqueueSequence,
    balance: LoadBalance,
    budget: u32,
    max_reads: usize,
    selected: HashMap<RequestId, Selected>,
    ready: VecDeque<RequestId>,
    pub(super) lookups: Vec<(RequestId, u32, u32)>,
    reads: VecDeque<(RequestId, RemoteReadPlan)>,
    recv_gid: u16,
    pub(super) reuse: Vec<crate::log::rows::KvStoreEventEntry>,
}

impl<P: PendingOrderPolicy> RemotePrefillState<P> {
    pub(super) fn new(
        policy: P,
        policy_context: P::Context,
        balance: LoadBalance,
        budget: Option<u32>,
        max_reads: usize,
        recv_gid: u16,
    ) -> Self {
        assert!(max_reads > 0);
        Self {
            policy,
            policy_context,
            sequence: EnqueueSequence::default(),
            balance,
            budget: budget.unwrap_or(u32::MAX),
            max_reads,
            selected: HashMap::new(),
            ready: VecDeque::new(),
            lookups: Vec::new(),
            reads: VecDeque::new(),
            recv_gid,
            reuse: Vec::new(),
        }
    }
    pub(super) fn enqueue(
        &mut self,
        kv: &impl PrefixKv,
        req: RequestId,
        context: &WorkerContext,
        stage: u16,
        remaining_output: u32,
    ) {
        let mut requests = context.requests.borrow_mut();
        let record = &mut requests[req];
        let session = record.request.definition.session;
        let fresh = record.request.definition.prompt_tokens;
        let arrival = record.request.core.arrival_time;
        context.stamp_stage(record, arrival, stage);
        let candidate = self.sequence.freeze(
            req,
            fresh,
            remaining_output,
            session,
            session.session_start_or(arrival),
            kv.resident_prefix_tokens(fresh, session),
        );
        self.policy.push(candidate, &mut self.policy_context);
    }
    pub(super) fn lookup_result(
        &mut self,
        req: RequestId,
        tokens: u32,
        gid: u16,
        worker: crate::common::WorkerId,
    ) {
        let Some(selected) = self.selected.get_mut(&req) else {
            return;
        };
        selected.read_tokens =
            tokens.min(selected.candidate.session_input.declared_prefix_tokens());
        if selected.read_tokens == 0 {
            self.ready.push_back(req);
        } else {
            self.reads.push_back((
                req,
                RemoteReadPlan {
                    store_send_gid: gid,
                    recv_gid: self.recv_gid,
                    tokens: u64::from(selected.read_tokens),
                    store_worker: worker,
                },
            ));
        }
    }
    pub(super) fn take_read(&mut self) -> Option<(RequestId, RemoteReadPlan)> {
        self.reads.pop_front()
    }
    pub(super) fn read_landed(&mut self, kv: &mut impl RemotePrefixKv, req: RequestId, now: Time) {
        let Some(selected) = self.selected.get_mut(&req) else {
            return;
        };
        if let Some(best) = kv.retained_prefix_partition(selected.candidate.session_input) {
            selected.partition = best;
        }
        kv.install_remote_prefix(
            req,
            selected.partition,
            selected.candidate.session_input.session_id().unwrap(),
            u64::from(selected.read_tokens),
            now,
        );
        self.ready.push_back(req);
    }
    pub(super) fn queued(&self) -> u32 {
        (self.policy.len() + self.selected.len()) as u32
    }
    pub(super) fn cancel(&mut self, req: RequestId) -> bool {
        let pending = self.policy.remove(req).is_some();
        let selected = self.selected.remove(&req).is_some();
        self.ready.retain(|r| *r != req);
        self.reads.retain(|(r, _)| *r != req);
        self.lookups.retain(|(r, _, _)| *r != req);
        pending || selected
    }

    pub(super) fn form_batch(
        &mut self,
        kv: &mut impl RemotePrefixKv,
        context: &WorkerContext,
        now: Time,
        stage: u16,
        max_prefills: usize,
    ) -> bool {
        let n = kv.num_partitions();
        let mut used = vec![0u32; n];
        let mut admitted = 0;
        loop {
            if admitted == max_prefills {
                break;
            }
            // Previously selected requests resume the gate chain in arrival order.
            let from_ready = self.ready.front().copied();
            let selected = if let Some(req) = from_ready {
                self.selected[&req]
            } else {
                self.policy.refresh_head(&mut |c| {
                    kv.resident_prefix_tokens(c.fresh_prompt_tokens, c.session_input)
                });
                let Some(candidate) = self.policy.peek() else {
                    break;
                };
                let partition = kv
                    .retained_prefix_partition(candidate.session_input)
                    .unwrap_or_else(|| self.balance.choose(n) as u16);
                let local = kv.preview_prefill_context(
                    partition,
                    candidate.fresh_prompt_tokens,
                    candidate.session_input,
                );
                if let Some(session) = candidate.session_input.session_id() {
                    let declared = candidate.session_input.declared_prefix_tokens();
                    // With no fresh suffix, the final cached token must still run.
                    let wanted =
                        declared.saturating_sub(u32::from(candidate.fresh_prompt_tokens == 0));
                    if local.resident_prefix_tokens() < wanted {
                        if self.selected.len() >= self.max_reads {
                            break;
                        }
                        self.policy.pop(&mut self.policy_context);
                        self.selected.insert(
                            candidate.request_id,
                            Selected {
                                candidate,
                                partition,
                                read_tokens: 0,
                            },
                        );
                        self.lookups.push((candidate.request_id, session, declared));
                        continue;
                    }
                }
                Selected {
                    candidate,
                    partition,
                    read_tokens: 0,
                }
            };
            let c = selected.candidate;
            let partition = selected.partition;
            let resolved =
                kv.preview_prefill_context(partition, c.fresh_prompt_tokens, c.session_input);
            let computed = resolved.prefill_tokens_to_compute();
            if !prefill_fits_budget(
                self.budget,
                kv.live_decode_count(partition),
                used[partition as usize],
                computed,
            ) {
                break;
            }
            let footprint = kv.footprint(
                c.request_id,
                resolved.post_prefill_context_tokens(),
                c.remaining_output_tokens,
            );
            if !kv.fits(partition, &footprint) {
                break;
            }
            if from_ready.is_some() {
                self.ready.pop_front();
                self.selected.remove(&c.request_id);
            } else {
                self.policy.pop(&mut self.policy_context);
            }
            let hit = resolved.resident_prefix_tokens();
            let remote = kv.remote_hit_tokens(c.request_id, partition, c.session_input, hit);
            self.reuse.push(crate::log::rows::KvStoreEventEntry {
                time_ms: now.as_ms(),
                request_id: c.request_id.0,
                session_id: c.session_input.session_id().unwrap_or_default(),
                operation: "prefix-reuse",
                reason: "admission",
                local_hit_tokens: Some(u64::from(hit - remote)),
                remote_hit_tokens: Some(u64::from(remote)),
                ..Default::default()
            });
            kv.reserve_prefill_context(c.request_id, partition, resolved, footprint, now);
            admitted += 1;
            used[partition as usize] = used[partition as usize]
                .checked_add(computed)
                .expect("prefill budget overflow");
            let mut requests = context.requests.borrow_mut();
            requests.mark_admitted(c.request_id);
            let record = &mut requests[c.request_id];
            record.record_prefix_cache_hit_tokens(resolved.resident_prefix_tokens());
            context.stamp_stage(record, now, stage);
        }
        kv.drain_ready();
        (0..n as u16).any(|p| kv.has_live_decode(p) || kv.has_prefill_admit(p))
    }
}
