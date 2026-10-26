//! `DecodeCore<K, E>` — the decode half of a PD deployment.
//!
//! The shell owns two independent timelines: a serialized KV-pull pipeline and
//! a whole-iteration decode FSM. It does not own KV membership or model input
//! shape: `K` owns resident/request-partition accounting, while `E` builds and
//! evaluates the opaque iteration input. A landed handoff waits in
//! `pending_decodes`; at most one transfer is in flight, and landed plus
//! in-transit tokens share one pull-backlog budget. Completion acknowledges the
//! prefill worker only after KV has landed.
//!
//! Reading order: types → construction → message handling (the `IterWorker`
//! entry points) → the pull/decode tick loop → helpers in call order → tests.

use std::collections::VecDeque;

use crate::common::{PdStage, RequestId, Time, WorkerId};
use crate::worker::admission::LoadBalance;
use crate::worker::execution::IterModelExecution;
use crate::worker::gpu_cluster::SharedGpuCluster;
use crate::worker::kv::IterWorkerKv;
use crate::worker::shared::advance_scope::AdvanceScope;
use crate::worker::shared::context::WorkerContext;
use crate::worker::types::{
    BatchFsmState, IterCursor, PdDecodeEvent, TransferPlan, WorkerFsmState, WorkerStatus,
};

/// Pull backlog ceiling as a fraction of one attention partition's token capacity.
pub(crate) const PULL_BUDGET_FRACTION: f64 = 0.05;

#[derive(Clone, Copy, Debug)]
pub(super) struct InFlightPull {
    pub(super) request: RequestId,
    pull_end: Time,
    prefill_worker: WorkerId,
    pub(super) tokens: u64,
}

pub(super) struct PullPipeline {
    /// KV is resident locally but has not entered a decode partition.
    pending_decodes: VecDeque<(RequestId, u64)>,
    /// O(1) token sum paired with every `pending_decodes` push/pop.
    pending_decode_tokens: u64,
    /// Handoffs not yet submitted; these tokens do not occupy local KV.
    pub(super) pending_pulls: VecDeque<(RequestId, TransferPlan)>,
    /// The one serialized cluster transfer currently in flight.
    pub(super) in_flight: Option<InFlightPull>,
    pull_budget_tokens: u64,
    cluster: SharedGpuCluster,
    receive_group_id: u16,
    kv_accounting: crate::worker::kv::SharedKvAccounting,
    transfer_kind: &'static str,
}

struct DecodeIterationFsm {
    worker_state: WorkerFsmState,
    batch_state: BatchFsmState,
    iteration: u32,
}

pub struct DecodeCore<K, E>
where
    K: IterWorkerKv,
    E: IterModelExecution<K>,
{
    pub(crate) context: WorkerContext,
    pub(crate) kv_store: K,
    execution: E,
    input: E::Input,
    batch_plan: crate::worker::types::IterBatchPlan,
    balance: LoadBalance,
    pub(super) pull_pipeline: PullPipeline,
    decode_fsm: DecodeIterationFsm,
}

impl<K, E> DecodeCore<K, E>
where
    K: IterWorkerKv,
    E: IterModelExecution<K>,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_components(
        context: WorkerContext,
        kv_store: K,
        execution: E,
        balance: LoadBalance,
        cluster: SharedGpuCluster,
        receive_group_id: u16,
        pull_budget_tokens: u64,
        kv_accounting: crate::worker::kv::SharedKvAccounting,
    ) -> Self {
        Self {
            context,
            kv_store,
            execution,
            input: Default::default(),
            batch_plan: Default::default(),
            balance,
            pull_pipeline: PullPipeline {
                pending_decodes: VecDeque::new(),
                pending_decode_tokens: 0,
                pending_pulls: VecDeque::new(),
                in_flight: None,
                pull_budget_tokens,
                cluster,
                receive_group_id,
                kv_accounting,
                transfer_kind: "pd_kv_pull",
            },
            decode_fsm: DecodeIterationFsm {
                worker_state: WorkerFsmState::Idle,
                batch_state: BatchFsmState {
                    cursor: IterCursor::Done,
                    compute_end: Time::ZERO,
                },
                iteration: 0,
            },
        }
    }
}

impl<K, E> DecodeCore<K, E>
where
    K: IterWorkerKv,
    E: IterModelExecution<K>,
{
    pub(crate) fn set_transfer_kind(&mut self, kind: &'static str) {
        self.pull_pipeline.transfer_kind = kind;
    }

    /// Direct-admit test path: the request's prompt KV is already resident.
    pub(crate) fn on_msg_request(&mut self, request: RequestId) {
        let tokens = {
            let store = self.context.requests.borrow();
            let record = &store[request];
            record.request.definition.prompt_tokens as u64
        };
        self.pull_pipeline
            .pending_decodes
            .push_back((request, tokens));
        self.pull_pipeline.pending_decode_tokens += tokens;

        let mut store = self.context.requests.borrow_mut();
        let arrival = store[request].request.core.arrival_time;
        self.context
            .stamp_stage(&mut store[request], arrival, PdStage::PendingDecode as u16);
    }

    /// Queue a prefill handoff for serialized transfer submission.
    pub(crate) fn on_msg_handoff(
        &mut self,
        request: RequestId,
        send_group_id: u16,
        tokens: u64,
        prefill_worker: WorkerId,
    ) {
        self.pull_pipeline.pending_pulls.push_back((
            request,
            TransferPlan {
                send_gid: send_group_id,
                recv_gid: self.pull_pipeline.receive_group_id,
                tokens,
                prefill_worker,
            },
        ));
    }

    /// Advance compute until it blocks or reaches the terminal ownership boundary.
    /// The caller must release/hold returned requests before calling again.
    pub(crate) fn advance_decode(&mut self, now: Time) -> Option<Vec<(RequestId, u16)>> {
        use IterCursor::{Computing, Done, NotStarted};
        use WorkerFsmState::{Active, Idle};
        loop {
            let keep_going = match self.decode_fsm.worker_state {
                Idle => self.tick_idle(now),
                Active => match self.decode_fsm.batch_state.cursor {
                    NotStarted => self.tick_not_started(now),
                    Computing => self.tick_computing(now),
                    Done => {
                        let completed = self.complete_iteration(now);
                        self.decode_fsm.worker_state = Idle;
                        return Some(completed);
                    }
                },
            };
            if !keep_going {
                return None;
            }
        }
    }

    pub(crate) fn status(&self) -> WorkerStatus {
        let active_requests = (0..self.kv_store.num_partitions() as u16)
            .map(|partition| self.kv_store.live_decode_count(partition))
            .sum();
        let queued_requests = self.pull_pipeline.pending_decodes.len()
            + self.pull_pipeline.pending_pulls.len()
            + usize::from(self.pull_pipeline.in_flight.is_some());
        WorkerStatus {
            queued_requests: queued_requests as u32,
            active_requests,
        }
    }

    /// Promote a landed pull, then submit the next handoff if the backlog permits.
    pub(crate) fn advance_pulls(&mut self, now: Time, events: &mut Vec<PdDecodeEvent>) {
        loop {
            if let Some(pull) = self.pull_pipeline.in_flight {
                if now < pull.pull_end {
                    return;
                }
                self.pull_pipeline
                    .pending_decodes
                    .push_back((pull.request, pull.tokens));
                self.pull_pipeline.pending_decode_tokens += pull.tokens;
                self.pull_pipeline.in_flight = None;

                {
                    let mut store = self.context.requests.borrow_mut();
                    self.context.stamp_stage(
                        &mut store[pull.request],
                        now,
                        PdStage::PendingDecode as u16,
                    );
                }
                events.push(PdDecodeEvent::PullComplete {
                    worker: self.context.id,
                    req: pull.request,
                    prefill_worker: pull.prefill_worker,
                });
            }

            let Some((_, next_transfer)) = self.pull_pipeline.pending_pulls.front() else {
                return;
            };
            let head_tokens = next_transfer.tokens;
            let backlog_tokens = self.pull_backlog_tokens();
            let fits_normally =
                backlog_tokens + head_tokens <= self.pull_pipeline.pull_budget_tokens;
            let single_request_exception =
                backlog_tokens == 0 && head_tokens > self.pull_pipeline.pull_budget_tokens;
            if !fits_normally && !single_request_exception {
                return;
            }

            let (request, transfer) = self.pull_pipeline.pending_pulls.pop_front().unwrap();
            let bytes = self
                .pull_pipeline
                .kv_accounting
                .transfer_bytes(transfer.tokens);
            // A standalone request in the remote composition still uses the
            // original direct GPU link, including its telemetry label.
            let kind = if self.context.requests.borrow()[request]
                .request
                .definition
                .session
                .session_id()
                .is_some()
            {
                self.pull_pipeline.transfer_kind
            } else {
                "pd_kv_pull"
            };
            let pull_end = self.pull_pipeline.cluster.borrow_mut().submit_transfer(
                now,
                transfer.send_gid,
                transfer.recv_gid,
                bytes,
                kind,
                &if kind == "kv_store_read" {
                    request.0.to_string()
                } else {
                    String::new()
                },
            );
            self.pull_pipeline.in_flight = Some(InFlightPull {
                request,
                pull_end,
                prefill_worker: transfer.prefill_worker,
                tokens: transfer.tokens,
            });

            let mut store = self.context.requests.borrow_mut();
            self.context
                .stamp_stage(&mut store[request], now, PdStage::Transfer as u16);
        }
    }

    #[inline]
    fn pull_backlog_tokens(&self) -> u64 {
        self.pull_pipeline.pending_decode_tokens
            + self
                .pull_pipeline
                .in_flight
                .map(|pull| pull.tokens)
                .unwrap_or(0)
    }

    fn tick_idle(&mut self, now: Time) -> bool {
        if !self.form_batch(now) {
            return false;
        }
        self.decode_fsm.worker_state = WorkerFsmState::Active;
        self.decode_fsm.batch_state.cursor = IterCursor::NotStarted;
        self.decode_fsm.batch_state.compute_end = Time::ZERO;
        true
    }

    /// Admit at most one landed request directly into its chosen decode partition.
    fn form_batch(&mut self, now: Time) -> bool {
        let num_partitions = self.kv_store.num_partitions();
        let had_decode =
            (0..num_partitions as u16).any(|partition| self.kv_store.has_live_decode(partition));

        if let Some(&(request, prompt_kv)) = self.pull_pipeline.pending_decodes.front() {
            let remaining = {
                let store = self.context.requests.borrow();
                let record = &store[request];
                record
                    .request
                    .definition
                    .target_output_tokens
                    .saturating_sub(record.progress.output_tokens_emitted)
            };
            let partition = self.balance.choose(num_partitions) as u16;
            let footprint = self
                .kv_store
                .footprint(request, prompt_kv as u32, remaining);
            if remaining == 0 || self.kv_store.fits(partition, &footprint) {
                self.pull_pipeline.pending_decodes.pop_front();
                self.pull_pipeline.pending_decode_tokens = self
                    .pull_pipeline
                    .pending_decode_tokens
                    .saturating_sub(prompt_kv);
                // `reserve` makes the KV partition sticky; `commit_resident`
                // immediately clears the transient promise and enters decode.
                self.kv_store.reserve(request, partition, footprint, now);
                self.kv_store
                    .commit_resident(request, partition, prompt_kv, remaining);

                let mut store = self.context.requests.borrow_mut();
                self.context
                    .stamp_stage(&mut store[request], now, PdStage::Decode as u16);
            }
        }

        let still_decoding =
            (0..num_partitions as u16).any(|partition| self.kv_store.has_live_decode(partition));
        if !had_decode && !still_decoding {
            return false;
        }
        self.decode_fsm.iteration += 1;
        true
    }

    fn tick_not_started(&mut self, now: Time) -> bool {
        let compute_end = self.start_iteration(now);
        self.decode_fsm.batch_state.cursor = IterCursor::Computing;
        self.decode_fsm.batch_state.compute_end = compute_end;
        false
    }

    fn start_iteration(&mut self, now: Time) -> Time {
        self.batch_plan
            .reset_decode_participation(self.kv_store.num_partitions(), true);
        self.execution.build_iteration_input(
            &self.kv_store,
            &self.context.requests,
            &self.batch_plan,
            &mut self.input,
        );
        let cost =
            self.execution
                .evaluate_iteration(&self.input, self.decode_fsm.iteration as u64, now);
        now + cost
    }

    fn tick_computing(&mut self, now: Time) -> bool {
        if now < self.decode_fsm.batch_state.compute_end {
            return false;
        }
        self.decode_fsm.batch_state.cursor = IterCursor::Done;
        true
    }

    fn complete_iteration(&mut self, now: Time) -> Vec<(RequestId, u16)> {
        let mut terminal = Vec::new();
        for partition in 0..self.kv_store.num_partitions() as u16 {
            let mut completed = Vec::new();
            {
                let mut store = self.context.requests.borrow_mut();
                self.kv_store.visit_decode_members(partition, |request, _| {
                    let record = &mut store[request];
                    record.record_token(now, self.context.log_tokens());
                    if record.is_complete() {
                        self.context.stamp_stage(record, now, PdStage::Done as u16);
                        completed.push(request);
                    }
                });
            }

            self.kv_store
                .advance(AdvanceScope::WholePartition(partition), 1);

            terminal.extend(completed.into_iter().map(|req| (req, partition)));
        }
        terminal
    }

    pub(crate) fn sample_partitions(&mut self, now: Time) {
        for partition in 0..self.kv_store.num_partitions() as u16 {
            self.kv_store.sample_submit(partition, now);
        }
    }

    pub(crate) fn next_wakeup(&self, now: Time) -> Option<Time> {
        let pull_wakeup = self.pull_pipeline.in_flight.map(|pull| pull.pull_end);
        let compute_wakeup = match self.decode_fsm.worker_state {
            WorkerFsmState::Idle => {
                if self.pull_pipeline.pending_decodes.is_empty() {
                    None
                } else {
                    Some(now)
                }
            }
            WorkerFsmState::Active => match self.decode_fsm.batch_state.cursor {
                IterCursor::NotStarted | IterCursor::Done => Some(now),
                IterCursor::Computing => Some(self.decode_fsm.batch_state.compute_end),
            },
        };

        match (pull_wakeup, compute_wakeup) {
            (Some(pull), Some(compute)) => Some(pull.min(compute)),
            (Some(wakeup), None) | (None, Some(wakeup)) => Some(wakeup),
            (None, None) => None,
        }
    }

    /// Discard a remote cancellation's queued/in-flight arrival state.
    pub(crate) fn cancel_pull(&mut self, request: RequestId) {
        self.pull_pipeline
            .pending_pulls
            .retain(|(req, _)| *req != request);
        if self
            .pull_pipeline
            .in_flight
            .is_some_and(|pull| pull.request == request)
        {
            // Submitted network time is not rolled back; discard only arrival state.
            self.pull_pipeline.in_flight = None;
        }
    }

    /// Remove a request from the landed backlog or its sticky KV partition.
    pub fn release_request(&mut self, request: RequestId, current_kv: u64) -> Option<u16> {
        if let Some(position) = self
            .pull_pipeline
            .pending_decodes
            .iter()
            .position(|&(pending, _)| pending == request)
        {
            let (_, tokens) = self
                .pull_pipeline
                .pending_decodes
                .remove(position)
                .expect("position came from the same pending-decode queue");
            self.pull_pipeline.pending_decode_tokens = self
                .pull_pipeline
                .pending_decode_tokens
                .saturating_sub(tokens);
            return None;
        }
        self.kv_store.release_external(request, current_kv)
    }
}
