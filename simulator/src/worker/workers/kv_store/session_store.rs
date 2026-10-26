//! Session storage and incoming capacity reservations, with one cache ledger.
//! Reservations consume space before transfer submission. A relay pin carries
//! its request identity so a late cancellation cannot unpin a later relay.

use crate::common::RequestId;
use crate::worker::kv::{PrefixCache, PrefixCacheMutation, PrefixCachePolicy};
use crate::worker::types::KvRetention;
use std::collections::HashMap;

pub(super) struct SessionStore {
    cache: PrefixCache,
    capacity: u64,
    reserved: u64,
    incoming: HashMap<u32, (RequestId, u64)>,
    pins: HashMap<u32, RequestId>,
}

impl SessionStore {
    pub(super) fn new(capacity: u64, policy: PrefixCachePolicy) -> Self {
        Self {
            cache: PrefixCache::new(
                std::sync::Arc::new(
                    crate::worker::kv::KvAccounting::resolve(
                        &crate::arch::kv_layout::KvCacheLayout::new()
                            .with_part(crate::arch::kv_layout::KvCachePart::per_token("remote", 1)),
                        crate::worker::kv::KvAccountingOptions::default(),
                        capacity,
                    )
                    .expect("linear remote storage accounting"),
                ),
                crate::worker::kv::PrefixCacheBudget {
                    max_retained_units: capacity,
                    policy,
                },
            ),
            capacity,
            reserved: 0,
            incoming: HashMap::new(),
            pins: HashMap::new(),
        }
    }

    pub(super) fn used(&self) -> u64 {
        self.cache.used_units() + self.reserved
    }
    pub(super) fn reserved(&self) -> u64 {
        self.reserved
    }
    pub(super) fn pinned(&self) -> u64 {
        self.cache.pinned_charge()
    }
    pub(super) fn cached(&self) -> u64 {
        self.cache.used_units()
    }
    pub(super) fn capacity(&self) -> u64 {
        self.capacity
    }
    pub(super) fn available_for_pin(&self) -> u64 {
        self.capacity
            .saturating_sub(self.cache.pinned_charge() + self.reserved)
    }
    pub(super) fn busy(&self, session: u32) -> bool {
        self.incoming.contains_key(&session) || self.cache.is_pinned(session)
    }
    pub(super) fn copy(&mut self, session: u32, requested: u32) -> u32 {
        self.cache.copy(session, requested)
    }

    /// Return the accepted length and exact evictions, or reject before any bytes move.
    pub(super) fn reserve(
        &mut self,
        session: u32,
        req: RequestId,
        tokens: u64,
        retention: KvRetention,
    ) -> Option<(u64, Vec<PrefixCacheMutation>)> {
        assert!(
            !self.busy(session),
            "serialize same-session writes before reserving"
        );
        let available = self.available_for_pin();
        if retention == KvRetention::PinnedUntilFetched && tokens > available {
            return None;
        }
        let accepted = tokens.min(available);
        self.reserved += accepted;
        self.incoming.insert(session, (req, accepted));
        let mutations = self.cache.shrink_to(self.capacity - self.reserved);
        Some((accepted, mutations))
    }

    pub(super) fn land(
        &mut self,
        session: u32,
        req: RequestId,
        retention: KvRetention,
    ) -> (u64, Vec<PrefixCacheMutation>) {
        let (owner, tokens) = self.incoming.remove(&session).expect("reserved push");
        assert_eq!(owner, req);
        self.reserved -= tokens;
        let result =
            self.cache
                .insert(session, tokens, tokens, self.capacity - self.reserved, None);
        assert_eq!(
            result.retained_tokens, tokens,
            "reserved bytes must fit on arrival"
        );
        if retention == KvRetention::PinnedUntilFetched {
            self.cache.set_pinned(session, true);
            self.pins.insert(session, req);
        }
        (tokens, result.mutations)
    }

    pub(super) fn consumed(&mut self, session: u32, req: RequestId) -> bool {
        if self.pins.get(&session) == Some(&req) {
            self.pins.remove(&session);
            self.cache.set_pinned(session, false);
            return true;
        }
        false
    }

    pub(super) fn cancel(&mut self, session: u32, req: RequestId) {
        if self
            .incoming
            .get(&session)
            .is_some_and(|(owner, _)| *owner == req)
        {
            let (_, tokens) = self.incoming.remove(&session).unwrap();
            self.reserved -= tokens;
        }
        self.consumed(session, req);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn out_of_order_landings_preserve_reserved_plus_resident_capacity() {
        let mut store = SessionStore::new(100, PrefixCachePolicy::Lru);
        store
            .reserve(7, RequestId(1), 60, KvRetention::Shared)
            .unwrap();
        store
            .reserve(8, RequestId(2), 40, KvRetention::Shared)
            .unwrap();
        assert_eq!(
            (store.used(), store.reserved(), store.cached()),
            (100, 100, 0)
        );
        assert_eq!(store.copy(7, 100), 0);
        assert_eq!(store.copy(8, 100), 0);
        assert_eq!(store.land(8, RequestId(2), KvRetention::Shared).0, 40);
        assert_eq!(
            (store.used(), store.reserved(), store.cached()),
            (100, 60, 40)
        );
        assert_eq!(store.land(7, RequestId(1), KvRetention::Shared).0, 60);
        assert_eq!(
            (store.used(), store.reserved(), store.cached()),
            (100, 0, 100)
        );
        assert_eq!(store.copy(7, 45), 45);
        assert_eq!(store.copy(7, 100), 60);
        assert_eq!(store.copy(8, 100), 40);
    }

    #[test]
    fn oversized_pin_rejection_keeps_existing_shared_entries_and_receipts_unchanged() {
        let mut store = SessionStore::new(100, PrefixCachePolicy::Lru);
        store
            .reserve(7, RequestId(1), 80, KvRetention::Shared)
            .unwrap();
        store.land(7, RequestId(1), KvRetention::Shared);
        assert!(store
            .reserve(8, RequestId(2), 101, KvRetention::PinnedUntilFetched)
            .is_none());
        assert_eq!((store.used(), store.reserved(), store.pinned()), (80, 0, 0));
        assert!(!store.busy(8));
        assert_eq!(store.copy(7, 100), 80);
        let (accepted, evictions) = store
            .reserve(8, RequestId(3), 101, KvRetention::Shared)
            .unwrap();
        assert_eq!(accepted, 100);
        assert_eq!(evictions.len(), 1);
        assert_eq!(
            (
                evictions[0].session_id,
                evictions[0].entry_tokens_before,
                evictions[0].cache_used_before,
                evictions[0].cache_used_after
            ),
            (7, 80, 80, 0)
        );
        store.land(8, RequestId(3), KvRetention::Shared);
        assert_eq!(store.copy(7, 100), 0);
        assert_eq!(store.copy(8, 200), 100);
    }

    #[test]
    fn full_pin_allows_zero_length_shared_write_without_evicting_relay() {
        let mut store = SessionStore::new(100, PrefixCachePolicy::Lru);
        store
            .reserve(7, RequestId(1), 100, KvRetention::PinnedUntilFetched)
            .unwrap();
        store.land(7, RequestId(1), KvRetention::PinnedUntilFetched);
        let (accepted, evictions) = store
            .reserve(8, RequestId(2), 50, KvRetention::Shared)
            .unwrap();
        assert_eq!(accepted, 0);
        assert_eq!(evictions.len(), 0);
        assert!(store.busy(8));
        assert_eq!(store.land(8, RequestId(2), KvRetention::Shared).0, 0);
        assert!(!store.busy(8));
        assert_eq!(
            (store.used(), store.pinned(), store.reserved()),
            (100, 100, 0)
        );
        assert_eq!(store.copy(7, 100), 100);
        assert_eq!(store.copy(8, 100), 0);
        assert!(!store.consumed(7, RequestId(99)));
        assert_eq!(store.pinned(), 100);
        assert!(store.consumed(7, RequestId(1)));
        assert_eq!(store.available_for_pin(), 100);
    }

    #[test]
    fn lru_shared_copies_and_fifo_reads_choose_different_capacity_victims() {
        for (policy, victim, survivor) in [
            (PrefixCachePolicy::Lru, 8, 7),
            (PrefixCachePolicy::Fifo, 7, 8),
        ] {
            let mut store = SessionStore::new(100, policy);
            for (session, req) in [(7, RequestId(1)), (8, RequestId(2))] {
                store
                    .reserve(session, req, 40, KvRetention::Shared)
                    .unwrap();
                store.land(session, req, KvRetention::Shared);
            }
            assert_eq!(store.copy(7, 20), 20);
            assert_eq!(store.copy(7, 100), 40);
            let (accepted, evictions) = store
                .reserve(9, RequestId(3), 50, KvRetention::Shared)
                .unwrap();
            assert_eq!(accepted, 50);
            assert_eq!(evictions.len(), 1);
            assert_eq!(evictions[0].session_id, victim);
            assert_eq!(
                (store.cached(), store.reserved(), store.used()),
                (40, 50, 90)
            );
            store.land(9, RequestId(3), KvRetention::Shared);
            assert_eq!(store.copy(victim, 100), 0);
            assert_eq!(store.copy(survivor, 100), 40);
            assert_eq!(store.copy(9, 100), 50);
        }
    }

    fn allocator_observation(
        operation: &str,
        requested: u64,
        accepted: u64,
        success: bool,
        store: &SessionStore,
    ) -> serde_json::Value {
        serde_json::json!({
            "operation": operation,
            "requested": requested,
            "accepted": accepted,
            "success": success,
            "used": store.used(),
            "free": store.available_for_pin(),
        })
    }

    #[test]
    fn pinned_reservations_exactly_match_the_pinned_kvcached_allocator_run() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/kv_storage_external_oracles.json"
        )))
        .expect("external KV oracle fixture must be valid JSON");
        let capacity = fixture["kvcached"]["capacity"].as_u64().unwrap();
        let mut store = SessionStore::new(capacity, PrefixCachePolicy::Lru);
        let mut actual = Vec::new();

        let first = store.reserve(1, RequestId(1), 5, KvRetention::PinnedUntilFetched);
        actual.push(allocator_observation(
            "allocate",
            5,
            first.as_ref().map_or(0, |(tokens, _)| *tokens),
            first.is_some(),
            &store,
        ));
        let rejected = store.reserve(2, RequestId(2), 4, KvRetention::PinnedUntilFetched);
        actual.push(allocator_observation(
            "allocate",
            4,
            rejected.as_ref().map_or(0, |(tokens, _)| *tokens),
            rejected.is_some(),
            &store,
        ));
        store.cancel(1, RequestId(1));
        actual.push(allocator_observation("release", 5, 5, true, &store));
        let full = store.reserve(2, RequestId(2), capacity, KvRetention::PinnedUntilFetched);
        actual.push(allocator_observation(
            "allocate",
            capacity,
            full.as_ref().map_or(0, |(tokens, _)| *tokens),
            full.is_some(),
            &store,
        ));

        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            fixture["kvcached"]["observations"]
        );
    }

    #[test]
    fn cancellation_releases_reservation_and_stale_ids_preserve_new_pin() {
        let mut store = SessionStore::new(100, PrefixCachePolicy::Lru);
        store
            .reserve(1, RequestId(1), 90, KvRetention::PinnedUntilFetched)
            .unwrap();
        store.cancel(1, RequestId(2));
        assert_eq!(store.used(), 90);
        store.cancel(1, RequestId(1));
        assert_eq!(store.used(), 0);
        store
            .reserve(1, RequestId(2), 100, KvRetention::PinnedUntilFetched)
            .unwrap();
        store.land(1, RequestId(2), KvRetention::PinnedUntilFetched);
        assert!(!store.consumed(1, RequestId(1)));
        store.cancel(1, RequestId(1));
        assert!(store.busy(1));
        assert!(store.consumed(1, RequestId(2)));
        assert!(!store.consumed(1, RequestId(2)));
        assert_eq!(store.available_for_pin(), 100);
    }

    #[test]
    fn concurrent_pushes_reserve_capacity_and_pins_survive_pressure() {
        let mut store = SessionStore::new(100, PrefixCachePolicy::Lru);
        store
            .reserve(1, RequestId(1), 70, KvRetention::PinnedUntilFetched)
            .unwrap();
        assert!(store
            .reserve(2, RequestId(2), 40, KvRetention::PinnedUntilFetched)
            .is_none());
        store
            .reserve(2, RequestId(2), 40, KvRetention::Shared)
            .unwrap();
        assert_eq!(store.used(), 100);
        store.land(1, RequestId(1), KvRetention::PinnedUntilFetched);
        store.land(2, RequestId(2), KvRetention::Shared);
        assert_eq!(store.copy(1, 100), 70);
        assert_eq!(store.copy(1, 100), 70);
        assert_eq!(store.copy(2, 100), 30);
        store.consumed(1, RequestId(99));
        assert!(store.busy(1));
        store.consumed(1, RequestId(1));
        assert!(!store.busy(1));
        store
            .reserve(3, RequestId(3), 100, KvRetention::Shared)
            .unwrap();
        assert_eq!(store.copy(1, 100), 0);
        assert_eq!(store.used(), 100);
    }
}
