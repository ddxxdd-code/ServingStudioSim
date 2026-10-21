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
            cache: PrefixCache::new(capacity, policy, 1, 0),
            capacity,
            reserved: 0,
            incoming: HashMap::new(),
            pins: HashMap::new(),
        }
    }

    pub(super) fn used(&self) -> u64 {
        self.cache.used_charge() + self.reserved
    }
    pub(super) fn cached(&self) -> u64 {
        self.cache.used_charge()
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
        let result = self
            .cache
            .insert(session, tokens, self.capacity - self.reserved, None);
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

    pub(super) fn consumed(&mut self, session: u32, req: RequestId) {
        if self.pins.get(&session) == Some(&req) {
            self.pins.remove(&session);
            self.cache.set_pinned(session, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
