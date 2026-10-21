//! Dedicated CPU DRAM store recipe; byte geometry is supplied once by deployment.
use super::{session_store::SessionStore, KvStoreWorker};
use crate::common::{PoolId, WorkerId};
use crate::worker::gpu_cluster::SharedGpuCluster;
use crate::worker::kv::PrefixCachePolicy;

pub(crate) fn build_kv_store_worker(
    id: WorkerId,
    pool: PoolId,
    capacity_bytes: u64,
    bytes_per_token: u64,
    policy: PrefixCachePolicy,
    cluster: SharedGpuCluster,
) -> KvStoreWorker {
    assert!(bytes_per_token > 0);
    let capacity = capacity_bytes / bytes_per_token;
    assert!(capacity > 0, "store must hold at least one KV token");
    let group = {
        let mut cluster = cluster.borrow_mut();
        let base = cluster.allocate_host(pool.0, id.0, "CPU DRAM");
        cluster.register_kv_capacity("kv_store", pool.0, id.0, 0, capacity);
        cluster.register_comm_group(base, 1, "kv_store", id.0)
    };
    KvStoreWorker::new(
        id,
        SessionStore::new(capacity, policy),
        cluster,
        group,
        bytes_per_token,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{RequestId, Time};
    use crate::test_helpers::test_cluster;
    use crate::worker::iter_worker::{IterWorker, KvStoreLoad};
    use crate::worker::types::{KvRetention, KvStoreEvent, KvStoreMsg};
    #[test]
    fn stores_only_after_arrival_and_preserves_a_shared_hit() {
        let cluster = test_cluster();
        let gpu = cluster.borrow_mut().allocate(0, 0, 1, "test", "main");
        let send_gid = cluster.borrow_mut().register_comm_group(gpu, 1, "main", 0);
        let mut worker = build_kv_store_worker(
            WorkerId(0),
            PoolId(2),
            1000,
            10,
            PrefixCachePolicy::Lru,
            cluster.clone(),
        );
        assert_eq!(cluster.borrow().num_gpus(), 1);
        worker.enqueue(KvStoreMsg::Push {
            origin: WorkerId(0),
            req: RequestId(0),
            session: 7,
            tokens: 80,
            send_gid,
            retention: KvRetention::Shared,
        });
        let mut events = Vec::new();
        let arrival = worker.tick(Time::ZERO, &mut events).unwrap();
        assert!(events.is_empty());
        assert_eq!(worker.used_tokens(), 80);
        worker.tick(arrival, &mut events);
        assert!(matches!(
            events[0],
            KvStoreEvent::PushComplete { tokens: 80, .. }
        ));
        for _ in 0..2 {
            worker.enqueue(KvStoreMsg::Lookup {
                origin: WorkerId(0),
                req: RequestId(1),
                session: 7,
                requested_tokens: 60,
            });
            events.clear();
            worker.tick(arrival, &mut events);
            assert!(matches!(
                events[0],
                KvStoreEvent::LookupResult {
                    resident_tokens: 60,
                    ..
                }
            ));
        }
    }

    #[test]
    fn empty_same_session_writes_keep_waking_until_acknowledged() {
        let cluster = test_cluster();
        let mut worker = build_kv_store_worker(
            WorkerId(0),
            PoolId(2),
            10,
            1,
            PrefixCachePolicy::Lru,
            cluster,
        );
        for id in 0..2 {
            worker.enqueue(KvStoreMsg::Push {
                origin: WorkerId(0),
                req: RequestId(id),
                session: 1,
                tokens: 0,
                send_gid: 0,
                retention: KvRetention::Shared,
            });
        }
        let mut events = Vec::new();
        assert_eq!(worker.tick(Time::ZERO, &mut events), Some(Time::ZERO));
        assert_eq!(events.len(), 1);
        assert_eq!(worker.tick(Time::ZERO, &mut events), None);
        assert_eq!(events.len(), 2);
    }
}
