from .merkle import MerkleTree, bucket, key_digest
from .vector_clock import reconcile


def both_own(cluster, key, a, b):
    replicas = cluster.ring.preference_list(key, cluster.N)
    return a.id in replicas and b.id in replicas


def shared_digests(cluster, x, y):
    # digests of keys that x and y should both hold
    out = {}
    for key, versions in x.store.items():
        if both_own(cluster, key, x, y):
            out[key] = key_digest(versions)
    return out


def sync_pair(cluster, a, b, stats):
    tree_a = MerkleTree(shared_digests(cluster, a, b))
    tree_b = MerkleTree(shared_digests(cluster, b, a))

    if tree_a.root() == tree_b.root():   # one compare = in sync
        stats["pairs_in_sync"] += 1
        return

    bad_buckets = set(tree_a.diff(tree_b))
    stats["buckets_diff"] += len(bad_buckets)

    # sync only keys in the differing buckets
    for key in set(a.store) | set(b.store):
        if bucket(key) in bad_buckets and both_own(cluster, key, a, b):
            merged = reconcile(a.store.get(key, []) + b.store.get(key, []))
            if a.store.get(key) != merged or b.store.get(key) != merged:
                a.store[key] = list(merged)
                b.store[key] = list(merged)
                stats["keys_synced"] += 1


def anti_entropy(cluster):
    # compare every pair of live nodes
    stats = {"pairs": 0, "pairs_in_sync": 0, "buckets_diff": 0, "keys_synced": 0}
    live = [n for n in cluster.nodes.values() if n.up]
    for i in range(len(live)):
        for j in range(i + 1, len(live)):
            stats["pairs"] += 1
            sync_pair(cluster, live[i], live[j], stats)
    return stats


def rebalance(cluster):
    # put every key exactly on its current replicas
    latest = {}   # key -> merged versions from all live nodes
    for node in cluster.nodes.values():
        if node.up:
            for key, versions in node.store.items():
                latest[key] = reconcile(latest.get(key, []) + versions)

    copied = 0
    for key, versions in latest.items():
        replicas = cluster.ring.preference_list(key, cluster.N)
        for node_id, node in cluster.nodes.items():
            if not node.up:
                continue
            if node_id in replicas:
                if node.store.get(key) != versions:
                    node.store[key] = list(versions)
                    copied += 1
            elif key in node.store:
                del node.store[key]   # no longer an owner
    return copied

