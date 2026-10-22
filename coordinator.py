from dataclasses import dataclass
from .config import MAX_VALUE_BYTES
from .errors import NodeDown, QuorumNotReached
from .vector_clock import Version, reconcile, merge_clocks


@dataclass
class GetResult:
    values: list
    context: dict    # give back to put() to overwrite what you read
    siblings: bool


class Coordinator:
    # Runs on whichever node the client talked to
    def __init__(self, node):
        self.node = node
        self.cluster = node.cluster

    # ---------- write path
    def write_targets(self, key):
        # returns [(node, hint_for)]
        c = self.cluster
        intended = c.ring.preference_list(key, c.N)
        if not c.sloppy:
            return [(n, None) for n in intended]   # strict quorum

        # sloppy quorum: stand-ins come from the rest of the ring
        standins = [n for n in c.ring.walk(key) if n not in intended]
        targets = []
        for n in intended:
            if self.node.is_alive(n):
                targets.append((n, None))
            else:
                for s in standins:
                    if self.node.is_alive(s):
                        standins.remove(s)
                        targets.append((s, n))   # s holds a hint for n
                        break
        return targets

    def put(self, key, value, context=None, w=None):
        c = self.cluster
        if len(value.encode()) > MAX_VALUE_BYTES:
            raise ValueError("value exceeds 10KB")
        w = w or c.W

        # new clock = old clock + 1 for this coordinator
        if context is None:
            context = merge_clocks(self.node.store.get(key, []))
        clock = dict(context)
        clock[self.node.id] = clock.get(self.node.id, 0) + 1
        version = Version(tuple(sorted(clock.items())), value, c.next_ts())

        acks = 0
        for target, hint_for in self.write_targets(key):
            try:
                c.call(self.node.id, target, "write", key, version, hint_for)
                acks += 1
            except NodeDown:
                pass

        if acks < w:
            raise QuorumNotReached(f"put({key}): {acks} acks < W={w}")
        return clock

    # ---------- read path
    def get(self, key, r=None, resolve="siblings"):
        c = self.cluster
        r = r or c.R

        # ask replicas
        replies = {}
        for n in c.ring.preference_list(key, c.N):
            if not self.node.is_alive(n):
                continue
            try:
                replies[n] = c.call(self.node.id, n, "read", key)
            except NodeDown:
                pass
        if len(replies) < r:
            raise QuorumNotReached(f"get({key}): {len(replies)} replies < R={r}")

        # reconcile what came back
        all_versions = []
        for versions in replies.values():
            all_versions += versions
        merged = reconcile(all_versions)
        if not merged:
            return GetResult([], {}, False)

        self.read_repair(key, replies, merged)

        context = merge_clocks(merged)
        if resolve == "lww" and len(merged) > 1:
            newest = max(merged, key=lambda v: v.ts)
            return GetResult([newest.value], context, False)
        return GetResult([v.value for v in merged], context, len(merged) > 1)

    def read_repair(self, key, replies, merged):
        # fix replicas that returned stale data
        for n, versions in replies.items():
            if sorted(versions, key=lambda v: v.clock) != merged:
                try:
                    self.cluster.call(self.node.id, n, "merge", key, merged)
                except NodeDown:
                    pass
