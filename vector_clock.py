from dataclasses import dataclass


def compare(a, b):
    # How does clock a relate to clock b?
    a_ge_b = all(a.get(n, 0) >= c for n, c in b.items())
    b_ge_a = all(b.get(n, 0) >= c for n, c in a.items())
    if a_ge_b and b_ge_a:
        return "equal"
    if a_ge_b:
        return "after"       # a saw everything b saw
    if b_ge_a:
        return "before"
    return "concurrent"      # neither saw the other


@dataclass(frozen=True)
class Version:
    clock: tuple   # sorted ((node_id, counter), ...)
    value: str
    ts: int        # only used for last-write-wins

    @property
    def vc(self):
        return dict(self.clock)


def merge_clocks(versions):
    # Per-node max across versions
    merged = {}
    for v in versions:
        for node, counter in v.clock:
            merged[node] = max(merged.get(node, 0), counter)
    return merged


def reconcile(versions):
    # 1. remove duplicates (same clock)
    unique = {}
    for v in versions:
        old = unique.get(v.clock)
        if old is None or v.ts > old.ts:
            unique[v.clock] = v
    candidates = list(unique.values())

    # 2. drop versions that a newer version supersedes
    keep = []
    for v in candidates:
        superseded = False
        for other in candidates:
            if compare(other.vc, v.vc) == "after":
                superseded = True
        if not superseded:
            keep.append(v)

    # what remains are siblings (concurrent versions)
    return sorted(keep, key=lambda v: v.clock)
