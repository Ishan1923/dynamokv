from .config import MERKLE_DEPTH
from .hashing import h_int, h_hex

LEAVES = 2 ** MERKLE_DEPTH


def bucket(key):
    # which leaf a key belongs to
    return h_int(key) % LEAVES


def key_digest(versions):
    # fingerprint of one key's data
    return h_hex(repr(sorted((v.clock, v.value) for v in versions)))


class MerkleTree:
    def __init__(self, digests):
        # digests: {key: digest}
        buckets = [[] for _ in range(LEAVES)]
        for key in sorted(digests):
            buckets[bucket(key)].append(key + digests[key])

        level = [h_hex("".join(b)) for b in buckets]   # leaf hashes
        levels = [level]
        while len(level) > 1:                           # hash pairs upward
            level = [h_hex(level[i] + level[i + 1]) for i in range(0, len(level), 2)]
            levels.append(level)
        self.levels = levels[::-1]                      # levels[0] = [root]

    def root(self):
        return self.levels[0][0]

    def diff(self, other):
        # leaf buckets that differ, only descending into mismatched subtrees
        bad = []
        todo = [(0, 0)]   # (level, index)
        last = len(self.levels) - 1
        while todo:
            level, i = todo.pop()
            if self.levels[level][i] == other.levels[level][i]:
                continue
            if level == last:
                bad.append(i)
            else:
                todo.append((level + 1, 2 * i))
                todo.append((level + 1, 2 * i + 1))
        return sorted(bad)
