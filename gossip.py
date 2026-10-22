import random
from .config import FAIL_TICKS, GOSSIP_FANOUT
from .errors import NodeDown


class Gossip:
    # Failure detection via heartbeat counters
    def __init__(self, node):
        self.node = node
        self.cluster = node.cluster
        now = self.cluster.now
        self.members = {node.id: [0, now]}   # peer -> [heartbeat, tick last seen changing]

    def is_alive(self, peer):
        if peer == self.node.id:
            return True
        if peer not in self.members:
            return False
        last_seen = self.members[peer][1]
        return self.cluster.now - last_seen <= FAIL_TICKS

    def merge(self, theirs):
        # keep the higher heartbeat for each peer
        for peer, (beat, _) in theirs.items():
            if peer not in self.members or beat > self.members[peer][0]:
                self.members[peer] = [beat, self.cluster.now]

    def on_gossip(self, theirs):
        self.merge(theirs)
        return {p: list(v) for p, v in self.members.items()}

    def round(self):
        me = self.node.id
        self.members[me] = [self.members[me][0] + 1, self.cluster.now]   # bump own heartbeat

        peers = [p for p in self.members if p != me and p in self.cluster.nodes]
        for peer in random.sample(peers, min(GOSSIP_FANOUT, len(peers))):
            try:
                theirs = self.cluster.call(me, peer, "gossip", self.members)
                self.merge(theirs)
            except NodeDown:
                pass

        self.handoff_hints()

    def handoff_hints(self):
        # give stored hints back to nodes that came back
        hints = self.node.hints
        for target in list(hints):
            if target not in self.cluster.nodes or not self.is_alive(target):
                continue
            try:
                for key, versions in hints[target].items():
                    self.cluster.call(self.node.id, target, "merge", key, versions)
                del hints[target]
            except NodeDown:
                pass
