from .config import VNODES
from .errors import NodeDown, QuorumNotReached
from .ring import Ring
from .node import Node
from . import repair


class Cluster:
    # Test harness: fake network + failure injection + client API.
    # In production this doesn't exist: nodes just talk over TCP.
    def __init__(self, N=3, R=2, W=2, sloppy=True, vnodes=VNODES):
        self.N, self.R, self.W = N, R, W
        self.sloppy = sloppy          # True = AP, False = CP-leaning
        self.ring = Ring(vnodes)
        self.nodes = {}
        self.now = 0                  # gossip clock
        self._ts = 0

    def next_ts(self):
        self._ts += 1
        return self._ts

    # ---------- fake network
    def call(self, src, dst, method, *args):
        node = self.nodes.get(dst)
        if node is None or not node.up:
            raise NodeDown(dst)
        return getattr(node, "rpc_" + method)(*args)

    # ---------- membership / scaling
    def add_node(self, node_id, dc, rebalance=True):
        node = Node(node_id, dc, self)
        if self.nodes:   # learn peers from any existing node
            seed = next(iter(self.nodes.values()))
            for peer, (beat, _) in seed.members.items():
                node.members[peer] = [beat, self.now]
        self.nodes[node_id] = node
        self.ring.add(node_id, dc)
        if rebalance and len(self.nodes) > 1:
            return repair.rebalance(self)
        return 0

    def remove_node(self, node_id, rebalance=True):
        # permanent failure: drop from ring, re-replicate from survivors
        self.ring.remove(node_id)
        self.nodes.pop(node_id, None)
        for node in self.nodes.values():
            node.members.pop(node_id, None)
            node.hints.pop(node_id, None)
        return repair.rebalance(self) if rebalance else 0

    def rebalance(self):
        return repair.rebalance(self)

    def anti_entropy(self):
        return repair.anti_entropy(self)

    # ---------- failure injection
    def kill(self, node_id):
        self.nodes[node_id].up = False

    def revive(self, node_id):
        node = self.nodes[node_id]
        node.up = True
        for m in node.members.values():   # avoid false "all dead" view
            m[1] = self.now

    def kill_dc(self, dc):
        for node in self.nodes.values():
            if node.dc == dc:
                node.up = False

    def revive_dc(self, dc):
        for node in self.nodes.values():
            if node.dc == dc:
                self.revive(node.id)

    def tick(self, rounds=1):
        # advance time, every live node gossips once per tick
        for _ in range(rounds):
            self.now += 1
            for node in list(self.nodes.values()):
                if node.up:
                    node.gossip_round()

    # ---------- client API
    def pick_coordinator(self, key, prefer=None):
        if prefer:
            return self.nodes[prefer]
        for node_id in self.ring.preference_list(key, self.N):
            if self.nodes[node_id].up:
                return self.nodes[node_id]
        for node in self.nodes.values():
            if node.up:
                return node
        raise QuorumNotReached("cluster down")

    def put(self, key, value, context=None, w=None, coordinator=None):
        node = self.pick_coordinator(key, coordinator)
        return node.coordinator.put(key, value, context, w)

    def get(self, key, r=None, resolve="siblings", coordinator=None):
        node = self.pick_coordinator(key, coordinator)
        return node.coordinator.get(key, r, resolve)

