import bisect
from .hashing import h_int


class Ring:
    def __init__(self, vnodes):
        self.vnodes = vnodes
        self.points = []   # sorted (position, node_id)
        self.nodes = {}    # node_id -> dc

    def add(self, node_id, dc):
        self.nodes[node_id] = dc
        for i in range(self.vnodes):
            point = (h_int(f"{node_id}#{i}"), node_id)
            bisect.insort(self.points, point)

    def remove(self, node_id):
        self.nodes.pop(node_id, None)
        self.points = [p for p in self.points if p[1] != node_id]

    def walk(self, key):
        # distinct nodes, clockwise from the key's position
        start = bisect.bisect(self.points, (h_int(key), ""))
        order = []
        for i in range(len(self.points)):
            node_id = self.points[(start + i) % len(self.points)][1]
            if node_id not in order:
                order.append(node_id)
            if len(order) == len(self.nodes):
                break
        return order

    def preference_list(self, key, n):
        # n replicas, spread over different DCs first
        order = self.walk(key)
        chosen = []
        seen_dcs = set()
        for node_id in order:
            dc = self.nodes[node_id]
            if len(chosen) < n and dc not in seen_dcs:
                chosen.append(node_id)
                seen_dcs.add(dc)
        for node_id in order:
            if len(chosen) < n and node_id not in chosen:
                chosen.append(node_id)
        return chosen
