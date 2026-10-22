from .storage import Storage
from .gossip import Gossip
from .coordinator import Coordinator


class Node:
    # One server = storage + gossip + coordinator
    def __init__(self, node_id, dc, cluster):
        self.id = node_id
        self.dc = dc
        self.cluster = cluster
        self.up = True

        self.storage = Storage()
        self.store = self.storage.data      # shortcuts
        self.hints = self.storage.hints
        self.gossip = Gossip(self)
        self.members = self.gossip.members
        self.coordinator = Coordinator(self)

    def is_alive(self, peer):
        return self.gossip.is_alive(peer)

    def gossip_round(self):
        self.gossip.round()

    # RPCs other nodes can call on this node
    def rpc_write(self, key, version, hint_for=None):
        self.storage.write(key, version, hint_for)

    def rpc_read(self, key):
        return self.storage.read(key)

    def rpc_merge(self, key, versions):
        self.storage.merge(key, versions)

    def rpc_gossip(self, theirs):
        return self.gossip.on_gossip(theirs)
