from collections import defaultdict
from .vector_clock import reconcile


class Storage:
    # In-memory store (real systems: commit log + memtable + SSTables)
    def __init__(self):
        self.data = {}                   # key -> versions
        self.hints = defaultdict(dict)   # intended_node -> {key: versions}

    def write(self, key, version, hint_for=None):
        if hint_for:   # holding data for a down node
            old = self.hints[hint_for].get(key, [])
            self.hints[hint_for][key] = reconcile(old + [version])
        else:
            old = self.data.get(key, [])
            self.data[key] = reconcile(old + [version])

    def read(self, key):
        return list(self.data.get(key, []))

    def merge(self, key, versions):
        old = self.data.get(key, [])
        self.data[key] = reconcile(old + list(versions))
