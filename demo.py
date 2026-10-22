from kvstore import Cluster, QuorumNotReached


def banner(t):
    print(f"\n=== {t} ===")


def build(sloppy=True):
    c = Cluster(N=3, R=2, W=2, sloppy=sloppy)
    for dc in ("dc1", "dc2", "dc3"):
        for i in range(3):
            c.add_node(f"{dc}-n{i}", dc)
    c.tick(15)  # let gossip converge
    return c


# 1 ---------------------------------------------------------------
banner("1. Basic put/get, consistent hashing + DC-aware replication")
c = build()
c.put("user:1", "alice")
print("get ->", c.get("user:1").values)
pl = c.ring.preference_list("user:1", c.N)
print("replicas:", pl, "(one per DC)")

# 2 ---------------------------------------------------------------
banner("2. Vector clocks: concurrent writes become siblings, then resolved")
c.put("cart", "milk")
ctx = c.get("cart").context
pl = c.ring.preference_list("cart", 3)
c.put("cart", "milk,eggs", context=ctx, coordinator=pl[0])   # two clients, same stale context,
c.put("cart", "milk,bread", context=ctx, coordinator=pl[1])  # different coordinators
r = c.get("cart")
print("siblings:", r.siblings, r.values)
print("LWW resolution:", c.get("cart", resolve="lww").values)
c.put("cart", "milk,eggs,bread", context=r.context)           # app-level merge
r = c.get("cart")
print("after merge:", r.values, "siblings:", r.siblings)

# 3 ---------------------------------------------------------------
banner("3. Temporary failure: gossip detects it, sloppy quorum + hinted handoff")
key = "order:42"
pl = c.ring.preference_list(key, 3)
victim = pl[1]
c.kill(victim)
c.tick(12)
alive_view = {n: c.nodes[pl[0]].is_alive(n) for n in pl}
print("gossip view from", pl[0], ":", alive_view)
c.put(key, "v2")  # W=2 still met; the dead replica's share goes to a stand-in node as a hint
holders = {n.id: list(n.hints) for n in c.nodes.values() if n.hints}
print("hints stored (stand-in -> intended nodes):", holders)
c.revive(victim)
c.tick(12)
print("victim has data after handoff:", key in c.nodes[victim].store,
      "| leftover hints:", any(n.hints for n in c.nodes.values()))

# 4 ---------------------------------------------------------------
banner("4. Whole data-center outage")
c.kill_dc("dc2")
c.tick(12)
c.put("k:dc", "survives")
print("get with dc2 down ->", c.get("k:dc").values, "(N=3,R=2,W=2 across 3 DCs)")
c.put("k:dc", "x", w=3)  # sloppy quorum: stand-in node in a live DC takes the hint, so even W=3 is met
print("W=3 write accepted via stand-in node (sloppy quorum); strict mode would refuse")
c.revive_dc("dc2")
c.tick(12)

banner("4b. CP-leaning mode (strict quorum): no stand-ins, writes fail instead")
s = build(sloppy=False)
k = "strict:1"
pl = s.ring.preference_list(k, 3)
s.kill(pl[1]); s.kill(pl[2]); s.tick(12)
try:
    s.put(k, "x")
except QuorumNotReached as e:
    print("strict quorum:", e)
s2 = build(sloppy=True)
pl = s2.ring.preference_list(k, 3)
s2.kill(pl[1]); s2.kill(pl[2]); s2.tick(12)
s2.put(k, "x")
print("sloppy quorum accepted the write (AP), hints held by stand-ins")

# 5 ---------------------------------------------------------------
banner("5. Merkle-tree anti-entropy repairs a replica that lost data")
c = build()
for i in range(200):
    c.put(f"item:{i}", f"val{i}")
print("clean cluster:", c.anti_entropy())
victim = c.nodes["dc1-n0"]
lost = list(victim.store)[:5]
for k in lost:
    del victim.store[k]                    # simulate disk loss / missed writes
print(f"deleted {len(lost)} keys from {victim.id}")
print("repair:", c.anti_entropy())
print("restored:", all(k in victim.store for k in lost))

# 6 ---------------------------------------------------------------
banner("6. Elastic scaling: add a node, then permanently lose one")
before = {k: set(c.ring.preference_list(k, 3)) for k in (f"item:{i}" for i in range(200))}
streamed = c.add_node("dc1-n3", "dc1")
after = {k: set(c.ring.preference_list(k, 3)) for k in before}
changed = sum(before[k] != after[k] for k in before)
print(f"added dc1-n3: {changed}/200 keys changed replica set (consistent hashing moves a fraction), "
      f"{streamed} replica copies streamed")
c.kill("dc3-n1")
c.remove_node("dc3-n1")                    # permanent failure -> re-replicate from survivors
ok = all(len(c.get(f"item:{i}").values) == 1 for i in range(200))
print("all 200 keys readable after permanent node loss:", ok)
