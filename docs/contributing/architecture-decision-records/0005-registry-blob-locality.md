# ADR-0005 — Registry blobs stay node-local; peers fetch on demand

**Status:** Accepted · **Date:** 2026-07-18

## Context

Every control node runs the embedded OCI registry (`internal/daemon/registry`), but a blob only
exists on the node whose registry received the push. Three facts make that visible on a multi-control
cluster:

- `registryClientAddr` returns each node's **own** registry address.
- The build dispatcher runs on the **raft leader** and pushes there.
- The join server hands a joining node the registry address of **whichever control node served the
  join**, and the node persists it in `mesh.json` and never refreshes it.

So a worker can be pointed at a control node that never received the push, and leadership changes
scatter images across nodes over time. Losing a control node makes the images only it held
unpullable: raft quorum keeps the control plane alive, not the image store. Single-control clusters
— the default — are unaffected, which is why this went unnoticed.

Platform state is small and replicated through raft. Image layers are neither.

## Decision

**Blobs stay out of raft and stay node-local.** A consensus log replicates every entry to every
member, in order, with the whole cluster's write throughput bounded by it. Multi-hundred-megabyte
layers in that path would trade a working control plane for an image store.

**Any control node can serve any blob by fetching it from a peer on demand** (T-101). On a miss the
registry asks the other control nodes — resolved from cluster state per call, so the set follows
joins and removals — fetches the object, commits it locally, and serves it. Manifests pull their
children and blobs first, then go through `PutManifest`, so the local refcount graph stays complete
and GC keeps working.

Properties this buys:

- **Storage stays ~1× cluster-wide.** A blob lands only on nodes that actually serve it, not on all
  of them.
- **Cold pulls cost one hop**; every later pull is local, so a cluster heals toward locality.
- **No configuration.** Single-node and dev clusters resolve zero peers and behave exactly as before.

Bounds, because this is a network fetch on a hot path: per-peer probe timeout, a total deadline
under the Docker client's, a concurrency cap, and single-flight collapsing of concurrent requests for
the same digest. A peer whose bytes do not match the requested digest is dropped, never committed.

**Scope is strictly intra-cluster.** Pull-through never reaches an external registry. Proxying Docker
Hub is a different feature with different security and licensing properties, and conflating the two
would let a typo'd image reference silently egress.

## Alternatives considered

- **Replicate every blob to every control node.** Simplest mental model, but multiplies storage by
  the control-node count — the opposite of what an operator with heavy images wants, and the reason
  the question was raised in the first place.
- **Always address the leader's registry.** Cheap, but makes image pulls depend on the current
  leader, so a leader election becomes an image-availability event. That undoes part of what HA is
  for.
- **Require an external/S3 registry.** Best durability, but breaks the "only dependency is Docker"
  promise and rules out air-gapped clusters. Kept as an **optional** backend instead (T-102), so a
  cluster can adopt it later without a migration cliff.

## Consequences

- Durability is unchanged by this ADR: a blob still lives on however many nodes happened to fetch
  it, and **backups do not include registry blobs**. Losing a control node can still lose images that
  no other node ever pulled. T-102 (opt-in object-store backend) is the answer for clusters that need
  more; until then, "can I rebuild every running image from source?" is the real recovery question.
- Refcount graphs remain per-node bbolt. That is correct while each node owns its own storage, but a
  **shared** backend (T-102) would let two nodes free the same blob independently — so T-102 must
  decide between moving refcounts into raft (small, unlike blobs) or leader-only GC.
- A pulled-through blob has no local manifest referencing it until one arrives, so it carries no
  refcount. Today's GC only sweeps from manifest teardown, so it is not treated as garbage; any
  future mark-and-sweep over the blob directory must account for this.
