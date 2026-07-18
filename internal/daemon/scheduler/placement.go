package scheduler

import (
	"fmt"
	"math"
	"sort"
	"strings"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/platform"
	"github.com/zattera-dev/zattera/internal/state"
)

// Default reservation for a spec that declares no resources — prevents infinite
// stacking of "free" replicas on one node.
const (
	defaultReserveCPUMillis = 100
	defaultReserveMemoryMB  = 256
)

// resources is a cpu/mem reservation pair.
type resources struct {
	cpuMillis uint32
	memoryMB  uint32
}

func (r resources) add(o resources) resources {
	return resources{cpuMillis: r.cpuMillis + o.cpuMillis, memoryMB: r.memoryMB + o.memoryMB}
}

// Place selects up to n nodes to run replicas of rel (its frozen ServiceSpec)
// for environment envID.
//
// It is a PURE function over state (no I/O), so it is fully table-testable.
// Capacity is judged by RESERVATIONS (sum of RUN assignments' declared
// resources), not live usage. `exclude` skips nodes entirely — the red/green
// orchestrator uses it to place a green set beside blue and to avoid retrying a
// candidate that already failed. Returns fewer than n plus an error when
// filters/capacity cannot satisfy the request.
func Place(st *state.Store, rel *zatterav1.Release, envID string, n int, exclude map[string]bool) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}
	spec := rel.GetService()
	platforms := rel.GetPlatforms()
	need := effectiveResources(spec)
	pinned := pinnedNodeID(st, spec, envID)
	reserved := reservationsByNode(st)

	// Filter candidates. An arch-excluded node is filtered exactly like an
	// unschedulable one: never scored, never picked. Empty platforms = runs
	// anywhere (legacy releases, uninspectable images) — the filter is additive
	// tightening, never a new hard requirement.
	var cands []*zatterav1.Node
	archRejected := false
	labelRejected := false
	for _, node := range st.ListNodes() {
		id := node.GetMeta().GetId()
		if exclude[id] {
			continue
		}
		if node.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE || !node.GetSchedulable() {
			continue
		}
		if !platform.Supports(node.GetOsArch(), platforms) {
			archRejected = true
			continue
		}
		if !labelsMatch(node.GetLabels(), spec.GetPlacementConstraints()) {
			labelRejected = true
			continue
		}
		if pinned != "" && id != pinned {
			continue // stateful + volume: only the volume's node
		}
		cands = append(cands, node)
	}
	if len(cands) == 0 && archRejected {
		return nil, fmt.Errorf("placement: no node with a supported architecture (need one of %s) for env %s", strings.Join(platforms, ", "), envID)
	}
	// An unsatisfiable placement constraint fails the same way an unsatisfiable
	// arch does. Returning no candidates instead would park the replicas with no
	// error anywhere, which reads as "deploying" forever — the failure mode is a
	// typo in a label, and it must be visible.
	if len(cands) == 0 && labelRejected {
		return nil, fmt.Errorf("placement: no node matches constraints %s for env %s", formatConstraints(spec.GetPlacementConstraints()), envID)
	}

	// Base spread counts: replicas of THIS env per node and per region.
	envByNode := map[string]int{}
	for _, a := range st.ListAssignments(envID) {
		if a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			envByNode[a.GetNodeId()]++
		}
	}
	envByRegion := map[string]int{}
	for nodeID, c := range envByNode {
		if node, ok := st.Node(nodeID); ok {
			envByRegion[regionOf(node)] += c
		}
	}

	// Greedy pick, re-scored after each selection.
	picked := make([]string, 0, n)
	addRes := map[string]resources{}
	pickEnv := map[string]int{}
	pickRegion := map[string]int{}

	for i := 0; i < n; i++ {
		var best *zatterav1.Node
		var bestKey scoreKey
		for _, node := range cands {
			id := node.GetMeta().GetId()
			cur := reserved[id].add(addRes[id])
			if !fits(cur, need, node.GetCapacity()) {
				continue
			}
			key := scoreKey{
				envReplicas:    envByNode[id] + pickEnv[id],
				regionReplicas: envByRegion[regionOf(node)] + pickRegion[regionOf(node)],
				negFreeMem:     -freeMemory(node.GetCapacity(), cur),
				nodeID:         id,
			}
			if best == nil || key.less(bestKey) {
				best, bestKey = node, key
			}
		}
		if best == nil {
			break // capacity/filters exhausted
		}
		id := best.GetMeta().GetId()
		picked = append(picked, id)
		pickEnv[id]++
		pickRegion[regionOf(best)]++
		addRes[id] = addRes[id].add(need)
	}

	if len(picked) < n {
		return picked, fmt.Errorf("placement: only %d of %d replicas placeable for env %s (constraints/capacity)", len(picked), n, envID)
	}
	return picked, nil
}

// scoreKey orders placement candidates: spread over the env first, then over
// regions, then most free memory, with a deterministic node-id tie-break.
type scoreKey struct {
	envReplicas    int
	regionReplicas int
	negFreeMem     int64 // negative free memory → smaller is more free
	nodeID         string
}

func (k scoreKey) less(o scoreKey) bool {
	switch {
	case k.envReplicas != o.envReplicas:
		return k.envReplicas < o.envReplicas
	case k.regionReplicas != o.regionReplicas:
		return k.regionReplicas < o.regionReplicas
	case k.negFreeMem != o.negFreeMem:
		return k.negFreeMem < o.negFreeMem
	default:
		return k.nodeID < o.nodeID
	}
}

// pinnedNodeID returns the node a stateful+volume service must run on, or "".
func pinnedNodeID(st *state.Store, spec *zatterav1.ServiceSpec, envID string) string {
	if !spec.GetStateful() || len(spec.GetVolumes()) == 0 {
		return ""
	}
	env, ok := st.Environment(envID)
	if !ok {
		return ""
	}
	for _, vm := range spec.GetVolumes() {
		if v, ok := st.VolumeByName(env.GetProjectId(), envID, vm.GetVolumeName()); ok && v.GetNodeId() != "" {
			return v.GetNodeId()
		}
	}
	return ""
}

// reservationsByNode sums the declared resources of every RUN assignment per
// node (each assignment's resources come from its release's frozen spec).
func reservationsByNode(st *state.Store) map[string]resources {
	out := map[string]resources{}
	for _, a := range st.ListAssignments("") {
		if a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			continue
		}
		var spec *zatterav1.ServiceSpec
		if rel, ok := st.Release(a.GetReleaseId()); ok {
			spec = rel.GetService()
		}
		out[a.GetNodeId()] = out[a.GetNodeId()].add(effectiveResources(spec))
	}
	return out
}

// effectiveResources applies the default reservation to zero-valued dimensions.
func effectiveResources(spec *zatterav1.ServiceSpec) resources {
	r := resources{
		cpuMillis: spec.GetResources().GetCpuMillis(),
		memoryMB:  spec.GetResources().GetMemoryMb(),
	}
	if r.cpuMillis == 0 {
		r.cpuMillis = defaultReserveCPUMillis
	}
	if r.memoryMB == 0 {
		r.memoryMB = defaultReserveMemoryMB
	}
	return r
}

// fits reports whether cur+need stays within cap. A zero capacity dimension
// means "unreported" and is treated as unlimited (never blocks scheduling).
func fits(cur, need resources, cap *zatterav1.ResourceLimits) bool {
	if c := cap.GetCpuMillis(); c != 0 && cur.cpuMillis+need.cpuMillis > c {
		return false
	}
	if m := cap.GetMemoryMb(); m != 0 && cur.memoryMB+need.memoryMB > m {
		return false
	}
	return true
}

// freeMemory returns remaining reservable MB, or MaxInt64 when unreported.
func freeMemory(cap *zatterav1.ResourceLimits, cur resources) int64 {
	m := cap.GetMemoryMb()
	if m == 0 {
		return math.MaxInt64
	}
	free := int64(m) - int64(cur.memoryMB)
	if free < 0 {
		return 0
	}
	return free
}

func regionOf(node *zatterav1.Node) string { return node.GetLabels()["region"] }

// formatConstraints renders a label map deterministically for error messages.
func formatConstraints(c map[string]string) string {
	pairs := make([]string, 0, len(c))
	for k, v := range c {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}
