//go:build chaos

// Package chaos holds slow simcluster-based fault-injection tests (`make
// test-chaos`, tasks T-32/T-70): leader kills in every deployment phase,
// partitions, quorum loss, stateful fencing invariants. No Docker needed.
package chaos
