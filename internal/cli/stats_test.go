package cli

import (
	"testing"
	"unicode/utf8"
)

func TestSparkline(t *testing.T) {
	if got := sparkline(nil); got != "" {
		t.Errorf("empty input = %q, want empty", got)
	}

	// One block per value.
	vals := []float64{1, 2, 3, 4, 5}
	got := sparkline(vals)
	if utf8.RuneCountInString(got) != len(vals) {
		t.Fatalf("sparkline %q has %d runes, want %d", got, utf8.RuneCountInString(got), len(vals))
	}

	// Ascending input → min at the first block, max at the last.
	runes := []rune(got)
	if runes[0] != sparkBlocks[0] {
		t.Errorf("first block = %q, want lowest %q", runes[0], sparkBlocks[0])
	}
	if runes[len(runes)-1] != sparkBlocks[len(sparkBlocks)-1] {
		t.Errorf("last block = %q, want highest %q", runes[len(runes)-1], sparkBlocks[len(sparkBlocks)-1])
	}

	// A flat series renders as the lowest block throughout (no divide-by-zero).
	flat := sparkline([]float64{7, 7, 7})
	for _, r := range flat {
		if r != sparkBlocks[0] {
			t.Fatalf("flat series produced %q, want all lowest blocks", flat)
		}
	}
}
