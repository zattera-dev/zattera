package cli

import (
	"reflect"
	"testing"
)

func TestParseLabelArgs(t *testing.T) {
	t.Run("sets and removes", func(t *testing.T) {
		sets, removes, err := parseLabelArgs([]string{"region=eu", "tier=db", "old-"})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if want := map[string]string{"region": "eu", "tier": "db"}; !reflect.DeepEqual(sets, want) {
			t.Errorf("sets = %v, want %v", sets, want)
		}
		if want := []string{"old"}; !reflect.DeepEqual(removes, want) {
			t.Errorf("removes = %v, want %v", removes, want)
		}
	})

	t.Run("empty value is allowed", func(t *testing.T) {
		sets, _, err := parseLabelArgs([]string{"region="})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if v, ok := sets["region"]; !ok || v != "" {
			t.Errorf(`want region set to "", got %q (present=%v)`, v, ok)
		}
	})

	t.Run("rejects garbage", func(t *testing.T) {
		for _, arg := range []string{"region", "=eu", "-"} {
			if _, _, err := parseLabelArgs([]string{arg}); err == nil {
				t.Errorf("%q should be rejected", arg)
			}
		}
	})

	t.Run("rejects the same key twice", func(t *testing.T) {
		if _, _, err := parseLabelArgs([]string{"region=eu", "region=us"}); err == nil {
			t.Error("duplicate key should be rejected")
		}
		if _, _, err := parseLabelArgs([]string{"region=eu", "region-"}); err == nil {
			t.Error("set+remove of one key should be rejected")
		}
	})
}

func TestMergeLabels(t *testing.T) {
	current := map[string]string{"builder": "true", "region": "eu", reservedLabelPrefix + "os-arch": "linux/arm64"}

	t.Run("merges without touching existing keys", func(t *testing.T) {
		got, err := mergeLabels(current, map[string]string{"tier": "db"}, nil, false)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		want := map[string]string{"builder": "true", "region": "eu", reservedLabelPrefix + "os-arch": "linux/arm64", "tier": "db"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("does not mutate the current map", func(t *testing.T) {
		before := len(current)
		if _, err := mergeLabels(current, map[string]string{"tier": "db"}, nil, false); err != nil {
			t.Fatalf("merge: %v", err)
		}
		if len(current) != before {
			t.Fatalf("current map was mutated: %v", current)
		}
	})

	t.Run("refuses to change an existing key without --overwrite", func(t *testing.T) {
		if _, err := mergeLabels(current, map[string]string{"region": "us"}, nil, false); err == nil {
			t.Fatal("changing region without --overwrite should fail")
		}
		got, err := mergeLabels(current, map[string]string{"region": "us"}, nil, true)
		if err != nil {
			t.Fatalf("--overwrite should allow it: %v", err)
		}
		if got["region"] != "us" {
			t.Errorf("region = %q, want us", got["region"])
		}
	})

	t.Run("re-setting a key to its current value is not a change", func(t *testing.T) {
		if _, err := mergeLabels(current, map[string]string{"region": "eu"}, nil, false); err != nil {
			t.Errorf("idempotent set should be allowed: %v", err)
		}
	})

	t.Run("removes keys", func(t *testing.T) {
		got, err := mergeLabels(current, nil, []string{"region"}, false)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if _, ok := got["region"]; ok {
			t.Error("region should be gone")
		}
		if got["builder"] != "true" {
			t.Error("removing one key must not drop the others")
		}
	})

	t.Run("refuses reserved labels", func(t *testing.T) {
		key := reservedLabelPrefix + "os-arch"
		if _, err := mergeLabels(current, map[string]string{key: "linux/amd64"}, nil, false); err == nil {
			t.Error("setting a reserved label should fail")
		}
		if _, err := mergeLabels(current, map[string]string{key: "linux/amd64"}, nil, true); err == nil {
			t.Error("--overwrite must not unlock reserved labels")
		}
		if _, err := mergeLabels(current, nil, []string{key}, false); err == nil {
			t.Error("removing a reserved label should fail")
		}
	})

	t.Run("builder stays writable", func(t *testing.T) {
		if _, err := mergeLabels(current, map[string]string{"builder": "false"}, nil, true); err != nil {
			t.Errorf("opting a worker out of builds is legitimate: %v", err)
		}
	})
}
