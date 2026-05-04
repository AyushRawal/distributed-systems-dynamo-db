package main

import "encoding/json"
import "testing"

func TestMerkleTreeRootStableForSameData(t *testing.T) {
	data := map[string]interface{}{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	}

	treeA := NewMerkleTree(data)
	treeB := NewMerkleTree(data)

	if treeA.Root() == "" {
		t.Fatal("expected non-empty root hash")
	}

	if treeA.Root() != treeB.Root() {
		t.Fatalf("expected equal roots for equal data, got %q and %q", treeA.Root(), treeB.Root())
	}
}

func TestMerkleTreeCompareTreesDetectsChangedKeys(t *testing.T) {
	left := map[string]interface{}{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	}
	right := map[string]interface{}{
		"k1": "v1",
		"k2": "v2-modified",
		"k4": "v4",
	}

	treeLeft := NewMerkleTree(left)
	treeRight := NewMerkleTree(right)

	diffs := treeLeft.CompareTrees(treeRight)
	if len(diffs) == 0 {
		t.Fatal("expected non-empty diff for diverged trees")
	}

	got := make(map[string]bool)
	for _, k := range diffs {
		got[k] = true
	}

	for _, expected := range []string{"k2", "k3", "k4"} {
		if !got[expected] {
			t.Fatalf("expected key %q in diff set, got %v", expected, diffs)
		}
	}
}

func TestMerkleTreeSerializeDeserializeRoundTrip(t *testing.T) {
	data := map[string]interface{}{
		"alpha": "one",
		"beta":  2,
	}

	original := NewMerkleTree(data)
	serialized := original.SerializeToMap()

	// Simulate payload shape received over JSON transport where slices become []interface{}.
	payload, err := json.Marshal(serialized)
	if err != nil {
		t.Fatalf("failed to marshal serialized tree: %v", err)
	}

	var generic map[string]interface{}
	if err := json.Unmarshal(payload, &generic); err != nil {
		t.Fatalf("failed to unmarshal serialized tree payload: %v", err)
	}

	reconstructed, err := DeserializeFromMap(generic)
	if err != nil {
		t.Fatalf("expected successful deserialize, got error: %v", err)
	}

	if reconstructed.Root() != original.Root() {
		t.Fatalf("root mismatch after round-trip: want %q got %q", original.Root(), reconstructed.Root())
	}

	if reconstructed.Version != original.Version {
		t.Fatalf("version mismatch after round-trip: want %d got %d", original.Version, reconstructed.Version)
	}
}

func TestDeserializeFromMapMissingFields(t *testing.T) {
	_, err := DeserializeFromMap(map[string]interface{}{"leaves": []interface{}{}})
	if err == nil {
		t.Fatal("expected error when required fields are missing")
	}
}
