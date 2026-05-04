package main

import "testing"

func TestVectorClockIncrementAndClone(t *testing.T) {
	vc := NewVectorClock()
	vc.Increment("nodeA")
	vc.Increment("nodeA")

	if got := vc.Clock["nodeA"]; got != 2 {
		t.Fatalf("expected nodeA counter 2, got %d", got)
	}

	clone := vc.Clone()
	clone.Increment("nodeA")

	if vc.Clock["nodeA"] != 2 {
		t.Fatalf("expected original clock unchanged at 2, got %d", vc.Clock["nodeA"])
	}
}

func TestVectorClockCompare(t *testing.T) {
	tests := []struct {
		name string
		a    *VectorClock
		b    *VectorClock
		want int
	}{
		{
			name: "equal clocks",
			a:    &VectorClock{Clock: map[string]int{"nodeA": 1}},
			b:    &VectorClock{Clock: map[string]int{"nodeA": 1}},
			want: 0,
		},
		{
			name: "a dominates b",
			a:    &VectorClock{Clock: map[string]int{"nodeA": 2, "nodeB": 1}},
			b:    &VectorClock{Clock: map[string]int{"nodeA": 1}},
			want: 1,
		},
		{
			name: "b dominates a",
			a:    &VectorClock{Clock: map[string]int{"nodeA": 1}},
			b:    &VectorClock{Clock: map[string]int{"nodeA": 2, "nodeB": 1}},
			want: -1,
		},
		{
			name: "concurrent clocks",
			a:    &VectorClock{Clock: map[string]int{"nodeA": 2}},
			b:    &VectorClock{Clock: map[string]int{"nodeB": 3}},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Fatalf("compare result mismatch: want %d got %d", tt.want, got)
			}
		})
	}
}

func TestVectorClockMergeAndIncrement(t *testing.T) {
	base := &VectorClock{Clock: map[string]int{"nodeA": 1}}
	other := &VectorClock{Clock: map[string]int{"nodeA": 2, "nodeB": 5}}

	base.MergeAndIncrement(other, "nodeA")

	if got := base.Clock["nodeA"]; got != 3 {
		t.Fatalf("expected merged+incremented nodeA=3, got %d", got)
	}

	if got := base.Clock["nodeB"]; got != 5 {
		t.Fatalf("expected merged nodeB=5, got %d", got)
	}
}

func TestCompareVectorClocksHelper(t *testing.T) {
	older := &VectorClock{Clock: map[string]int{"nodeA": 1}}
	newer := &VectorClock{Clock: map[string]int{"nodeA": 2}}
	concurrentA := &VectorClock{Clock: map[string]int{"nodeA": 2}}
	concurrentB := &VectorClock{Clock: map[string]int{"nodeB": 2}}

	if got := compareVectorClocks(older, newer); got != "newer" {
		t.Fatalf("expected newer, got %q", got)
	}

	if got := compareVectorClocks(newer, older); got != "older" {
		t.Fatalf("expected older, got %q", got)
	}

	if got := compareVectorClocks(concurrentA, concurrentB); got != "concurrent" {
		t.Fatalf("expected concurrent, got %q", got)
	}
}
