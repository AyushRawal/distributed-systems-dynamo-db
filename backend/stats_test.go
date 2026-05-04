package main

import (
	"fmt"
	"math"
	"testing"
)

func TestNodeStatsGetSummaryCalculations(t *testing.T) {
	s := &NodeStats{
		GetCount:           10,
		PutCount:           4,
		SuccessfulGets:     8,
		SuccessfulPuts:     3,
		FailedGets:         2,
		FailedPuts:         1,
		TotalGetLatency:    80,
		TotalPutLatency:    45,
		MaxGetLatency:      20,
		MaxPutLatency:      25,
		SloppyQuorumUsed:   5,
		ReadRepairCount:    6,
		HintStoreCount:     7,
		HintDeliverCount:   8,
		ConflictsDetected:  9,
		ConflictsResolved:  10,
		KeyCount:           11,
		TotalDataSizeBytes: 2 * 1024 * 1024,
	}

	summary := s.GetSummary()

	ops := summary["operations"].(map[string]interface{})
	lat := summary["latency"].(map[string]interface{})
	features := summary["features"].(map[string]interface{})

	if got := ops["get_count"].(int64); got != 10 {
		t.Fatalf("expected get_count=10, got %d", got)
	}
	if got := ops["put_count"].(int64); got != 4 {
		t.Fatalf("expected put_count=4, got %d", got)
	}

	assertFloatClose(t, ops["get_success_rate"].(float64), 80.0, 1e-9)
	assertFloatClose(t, ops["put_success_rate"].(float64), 75.0, 1e-9)

	if got := lat["avg_get_latency_ms"].(int64); got != 10 {
		t.Fatalf("expected avg_get_latency_ms=10, got %d", got)
	}
	if got := lat["avg_put_latency_ms"].(int64); got != 15 {
		t.Fatalf("expected avg_put_latency_ms=15, got %d", got)
	}

	if got := features["key_count"].(int64); got != 11 {
		t.Fatalf("expected key_count=11, got %d", got)
	}
	assertFloatClose(t, features["data_size_mb"].(float64), 2.0, 1e-9)
}

func TestEstimateValueSizeIncludesVectorClockAndConflicts(t *testing.T) {
	conflict := storedValue{Value: "x"}
	v := storedValue{
		Value:       "abc",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1, "nodeB": 2}},
		Conflicts:   []storedValue{conflict},
	}

	got := EstimateValueSize(v)
	want := int64(110) // 50+3 + (16+8*2) + (EstimateValueSize(conflict)/2=25)
	if got != want {
		t.Fatalf("expected estimated size %d, got %d", want, got)
	}
}

func TestEstimateDataSize(t *testing.T) {
	t.Run("empty datastore", func(t *testing.T) {
		c := newTestCoordinator(t, "testEstimateEmpty", 1)

		keys, size := EstimateDataSize(c)
		if keys != 0 || size != 0 {
			t.Fatalf("expected empty size and key count, got keys=%d size=%d", keys, size)
		}
	})

	t.Run("small datastore exact calculation", func(t *testing.T) {
		c := newTestCoordinator(t, "testEstimateSmall", 1)

		v1 := storedValue{Value: "hello", VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1}}}
		v2 := storedValue{Value: []byte{1, 2, 3}, VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 2, "nodeB": 1}}}

		c.DataStore["a"] = v1
		c.DataStore["bb"] = v2

		wantKeys := int64(2)
		wantSize := int64(len("a")) + EstimateValueSize(v1) + int64(len("bb")) + EstimateValueSize(v2)

		gotKeys, gotSize := EstimateDataSize(c)
		if gotKeys != wantKeys {
			t.Fatalf("expected key count %d, got %d", wantKeys, gotKeys)
		}
		if gotSize != wantSize {
			t.Fatalf("expected data size %d, got %d", wantSize, gotSize)
		}
	})

	t.Run("large datastore sampled estimation", func(t *testing.T) {
		c := newTestCoordinator(t, "testEstimateLarge", 1)

		common := storedValue{Value: "uniform", VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1}}}
		for i := 0; i < 120; i++ {
			key := fmt.Sprintf("k%03d", i)
			c.DataStore[key] = common
		}

		gotKeys, gotSize := EstimateDataSize(c)
		if gotKeys != 120 {
			t.Fatalf("expected key count 120, got %d", gotKeys)
		}

		wantSize := EstimateValueSize(common) * 120
		if gotSize != wantSize {
			t.Fatalf("expected sampled size %d, got %d", wantSize, gotSize)
		}
	})
}

func assertFloatClose(t *testing.T, got, want, eps float64) {
	t.Helper()
	if math.Abs(got-want) > eps {
		t.Fatalf("float mismatch: want %.6f got %.6f", want, got)
	}
}
