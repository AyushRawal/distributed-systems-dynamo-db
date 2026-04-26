package main

import (
	"sync"
)

// NodeStats tracks performance metrics
type NodeStats struct {
	mu                 sync.Mutex
	GetCount           int64 `json:"get_count"`
	PutCount           int64 `json:"put_count"`
	DeleteCount        int64 `json:"delete_count"`
	SuccessfulGets     int64 `json:"successful_gets"`
	SuccessfulPuts     int64 `json:"successful_puts"`
	FailedGets         int64 `json:"failed_gets"`
	FailedPuts         int64 `json:"failed_puts"`
	SloppyQuorumUsed   int64 `json:"sloppy_quorum_used"`
	ReadRepairCount    int64 `json:"read_repair_count"`
	HintStoreCount     int64 `json:"hint_store_count"`
	HintDeliverCount   int64 `json:"hint_deliver_count"`
	TotalGetLatency    int64 `json:"total_get_latency_ms"`
	TotalPutLatency    int64 `json:"total_put_latency_ms"`
	MaxGetLatency      int64 `json:"max_get_latency_ms"`
	MaxPutLatency      int64 `json:"max_put_latency_ms"`
	ConflictsDetected  int64 `json:"conflicts_detected"`
	ConflictsResolved  int64 `json:"conflicts_resolved"`
	KeyCount           int64 `json:"key_count"`
	TotalDataSizeBytes int64 `json:"total_data_size_bytes"`
}

func (s *NodeStats) UpdateStorageStats(keyCount, dataSize int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.KeyCount = keyCount
	s.TotalDataSizeBytes = dataSize
}

func (s *NodeStats) GetSummary() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	avgGetLatency := int64(0)
	if s.SuccessfulGets > 0 {
		avgGetLatency = s.TotalGetLatency / s.SuccessfulGets
	}

	avgPutLatency := int64(0)
	if s.SuccessfulPuts > 0 {
		avgPutLatency = s.TotalPutLatency / s.SuccessfulPuts
	}

	getSuccessRate := float64(0)
	if s.GetCount > 0 {
		getSuccessRate = float64(s.SuccessfulGets) / float64(s.GetCount) * 100
	}

	putSuccessRate := float64(0)
	if s.PutCount > 0 {
		putSuccessRate = float64(s.SuccessfulPuts) / float64(s.PutCount) * 100
	}

	return map[string]interface{}{
		"operations": map[string]interface{}{
			"get_count":        s.GetCount,
			"put_count":        s.PutCount,
			"successful_gets":  s.SuccessfulGets,
			"successful_puts":  s.SuccessfulPuts,
			"failed_gets":      s.FailedGets,
			"failed_puts":      s.FailedPuts,
			"get_success_rate": getSuccessRate,
			"put_success_rate": putSuccessRate,
		},
		"latency": map[string]interface{}{
			"avg_get_latency_ms": avgGetLatency,
			"avg_put_latency_ms": avgPutLatency,
			"max_get_latency_ms": s.MaxGetLatency,
			"max_put_latency_ms": s.MaxPutLatency,
		},
		"features": map[string]interface{}{
			"sloppy_quorum_used": s.SloppyQuorumUsed,
			"read_repairs":       s.ReadRepairCount,
			"hints_stored":       s.HintStoreCount,
			"hints_delivered":    s.HintDeliverCount,
			"conflicts_detected": s.ConflictsDetected,
			"conflicts_resolved": s.ConflictsResolved,
			"data_size_mb":       float64(s.TotalDataSizeBytes) / (1024 * 1024),
			"key_count":          s.KeyCount,
		},
	}
}

func EstimateDataSize(c *Coordinator) (int64, int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keyCount := int64(len(c.DataStore))
	if keyCount == 0 {
		return 0, 0
	}

	if keyCount < 100 {
		var totalSize int64
		for k, v := range c.DataStore {
			keySize := int64(len(k))
			valueSize := EstimateValueSize(v)
			totalSize += keySize + valueSize
		}
		return keyCount, totalSize
	}

	sampleCount := keyCount / 10
	if sampleCount < 10 {
		sampleCount = 10
	}

	var totalSize int64
	var sampled int64
	for _, v := range c.DataStore {
		if sampled >= sampleCount {
			break
		}
		totalSize += EstimateValueSize(v)
		sampled++
	}

	avgSize := totalSize / sampled
	return keyCount, avgSize * keyCount
}

func EstimateValueSize(v storedValue) int64 {
	var size int64 = 50

	switch val := v.Value.(type) {
	case string:
		size += int64(len(val))
	case []byte:
		size += int64(len(val))
	case map[string]interface{}:
		size += 100
	default:
		size += 20
	}

	if v.VectorClock != nil {
		size += int64(16 + 8*len(v.VectorClock.Clock))
	}

	for _, conflict := range v.Conflicts {
		size += EstimateValueSize(conflict) / 2
	}

	return size
}
