package kvstore

import (
	"math"
	"strconv"
	"time"
)

// CounterConstraints contains the current integer value of a counter, it can be controlled with min and max values
type CounterConstraints struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

// ValueItem stores the data associated with a key.
// The data can be in and unloaded state which means that it does not exist in memory but has been persisted.
// Unloaded data will get reloaded when accessed.
type ValueItem struct {
	Data       []byte              `json:"-"`
	Counter    *CounterConstraints `json:"counterContraints,omitempty"`
	Ts         time.Time           `json:"timestamp"`
	TTL        TTLType             `json:"ttl"`
	dataLoaded bool                `json:"-"`
}

// NewValueItem creates a new ValueItem with a timestamp
func NewValueItem(dataBytes []byte, ts time.Time) *ValueItem {
	if _, err := strconv.ParseInt(string(dataBytes), 10, 64); err == nil {
		return &ValueItem{
			Data:       dataBytes,
			Counter:    &CounterConstraints{Min: math.MinInt64, Max: math.MaxInt64},
			Ts:         ts,
			TTL:        TTLNoExpirySet,
			dataLoaded: true,
		}
	}
	return &ValueItem{
		Data:       dataBytes,
		Ts:         ts,
		TTL:        TTLNoExpirySet,
		dataLoaded: true,
	}
}

// SetData sets the data
func (mv *ValueItem) SetData(dataBytes []byte) error {
	if _, err := strconv.ParseInt(string(dataBytes), 10, 64); err == nil && mv.Counter == nil {
		mv.Counter = &CounterConstraints{Min: math.MinInt64, Max: math.MaxInt64}
	}

	mv.Data = dataBytes
	mv.dataLoaded = true
	return nil
}

func (mv *ValueItem) expired(now time.Time) bool {
	if mv.TTL <= 0 {
		return false
	}
	return mv.Ts.Add(time.Duration(time.Duration(mv.TTL) * time.Second)).Before(now)
}

func (mv *ValueItem) unload(now time.Time, unloadAfter time.Duration) bool {
	if unloadAfter == 0 {
		return false
	}
	return now.Sub(mv.Ts) > unloadAfter
}
