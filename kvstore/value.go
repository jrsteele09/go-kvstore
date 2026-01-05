package kvstore

import (
	"time"
)

// CounterConstraints holds the current integer value of a counter, bounded by Min and Max.
type CounterConstraints struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

// ValueItem represents the value associated with a key.
// The data can be in a loaded or unloaded state, which indicates whether it's in memory.
// Unloaded data will be reloaded when accessed.
type ValueItem struct {
	Data       []byte              `json:"-"`
	Counter    *CounterConstraints `json:"counterConstraints,omitempty"`
	Ts         time.Time           `json:"timestamp"`
	TTL        TTLType             `json:"ttl"`
	dataLoaded bool                `json:"-"`
}

// NewValueItem initializes a new ValueItem with a given timestamp.
func NewValueItem(dataBytes []byte, ts time.Time) *ValueItem {
	return &ValueItem{
		Data:       dataBytes,
		Ts:         ts,
		TTL:        TTLNoExpirySet,
		dataLoaded: true,
	}
}

// SetData updates the Data field of a ValueItem.
func (item *ValueItem) SetData(dataBytes []byte) {
	item.Data = dataBytes
	item.dataLoaded = true
}

// expired checks if a ValueItem is expired based on its TTL.
func (item *ValueItem) expired(now time.Time) bool {
	if item.TTL <= 0 {
		return false
	}
	return item.Ts.Add(time.Duration(item.TTL) * time.Second).Before(now)
}

// unload checks if a ValueItem should be unloaded based on a duration.
func (item *ValueItem) unload(now time.Time, unloadAfter time.Duration) bool {
	if unloadAfter == 0 {
		return false
	}
	return now.Sub(item.Ts) > unloadAfter
}

// unloadData removes the data from memory while keeping metadata.
func (item *ValueItem) unloadData() {
	item.Data = nil
	item.dataLoaded = false
}
