package kvstore

import "time"

// StoreOption defines a function to use to configure a KV Store
type StoreOption func(s *Store)

// WithUnloadFrequencyOption configures how often the cache checks for eviction and how long the objects stay in memory before being unloaded.
func WithUnloadFrequencyOption(ef time.Duration, uf time.Duration) StoreOption {
	return func(s *Store) {
		s.evictionFreq = ef
		s.unloadAfterTime = uf
	}
}

// WithPersistenceOption allows the setting up of a number of persitence controllers
func WithPersistenceOption(persistence ...PersistenceController) StoreOption {
	return func(s *Store) {
		s.persistence = persistence
	}
}

// WithNowFuncOption allows configuration of the function that returns the current time. Useful for testing
func WithNowFuncOption(nowFunc func() time.Time) StoreOption {
	return func(s *Store) {
		s.nowFunc = nowFunc
	}
}
