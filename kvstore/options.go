package kvstore

import "time"

// StoreOption is a type for functions that configure a Store.
// These functions are intended to be used with the NewStore function
// to create a customized Store instance.
type StoreOption func(s *Store)

// WithUnloadFrequencyOption returns a StoreOption that configures the eviction frequency
// and the unload-after time of objects in the cache.
//
// - 'ef' sets how often the store should check and evict expired items.
// - 'uf' sets the duration an object will stay in memory before being unloaded.
//
// Example:
//
//	NewStore(WithUnloadFrequencyOption(time.Minute, time.Hour))
func WithUnloadFrequencyOption(ef time.Duration, uf time.Duration) StoreOption {
	return func(s *Store) {
		s.evictionFreq = ef
		s.unloadAfterTime = uf
	}
}

// WithPersistenceOption returns a StoreOption that sets up the persistence controllers
// for the Store. Multiple PersistenceControllers can be passed in.
//
// Example:
//
//	NewStore(WithPersistenceOption(persister1, persister2))
func WithPersistenceOption(persistence ...DataPersister) StoreOption {
	return func(s *Store) {
		s.persistence = persistence
	}
}

// WithNowFuncOption returns a StoreOption that allows you to configure the function
// used to fetch the current time. This is especially useful for testing scenarios where
// you want to control the time flow.
//
// Example:
//
//	NewStore(WithNowFuncOption(func() time.Time { return someFixedTime }))
func WithNowFuncOption(nowFunc func() time.Time) StoreOption {
	return func(s *Store) {
		s.nowFunc = nowFunc
	}
}
