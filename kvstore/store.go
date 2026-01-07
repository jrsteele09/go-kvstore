package kvstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var nowFunc = time.Now

// TTLType defines the time-to-live (TTL) in seconds for a key/data pair.
type TTLType int64

// Constants for special TTL values.
const (
	// TTLKeyNotExist used to signal that the key does not exist when querying a TTL of a key
	TTLKeyNotExist TTLType = -2 // Indicates that a queried key does not exist.
	TTLNoExpirySet TTLType = -1 // Indicates that a key does not have an expiry time.
)

// Error definitions for common error cases.
var (
	// ErrNotFound returned when a key is not found during read or delete operations.
	ErrNotFound = errors.New("key not found")

	// ErrKeyInvalid returned when a key contains invalid characters.
	ErrKeyInvalid = errors.New("key contains invalid characters")
)

// Store represents the key-value storage system.
// It is thread-safe and allows for optional data persistence.
type Store struct {
	data            map[string]*ValueItem
	persistence     []DataPersister
	evictionFreq    time.Duration
	unloadAfterTime time.Duration
	lock            sync.RWMutex
	ctx             context.Context
	cancelFunc      context.CancelFunc
	wg              sync.WaitGroup
}

// New initializes a new Store with optional configurations.
// It takes a variadic number of StoreOption functions to customize its behavior.
func New(options ...StoreOption) (*Store, error) {
	store := &Store{
		data:            make(map[string]*ValueItem),
		persistence:     make([]DataPersister, 0),
		evictionFreq:    time.Minute,
		unloadAfterTime: 0,
	}

	for _, opt := range options {
		opt(store)
	}

	if err := store.initPersistence(); err != nil {
		return nil, err
	}
	store.ctx, store.cancelFunc = context.WithCancel(context.Background())
	store.wg.Add(1)
	go store.evictionController()
	return store, nil
}

// Close stops the internal cache management routines and closes all persistence layers.
func (s *Store) Close() {
	s.cancelFunc()
	s.wg.Wait()

	// Close all persistence layers
	for _, p := range s.persistence {
		p.Close()
	}
}

// Set stores a key-value pair into the Store.
func (s *Store) Set(key string, value []byte) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.setValue(key, value)
}

// Get retrieves the value associated with a key from the Store.
func (s *Store) Get(key string) ([]byte, error) {
	if !KeyValid(key) {
		return nil, ErrKeyInvalid
	}

	var expired, dataloaded bool
	var loadedData []byte

	s.lock.RLock()
	mv, ok := s.data[key]
	if ok {
		expired = mv.expired(nowFunc())
		dataloaded = mv.dataLoaded
		if dataloaded {
			loadedData = make([]byte, len(mv.Data))
			copy(loadedData, mv.Data)
		}
	}
	s.lock.RUnlock()

	if !ok || expired {
		return nil, ErrNotFound
	}

	if dataloaded {
		return loadedData, nil
	}

	data, err := s.readFromFirstStore(key)
	if err != nil {
		return nil, fmt.Errorf("Store.Get s.readFromFirstStore: %w", err)
	}

	s.Set(key, data)
	return data, nil
}

// Delete removes a key and its value from the Store.
func (s *Store) Delete(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.delete(key)
}

// InMemory checks if the value for a given key is loaded into memory.
func (s *Store) InMemory(key string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if _, ok := s.data[key]; !ok {
		return false
	}
	return s.data[key].dataLoaded
}

// Keys returns a slice of all keys currently in the Store.
func (s *Store) Keys() []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// QueryKeys returns the keys that have been created between a time period
func (s *Store) QueryKeys(from, to time.Time) ([]string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k, v := range s.data {
		if v.expired(nowFunc()) {
			continue
		}
		if !v.Ts.Before(from) && !v.Ts.After(to) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// SetTTL sets the time-to-live (TTL) for a specific key.
func (s *Store) SetTTL(key string, ttl int64) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	return s.setTTL(key, TTLType(ttl))
}

// TTL retrieves the remaining TTL for a given key.
func (s *Store) TTL(key string) TTLType {
	if !KeyValid(key) {
		return TTLKeyNotExist
	}

	s.lock.RLock()
	if _, ok := s.data[key]; !ok {
		s.lock.RUnlock()
		return TTLKeyNotExist
	}
	mv := s.data[key]

	ts := mv.Ts
	ttl := mv.TTL
	s.lock.RUnlock()

	expireTime := ts.Add(time.Duration(ttl) * time.Second)
	remaining := expireTime.Sub(nowFunc()).Seconds()
	remaining = math.Ceil(remaining)
	if remaining < 0 {
		remaining = 0
	}
	return TTLType(remaining)
}

// Touch updates the last-accessed time for a given key.
func (s *Store) Touch(key string) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	mv, ok := s.data[key]
	if !ok || mv.expired(nowFunc()) {
		return ErrNotFound
	}
	mv.Ts = nowFunc()
	if err := s.persistData(key); err != nil {
		return fmt.Errorf("Store.Touch s.persist: %w", err)
	}
	return nil
}

// Counter initializes or updates a counter value for a given key.
func (s *Store) Counter(key string, delta int64) (int64, error) {
	if !KeyValid(key) {
		return 0, ErrKeyInvalid
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	var (
		mv *ValueItem
		ok bool
	)
	if mv, ok = s.data[key]; !ok {
		// Create new counter with default constraints
		mv = NewValueItem([]byte(fmt.Sprintf("%d", delta)), nowFunc())
		mv.Counter = &CounterConstraints{Min: math.MinInt64, Max: math.MaxInt64}
		s.data[key] = mv
		if err := s.persistData(key); err != nil {
			return 0, fmt.Errorf("Store.Counter s.persist: %w", err)
		}
		return delta, nil
	}
	i, err := strconv.ParseInt(string(mv.Data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Store.Counter strconv.ParseInt: %w", err)
	}
	if mv.Counter == nil {
		return 0, errors.New("Store.Counter counter boundaries not set")
	}
	i += delta
	if i > mv.Counter.Max {
		return 0, errors.New("Store.Counter maximum value reached")
	}
	if i < mv.Counter.Min {
		return 0, errors.New("Store.Counter minimum value reached")
	}
	if err := s.setValue(key, []byte(fmt.Sprintf("%d", i))); err != nil {
		return 0, fmt.Errorf("Store.Counter setData: %w", err)
	}
	return i, nil
}

// SetCounterLimits sets the min/max limits for a counter associated with a key.
func (s *Store) SetCounterLimits(key string, min, max int64) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}
	var (
		mv *ValueItem
		ok bool
	)
	s.lock.Lock()
	defer s.lock.Unlock()

	if mv, ok = s.data[key]; !ok {
		// Create new counter starting at min value
		mv = NewValueItem([]byte(fmt.Sprintf("%d", min)), nowFunc())
		mv.Counter = &CounterConstraints{Min: min, Max: max}
		s.data[key] = mv
		return s.persistData(key)
	}
	if mv.Counter == nil {
		return fmt.Errorf("Store.SetCounterLimits key \"%s\" is not a counter", key)
	}
	mv.Counter.Max = max
	mv.Counter.Min = min
	return s.persistData(key)
}

func (s *Store) setValue(key string, data []byte) error {
	mv, ok := s.data[key]
	if !ok {
		mv = NewValueItem(data, nowFunc())
	}

	mv.SetData(data)
	mv.Ts = nowFunc()
	s.data[key] = mv
	return s.persistData(key)
}

func (s *Store) delete(key string) error {
	if _, ok := s.data[key]; !ok {
		return ErrNotFound
	}
	delete(s.data, key)

	var returnError error
	for _, p := range s.persistence {
		if err := p.Delete(key); err != nil {
			returnError = fmt.Errorf("p.Delete: %w", err)
		}
	}
	return returnError
}

func (s *Store) readFromFirstStore(key string) ([]byte, error) {
	if len(s.persistence) == 0 {
		return nil, nil
	}

	mv, err := s.persistence[0].Read(key, true)
	if err != nil {
		return nil, err
	}
	s.lock.Lock()
	if existing, ok := s.data[key]; ok && !existing.dataLoaded {
		s.data[key] = mv
	}
	s.lock.Unlock()
	return mv.Data, nil
}

func (s *Store) setTTL(key string, ttl TTLType) error {
	if _, ok := s.data[key]; !ok {
		return ErrNotFound
	}
	s.data[key].TTL = ttl
	if err := s.persistData(key); err != nil {
		return fmt.Errorf("store.setTTL s.persist: %w", err)
	}
	return nil
}

func (s *Store) initPersistence() error {
	if len(s.persistence) == 0 {
		return nil
	}

	keys, err := s.persistence[0].Keys()
	if err != nil {
		log.Info().Msgf("store.InitialisePersistenceControllers %s", err.Error())
		return nil
	}

	for _, k := range keys {
		mv, err := s.persistence[0].Read(k, false)
		if err != nil {
			s.data[k] = &ValueItem{
				Ts:         nowFunc(),
				dataLoaded: false,
			}
			continue
		}
		s.data[k] = mv
	}

	return nil
}

func (s *Store) persistData(key string) error {
	if len(s.persistence) == 0 {
		return nil
	}

	if _, ok := s.data[key]; !ok {
		return fmt.Errorf("persist key: %s does not exist", key)
	}

	mv := s.data[key]
	for _, d := range s.persistence {
		if err := d.Write(key, mv); err != nil {
			return fmt.Errorf("Store.persist Write error: %w", err)
		}
	}
	return nil
}

func (s *Store) evictionController() {
	defer s.wg.Done()
	if s.evictionFreq <= 0 {
		return
	}

	timer := time.NewTimer(s.evictionFreq)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			s.runEvictionCheck()
			timer.Reset(s.evictionFreq)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Store) runEvictionCheck() {
	s.lock.RLock()
	timeNow := nowFunc()
	var deletionKeys, unloadKeys []string
	for k, v := range s.data {
		if v.expired(timeNow) {
			deletionKeys = append(deletionKeys, k)
		} else if v.unload(timeNow, s.unloadAfterTime) && len(s.persistence) > 0 {
			unloadKeys = append(unloadKeys, k)
		}
	}
	s.lock.RUnlock()

	// Nothing to do
	if len(deletionKeys) == 0 && len(unloadKeys) == 0 {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// Delete expired keys
	for _, k := range deletionKeys {
		if err := s.delete(k); err != nil {
			log.Error().Str("key", k).Err(err).Msg("kvstore eviction: failed to delete key")
		}
	}

	// Unload data from memory (but keep metadata)
	for _, k := range unloadKeys {
		if v, exists := s.data[k]; exists {
			v.unloadData()
		}
	}
}
