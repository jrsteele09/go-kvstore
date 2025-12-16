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
	ErrNotFound error = errors.New("key not found")

	// ErrKeyInvalid returned when a key contains invalid characters.
	ErrKeyInvalid error = errors.New("key contains invalid characters")
)

// Store represents the key-value storage system.
// It is thread-safe and allows for optional data persistence.
type Store struct {
	lock            sync.RWMutex
	nowFunc         func() time.Time
	data            map[string]*ValueItem
	persistence     []DataPersister
	evictionFreq    time.Duration
	unloadAfterTime time.Duration
	ctx             context.Context
	cancelFunc      context.CancelFunc
}

// New initializes a new Store with optional configurations.
// It takes a variadic number of StoreOption functions to customize its behavior.
func New(options ...StoreOption) (*Store, error) {
	store := &Store{
		data:            make(map[string]*ValueItem),
		persistence:     make([]DataPersister, 0),
		evictionFreq:    0,
		unloadAfterTime: 0,
		nowFunc:         time.Now,
	}

	for _, opt := range options {
		opt(store)
	}

	store.ctx, store.cancelFunc = context.WithCancel(context.Background())

	if err := store.initPersistence(); err != nil {
		return nil, err
	}
	go store.evictionController()
	return store, nil
}

// Close stops the internal cache management routines.
func (kv *Store) Close() {
	kv.cancelFunc()
}

// Set stores a key-value pair into the Store.
func (kv *Store) Set(key string, value []byte) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}
	kv.lock.Lock()
	defer kv.lock.Unlock()
	return kv.setValue(key, value)
}

// Get retrieves the value associated with a key from the Store.
func (kv *Store) Get(key string) ([]byte, error) {
	if !KeyValid(key) {
		return nil, ErrKeyInvalid
	}

	kv.lock.RLock()
	mv, ok := kv.data[key]
	kv.lock.RUnlock()

	if !ok || mv.expired(kv.nowFunc()) {
		return nil, ErrNotFound
	}

	if mv.dataLoaded {
		return mv.Data, nil
	}
	data, err := kv.readFromFirstStore(key)
	if err != nil {
		return nil, fmt.Errorf("Store.Get kv.readFromFirstStore: %w", err)
	}

	kv.Set(key, data)
	return data, nil
}

// Delete removes a key and its value from the Store.
func (kv *Store) Delete(key string) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	return kv.delete(key)
}

// InMemory checks if the value for a given key is loaded into memory.
func (kv *Store) InMemory(key string) bool {
	kv.lock.RLock()
	defer kv.lock.RUnlock()
	if _, ok := kv.data[key]; !ok {
		return false
	}
	return kv.data[key].dataLoaded
}

// Keys returns a slice of all keys currently in the Store.
func (kv *Store) Keys() ([]string, error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()
	keys := make([]string, 0)
	for k := range kv.data {
		keys = append(keys, k)
	}
	return keys, nil
}

// QueryKeys returns the keys that have been created between a time period
func (kv *Store) QueryKeys(from, to time.Time) ([]string, error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()

	keys := make([]string, 0)
	for k, v := range kv.data {
		if v.expired(kv.nowFunc()) {
			continue
		}
		if (v.Ts.Equal(from) || v.Ts.After(from)) && (v.Ts.Equal(to) || v.Ts.Before(to)) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// SetTTL sets the time-to-live (TTL) for a specific key.
func (kv *Store) SetTTL(key string, ttl int64) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}

	kv.lock.Lock()
	defer kv.lock.Unlock()
	return kv.setTTL(key, TTLType(ttl))
}

// TTL retrieves the remaining TTL for a given key.
func (kv *Store) TTL(key string) TTLType {
	if !KeyValid(key) {
		return TTLKeyNotExist
	}

	kv.lock.RLock()
	defer kv.lock.RUnlock()
	if _, ok := kv.data[key]; !ok {
		return TTLKeyNotExist
	}
	mv := kv.data[key]
	expireTime := mv.Ts.Add(time.Duration(mv.TTL) * time.Second)
	ttl := expireTime.Sub(kv.nowFunc()).Seconds()
	ttl = math.Ceil(ttl)
	if ttl < 0 {
		ttl = 0
	}
	return TTLType(ttl)
}

// Touch updates the last-accessed time for a given key.
func (kv *Store) Touch(key string) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}

	kv.lock.Lock()
	defer kv.lock.Unlock()
	mv, ok := kv.data[key]
	if !ok || mv.expired(kv.nowFunc()) {
		return ErrNotFound
	}
	mv.Ts = kv.nowFunc()
	if err := kv.persistData(key); err != nil {
		return fmt.Errorf("Store.Touch kv.persist: %w", err)
	}
	return nil
}

// Counter initializes or updates a counter value for a given key.
func (kv *Store) Counter(key string, delta int64) (int64, error) {
	if !KeyValid(key) {
		return 0, ErrKeyInvalid
	}

	kv.lock.Lock()
	defer kv.lock.Unlock()

	var mv *ValueItem
	var ok bool
	if mv, ok = kv.data[key]; !ok {
		intStr := fmt.Sprintf("%d", delta)
		if err := kv.setValue(key, []byte(intStr)); err != nil {
			return 0, fmt.Errorf("Store.Counter kv.setData: %w", err)
		}
		return delta, nil
	}
	i, err := strconv.ParseInt(string(mv.Data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Store.Counter strconv.ParseInt: %w", err)
	}
	if mv.Counter == nil {
		return 0, fmt.Errorf("Store.Counter counter boundaries not set: %w", err)
	}
	i += delta
	if i > mv.Counter.Max {
		return 0, errors.New("Store.Counter maximum value reached")
	} else if i < mv.Counter.Min {
		return 0, errors.New("Store.Counter minimum value reached")
	}
	if err := kv.setValue(key, []byte(fmt.Sprintf("%d", i))); err != nil {
		return 0, fmt.Errorf("Store.Counter setData: %w", err)
	}
	return i, nil
}

// SetCounterLimits sets the min/max limits for a counter associated with a key.
func (kv *Store) SetCounterLimits(key string, min, max int64) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}
	var mv *ValueItem
	var ok bool
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if mv, ok = kv.data[key]; !ok {
		intStr := fmt.Sprintf("%d", min)
		if err := kv.setValue(key, []byte(intStr)); err != nil {
			return fmt.Errorf("Store.SetCounterLimits kv.setValue: %w", err)
		}
		mv = kv.data[key]
	}
	if mv.Counter == nil {
		return fmt.Errorf("Store.SetCounterLimits key \"%s\" is not a counter", key)
	}
	kv.data[key].Counter.Max = max
	kv.data[key].Counter.Min = min
	return kv.persistData(key)
}

func (kv *Store) setValue(key string, data []byte) error {
	mv, ok := kv.data[key]
	if !ok {
		mv = NewValueItem(data, kv.nowFunc())
	}

	if err := mv.SetData(data); err != nil {
		return fmt.Errorf("Store.setData mv.SetData: %w", err)
	}
	mv.Ts = kv.nowFunc()
	kv.data[key] = mv
	return kv.persistData(key)
}

func (kv *Store) delete(key string) error {
	if _, ok := kv.data[key]; !ok {
		return ErrNotFound
	}
	delete(kv.data, key)

	var returnError error
	for _, p := range kv.persistence {
		if err := p.Delete(key); err != nil {
			returnError = fmt.Errorf("p.Delete: %w", err)
		}
	}
	return returnError
}

func (kv *Store) readFromFirstStore(key string) ([]byte, error) {
	if len(kv.persistence) == 0 {
		return nil, nil
	}

	mv, err := kv.persistence[0].Read(key, true)
	if err != nil {
		return nil, err
	}
	kv.lock.Lock()
	kv.data[key] = mv
	kv.lock.Unlock()
	return mv.Data, nil
}

func (kv *Store) setTTL(key string, ttl TTLType) error {
	if _, ok := kv.data[key]; !ok {
		return ErrNotFound
	}
	kv.data[key].TTL = ttl
	if err := kv.persistData(key); err != nil {
		return fmt.Errorf("store.setTTL kv.persist: %w", err)
	}
	return nil
}

func (kv *Store) initPersistence() error {
	if len(kv.persistence) == 0 {
		return nil
	}

	keys, err := kv.persistence[0].Keys()
	if err != nil {
		log.Info().Msgf("store.InitialisePersistenceControllers %s", err.Error())
		return nil
	}

	for _, k := range keys {
		mv, err := kv.persistence[0].Read(k, false)
		if err != nil {
			kv.data[k] = &ValueItem{
				Ts:         time.Now(),
				dataLoaded: false,
			}
			continue
		}
		kv.data[k] = mv
	}

	return nil
}

func (kv *Store) persistData(key string) error {
	if len(kv.persistence) == 0 {
		return nil
	}

	if _, ok := kv.data[key]; !ok {
		return fmt.Errorf("persist key: %s does not exist", key)
	}

	mv := kv.data[key]
	for _, d := range kv.persistence {
		if err := d.Write(key, mv); err != nil {
			return fmt.Errorf("Store.persist Write error: %w", err)
		}
	}
	return nil
}

func (kv *Store) evictionController() {
	if kv.evictionFreq <= 0 {
		return
	}

	timer := time.NewTimer(kv.evictionFreq)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			kv.runEvictionCheck()
			timer.Reset(kv.evictionFreq)
		case <-kv.ctx.Done():
			return
		}
	}
}

func (kv *Store) runEvictionCheck() {
	kv.lock.RLock()
	timeNow := kv.nowFunc()
	deletionKeys := make([]string, 0)
	unloadKeys := make([]string, 0)
	for k, v := range kv.data {
		if v.expired(timeNow) {
			deletionKeys = append(deletionKeys, k)
		} else if v.unload(timeNow, kv.unloadAfterTime) && len(kv.persistence) > 0 {
			unloadKeys = append(unloadKeys, k)
		}
	}
	kv.lock.RUnlock()
	kv.lock.Lock()
	for _, k := range deletionKeys {
		if err := kv.delete(k); err != nil {
			log.Error().Msgf("[kvstore eviction] error deleting key %s error: %s", k, err.Error())
		}
	}
	for _, k := range unloadKeys {
		kv.data[k].dataLoaded = false
		kv.data[k].Data = nil
	}
	kv.lock.Unlock()
}
