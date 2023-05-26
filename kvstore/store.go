package kvstore

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// TTLType is the number of seconds for the key / data to be alive
type TTLType int64

const (
	// TTLKeyNotExist used to signal that the key does not exist when querying a TTL of a key
	TTLKeyNotExist TTLType = -2
	// TTLNoExpirySet used to signal that the key does not have an expiry when querying the TTL of a key
	TTLNoExpirySet TTLType = -1
)

var (
	// ErrNotFoundErr is returned when a key is not found when Reading or deleting
	ErrNotFoundErr error = errors.New("key not found")

	// ErrKeyInvalid is returned when the key contains invalid characters
	ErrKeyInvalid error = errors.New("key contains invalid characters")
)

// Store is the structure that handles concurrent access to the map
type Store struct {
	lock            sync.RWMutex
	nowFunc         func() time.Time
	data            map[string]*ValueItem
	persistence     []PersistenceController
	evictionFreq    time.Duration
	unloadAfterTime time.Duration
	ctx             context.Context
	cancelFunc      context.CancelFunc
}

// NewStore initializes and sets up a new Store with customizable settings.
// These settings encompass the rate at which the store scans for values to be evicted, as well as the frequency at which values are purged from memory.
// For persistent entities, the store uses the first item in the list for initialization upon startup, thus enabling the reload of persisted key-value pairs after a restart.
func NewStore(options ...StoreOption) (*Store, error) {
	ds := &Store{
		data:            make(map[string]*ValueItem),
		persistence:     make([]PersistenceController, 0),
		evictionFreq:    0,
		unloadAfterTime: 0,
		nowFunc:         time.Now,
	}

	for _, opt := range options {
		opt(ds)
	}

	ds.ctx, ds.cancelFunc = context.WithCancel(context.Background())

	if err := ds.initialisePersistenceControllers(); err != nil {
		return ds, err
	}
	go ds.cacheController()
	return ds, nil
}

// Close cancels the internal context, stopping the cache controller
func (kv *Store) Close() {
	kv.cancelFunc()
}

// Set sets a keys with a value
func (kv *Store) Set(key string, value []byte) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}
	kv.lock.Lock()
	defer kv.lock.Unlock()
	return kv.setData(key, value)
}

// Get gets the value associated with a key
func (kv *Store) Get(key string) ([]byte, error) {
	if !KeyValid(key) {
		return nil, ErrKeyInvalid
	}

	kv.lock.RLock()
	mv, ok := kv.data[key]
	kv.lock.RUnlock()

	if !ok || mv.expired(kv.nowFunc()) {
		return nil, ErrNotFoundErr
	}

	if mv.dataLoaded {
		return mv.Data, nil
	}

	return kv.readFromFirstStore(key)
}

// Delete deletes the data associated with a specific key
func (kv *Store) Delete(key string) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	return kv.delete(key)
}

// InMemory returns true if the data is loaded in memory
func (kv *Store) InMemory(key string) bool {
	kv.lock.RLock()
	defer kv.lock.RUnlock()
	if _, ok := kv.data[key]; !ok {
		return false
	}
	return kv.data[key].dataLoaded
}

// Keys returns all the keys in the map
func (kv *Store) Keys() ([]string, error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()
	keys := make([]string, len(kv.data))
	idx := 0
	for k := range kv.data {
		keys[idx] = k
		idx++
	}
	return keys, nil
}

// QueryKeys returns the keys that have been created between a time period
func (kv *Store) QueryKeys(from, to time.Time) ([]string, error) {
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

// SetTTL sets the number of seconds until the key is evicted from the cache.
func (kv *Store) SetTTL(key string, ttl int64) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}

	kv.lock.Lock()
	defer kv.lock.Unlock()
	return kv.setTTL(key, TTLType(ttl))
}

// TTL Returns the number of seconds remaining
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

// Touch updates the timestamp
func (kv *Store) Touch(key string) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}

	kv.lock.Lock()
	defer kv.lock.Unlock()
	mv, ok := kv.data[key]
	if !ok || mv.expired(kv.nowFunc()) {
		return ErrNotFoundErr
	}
	mv.Ts = kv.nowFunc()
	if err := kv.persist(key); err != nil {
		return errors.Wrap(err, "Store.Touch kv.persist")
	}
	return nil
}

// Counter initialises a key to a specific counter value
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
		if err := kv.setData(key, []byte(intStr)); err != nil {
			return 0, errors.Wrap(err, "Store.Counter kv.setData")
		}
		return delta, nil
	}
	i, err := strconv.ParseInt(string(mv.Data), 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "Store.Counter strconv.ParseInt")
	}
	if mv.Counter == nil {
		return 0, errors.Wrap(err, "Store.Counter counter boundaries not set")
	}
	i += delta
	if i > mv.Counter.Max {
		return 0, errors.New("Store.Counter maximum value reached")
	} else if i < mv.Counter.Min {
		return 0, errors.New("Store.Counter minimum value reached")
	}
	if err := kv.setData(key, []byte(fmt.Sprintf("%d", i))); err != nil {
		return 0, errors.New("Store.Counter minimum value reached")
	}
	return i, nil
}

// SetCounterLimits sets the max / min values that a counter can reach
func (kv *Store) SetCounterLimits(key string, min, max int64) error {
	if !KeyValid(key) {
		return ErrKeyInvalid
	}
	var mv *ValueItem
	var ok bool
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if mv, ok = kv.data[key]; !ok {
		return fmt.Errorf("Store.SetCounterLimits key \"%s\" does not exist", key)
	}
	if mv.Counter == nil {
		return fmt.Errorf("Store.SetCounterLimits key \"%s\" is not a counter", key)
	}
	kv.data[key].Counter.Max = max
	kv.data[key].Counter.Min = min
	return kv.persist(key)
}

func (kv *Store) setData(key string, data []byte) error {
	mv, ok := kv.data[key]
	if !ok {
		mv = NewValueItem(data, kv.nowFunc())
	}

	if err := mv.SetData(data, kv.nowFunc()); err != nil {
		return errors.Wrap(err, "Store.get mv.SetData")
	}
	kv.data[key] = mv
	return kv.persist(key)
}

func (kv *Store) delete(key string) error {
	if _, ok := kv.data[key]; !ok {
		return ErrNotFoundErr
	}
	delete(kv.data, key)

	var returnError error
	for _, p := range kv.persistence {
		if err := p.Delete(key); err != nil {
			returnError = errors.Wrap(err, "p.Delete")
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
		return ErrNotFoundErr
	}
	kv.data[key].TTL = ttl
	if err := kv.persist(key); err != nil {
		return errors.Wrap(err, "store.setTTL kv.persist")
	}
	return nil
}

func (kv *Store) initialisePersistenceControllers() error {
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

func (kv *Store) persist(key string) error {
	if len(kv.persistence) == 0 {
		return nil
	}

	if _, ok := kv.data[key]; !ok {
		return fmt.Errorf("persist key: %s does not exist", key)
	}

	mv := kv.data[key]
	var returnError error
	for _, d := range kv.persistence {
		if err := d.Write(key, mv); err != nil {
			log.Error().Msgf("Store.persist d.Write error: %v", err)
			returnError = errors.Wrap(err, "d.Write")
		}
	}
	return returnError
}

func (kv *Store) cacheController() {
	if kv.evictionFreq <= 0 {
		return
	}

	timer := time.NewTimer(kv.evictionFreq)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			log.Info().Msg("[kvstore cacheController] timer expired")
			kv.eviction()
			timer.Reset(kv.evictionFreq)
		case <-kv.ctx.Done():
			log.Info().Msg("[kvstore cacheController] cancelled")
			return
		}
	}
}

func (kv *Store) eviction() {
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
