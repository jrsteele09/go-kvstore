package kvstore_test

import (
	"fmt"
	"os"
	"path"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jrsteele09/go-kvstore/kvstore"
	"github.com/jrsteele09/go-kvstore/persistence"
	"github.com/stretchr/testify/require"
)

func TestStoreCrud(t *testing.T) {
	const key = "k1:102"
	const data = "TestStoreCrud"
	const folder = "TestStoreCrud"
	require.NoError(t, os.MkdirAll(folder, 0755))
	defer os.RemoveAll(folder)
	p, err := persistence.New(folder)
	require.NoError(t, err)
	buf, err := persistence.NewBuffer(p, 10)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithPersistenceOption(buf))
	require.NoError(t, err)
	require.NoError(t, s.Set(key, []byte(data)))
	b, err := s.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte(data), b)
	require.NoError(t, s.Delete(key))
	time.Sleep(100 * time.Millisecond)
	_, readErr := s.Get(key)
	require.Error(t, readErr)
}

func BenchmarkMemcacheDSetGetDelete(b *testing.B) {
	const folder = "TestStoreCrud"
	os.MkdirAll(folder, 0755)
	defer os.RemoveAll(folder)
	p, _ := persistence.New(folder)
	buf, _ := persistence.NewBuffer(p, 10)
	s, _ := kvstore.New(kvstore.WithPersistenceOption(buf))

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("Key-%d", i)
		value := fmt.Sprintf("Value-%d", i)
		s.Set(key, []byte(value))
		s.Get(key)
		s.Delete(key)
	}
}

func TestStoreCrudInteger(t *testing.T) {
	const key = "k1:200"
	const data = "10"
	const folder = "TestStoreCrudInteger"
	require.NoError(t, os.MkdirAll(folder, 0755))
	defer os.RemoveAll(folder)
	p, err := persistence.New(folder)
	require.NoError(t, err)
	buf, err := persistence.NewBuffer(p, 10)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithPersistenceOption(buf))
	require.NoError(t, err)
	require.NoError(t, s.Set(key, []byte(data)))
	b, err := s.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte(data), b)
	require.NoError(t, s.Delete(key))
	time.Sleep(100 * time.Millisecond)
	_, readErr := s.Get(key)
	require.Error(t, readErr)
}

func TestStoreIntegerCounter(t *testing.T) {
	const key = "k1:200"
	const folder = "TestStoreIntegerCounter"
	require.NoError(t, os.MkdirAll(folder, 0755))
	defer os.RemoveAll(folder)
	p, err := persistence.New(folder)
	require.NoError(t, err)
	buf, err := persistence.NewBuffer(p, 10)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithPersistenceOption(buf))
	require.NoError(t, err)
	require.NoError(t, s.SetCounterLimits(key, -1, 1))

	i, err := s.Counter(key, 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), i)

	i, err = s.Counter(key, -1)
	require.NoError(t, err)
	require.Equal(t, int64(-1), i)

	i, err = s.Counter(key, -1)
	require.Error(t, err)
	require.Equal(t, int64(0), i)

	s.SetCounterLimits(key, -1, 1)
	require.NoError(t, s.Set(key, []byte("0")))
	i, err = s.Counter(key, 2)
	require.Error(t, err)
	require.Equal(t, int64(0), i)

	i, err = s.Counter(key, -2)
	require.Error(t, err)
	require.Equal(t, int64(0), i)

	i, err = s.Counter(key, -1)
	require.NoError(t, err)
	require.Equal(t, int64(-1), i)

	s.Delete(key)
	time.Sleep(100 * time.Millisecond)
}

func TestEvictionCrud(t *testing.T) {
	const key = "k1:102"
	const data = "TestStoreCrud"
	const folder = "TestEvictionCrud"
	require.NoError(t, os.MkdirAll(folder, 0755))
	defer os.RemoveAll(folder)
	p, err := persistence.New(folder)
	require.NoError(t, err)
	buf, err := persistence.NewBuffer(p, 10)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithUnloadFrequencyOption(100*time.Millisecond, 0), kvstore.WithPersistenceOption(buf))
	require.NoError(t, err)
	require.NoError(t, s.Set(key, []byte(data)))
	s.SetTTL(key, 1)
	time.Sleep(1 * time.Second)
	_, readErr := s.Get(key)
	require.Error(t, readErr)
}

func TestMemoryUnload(t *testing.T) {
	const key = "k1:102"
	const data = "TestStoreCrud"
	const folder = "TestMemoryUnload"
	require.NoError(t, os.MkdirAll(folder, 0755))
	defer os.RemoveAll(folder)

	p, err := persistence.New(folder)
	require.NoError(t, err)
	buf, err := persistence.NewBuffer(p, 10)
	require.NoError(t, err)
	s, err := kvstore.New(
		kvstore.WithUnloadFrequencyOption(10*time.Millisecond, 100*time.Millisecond),
		kvstore.WithPersistenceOption(buf),
	)

	require.NoError(t, err)
	require.NoError(t, s.Set(key, []byte(data)))
	require.True(t, s.InMemory(key))
	time.Sleep(500 * time.Millisecond)
	require.False(t, s.InMemory(key))
	readData, readErr := s.Get(key)
	require.NoError(t, readErr)
	require.NotNil(t, readData)
}

func TestGettingKeys(t *testing.T) {
	const data = "TestStoreCrud"
	s, err := kvstore.New()
	require.NoError(t, err)

	keys := []string{"a", "b", "c", "d", "e"}

	for _, k := range keys {
		require.NoError(t, s.Set(k, []byte(data)))
	}

	retrievedKeys := s.Keys()
	sort.Slice(retrievedKeys, func(i, j int) bool {
		return retrievedKeys[i] < retrievedKeys[j]
	})

	for i := range retrievedKeys {
		require.Equal(t, retrievedKeys[i], keys[i])
	}
}

func TestPersistenceStartup(t *testing.T) {
	const key = "k1:103"
	const data = "TestStoreCrud"
	const folder = "TestPersistenceStartup"
	require.NoError(t, os.MkdirAll(folder, 0755))
	defer os.RemoveAll(folder)
	p, err := persistence.New(folder)
	require.NoError(t, err)
	buf, err := persistence.NewBuffer(p, 10)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithPersistenceOption(buf))
	require.NoError(t, err)
	require.NoError(t, s.Set(key, []byte(data)))
	time.Sleep(100 * time.Millisecond) // Wait for the write to happen
	p2, err := persistence.New(folder)
	require.NoError(t, err)
	buf2, err := persistence.NewBuffer(p2, 10)
	require.NoError(t, err)
	s2, err := kvstore.New(kvstore.WithPersistenceOption(buf2))
	require.NoError(t, err)
	bytes, err := s2.Get(key)
	require.NoError(t, err)
	require.Equal(t, data, string(bytes))

	require.NoError(t, s2.Delete(key))
}

func TestStoreCrudThreaded(t *testing.T) {
	const testFolder = "TestStoreCrudThreaded"
	const keyFormat = "Key:%d"
	const dataFormat = "Key%d-DataStore"
	const nRoutines = 100
	require.NoError(t, os.MkdirAll(testFolder, 0755))
	defer os.RemoveAll(testFolder)
	p, err := persistence.New(testFolder)
	require.NoError(t, err)
	buf, err := persistence.NewBuffer(p, 10)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithPersistenceOption(buf))
	require.NoError(t, err)

	var wg sync.WaitGroup

	// Store
	wg.Add(nRoutines)
	for i := 0; i < nRoutines; i++ {
		go func(n int) {
			defer wg.Done()
			s.Set(fmt.Sprintf(keyFormat, n), []byte(fmt.Sprintf(dataFormat, n)))
		}(i)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond) // Wait for the write to happen

	p2, err := persistence.New(testFolder)
	require.NoError(t, err)
	buf2, err := persistence.NewBuffer(p2, 10)
	require.NoError(t, err)
	s2, err := kvstore.New(kvstore.WithPersistenceOption(buf2))
	require.NoError(t, err)

	// Read
	wg.Add(nRoutines)
	for i := 0; i < nRoutines; i++ {
		go func(n int) {
			defer wg.Done()
			dataBytes, readErr := s2.Get(fmt.Sprintf(keyFormat, n))
			require.NoError(t, readErr)
			require.Equal(t, fmt.Sprintf(dataFormat, n), string(dataBytes))
		}(i)
	}

	// Delete
	wg.Wait()
	wg.Add(nRoutines)
	for i := 0; i < nRoutines; i++ {
		go func(n int) {
			defer wg.Done()
			deleteErr := s2.Delete(fmt.Sprintf(keyFormat, n))
			require.NoError(t, deleteErr)
		}(i)
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond) // Wait for Deletes to finish
}

func TestThreadedEviction(t *testing.T) {
	const testFolder = "TestThreadedEviction"
	const keyFormat = "Key:%d"
	const dataFormat = "Key%d-DataStore"
	const nRoutines = 100
	require.NoError(t, os.MkdirAll(testFolder, 0755))
	defer os.RemoveAll(testFolder)
	p, err := persistence.New(testFolder)
	require.NoError(t, err)
	buf, err := persistence.NewBuffer(p, 10)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithUnloadFrequencyOption(100*time.Millisecond, 0), kvstore.WithPersistenceOption(buf))
	require.NoError(t, err)

	var wg sync.WaitGroup

	// Store
	wg.Add(nRoutines)
	for i := 0; i < nRoutines; i++ {
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf(keyFormat, n)
			s.Set(key, []byte(fmt.Sprintf(dataFormat, n)))
			s.SetTTL(key, 1)
		}(i)
	}
	wg.Wait()
	time.Sleep(1 * time.Second)

	for i := 0; i < nRoutines; i++ {
		key := fmt.Sprintf(keyFormat, i)
		_, err := s.Get(key)
		require.Error(t, err)
	}
}

func TestLoadFailure(t *testing.T) {
	const testFolder = "TestLoadFailure"
	const failKey = "key1"
	defer os.RemoveAll(testFolder)
	os.MkdirAll(path.Join(testFolder, failKey), 0700)

	p, err := persistence.New(testFolder)
	require.NoError(t, err)
	buf, err := persistence.NewBuffer(p, 10)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithPersistenceOption(buf))
	require.NoError(t, err)

	rd, err := s.Get(failKey)
	require.Nil(t, rd)
	require.Error(t, err)

	require.NoError(t, s.Delete(failKey))
}

func TestMultiPersistence(t *testing.T) {
	const key = "k1:103"
	const data = "TestStoreCrud"
	const folder = "TestMultiPersistence"
	const backupFolder = "TestMultiPersistenceBackup"

	require.NoError(t, os.MkdirAll(folder, 0755))
	require.NoError(t, os.MkdirAll(backupFolder, 0755))
	defer func() {
		os.RemoveAll(folder)
		os.RemoveAll(backupFolder)
	}()

	p1, err := persistence.New(folder)
	require.NoError(t, err)
	buf1, err := persistence.NewBuffer(p1, 10)
	require.NoError(t, err)
	p2, err := persistence.New(backupFolder)
	require.NoError(t, err)
	buf2, err := persistence.NewBuffer(p2, 10)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithPersistenceOption(buf1), kvstore.WithPersistenceOption(buf2))
	require.NoError(t, err)
	require.NoError(t, s.Set(key, []byte(data)))
	time.Sleep(100 * time.Millisecond)
	p3, err := persistence.New(backupFolder)
	require.NoError(t, err)
	s2, err := kvstore.New(kvstore.WithPersistenceOption(p3))
	require.NoError(t, err)
	readData, err := s2.Get(key)
	require.NoError(t, err)
	require.Equal(t, data, string(readData))
}

func TestTTL(t *testing.T) {
	const key = "k1:101"
	const data = "TestTTL"
	const folder = "TestTTL"
	require.NoError(t, os.MkdirAll(folder, 0755))
	defer os.RemoveAll(folder)
	p, err := persistence.New(folder)
	require.NoError(t, err)
	s, err := kvstore.New(kvstore.WithPersistenceOption(p))

	require.NoError(t, err)
	require.NoError(t, s.Set(key, []byte(data)))
	s.SetTTL(key, 4)
	time.Sleep(1 * time.Second)
	s.Touch(key)
	ttl := s.TTL(key)

	require.Equal(t, kvstore.TTLType(4), ttl)
	require.NoError(t, s.Delete(key))
	time.Sleep(100 * time.Millisecond)
}
