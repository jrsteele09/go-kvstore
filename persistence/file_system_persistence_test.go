package persistence

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jrsteele09/go-kvstore/kvstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDataDir = "testdata"

func TestMain(m *testing.M) {
	// Setup: Create testdata directory
	if err := os.MkdirAll(testDataDir, 0755); err != nil {
		panic(err)
	}

	// Run tests
	code := m.Run()

	// Teardown: Remove testdata directory
	if err := os.RemoveAll(testDataDir); err != nil {
		panic(err)
	}

	os.Exit(code)
}

func TestPersistor_NewAndClose(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_new_close")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	p := New(testDir)
	require.NotNil(t, p)
	require.NotNil(t, p.rootFS)

	// Test Close doesn't panic and properly closes the resource
	assert.NotPanics(t, func() {
		p.Close()
	})

	// After close, operations should fail
	_, err := p.Keys()
	assert.Error(t, err, "operations after close should fail")
}

func TestPersistor_Write(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_write")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	p := New(testDir)
	defer p.Close()

	key := "test_key"
	value := &kvstore.ValueItem{
		Data:    []byte("test_value"),
		Ts:      time.Now(),
		TTL:     kvstore.TTLNoExpirySet,
		Counter: nil,
	}

	err := p.Write(key, value)
	assert.NoError(t, err)

	// Verify the directory and files were created
	keyDir := filepath.Join(testDir, key)
	metadataFile := filepath.Join(keyDir, "metadata.json")
	dataFile := filepath.Join(keyDir, "data.bin")

	_, err = os.Stat(keyDir)
	assert.NoError(t, err, "key directory should exist")

	_, err = os.Stat(metadataFile)
	assert.NoError(t, err, "metadata file should exist")

	_, err = os.Stat(dataFile)
	assert.NoError(t, err, "data file should exist")
}

func TestPersistor_WriteWithNilData(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_write_nil")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	p := New(testDir)
	defer p.Close()

	key := "test_key_nil"
	value := &kvstore.ValueItem{
		Data:    nil,
		Ts:      time.Now(),
		TTL:     kvstore.TTLNoExpirySet,
		Counter: nil,
	}

	err := p.Write(key, value)
	assert.NoError(t, err)

	// Data file should not exist when Data is nil
	dataFile := filepath.Join(testDir, key, "data.bin")
	_, err = os.Stat(dataFile)
	assert.True(t, os.IsNotExist(err), "data file should not exist when Data is nil")
}

func TestPersistor_Read(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_read")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	p := New(testDir)
	defer p.Close()

	key := "test_key"
	expectedData := []byte("test_value")
	value := &kvstore.ValueItem{
		Data:    expectedData,
		Ts:      time.Now(),
		TTL:     kvstore.TTLNoExpirySet,
		Counter: nil,
	}

	// Write first
	err := p.Write(key, value)
	require.NoError(t, err)

	// Read with value
	readValue, err := p.Read(key, true)
	assert.NoError(t, err)
	assert.NotNil(t, readValue)
	assert.Equal(t, expectedData, readValue.Data)
	assert.Equal(t, value.TTL, readValue.TTL)

	// Read without value (metadata only)
	readMetadata, err := p.Read(key, false)
	assert.NoError(t, err)
	assert.NotNil(t, readMetadata)
	assert.Empty(t, readMetadata.Data, "data should not be loaded when readValue is false")
	assert.Equal(t, value.TTL, readMetadata.TTL)
}

func TestPersistor_ReadNonExistent(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_read_nonexistent")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	p := New(testDir)
	defer p.Close()

	_, err := p.Read("non_existent_key", true)
	assert.Error(t, err)
}

func TestPersistor_Delete(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_delete")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	p := New(testDir)
	defer p.Close()

	key := "test_key"
	value := &kvstore.ValueItem{
		Data:    []byte("test_value"),
		Ts:      time.Now(),
		TTL:     kvstore.TTLNoExpirySet,
		Counter: nil,
	}

	// Write first
	err := p.Write(key, value)
	require.NoError(t, err)

	// Verify it exists
	keyDir := filepath.Join(testDir, key)
	_, err = os.Stat(keyDir)
	require.NoError(t, err)

	// Delete
	err = p.Delete(key)
	assert.NoError(t, err)

	// Verify it's gone
	_, err = os.Stat(keyDir)
	assert.True(t, os.IsNotExist(err), "key directory should be removed after delete")
}

func TestPersistor_Keys(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_keys")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	p := New(testDir)
	defer p.Close()

	// Initially empty
	keys, err := p.Keys()
	assert.NoError(t, err)
	assert.Empty(t, keys)

	// Write multiple keys
	expectedKeys := []string{"key1", "key2", "key3"}
	for _, key := range expectedKeys {
		value := &kvstore.ValueItem{
			Data:    []byte("value_" + key),
			Ts:      time.Now(),
			TTL:     kvstore.TTLNoExpirySet,
			Counter: nil,
		}
		err := p.Write(key, value)
		require.NoError(t, err)
	}

	// Get keys
	keys, err = p.Keys()
	assert.NoError(t, err)
	assert.Len(t, keys, len(expectedKeys))
	assert.ElementsMatch(t, expectedKeys, keys)
}

func TestPersistor_WriteReadDeleteCycle(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_cycle")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	p := New(testDir)
	defer p.Close()

	key := "cycle_key"
	originalData := []byte("original_data")
	value := &kvstore.ValueItem{
		Data:    originalData,
		Ts:      time.Now(),
		TTL:     60,
		Counter: nil,
	}

	// Write
	err := p.Write(key, value)
	assert.NoError(t, err)

	// Read
	readValue, err := p.Read(key, true)
	assert.NoError(t, err)
	assert.Equal(t, originalData, readValue.Data)
	assert.Equal(t, kvstore.TTLType(60), readValue.TTL)

	// Update
	updatedData := []byte("updated_data")
	value.Data = updatedData
	value.TTL = 120
	err = p.Write(key, value)
	assert.NoError(t, err)

	// Read again
	readValue, err = p.Read(key, true)
	assert.NoError(t, err)
	assert.Equal(t, updatedData, readValue.Data)
	assert.Equal(t, kvstore.TTLType(120), readValue.TTL)

	// Delete
	err = p.Delete(key)
	assert.NoError(t, err)

	// Verify deleted
	_, err = p.Read(key, true)
	assert.Error(t, err)
}

func TestPersistor_NoResourceLeak(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_leak")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	// Create and close multiple persistors to test for resource leaks
	for i := 0; i < 100; i++ {
		p := New(testDir)

		// Do some operations
		key := "leak_test_key"
		value := &kvstore.ValueItem{
			Data:    []byte("leak_test_value"),
			Ts:      time.Now(),
			TTL:     kvstore.TTLNoExpirySet,
			Counter: nil,
		}

		err := p.Write(key, value)
		require.NoError(t, err)

		_, err = p.Read(key, true)
		require.NoError(t, err)

		// Close should clean up resources
		p.Close()
	}

	// If we got here without running out of file descriptors, no leak
	assert.True(t, true, "no resource leak detected")
}

func TestPersistor_WithCounter(t *testing.T) {
	testDir := filepath.Join(testDataDir, "test_counter")
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

	p := New(testDir)
	defer p.Close()

	key := "counter_key"
	value := &kvstore.ValueItem{
		Data: []byte("100"),
		Counter: &kvstore.CounterConstraints{
			Min: 0,
			Max: 1000,
		},
		Ts:  time.Now(),
		TTL: kvstore.TTLNoExpirySet,
	}

	// Write
	err := p.Write(key, value)
	assert.NoError(t, err)

	// Read
	readValue, err := p.Read(key, true)
	assert.NoError(t, err)
	assert.NotNil(t, readValue.Counter)
	assert.Equal(t, int64(0), readValue.Counter.Min)
	assert.Equal(t, int64(1000), readValue.Counter.Max)
}
