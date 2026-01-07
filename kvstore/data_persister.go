package kvstore

// DataPersister defines the methods that must be implemented for data persistence in a key-value store.
// Multiple DataPersisters can be associated with a single store to allow for various persistence strategies.
//
// Write: Persists a key-value pair.
//
// Read: Retrieves the value associated with a key.
// If 'readValue' is true, the actual value is fetched; otherwise, metadata is fetched.
//
// Delete: Removes a key-value pair.
//
// Keys: Retrieves all keys from the store.
type DataPersister interface {

	// Write persists the ValueItem associated with the given key.
	Write(key string, data *ValueItem) error

	// Read retrieves the ValueItem associated with the given key.
	// The 'readValue' parameter controls whether to fetch the actual data value or just metadata.
	Read(key string, readValue bool) (*ValueItem, error)

	// Delete removes the key-value pair associated with the given key.
	Delete(key string) error

	// Keys returns a slice containing all keys stored in the persistence layer.
	Keys() ([]string, error)

	// Close performs any necessary cleanup for the persister.
	Close()
}
