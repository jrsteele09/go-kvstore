package kvstore

// PersistenceController interface is the primary interface that the KV Store uses to persist data
// One or more PersistenceControllers can be initialised on a store.
type PersistenceController interface {
	Write(key string, data *ValueItem) error
	Read(key string, readValue bool) (*ValueItem, error)
	Delete(key string) error
	Keys() ([]string, error)
}
