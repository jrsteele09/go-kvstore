# go-kvstore

A Go Key / Value store for caching and optionally persisting data

# Example Usage

kv := kvstore.NewStore(kvstore.WithPersistenceOption(persistence.NewPersistenceBuffer(persistence.NewFsPersistence(testFolder), 10)))
