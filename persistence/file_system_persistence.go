package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/jrsteele09/go-kvstore/kvstore"
)

const (
	metaDataFilename = "metadata.json"
	dataFilename     = "data.bin"
	fileMode         = 0700
)

// Persistor is responsible for persisting key-values to a filesystem.
// It uses folders as keys and files within those folders as values.
type Persistor struct {
	rootFS *os.Root
}

// New initializes a new Filesystem persistence object.
func New(folder string) (*Persistor, error) {
	rootFS, err := os.OpenRoot(folder)
	if err != nil {
		return nil, fmt.Errorf("failed to open root: %w", err)
	}
	return &Persistor{
		rootFS: rootFS,
	}, nil
}

// Close cleans up resources. Currently, it does nothing.
func (p *Persistor) Close() {
	p.rootFS.Close()
}

// Keys returns a list of keys available in the folder.
func (p *Persistor) Keys() ([]string, error) {
	f, err := p.rootFS.Open(".")
	if err != nil {
		return nil, fmt.Errorf("Keys: Open: %w", err)
	}
	defer f.Close()

	fileInfoList, err := f.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("Keys: ReadDir: %w", err)
	}

	var keys []string
	for _, fileInfo := range fileInfoList {
		if fileInfo.IsDir() {
			keys = append(keys, fileInfo.Name())
		}
	}

	return keys, nil
}

// Write writes the ValueItem to the folder specified by the key.
func (p *Persistor) Write(key string, data *kvstore.ValueItem) error {
	if err := p.rootFS.MkdirAll(key, fileMode); err != nil {
		return fmt.Errorf("Write: MkdirAll: %w", err)
	}

	serializedData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("Write: Marshal: %w", err)
	}

	metadataPath := path.Join(key, metaDataFilename)
	if err := p.rootFS.WriteFile(metadataPath, serializedData, fileMode); err != nil {
		return fmt.Errorf("Write: WriteFile metadata: %w", err)
	}

	if data.Data != nil {
		dataPath := path.Join(key, dataFilename)
		if err := p.rootFS.WriteFile(dataPath, data.Data, fileMode); err != nil {
			return fmt.Errorf("Write: WriteFile data: %w", err)
		}
	}

	return nil
}

// Delete removes the folder specified by the key.
func (p *Persistor) Delete(key string) error {
	if err := p.rootFS.RemoveAll(key); err != nil {
		return fmt.Errorf("Delete: RemoveAll: %w", err)
	}
	return nil
}

// Read retrieves the ValueItem identified by the key.
func (p *Persistor) Read(key string, readValue bool) (*kvstore.ValueItem, error) {
	metadataPath := path.Join(key, metaDataFilename)

	metaData, err := p.rootFS.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("Read: ReadFile metadata: %w", err)
	}

	var valueItem kvstore.ValueItem
	if err := json.Unmarshal(metaData, &valueItem); err != nil {
		return nil, fmt.Errorf("Read: Unmarshal: %w", err)
	}

	if readValue {
		dataPath := path.Join(key, dataFilename)
		data, err := p.rootFS.ReadFile(dataPath)
		if err != nil {
			return nil, fmt.Errorf("Read: ReadFile data: %w", err)
		}

		valueItem.SetData(data)
	}

	return &valueItem, nil
}
