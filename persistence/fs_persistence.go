package persistence

import (
	"encoding/json"
	"os"
	"path"

	"github.com/jrsteele09/go-kvstore/kvstore"
	"github.com/pkg/errors"
)

// FsPersistence writes key/values to a file system.
// The keys sre folders and values are written to a file
type FsPersistence struct {
	folder string
}

// NewFsPersistence creates a new FsPersistence
func NewFsPersistence(folder string) *FsPersistence {
	return &FsPersistence{
		folder: folder,
	}
}

// Close closes the Persistence controller
func (d FsPersistence) Close() {}

// Keys returns a list of keys from the File System
func (d FsPersistence) Keys() ([]string, error) {
	files, err := os.ReadDir(d.folder)
	if err != nil {
		return []string{}, errors.Wrap(err, "FsPersistence.Keys")
	}
	keys := make([]string, 0)
	for _, f := range files {
		if f.IsDir() {
			keys = append(keys, f.Name())
		}
	}

	return keys, nil
}

// Write writes data to the file system
func (d FsPersistence) Write(key string, data *kvstore.ValueItem) error {
	folder := path.Join(d.folder, key)
	if err := os.MkdirAll(folder, fileMode); err != nil {
		return errors.Wrap(err, "FsPersistence.Write MkdirAll")
	}
	metaDataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "FsPersistence.Write json.Marshal metaDataBytes")
	}
	err = os.WriteFile(path.Join(folder, metaDataFilename), metaDataBytes, fileMode)
	if err != nil {
		return errors.Wrap(err, "FsPersistence.Write os.WriteFile metaData")
	}

	if data.Data == nil {
		return nil
	}
	err = os.WriteFile(path.Join(folder, dataFilename), data.Data, fileMode)
	if err != nil {
		return errors.Wrap(err, "FsPersistence.Write os.WriteFile data")
	}

	return nil
}

// Delete deletes a key from the file system
func (d FsPersistence) Delete(key string) error {
	folder := path.Join(d.folder, key)
	if err := os.RemoveAll(folder); err != nil {
		return errors.Wrap(err, "FsPersistence.Delete os.RemoveAll")
	}
	return nil
}

// Read reads data from a file system
func (d FsPersistence) Read(key string, readValue bool) (*kvstore.ValueItem, error) {
	folder := path.Join(d.folder, key)
	metaDataBytes, err := os.ReadFile(path.Join(folder, metaDataFilename))
	if err != nil {
		return nil, errors.Wrap(err, "FsPersistence.Read os.ReadFile metadata")
	}
	var mv kvstore.ValueItem
	if unmarshalErr := json.Unmarshal(metaDataBytes, &mv); unmarshalErr != nil {
		return nil, errors.Wrap(err, "FsPersistence.Read json.Unmarshal")
	}
	if !readValue {
		return &mv, nil
	}

	valueBytes, err := os.ReadFile(path.Join(folder, dataFilename))
	if err != nil {
		return nil, errors.Wrap(err, "FsPersistence.Read os.ReadFile")

	}
	mv.SetData(valueBytes)
	return &mv, nil
}
