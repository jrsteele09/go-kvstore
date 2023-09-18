package persistence

import (
	"encoding/json"
	"os"
	"path"

	"github.com/jrsteele09/go-kvstore/kvstore"
	"github.com/pkg/errors"
)

// Filesystem is responsible for persisting key-values to a filesystem.
// It uses folders as keys and files within those folders as values.
type Filesystem struct {
	folder string
}

// NewFsPersistence initializes a new Filesystem persistence object.
func NewFsPersistence(folder string) *Filesystem {
	return &Filesystem{folder: folder}
}

// Close cleans up resources. Currently, it does nothing.
func (fs Filesystem) Close() {
	// Intentionally left empty
}

// Keys returns a list of keys available in the folder.
func (fs Filesystem) Keys() ([]string, error) {
	fileInfoList, err := os.ReadDir(fs.folder)
	if err != nil {
		return nil, errors.Wrap(err, "Keys: ReadDir")
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
func (fs Filesystem) Write(key string, data *kvstore.ValueItem) error {
	targetFolder := path.Join(fs.folder, key)

	if err := os.MkdirAll(targetFolder, fileMode); err != nil {
		return errors.Wrap(err, "Write: MkdirAll")
	}

	serializedData, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "Write: Marshal")
	}

	if err := os.WriteFile(path.Join(targetFolder, metaDataFilename), serializedData, fileMode); err != nil {
		return errors.Wrap(err, "Write: WriteFile metadata")
	}

	if data.Data != nil {
		if err := os.WriteFile(path.Join(targetFolder, dataFilename), data.Data, fileMode); err != nil {
			return errors.Wrap(err, "Write: WriteFile data")
		}
	}

	return nil
}

// Delete removes the folder specified by the key.
func (fs Filesystem) Delete(key string) error {
	targetFolder := path.Join(fs.folder, key)
	if err := os.RemoveAll(targetFolder); err != nil {
		return errors.Wrap(err, "Delete: RemoveAll")
	}
	return nil
}

// Read retrieves the ValueItem identified by the key.
func (fs Filesystem) Read(key string, readValue bool) (*kvstore.ValueItem, error) {
	targetFolder := path.Join(fs.folder, key)

	metaData, err := os.ReadFile(path.Join(targetFolder, metaDataFilename))
	if err != nil {
		return nil, errors.Wrap(err, "Read: ReadFile metadata")
	}

	var valueItem kvstore.ValueItem
	if err := json.Unmarshal(metaData, &valueItem); err != nil {
		return nil, errors.Wrap(err, "Read: Unmarshal")
	}

	if readValue {
		data, err := os.ReadFile(path.Join(targetFolder, dataFilename))
		if err != nil {
			return nil, errors.Wrap(err, "Read: ReadFile data")
		}

		if err := valueItem.SetData(data); err != nil {
			return nil, errors.Wrap(err, "Read: SetData")
		}
	}

	return &valueItem, nil
}
