package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/jrsteele09/go-kvstore/kvstore"
)

const (
	metaDataExtension = ".meta"
	dataExtension     = ".data"
	fileMode          = 0700
)

// Persistor is responsible for persisting key-values to a filesystem.
// Keys can contain "/" to represent folder structures.
// Each key is stored as two files: <key>.meta (metadata) and <key>.data (data).
type Persistor struct {
	rootFS *os.Root
}

// sanitizeFileName replaces characters that are invalid in filenames.
// Colons are replaced with a safe character sequence.
func sanitizeFileName(name string) string {
	// Replace : with _COLON_ to avoid file system issues
	name = strings.ReplaceAll(name, ":", "_COLON_")
	return name
}

// unsanitizeFileName reverses the sanitization.
func unsanitizeFileName(name string) string {
	name = strings.ReplaceAll(name, "_COLON_", ":")
	return name
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

// Keys returns a list of keys by walking the directory tree and finding all .meta files.
func (p *Persistor) Keys() ([]string, error) {
	var keys []string
	err := p.walkDir(".", func(filePath string) error {
		// Check if this is a metadata file
		if len(filePath) > len(metaDataExtension) && filePath[len(filePath)-len(metaDataExtension):] == metaDataExtension {
			// Remove .meta extension to get the sanitized key
			sanitizedKey := filePath[:len(filePath)-len(metaDataExtension)]
			// Unsanitize to get the original key
			key := unsanitizeFileName(sanitizedKey)
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Keys: %w", err)
	}
	return keys, nil
}

// walkDir recursively walks the directory tree and calls fn for each file.
func (p *Persistor) walkDir(dirPath string, fn func(filePath string) error) error {
	f, err := p.rootFS.Open(dirPath)
	if err != nil {
		return fmt.Errorf("walkDir Open: %w", err)
	}
	defer f.Close()

	entries, err := f.ReadDir(-1)
	if err != nil {
		return fmt.Errorf("walkDir ReadDir: %w", err)
	}

	for _, entry := range entries {
		var entryPath string
		if dirPath == "." {
			entryPath = entry.Name()
		} else {
			entryPath = path.Join(dirPath, entry.Name())
		}

		if entry.IsDir() {
			// Recursively walk subdirectories
			if err := p.walkDir(entryPath, fn); err != nil {
				return err
			}
		} else {
			// Call function for files
			if err := fn(entryPath); err != nil {
				return err
			}
		}
	}
	return nil
}

// Write writes the ValueItem to files based on the key.
// If the key contains "/", the directory structure is created.
// Files are written as <key>.meta and <key>.data.
func (p *Persistor) Write(key string, data *kvstore.ValueItem) error {
	// Sanitize the key for file system compatibility
	sanitizedKey := sanitizeFileName(key)

	// If key contains a directory path, create the directory structure
	dirPath := path.Dir(sanitizedKey)
	if dirPath != "." && dirPath != "" {
		if err := p.rootFS.MkdirAll(dirPath, fileMode); err != nil {
			return fmt.Errorf("Write: MkdirAll: %w", err)
		}
	}

	serializedData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("Write: Marshal: %w", err)
	}

	metadataPath := sanitizedKey + metaDataExtension
	if err := p.rootFS.WriteFile(metadataPath, serializedData, fileMode); err != nil {
		return fmt.Errorf("Write: WriteFile metadata: %w", err)
	}

	if data.Data != nil {
		dataPath := sanitizedKey + dataExtension
		if err := p.rootFS.WriteFile(dataPath, data.Data, fileMode); err != nil {
			return fmt.Errorf("Write: WriteFile data: %w", err)
		}
	}

	return nil
}

// Delete removes the metadata and data files for the key.
func (p *Persistor) Delete(key string) error {
	sanitizedKey := sanitizeFileName(key)

	metadataPath := sanitizedKey + metaDataExtension
	if err := p.rootFS.Remove(metadataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Delete: Remove metadata: %w", err)
	}

	dataPath := sanitizedKey + dataExtension
	if err := p.rootFS.Remove(dataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Delete: Remove data: %w", err)
	}

	return nil
}

// Read retrieves the ValueItem identified by the key.
func (p *Persistor) Read(key string, readValue bool) (*kvstore.ValueItem, error) {
	sanitizedKey := sanitizeFileName(key)
	metadataPath := sanitizedKey + metaDataExtension

	metaData, err := p.rootFS.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("Read: ReadFile metadata: %w", err)
	}

	var valueItem kvstore.ValueItem
	if err := json.Unmarshal(metaData, &valueItem); err != nil {
		return nil, fmt.Errorf("Read: Unmarshal: %w", err)
	}

	if readValue {
		dataPath := sanitizedKey + dataExtension
		data, err := p.rootFS.ReadFile(dataPath)
		if err != nil {
			return nil, fmt.Errorf("Read: ReadFile data: %w", err)
		}

		valueItem.SetData(data)
	}

	return &valueItem, nil
}
