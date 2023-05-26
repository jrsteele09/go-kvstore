package kvstore

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	metaDataFilename = "metadata.json"
	dataFilename     = "data.bin"
	fileMode         = 0700
)

type commandTyoe int

const (
	writeCommand commandTyoe = iota + 1
	deleteCommand
	readMetadataCommand
	readValueCommand
)

type responseType struct {
	mv  *ValueItem
	err error
}

type commandBuffer struct {
	cmdType  commandTyoe
	key      string
	mv       *ValueItem
	response chan responseType
}

// PersistenceBuffer contains a Persistence controller and provides concurrent safe read / writes to that controller.
type PersistenceBuffer struct {
	cb          chan commandBuffer
	cancel      context.CancelFunc
	persistence PersistenceController
}

// NewPersistenceBuffer creates a new instance of a PersistenceBuffer.
func NewPersistenceBuffer(persistence PersistenceController, bufferSize uint) PersistenceBuffer {
	ctx, cancelFunc := context.WithCancel(context.Background())
	dp := PersistenceBuffer{
		cb:          make(chan commandBuffer, bufferSize),
		cancel:      cancelFunc,
		persistence: persistence,
	}
	go dp.commandBuffer(ctx)
	return dp
}

// Close closes the Persistence buffer
func (d PersistenceBuffer) Close() {
	d.cancel()
}

func (d PersistenceBuffer) Write(key string, data *ValueItem) error {
	d.cb <- commandBuffer{cmdType: writeCommand, key: key, mv: data}
	return nil
}

func (d PersistenceBuffer) Read(key string, readValue bool) (*ValueItem, error) {
	cmd := readMetadataCommand
	if readValue {
		cmd = readValueCommand
	}

	response := make(chan responseType)
	d.cb <- commandBuffer{cmdType: cmd, key: key, response: response}
	r := <-response
	if r.err != nil {
		return nil, errors.Wrap(r.err, "DiskPersisatence Read")
	}
	return r.mv, nil
}

// Delete deletes the persisted object associated with the key
func (d PersistenceBuffer) Delete(key string) error {
	d.cb <- commandBuffer{cmdType: deleteCommand, key: key}
	return nil
}

// Keys returns a list of keys in the cache
func (d PersistenceBuffer) Keys() ([]string, error) {
	return d.persistence.Keys()
}

func (d PersistenceBuffer) commandBuffer(ctx context.Context) {
	var err error
	for {
		select {
		case command := <-d.cb:
			err = nil
			if command.cmdType == writeCommand {
				err = d.persistence.Write(command.key, command.mv)
			} else if command.cmdType == deleteCommand {
				err = d.persistence.Delete(command.key)
			} else if command.cmdType == readMetadataCommand {
				mv, metaErr := d.persistence.Read(command.key, false)
				command.response <- responseType{mv: mv, err: metaErr}
			} else if command.cmdType == readValueCommand {
				mv, valueErr := d.persistence.Read(command.key, true)
				command.response <- responseType{mv: mv, err: valueErr}
			}
			if err != nil {
				log.Error().Msgf("commandBuffer command: %d error: %s", command.cmdType, err.Error())
			}

		case <-ctx.Done():
			log.Info().Msg("DiskPersistence writeBuffer cancelled")
		}
	}
}
