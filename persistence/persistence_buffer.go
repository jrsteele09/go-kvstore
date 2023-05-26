package persistence

import (
	"context"

	"github.com/jrsteele09/go-kvstore/kvstore"
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
	mv  *kvstore.ValueItem
	err error
}

type commandBuffer struct {
	cmdType  commandTyoe
	key      string
	mv       *kvstore.ValueItem
	response chan responseType
}

// Buffer contains a Persistence controller and provides concurrent safe read / writes to that controller.
type Buffer struct {
	cb          chan commandBuffer
	cancel      context.CancelFunc
	persistence kvstore.PersistenceController
}

// NewPersistenceBuffer creates a new instance of a PersistenceBuffer.
func NewPersistenceBuffer(persistence kvstore.PersistenceController, bufferSize uint) Buffer {
	ctx, cancelFunc := context.WithCancel(context.Background())
	dp := Buffer{
		cb:          make(chan commandBuffer, bufferSize),
		cancel:      cancelFunc,
		persistence: persistence,
	}
	go dp.commandBuffer(ctx)
	return dp
}

// Close closes the Persistence buffer
func (b Buffer) Close() {
	b.cancel()
}

// Write adds a write command to the command buffer
func (b Buffer) Write(key string, data *kvstore.ValueItem) error {
	b.cb <- commandBuffer{cmdType: writeCommand, key: key, mv: data}
	return nil
}

// Read adds a read command to the command buffer and waits for a response
func (b Buffer) Read(key string, readValue bool) (*kvstore.ValueItem, error) {
	cmd := readMetadataCommand
	if readValue {
		cmd = readValueCommand
	}

	response := make(chan responseType)
	b.cb <- commandBuffer{cmdType: cmd, key: key, response: response}
	r := <-response
	if r.err != nil {
		return nil, errors.Wrap(r.err, "PersistenceBuffer.Read")
	}
	return r.mv, nil
}

// Delete deletes the persisted object associated with the key
func (b Buffer) Delete(key string) error {
	b.cb <- commandBuffer{cmdType: deleteCommand, key: key}
	return nil
}

// Keys returns a list of keys in the cache
func (b Buffer) Keys() ([]string, error) {
	return b.persistence.Keys()
}

func (b Buffer) commandBuffer(ctx context.Context) {
	var err error
	for {
		select {
		case command := <-b.cb:
			err = nil
			if command.cmdType == writeCommand {
				err = b.persistence.Write(command.key, command.mv)
			} else if command.cmdType == deleteCommand {
				err = b.persistence.Delete(command.key)
			} else if command.cmdType == readMetadataCommand {
				mv, metaErr := b.persistence.Read(command.key, false)
				command.response <- responseType{mv: mv, err: metaErr}
			} else if command.cmdType == readValueCommand {
				mv, valueErr := b.persistence.Read(command.key, true)
				command.response <- responseType{mv: mv, err: valueErr}
			}
			if err != nil {
				log.Error().Msgf("PersistenceBuffer.commandBuffer command: %d error: %s", command.cmdType, err.Error())
			}

		case <-ctx.Done():
			log.Info().Msg("PersistenceBuffer.commandBuffer cancelled")
		}
	}
}
