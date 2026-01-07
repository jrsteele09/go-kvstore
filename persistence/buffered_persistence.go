package persistence

import (
	"context"
	"fmt"
	"sync"

	"github.com/jrsteele09/go-kvstore/kvstore"
	"github.com/rs/zerolog/log"
)

type commandType int

const (
	writeCommand commandType = iota + 1
	deleteCommand
	readMetadataCommand
	readValueCommand
)

type responseType struct {
	mv  *kvstore.ValueItem
	err error
}

type commandBuffer struct {
	cmdType  commandType
	key      string
	mv       *kvstore.ValueItem
	response chan responseType
}

// Buffer provides a thread-safe way to interact with a DataPersister.
type Buffer struct {
	persistence kvstore.DataPersister
	cb          chan commandBuffer
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewBuffer creates a new Buffer.
func NewBuffer(persistence kvstore.DataPersister, bufferSize uint) (*Buffer, error) {
	if persistence == nil {
		return nil, fmt.Errorf("persistence cannot be nil")
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	buffer := Buffer{
		cb:          make(chan commandBuffer, bufferSize),
		cancel:      cancelFunc,
		persistence: persistence,
	}
	buffer.wg.Add(1)
	go buffer.commandBuffer(ctx)
	return &buffer, nil
}

// Close cancels the background command processing.
func (b *Buffer) Close() {
	b.persistence.Close()
	b.cancel()
	b.wg.Wait()
	close(b.cb)
}

// Write queues a write command.
func (b *Buffer) Write(key string, data *kvstore.ValueItem) error {
	b.cb <- commandBuffer{cmdType: writeCommand, key: key, mv: data}
	return nil
}

// Read queues a read command and waits for a response.
func (b *Buffer) Read(key string, readValue bool) (*kvstore.ValueItem, error) {
	cmd := readMetadataCommand
	if readValue {
		cmd = readValueCommand
	}

	response := make(chan responseType)
	b.cb <- commandBuffer{cmdType: cmd, key: key, response: response}
	r := <-response
	if r.err != nil {
		return nil, fmt.Errorf("Buffer.Read: %w", r.err)
	}
	return r.mv, nil
}

// Delete queues a delete command.
func (b *Buffer) Delete(key string) error {
	b.cb <- commandBuffer{cmdType: deleteCommand, key: key}
	return nil
}

// Keys retrieves keys from the persistence layer.
func (b *Buffer) Keys() ([]string, error) {
	return b.persistence.Keys()
}

// commandBuffer processes commands.
func (b *Buffer) commandBuffer(ctx context.Context) {
	defer b.wg.Done()
	for {
		select {
		case command := <-b.cb:
			b.processCommand(command)
		case <-ctx.Done():
			log.Info().Msg("Buffer.commandBuffer cancelled")
			return
		}
	}
}

// processCommand processes an individual command.
func (b *Buffer) processCommand(command commandBuffer) {
	var err error
	switch command.cmdType {
	case writeCommand:
		err = b.persistence.Write(command.key, command.mv)
	case deleteCommand:
		err = b.persistence.Delete(command.key)
	case readMetadataCommand:
		mv, readErr := b.persistence.Read(command.key, false)
		command.response <- responseType{mv: mv, err: readErr}
	case readValueCommand:
		mv, readErr := b.persistence.Read(command.key, true)
		command.response <- responseType{mv: mv, err: readErr}
	}

	if err != nil {
		log.Error().Msgf("Buffer.processCommand command: %d error: %s", command.cmdType, err.Error())
	}
}
