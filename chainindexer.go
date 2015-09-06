// Copyright (c) 2015 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"

	"github.com/btcsuite/btcd/blockchain"
	database "github.com/btcsuite/btcd/database2"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/golangcrypto/ripemd160"
)

var (
	// errUnsupportedAddressType is an error that is used to signal an
	// unsupported address type has been used.
	errUnsupportedAddressType = errors.New("address type is not supported " +
		"by the address index")

	// addrIndexBucketName is the name of the db bucket used to house the
	// address index.
	addrIndexBucketName = []byte("addridx")

	// byteOrder is the preferred byte order used for serializing numeric
	// fields for storage in the database.
	byteOrder = binary.LittleEndian

	// Limit the number of goroutines that concurrently build the index to
	// catch up based on the number of processor cores.  This help ensure
	// the system stays reasonably responsive under heavy load.
	numCatchUpWorkers = runtime.NumCPU() * 3
)

// indexState identifies the possible states the of chain indexer.
type indexState uint8

const (
	// indexStateCatchup is the mode used when when, on boot, the current
	// best chain height is greater than the last indexed block.  This mode
	// makes use of several concurrent worker goroutines indexing blocks
	// organized by a manager goroutine.  All chain downloading is stopped
	// until the address index has caught up, at which point "maintainence"
	// mode is entered.
	indexStateCatchup indexState = iota

	// indexStateMaintain is "maintainence" mode.  It uses a single worker
	// to serially process incoming jobs to index newly connected blocks.
	indexStateMaintain
)

// addrToHash160 converts known address types to a hash160.  An error is
// returned for unsupported types.
func addrToHash160(addr btcutil.Address) (*[ripemd160.Size]byte, error) {
	switch addr := addr.(type) {
	case *btcutil.AddressPubKeyHash:
		return addr.Hash160(), nil

	case *btcutil.AddressScriptHash:
		return addr.Hash160(), nil

	case *btcutil.AddressPubKey:
		return addr.AddressPubKeyHash().Hash160(), nil
	}

	return nil, errUnsupportedAddressType
}

// -----------------------------------------------------------------------------
// The address index consists of an entry for every address referenced by the
// transactions in the blocks in the main chain.  Each entry consists of one or
// more records that indicate the transactions involved.
//
// The serialized format is:
//
//   <num entries>[<block height><start offset><tx length>,...]
//
//   Field           Type      Size
//   num entries     uint8     4 bytes
//   block height    uint32    4 bytes
//   start offset    uint32    4 bytes
//   tx length       uint32    4 bytes
// -----------------------------------------------------------------------------

// serializeAddrIndexEntry returns an addr index entry serialized to a format
// that is suitable for long-term storage.  The format is described in detail
// above.
func serializeAddrIndexEntry(existingEntry []byte, blockHeight int32, txLoc wire.TxLoc) []byte {
	// Since a new entry is being added, there will be one more than is
	// already there (if any).
	numEntries := uint32(0)
	if len(existingEntry) > 0 {
		numEntries = byteOrder.Uint32(existingEntry[0:4])
	}
	numEntries++

	// Serialize the entry.
	serializedData := make([]byte, (uint32(numEntries)*12)+4)
	byteOrder.PutUint32(serializedData[0:4], numEntries)
	offset := uint32(4)
	if len(existingEntry) > 0 {
		copy(serializedData, existingEntry[4:])
		offset += uint32(len(existingEntry) - 4)
	}
	byteOrder.PutUint32(serializedData[offset:], uint32(blockHeight))
	offset += 4
	byteOrder.PutUint32(serializedData[offset:], uint32(txLoc.TxStart))
	offset += 4
	byteOrder.PutUint32(serializedData[offset:], uint32(txLoc.TxLen))

	return serializedData
}

// TODO(davec): Finish...
// blockHeights, tx locations
func deserializeAddrIndexEntry(serializedData []byte) ([]int32, []wire.TxLoc, error) {
	// TODO(davec): Refactor deserialize code from dbFetchAddrIndexEntries
	// to here so it is more easily testable.
	return nil, nil, errors.New("Not implemented")
}

// dbHasAddrIndexEntry uses an existing database transaction to return whether
// or not the address index contains the provided address.
func dbHasAddrIndexEntry(dbTx database.Tx, addrHash []byte) bool {
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	return addrIndex.Get(addrHash) != nil
}

// dbPutAddrIndexEntry uses an existing database transaction to update the
// address index given the provided values.  When there is already an entry for
// existing hash, a new record will be added.
func dbPutAddrIndexEntry(dbTx database.Tx, addrHash []byte, blockHeight int32, txLoc wire.TxLoc) error {
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	existing := addrIndex.Get(addrHash)
	serializedData := serializeAddrIndexEntry(existing, blockHeight, txLoc)
	return addrIndex.Put(addrHash, serializedData)
}

// Returns block regions for all referenced transactions and the number of
// entries skipped since it could have been less in the case there are less
// total entries than the requested number of entries to skip.
func dbFetchAddrIndexEntries(chain *blockchain.BlockChain, dbTx database.Tx, addrHash []byte, numToSkip, numRequested uint32) ([]database.BlockRegion, uint32, error) {
	// Load the record from the database and return now if it doesn't exist.
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	serializedData := addrIndex.Get(addrHash)
	if len(serializedData) == 0 {
		return nil, 0, nil
	}

	// Ensure the serialized data has enough bytes to properly deserialize
	// the number of entries.
	if len(serializedData) < 4 {
		return nil, 0, database.Error{
			ErrorCode: database.ErrCorruption,
			Description: fmt.Sprintf("corrupt address index entry "+
				"for %x", addrHash),
		}
	}

	// When the requested number of entries to skip is larger than the
	// number available, skip them all and return now with the actual number
	// skipped.
	numEntries := byteOrder.Uint32(serializedData[0:4])
	if numToSkip >= numEntries {
		return nil, numEntries, nil
	}

	// Ensure the serialized data has enough bytes to properly deserialize
	// all entries.
	if len(serializedData) < (int(numEntries)*12 + 1) {
		return nil, 0, database.Error{
			ErrorCode: database.ErrCorruption,
			Description: fmt.Sprintf("corrupt address index entry "+
				"for %x", addrHash),
		}
	}

	// Nothing more to do there are no requested entries.
	if numRequested == 0 {
		return nil, numToSkip, nil
	}

	// Limit the number to load based on the number of available entries,
	// the number to skip, and the number requested.
	numToLoad := numEntries - numToSkip
	if numToLoad > numRequested {
		numToLoad = numRequested
	}

	// Start the offset after all skipped entries and load the calculated
	// number.
	offset := 4 + numToSkip*12
	results := make([]database.BlockRegion, numToLoad)
	for i := uint32(0); i < numToLoad; i++ {
		// Deserialize the block height that contains the tx.
		blockHeight := int32(byteOrder.Uint32(serializedData[offset:]))
		offset += 4

		// Fetch the hash associated with the height.
		blockHash, err := chain.BlockHashByHeight(dbTx, blockHeight)
		if err != nil {
			return nil, 0, err
		}

		// Deserialize the transaction offset and length and populate
		// result.
		result := &results[i]
		result.Hash = blockHash
		result.Offset = byteOrder.Uint32(serializedData[offset:])
		offset += 4
		result.Len = byteOrder.Uint32(serializedData[offset:])
		offset += 4
	}

	return results, numToSkip, nil
}

// dropAddrIndex removes the entire address index.
func dropAddrIndex(db database.DB) error {
	err := db.Update(func(dbTx database.Tx) error {
		return dbTx.Metadata().DeleteBucket(addrIndexBucketName)
	})
	return err
}

// TODO(davec): Add the code to do the actual indexing.
// Should have following properties:
// - When btcd starts, check if the indexer is caught up.
//   - When it's not:
//     - Hold off getting any new blocks until it is (this option seems better)
//       *or*
//     - Wait until the chain is fully synced before catching up
//   - When it is:
//     - Do the indexing when a block is connected
//
// - Use multiple goroutines (numCatchUpWorkers) for extracting the addresses
//   and push data, etc...
//   - Likely a function -- IndexBlock(block *btcutil.Block)

type addrIndexer struct {
}
