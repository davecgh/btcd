// Copyright (c) 2015 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/btcsuite/btcd/blockchain"
	database "github.com/btcsuite/btcd/database2"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

var (
	// errUnsupportedAddressType is an error that is used to signal an
	// unsupported address type has been used.
	errUnsupportedAddressType = errors.New("address type is not supported " +
		"by the address index")

	// errNoAddrIndex is an error that is used by addrindex functions
	// to signal that the address index is not created.
	errNoAddrIndex = errors.New("Address index does not exist in db")

	// addrIndexBucketName is the name of the db bucket used to house the
	// address index.
	addrIndexBucketName = []byte("addridx")

	// addrIndexTipKey is the name of the key in the addrindex bucket that
	// stores the current address index tip block hash.
	addrIndexTipKey = []byte("tip")

	// byteOrder is the preferred byte order used for serializing numeric
	// fields for storage in the database.
	byteOrder = binary.LittleEndian

	// Limit the number of goroutines that concurrently build the index to
	// catch up based on the number of processor cores.  This help ensure
	// the system stays reasonably responsive under heavy load.
	numIndexWorkers = runtime.NumCPU()
)

const (
	// Maximum number of txs that are stored in level 0 of an address.
	// Subsequent levels store maximum double of the previous level.
	firstLevelMaxSize = 8

	// Size of an address key: 1 byte of address type plus 20 bytes of
	// hash160
	addrKeySize = 1 + 20

	// Size of a level key: one addrKey + 1 byte for level number
	levelKeySize = addrKeySize + 1

	// Size of a transaction entry
	txEntrySize = 4 + 4 + 4
)

type addrKey [addrKeySize]byte
type levelKey [levelKeySize]byte

// -----------------------------------------------------------------------------
// The address index maps addresses referenced in the blockchain to a list of
// all the transactions involving that address. Transactions are stored
// according to order of appearance in the blockchain: first by block height and
// then by offset inside the block.
//
// Every address has one or more entries in the addrindex bucket, identified by
// a 'level' starting from 0. Level 0 holds maximum maxEntriesFirstLevel txs,
// and next levels hold maximum twice as much as the previous level.
//
// When inserting a new tx, it's apended into level 0. If level 0 becomes full,
// the whole data from level 0 is appended to level 1 and level 0 becomes empty.
// In this case level 1 may also become full, in this case it's appended to
// level 2 and emptied, and so on.
//
// Lower levels contain newer txs, inside each level txs are ordered from old
// to new.
//
// The intent of this approach is to get a balance between storing one entry
// per transaction (wastes space because the same address hash is stored
// per every tx as a key) and storing one entry per address (most space
// efficient, but indexing cost grows quadratically with the number of txs in
// with the same address). Insertion cost is amortized logarithmic, and
// retrieval is fast too because the number of levels grows logarithmically.
// This is similar to how leveldb works internally.
//
// The serialized key format is:
//
//   <addr type><addr hash><level>
//
//   Field           Type      Size
//   Addr type       uint8     1 byte
//   Addr hash       hash160   20 bytes
//   Level           uint8     1 byte
//   Total: 22 bytes
//
// The serialized value format is:
//
//   <block height><start offset><tx length>,...
//
//   Field           Type      Size
//   block height    uint32    4 bytes
//   start offset    uint32    4 bytes
//   tx length       uint32    4 bytes
//   Total: 12 bytes per indexed tx
//
// -----------------------------------------------------------------------------

// addrToKey converts known address types to an addrindex key (type byte +
// the hash160, see above for details).
// An error is returned for unsupported types.
func addrToKey(addr btcutil.Address) (*addrKey, error) {
	switch addr := addr.(type) {
	case *btcutil.AddressPubKeyHash:
		var res addrKey
		res[0] = 0
		copy(res[1:], addr.Hash160()[:])
		return &res, nil

	case *btcutil.AddressScriptHash:
		var res addrKey
		res[0] = 1
		copy(res[1:], addr.Hash160()[:])
		return &res, nil

	case *btcutil.AddressPubKey:
		var res addrKey
		res[0] = 0
		copy(res[1:], addr.AddressPubKeyHash().Hash160()[:])
		return &res, nil
	}

	return nil, errUnsupportedAddressType
}

func addrKeyToLevelKey(key *addrKey, level uint8) *levelKey {
	var res levelKey
	copy(res[:], key[:])
	res[addrKeySize] = level
	return &res
}

type addrIndexTxEntry struct {
	blockHeight int32
	txLoc       wire.TxLoc
}

// serializeAddrIndexEntry serializes a tx entry. The format is described in
// detail above.
func serializeAddrIndexTxEntry(e addrIndexTxEntry) []byte {
	serializedData := make([]byte, txEntrySize)
	offset := 0
	byteOrder.PutUint32(serializedData[offset:], uint32(e.blockHeight))
	offset += 4
	byteOrder.PutUint32(serializedData[offset:], uint32(e.txLoc.TxStart))
	offset += 4
	byteOrder.PutUint32(serializedData[offset:], uint32(e.txLoc.TxLen))

	return serializedData
}

func deserializeAddrIndexTxEntry(serializedData []byte) (addrIndexTxEntry, error) {
	var res addrIndexTxEntry
	offset := 0
	res.blockHeight = int32(byteOrder.Uint32(serializedData[offset:]))
	offset += 4
	res.txLoc.TxStart = int(byteOrder.Uint32(serializedData[offset:]))
	offset += 4
	res.txLoc.TxStart = int(byteOrder.Uint32(serializedData[offset:]))

	return res, nil
}

// dbAppendToAddrIndexEntry uses an existing database transaction to update the
// address index given the provided values.  When there is already an entry for
// existing hash, a new record will be added.
func dbAppendToAddrIndexEntry(dbTx database.Tx, key *addrKey, entry addrIndexTxEntry) error {
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	if addrIndex == nil {
		return errNoAddrIndex
	}

	// Serialize the entry to append
	dataToAppend := serializeAddrIndexTxEntry(entry)

	// Start with level 0, with the initial max size
	level := uint8(0)
	maxLevelSize := firstLevelMaxSize

	// Loop over all levels.
	for true {
		// Get the level key for the current level.
		levelKey := addrKeyToLevelKey(key, level)
		// Get the old data. If it does not exist, it will return nil,
		// which is convenient because it's treated as a zero-length slice.
		oldData := addrIndex.Get(levelKey[:])

		// Concat oldData and dataToAppend into newData.
		newData := make([]byte, len(oldData)+len(dataToAppend))
		copy(newData, oldData)
		copy(newData[len(oldData):], dataToAppend)

		// Check new data length against the maximum.
		if len(newData) <= maxLevelSize*txEntrySize {
			// If it fits, save it and we're done.
			err := addrIndex.Put(levelKey[:], newData)
			if err != nil {
				return err
			}
			break
		} else {
			// If it doesn't fit, clear it...
			err := addrIndex.Put(levelKey[:], []byte{})
			if err != nil {
				return err
			}
			// and save everything to append into a higher level.
			dataToAppend = newData
		}
		level++
		maxLevelSize *= 2
	}

	return nil
}

// dbAppendToAddrIndexEntry uses an existing database transaction to update the
// address index given the provided values.  When there is already an entry for
// existing hash, a new record will be added.
func dbRemoveFromAddrIndexEntry(dbTx database.Tx, key *addrKey, count int) error {
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	if addrIndex == nil {
		return errNoAddrIndex
	}

	// Start with level 0, with the initial max size
	level := uint8(0)

	// Loop over levels until we have no more entries to remove.
	for count > 0 {
		// Get the level key for the current level.
		levelKey := addrKeyToLevelKey(key, level)
		// Get the old data.
		levelData := addrIndex.Get(levelKey[:])

		// Calculate how many entries to remove.
		levelCount := len(levelData) / txEntrySize
		removeCount := levelCount
		if removeCount > count {
			removeCount = count
		}

		levelData = levelData[:len(levelData)-removeCount*txEntrySize]

		count -= removeCount
	}

	return nil
}

// BlockRegion specifies a particular region of a block identified by the
// specified hash, given an offset and length.
type addrIndexResult struct {
	Height int32
	Offset uint32
	Len    uint32
}

// Returns block regions for all referenced transactions and the number of
// entries skipped since it could have been less in the case there are less
// total entries than the requested number of entries to skip.
// This function returns block heights instead of hashes to avoid a dependency
// on BlockChain. See fetchAddrIndexEntries below for a version that
// returns block hashes.
func dbFetchAddrIndexEntries(dbTx database.Tx, key *addrKey, numToSkip, numRequested uint32, reverse bool) ([]addrIndexResult, uint32, error) {
	// Load the record from the database and return now if it doesn't exist.
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	if addrIndex == nil {
		return nil, 0, errNoAddrIndex
	}

	// Load all
	level := uint8(0)
	var serializedData []byte

	// If reverse is false, we need to fetch all the levels because numToSkip
	// and numRequested are counted from oldest transactions (highest level),
	// so we need to know the total count.
	// If reverse is true, they're counted from lowest level, so we can stop
	// fetching from database as soon as we have enough transactions.
	for !reverse || len(serializedData) < int(numToSkip+numRequested)*txEntrySize {
		levelData := addrIndex.Get(addrKeyToLevelKey(key, level)[:])
		if levelData == nil {
			// If we have no more levels, stop.
			break
		}
		// Append the new data to the beginning, since it's older data.
		serializedData = append(levelData, serializedData...)
		level++
	}

	// When the requested number of entries to skip is larger than the
	// number available, skip them all and return now with the actual number
	// skipped.
	numEntries := uint32(len(serializedData) / txEntrySize)
	if numToSkip >= numEntries {
		return nil, numEntries, nil
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
	results := make([]addrIndexResult, numToLoad)
	for i := uint32(0); i < numToLoad; i++ {
		var offset uint32
		// Calculate the offset we need to read from, according to the
		// reverse flag.
		if reverse {
			offset = (numEntries - numToSkip - i - 1) * txEntrySize
		} else {
			offset = (numToSkip + i) * txEntrySize
		}

		// Deserialize and populate the result.
		result := &results[i]
		result.Height = int32(byteOrder.Uint32(serializedData[offset:]))
		offset += 4
		result.Offset = byteOrder.Uint32(serializedData[offset:])
		offset += 4
		result.Len = byteOrder.Uint32(serializedData[offset:])
		offset += 4
	}

	return results, numToSkip, nil
}

// Returns block regions for all referenced transactions and the number of
// entries skipped since it could have been less in the case there are less
// total entries than the requested number of entries to skip.
func fetchAddrIndexEntries(chain *blockchain.BlockChain, dbTx database.Tx, key *addrKey, numToSkip, numRequested uint32, reverse bool) ([]database.BlockRegion, uint32, error) {
	res, skipped, err := dbFetchAddrIndexEntries(dbTx, key, numToSkip, numRequested, reverse)
	if err != nil {
		return nil, 0, err
	}

	regions := make([]database.BlockRegion, len(res))
	for i := 0; i < len(res); i++ {
		// Fetch the hash associated with the height.
		regions[i].Hash, err = chain.BlockHashByHeight(dbTx, res[i].Height)
		if err != nil {
			return nil, 0, err
		}
		regions[i].Len = res[i].Len
		regions[i].Offset = res[i].Offset
	}

	return regions, skipped, nil
}

// dbFetchAddrIndexTip uses an existing database tx to fetch the hash and block
// height of the most recent block which has had its address index populated.
// It will return errNoAddrIndex if the address index does not exist, in this
// case the returned hash and height are invalid.
func dbFetchAddrIndexTip(dbTx database.Tx) (*wire.ShaHash, int32, error) {
	// Find the addrindex bucket and fail if it doesn't exist.
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	if addrIndex == nil {
		return nil, 0, errNoAddrIndex
	}

	data := addrIndex.Get(addrIndexTipKey)

	hash, err := wire.NewShaHash(data[:wire.HashSize])
	if err != nil {
		return nil, 0, err
	}
	height := int32(byteOrder.Uint32(data[wire.HashSize:]))

	return hash, height, nil
}

// dbUpdateAddrIndexTip uses an existing database tx to update the hash and block
// height of the most recent block which has had its address index populated.
// It will return errNoAddrIndex if the address index does not exist.
func dbUpdateAddrIndexTip(dbTx database.Tx, hash *wire.ShaHash, height int32) error {
	// Find the addrindex bucket and fail if it doesn't exist.
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	if addrIndex == nil {
		return errNoAddrIndex
	}

	data := make([]byte, wire.HashSize+4)
	copy(data, hash[:])
	byteOrder.PutUint32(data[wire.HashSize:], uint32(height))

	addrIndex.Put(addrIndexTipKey, data)
	return nil
}

// writeIndexData represents the address index data to be written from one block
type writeIndexData map[addrKey][]*wire.TxLoc

// appendAddrIndexDataForBlock uses an existing database transaction to write
// the addrindex data from one block to the database. The addrindex tip before
// calling this function should be the block previous to the one being written.
func appendAddrIndexDataForBlock(dbTx database.Tx, blk *btcutil.Block, data writeIndexData) error {
	// Find the addrindex bucket and fail if it doesn't exist.
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	if addrIndex == nil {
		return errNoAddrIndex
	}

	for addr, txs := range data {
		for _, tx := range txs {
			dbAppendToAddrIndexEntry(dbTx, &addr, addrIndexTxEntry{blockHeight: blk.Height(), txLoc: *tx})
		}
	}

	err := dbUpdateAddrIndexTip(dbTx, blk.Sha(), blk.Height())
	if err != nil {
		return err
	}

	return nil
}

// removeAddrIndexDataForBlock uses an existing database transaction to erase
// the addrindex data from one block to the database. The addrindex tip before
// calling this function should be the block being erased, after the function
// finishes the tip will be the block before.
func removeAddrIndexDataForBlock(dbTx database.Tx, blk *btcutil.Block, data writeIndexData) error {
	// Find the addrindex bucket and fail if it doesn't exist.
	addrIndex := dbTx.Metadata().Bucket(addrIndexBucketName)
	if addrIndex == nil {
		return errNoAddrIndex
	}

	for addr, txs := range data {
		dbRemoveFromAddrIndexEntry(dbTx, &addr, len(txs))
	}

	// Update the tip to the parent
	err := dbUpdateAddrIndexTip(dbTx, &blk.MsgBlock().Header.PrevBlock, blk.Height()-1)
	if err != nil {
		return err
	}

	return nil
}

// dbCreateAddrIndex creates the address index bucket if it does not exist.
func dbCreateAddrIndex(db database.DB, genesisHash *wire.ShaHash) error {
	err := db.Update(func(dbTx database.Tx) error {
		if dbTx.Metadata().Bucket(addrIndexBucketName) == nil {
			adxrLog.Info("Creating address index")
			_, err := dbTx.Metadata().CreateBucket(addrIndexBucketName)
			if err != nil {
				return err
			}
			return dbUpdateAddrIndexTip(dbTx, genesisHash, 0)
		}
		return nil
	})
	return err
}

// dbDropAddrIndex removes the entire address index.
func dbDropAddrIndex(db database.DB) error {
	err := db.Update(func(dbTx database.Tx) error {
		return dbTx.Metadata().DeleteBucket(addrIndexBucketName)
	})
	return err
}

// indexScriptPubKey indexes the tx as relevant for all the addresses found in
// the SPK.
func (a *addrIndexer) indexScriptPubKey(idx writeIndexData, scriptPubKey []byte, loc *wire.TxLoc) error {
	// Any errors are intentionally ignored: if the tx is non-standard, it
	// simply won't be indexed
	_, addrs, _, _ := txscript.ExtractPkScriptAddrs(scriptPubKey, a.server.chainParams)

	for _, addr := range addrs {
		addrKey, err := addrToKey(addr)
		if err != nil {
			// If the address type is not supported, just ignore it.
			continue
		}
		idx[*addrKey] = append(idx[*addrKey], loc)
	}
	return nil
}

// indexBlockAddrs returns a populated index of the all the transactions in the
// passed block based on the addresses involved in each transaction.
func (a *addrIndexer) indexBlockAddrs(blk *btcutil.Block) (writeIndexData, error) {
	addrIndex := make(writeIndexData)
	txLocs, err := blk.TxLoc()
	if err != nil {
		return nil, err
	}
	for txIdx, tx := range blk.Transactions() {
		// Tx's offset and length in the block.
		locInBlock := &txLocs[txIdx]

		// Coinbases don't have any inputs.
		if !blockchain.IsCoinBase(tx) {
			// Index the SPK's of each input's previous outpoint
			// transaction.
			for _, txIn := range tx.MsgTx().TxIn {
				// Lookup and fetch the referenced output's tx.
				prevOut := txIn.PreviousOutPoint
				// Look up the location of the transaction.
				blockRegion, err := a.server.blockManager.chain.TxBlockRegion(nil, &prevOut.Hash)
				if err != nil {
					adxrLog.Errorf("Error fetching tx %v: %v",
						prevOut.Hash, err)
					return nil, err
				}
				if blockRegion == nil {
					return nil, fmt.Errorf("transaction %v not found",
						prevOut.Hash)
				}

				// Load the raw transaction bytes from the database.
				var txBytes []byte
				err = a.server.db.View(func(dbTx database.Tx) error {
					var err error
					txBytes, err = dbTx.FetchBlockRegion(blockRegion)
					return err
				})
				if err != nil {
					adxrLog.Errorf("Error fetching tx %v: %v",
						prevOut.Hash, err)
					return nil, err
				}

				// Deserialize the transaction
				var prevOutTx wire.MsgTx
				err = prevOutTx.Deserialize(bytes.NewReader(txBytes))

				inputOutPoint := prevOutTx.TxOut[prevOut.Index]

				a.indexScriptPubKey(addrIndex, inputOutPoint.PkScript, locInBlock)
			}
		}

		for _, txOut := range tx.MsgTx().TxOut {
			a.indexScriptPubKey(addrIndex, txOut.PkScript, locInBlock)
		}
	}
	return addrIndex, nil
}

// -----------------------------------------------------------------------------
// The addrindexer is a set of workers that are in charge of adding and removing
// blocks from the address index.
// The address index stores its own tip. At any moment of time, the tip
// reflects the highest block that has been indexed. It is updated atomically
// in a transaction together with the indexed data.
// There are three types of workers involved:
// - indexWorker: Takes jobs, processes blocks to extract the index data
//                in form of a map of address -> txs.
// - writeWorker: Takes jobs, waits for an indexWorker to finish indexing
//                the block, and then writes it to the database.
// - indexManager: generates the jobs that are consumed by the workers.
//
// It is critical the jobs are written to the database in the correct order.
// Otherwise, there could be "holes" in the index and the txs could be indexed
// out of order. To ensure this, there are two job queues (indexJobs and
// writeJobs). Every job is enqueued in both. Each job has its own result chan,
// and writeWorker processes jobs serially, waiting if necessary, so blocks
// are indexed in the correct order even if the indexWorkers finish indexing
// them out-of-order.
type addrIndexer struct {
	server         *server
	started        int32
	shutdown       int32
	quit           chan struct{}
	wg             sync.WaitGroup
	catchupJob     chan *addrIndexCatchupJob
	jobEvents      chan *addrIndexJob
	indexJobs      chan *addrIndexJob
	writeJobs      chan *addrIndexJob
	progressLogger *blockProgressLogger
	sync.Mutex
}

// addrIndexCatchupJob contains the necessary information for the addr index
// to catch up with the current chain state.
type addrIndexCatchupJob struct {
	hash   *wire.ShaHash
	height int32
}

// addrIndexJob represents a job to append or remove a block to the addrindex.
// The 'block' field is the block, the 'remove' field is true if the block
// should be removed. The 'data' channel is used to pass the indexing result
// from the index workers to the write worker.
type addrIndexJob struct {
	blk    *btcutil.Block
	remove bool
	data   chan writeIndexData
}

// newAddrIndexer creates a new block address indexer.
// Use Start to begin processing incoming index jobs.
func newAddrIndexer(s *server) (*addrIndexer, error) {
	ai := &addrIndexer{
		server:     s,
		quit:       make(chan struct{}),
		catchupJob: make(chan *addrIndexCatchupJob, 1),
		jobEvents:  make(chan *addrIndexJob, 5*numIndexWorkers),
		indexJobs:  make(chan *addrIndexJob, 5*numIndexWorkers),
		writeJobs:  make(chan *addrIndexJob, 5*numIndexWorkers),
		progressLogger: newBlockProgressLogger("Indexed addresses of",
			adxrLog),
	}
	return ai, nil
}

// Start begins processing of incoming indexing jobs.
func (a *addrIndexer) Start() {
	// Already started?
	if atomic.AddInt32(&a.started, 1) != 1 {
		return
	}
	adxrLog.Trace("Starting address indexer")

	err := dbCreateAddrIndex(a.server.db, a.server.chainParams.GenesisHash)
	if err != nil {
		adxrLog.Errorf("Error creating address index: %v", err)
		a.server.Stop()
		return
	}

	a.wg.Add(2 + numIndexWorkers)

	for i := 0; i < numIndexWorkers; i++ {
		go a.indexWorker()
	}

	go a.indexManager()
	go a.writeWorker()
}

// Stop gracefully shuts down the address indexer by stopping all ongoing
// worker goroutines, waiting for them to finish their current task.
func (a *addrIndexer) Stop() error {
	if atomic.AddInt32(&a.shutdown, 1) != 1 {
		adxrLog.Warnf("Address indexer is already in the process of " +
			"shutting down")
		a.wg.Wait()
		return nil
	}
	adxrLog.Infof("Address indexer shutting down")
	close(a.quit)
	a.wg.Wait()
	return nil
}

// IsCaughtUp returns a bool representing if the address indexer has
// caught up with the best height on the main chain.
func (a *addrIndexer) IsCaughtUp() bool {
	a.Lock()
	defer a.Unlock()
	//TODO(dirbaio): Implement this
	return true
}

// indexWorker indexes the transactions of previously validated and stored
// blocks. It will take jobs from the job queue and post them to the write
// queue.
// NOTE: Must be run as a goroutine
func (a *addrIndexer) indexWorker() {
	defer func() {
		a.wg.Done()
		adxrLog.Trace("Address index worker done")
	}()

	for {
		select {
		case indexJob := <-a.indexJobs:
			data, err := a.indexBlockAddrs(indexJob.blk)
			if err != nil {
				adxrLog.Errorf("Unable to index transactions of"+
					" block: %v", err)
				a.server.Stop()
				return
			}
			select {
			case indexJob.data <- data:
				// Do nothing
			case <-a.quit:
				return
			}
		case <-a.quit:
			return
		}
	}
}

// writeWorker commits the populated address indexes created by the
// index workers to the database.
// NOTE: Must be run as a goroutine
func (a *addrIndexer) writeWorker() {
	defer func() {
		a.wg.Done()
		adxrLog.Trace("Address index write worker done")
	}()

	for {
		select {
		case job := <-a.writeJobs:
			var data writeIndexData
			select {
			case data = <-job.data:
				// Do nothing
			case <-a.quit:
				return
			}
			err := a.server.db.Update(func(dbTx database.Tx) error {
				if job.remove {
					return removeAddrIndexDataForBlock(dbTx, job.blk, data)
				}
				return appendAddrIndexDataForBlock(dbTx, job.blk, data)
			})
			if err != nil {
				adxrLog.Errorf("Unable to write addr index for "+
					" block: %v", err)
				a.server.Stop()
				return
			}
			a.progressLogger.LogBlockHeight(job.blk)
		case <-a.quit:
			return
		}
	}
}

// enqueueJob enqueues a new job to be processed by the address index. It
// is enqueued both into the index and write queues. It returns true if
// the enqueuing has been interrupted due to the quit signal.
func (a *addrIndexer) enqueueJob(block *btcutil.Block, remove bool) bool {
	job := &addrIndexJob{
		blk:    block,
		remove: false,
		data:   make(chan writeIndexData),
	}

	// Enqueue it in both queues.
	// We need to check for quit signal to avoid deadlocks where the workers
	// have already quit and the channels are full.
	select {
	case a.indexJobs <- job:
		// Do nothing
	case <-a.quit:
		return true
	}
	select {
	case a.writeJobs <- job:
		// Do nothing
	case <-a.quit:
		return true
	}

	return false
}

// indexManager  is the main goroutine for the addresses indexer. It watches
// the block manager, and enqueues indexing jobs to drive the address index tip
// towards the chain tip.
// NOTE: Must be run as a goroutine.
func (a *addrIndexer) indexManager() {
	defer func() {
		a.wg.Done()
		adxrLog.Trace("Address index manager done")
	}()

	// Read the current addr index tip.
	var hash *wire.ShaHash
	var height int32
	err := a.server.db.View(func(dbTx database.Tx) error {
		var err error
		hash, height, err = dbFetchAddrIndexTip(dbTx)
		return err
	})
	if err != nil {
		adxrLog.Errorf("Unable to fetch addrindex tip %v", err)
		a.server.Stop()
		return
	}

	// Catch up with the current chain state.
	// Wait for the block manager to send us the current state.

	var catchup *addrIndexCatchupJob
	select {
	case catchup = <-a.catchupJob:
		// Do nothing
	case <-a.quit:
		return
	}

	// Check whether we're behind the chain state.
	if catchup.height > height {
		adxrLog.Infof("Building up address index from height %v to %v.",
			height+1, catchup.height)

		for i := height + 1; i <= catchup.height; i++ {
			// Check for Shutdown
			select {
			case <-a.quit:
				return
			default:
				// Do nothing
			}

			blk, err := a.server.blockManager.chain.BlockByHeight(i)
			if err != nil {
				adxrLog.Errorf("Unable to fetch block at height %v: %v", i, err)
				a.server.Stop()
				return
			}
			quit := a.enqueueJob(blk, false)
			if quit {
				return
			}
			hash = blk.Sha()
			height = i
		}
	}

	adxrLog.Infof("Address indexer has caught up to best height, entering " +
		"maintainence mode")

	// We're all caught up at this point. We now serially process new jobs
	// coming in.

	for {
		select {
		case job := <-a.jobEvents:
			quit := a.enqueueJob(job.blk, job.remove)
			if quit {
				return
			}
		case <-a.quit:
			return
		}
	}
}

// AddrIndexConnectBlock asynchronously queues a newly solved block to have its
// transactions indexed by address.
func (a *addrIndexer) AddrIndexStartCatchup(hash *wire.ShaHash, height int32) {
	a.catchupJob <- &addrIndexCatchupJob{hash: hash, height: height}
}

// AddrIndexConnectBlock asynchronously queues a newly solved block to have its
// transactions indexed by address.
func (a *addrIndexer) AddrIndexConnectBlock(block *btcutil.Block) {
	a.jobEvents <- &addrIndexJob{blk: block, remove: false}
}

// AddrIndexDisonnectBlock asynchronously queues a disconnected block to be
// removed from the address index.
func (a *addrIndexer) AddrIndexDisonnectBlock(block *btcutil.Block) {
	a.jobEvents <- &addrIndexJob{blk: block, remove: true}
}
