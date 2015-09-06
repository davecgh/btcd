// Copyright (c) 2015 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"testing"

	"github.com/btcsuite/btcd/wire"
)

// TestAddrIndexSerialization ensures serializing and deserializing address
// index entries works as expected.
func TestAddrIndexSerialization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		addrHash      []byte
		existingEntry []byte
		blockHeight   int32
		txLoc         wire.TxLoc
		serialized    []byte
	}{}

	for i, test := range tests {
		// Ensure the entry serializes to the expected value.
		gotBytes := serializeAddrIndexEntry(test.existingEntry,
			test.blockHeight, test.txLoc)
		if !bytes.Equal(gotBytes, test.serialized) {
			t.Errorf("serializeAddrIndexEntry #%d (%s): mismatched "+
				"bytes - got %x, want %x", i, test.name,
				gotBytes, test.serialized)
			continue
		}

		// TODO(davec): Deserialize and ensure accurate...
	}
}
