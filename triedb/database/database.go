// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package database

import (
	"github.com/ethereum/go-ethereum/common"
)

// NodeReader wraps the Node method of a backing trie reader.
type NodeReader interface {
	// Node retrieves the trie node blob with the provided trie identifier,
	// node path and the corresponding node hash. No error will be returned
	// if the node is not found.
	Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error)
}

// NodeDatabase warps the methods of a backing trie store.
type NodeDatabase interface {
	// NodeReader returns a node reader associated with the specific state.
	// An error will be returned if the specified state is not available.
	NodeReader(stateRoot common.Hash) (NodeReader, error)
}

// StateReader wraps the Account and Storage method of a backing state reader.
type StateReader interface {
	Account(hash common.Hash) ([]byte, error)
	Storage(accountHash, storageHash common.Hash) ([]byte, error)
}

// StateDatabase warps the methods of a backing trie store.
type StateDatabase interface {
	// StateReader returns a state reader associated with the specific state.
	// An error will be returned if the specified state is not available.
	StateReader(stateRoot common.Hash) (StateReader, error)
}

// PreimageStore wraps the methods of a backing store for reading and writing
// trie node preimages.
type PreimageStore interface {
	// Preimage retrieves the preimage of the specified hash.
	Preimage(hash common.Hash) []byte

	// InsertPreimage commits a set of preimages along with their hashes.
	InsertPreimage(preimages map[common.Hash][]byte)
}

// Database wraps the methods of a backing trie store.
type Database interface {
	PreimageStore
	NodeDatabase
}