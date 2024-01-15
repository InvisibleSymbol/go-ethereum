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

package triedb

import "github.com/ethereum/go-ethereum/common"

// reader is an interface for accessing trie nodes. It should be implemented
// by different backends.
type reader interface {
	// Node retrieves the value associated with a trie node identified by its
	// owner hash, node path, and node hash.
	Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error)
}

// Reader is a public structure used to access nodes from the trie database.
type Reader struct {
	reader reader
}

// Node retrieves the trie node blob with the provided trie identifier,
// node path and the corresponding node hash. No error will be returned
// if the node is not found.
//
// When looking up nodes in the account trie, 'owner' is the zero hash.
// For contract storage trie nodes, 'owner' is the hash of the account
// address that containing the storage.
//
// Don't modify the returned byte slice since it's not deep-copied and
// still be referenced by database.
func (r *Reader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	return r.reader.Node(owner, path, hash)
}
