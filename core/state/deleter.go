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

package state

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

// merkleStorageDeleter implements StorageDeleter interface, providing
// functionality to wipe the storage belonging to a specific account,
// in the manner of Merkle-Patricia-Trie.
type merkleStorageDeleter struct {
	snap *snapshot.Tree
	db   *triedb.Database
	root common.Hash
}

// newMerkleStorageDeleter creates a merkle storage deleter.
func newMerkleStorageDeleter(snap *snapshot.Tree, db *triedb.Database, root common.Hash) *merkleStorageDeleter {
	return &merkleStorageDeleter{
		snap: snap,
		db:   db,
		root: root,
	}
}

// Delete implements StorageDeleter interface. It's designed to delete the storage
// slots of a designated account. It could potentially be terminated if the storage
// size is excessively large, potentially leading to an out-of-memory panic. The
// function will make an attempt to utilize an efficient strategy if the associated
// state snapshot is reachable; otherwise, it will resort to a less-efficient approach.
func (d *merkleStorageDeleter) Delete(addr common.Address, root common.Hash) (map[common.Hash][]byte, *trienode.NodeSet, error) {
	var (
		err      error
		slots    map[common.Hash][]byte
		nodes    *trienode.NodeSet
		addrHash = crypto.Keccak256Hash(addr.Bytes())
	)
	// The fast approach can be failed if the snapshot is not fully
	// generated, or it's internally corrupted. Fallback to the slow
	// one just in case.
	if d.snap != nil {
		slots, nodes, err = d.fastDeleteStorage(addrHash, root)
	}
	if d.snap == nil || err != nil {
		slots, nodes, err = d.slowDeleteStorage(addrHash, root)
	}
	if err != nil {
		return nil, nil, err
	}
	return slots, nodes, nil
}

// fastDeleteStorage is the function that efficiently deletes the storage trie
// of a specific account. It leverages the associated state snapshot for fast
// storage iteration and constructs trie node deletion markers by creating
// stack trie with iterated slots.
func (d *merkleStorageDeleter) fastDeleteStorage(addrHash common.Hash, root common.Hash) (map[common.Hash][]byte, *trienode.NodeSet, error) {
	iter, err := d.snap.StorageIterator(d.root, addrHash, common.Hash{})
	if err != nil {
		return nil, nil, err
	}
	defer iter.Release()

	var (
		size  common.StorageSize
		nodes = trienode.NewNodeSet(addrHash)
		slots = make(map[common.Hash][]byte)
	)
	options := trie.NewStackTrieOptions()
	options = options.WithWriter(func(path []byte, hash common.Hash, blob []byte) {
		nodes.AddNode(path, trienode.NewDeleted())
		size += common.StorageSize(len(path))
	})
	stack := trie.NewStackTrie(options)
	for iter.Next() {
		slot := common.CopyBytes(iter.Slot())
		if err := iter.Error(); err != nil { // error might occur after Slot function
			return nil, nil, err
		}
		size += common.StorageSize(common.HashLength + len(slot))
		slots[iter.Hash()] = slot

		if err := stack.Update(iter.Hash().Bytes(), slot); err != nil {
			return nil, nil, err
		}
	}
	if err := iter.Error(); err != nil { // error might occur during iteration
		return nil, nil, err
	}
	if stack.Hash() != root {
		return nil, nil, fmt.Errorf("snapshot is not matched, exp %x, got %x", root, stack.Hash())
	}
	return slots, nodes, nil
}

// slowDeleteStorage serves as a less-efficient alternative to "fastDeleteStorage,"
// employed when the associated state snapshot is not available. It iterates the
// storage slots along with all internal trie nodes via trie directly.
func (d *merkleStorageDeleter) slowDeleteStorage(addrHash common.Hash, root common.Hash) (map[common.Hash][]byte, *trienode.NodeSet, error) {
	if d.db == nil {
		return nil, nil, errors.New("trie loading is not supported")
	}
	tr, err := trie.NewStateTrie(trie.StorageTrieID(d.root, addrHash, root), d.db)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open storage trie, err: %w", err)
	}
	it, err := tr.NodeIterator(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open storage iterator, err: %w", err)
	}
	var (
		nodes = trienode.NewNodeSet(addrHash)
		slots = make(map[common.Hash][]byte)
	)
	for it.Next(true) {
		if it.Leaf() {
			slots[common.BytesToHash(it.LeafKey())] = common.CopyBytes(it.LeafBlob())
			continue
		}
		if it.Hash() == (common.Hash{}) {
			continue
		}
		nodes.AddNode(it.Path(), trienode.NewDeleted())
	}
	if err := it.Error(); err != nil {
		return nil, nil, err
	}
	return slots, nodes, nil
}
