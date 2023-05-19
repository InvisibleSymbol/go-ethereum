// Copyright 2022 The go-ethereum Authors
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

package pathdb

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
)

// diffLayer represents a collection of modifications made to the in-memory tries
// after running a block on top.
//
// The goal of a diff layer is to act as a journal, tracking recent modifications
// made to the state, that have not yet graduated into a semi-immutable state.
type diffLayer struct {
	// Immutables
	rootHash common.Hash                               // Root hash to which this layer diff belongs to
	id       uint64                                    // Corresponding state id
	nodes    map[common.Hash]map[string]*trienode.Node // Cached trie nodes indexed by owner and path
	states   *triestate.Set                            // Associated state change set for building history
	memory   uint64                                    // Approximate guess as to how much memory we use

	parentLayer layer        // Parent layer modified by this one, never nil, **can be changed**
	lock        sync.RWMutex // Lock used to protect parent
}

// newDiffLayer creates a new diff on top of an existing layer.
func newDiffLayer(parent layer, root common.Hash, id uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	var (
		size  int64
		count int
	)
	dl := &diffLayer{
		rootHash:    root,
		id:          id,
		nodes:       nodes,
		states:      states,
		parentLayer: parent,
	}
	for _, subset := range nodes {
		for path, n := range subset {
			dl.memory += uint64(n.Size() + len(path))
			size += int64(len(n.Blob) + len(path))
		}
		count += len(subset)
	}
	if states != nil {
		dl.memory += uint64(states.Size())
	}
	dirtyWriteMeter.Mark(size)
	diffLayerNodesMeter.Mark(int64(count))
	diffLayerSizeMeter.Mark(int64(dl.memory))
	log.Debug("Created new diff layer", "id", id, "nodes", count, "size", common.StorageSize(dl.memory))
	return dl
}

// Root returns the root hash of corresponding state.
func (dl *diffLayer) root() common.Hash {
	return dl.rootHash
}

// ID returns the state id represented by layer.
func (dl *diffLayer) stateID() uint64 {
	return dl.id
}

// Parent returns the subsequent layer of a diff layer.
func (dl *diffLayer) parent() layer {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	return dl.parentLayer
}

// node retrieves the node with provided node information. It's the internal
// version of Node function with additional accessed layer tracked. No error
// will be returned if node is not found.
func (dl *diffLayer) node(owner common.Hash, path []byte, hash common.Hash, depth int) ([]byte, error) {
	// Hold the lock, ensure the parent won't be changed during the
	// state accessing.
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	// If the trie node is known locally, return it
	subset, ok := dl.nodes[owner]
	if ok {
		n, ok := subset[string(path)]
		if ok {
			// If the trie node is not hash matched, or marked as removed,
			// bubble up an error here. It shouldn't happen at all.
			if n.Hash != hash {
				return nil, &UnexpectedNodeError{
					typ:      "diff",
					expected: hash,
					hash:     n.Hash,
					owner:    owner,
					path:     path,
				}
			}
			dirtyHitMeter.Mark(1)
			dirtyNodeHitDepthHist.Update(int64(depth))
			dirtyReadMeter.Mark(int64(len(n.Blob)))
			return n.Blob, nil
		}
	}
	// Trie node unknown to this layer, resolve from parent
	if diff, ok := dl.parentLayer.(*diffLayer); ok {
		return diff.node(owner, path, hash, depth+1)
	}
	// Failed to resolve through diff layers, fallback to disk layer
	return dl.parentLayer.Node(owner, path, hash)
}

// Node retrieves the trie node blob with the provided node information. No error
// will be returned if the node is not found.
func (dl *diffLayer) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	return dl.node(owner, path, hash, 0)
}

// Update creates a new layer on top of the existing layer tree with the specified
// data items.
func (dl *diffLayer) update(stateRoot common.Hash, id uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	return newDiffLayer(dl, stateRoot, id, nodes, states)
}

// persist flushes the diff layer and all its parent layers to disk layer.
func (dl *diffLayer) persist(force bool) (layer, error) {
	if parent, ok := dl.parent().(*diffLayer); ok {
		// Hold the lock to prevent any read operation until the new
		// parent is linked correctly.
		dl.lock.Lock()

		// The merging of diff layers starts at the bottom-most layer,
		// therefore we recurse down here, flattening on the way up
		// (diffToDisk).
		result, err := parent.persist(force)
		if err != nil {
			dl.lock.Unlock()
			return nil, err
		}
		dl.parentLayer = result
		dl.lock.Unlock()
	}
	return diffToDisk(dl, force)
}

// diffToDisk merges a bottom-most diff into the persistent disk layer underneath
// it. The method will panic if called onto a non-bottom-most diff layer.
func diffToDisk(layer *diffLayer, force bool) (layer, error) {
	disk, ok := layer.parent().(*diskLayer)
	if !ok {
		panic(fmt.Sprintf("unknown layer type: %T", layer.parent()))
	}
	return disk.commit(layer, force)
}