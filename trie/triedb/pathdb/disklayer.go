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
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
)

// diskLayer is a low level persistent layer built on top of a key-value store.
type diskLayer struct {
	rootHash common.Hash  // Immutable, root hash of the base layer
	id       uint64       // Immutable, corresponding state id
	db       *Database    // Path-based trie database
	buffer   *nodebuffer  // Node buffer to aggregate writes.
	stale    bool         // Signals that the layer became stale (state progressed)
	lock     sync.RWMutex // Lock used to protect stale flag
}

// newDiskLayer creates a new disk layer based on the passing arguments.
func newDiskLayer(root common.Hash, id uint64, db *Database, buffer *nodebuffer) *diskLayer {
	return &diskLayer{
		rootHash: root,
		id:       id,
		db:       db,
		buffer:   buffer,
	}
}

// root returns root hash of corresponding state.
func (dl *diskLayer) root() common.Hash {
	return dl.rootHash
}

// parent always returns nil as there's no layer below the disk.
func (dl *diskLayer) parent() layer {
	return nil
}

// stateID returns the state id of disk layer.
func (dl *diskLayer) stateID() uint64 {
	return dl.id
}

// isStale return whether this layer has become stale (was flattened across) or if
// it's still live.
func (dl *diskLayer) isStale() bool {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	return dl.stale
}

// markStale sets the stale flag as true.
func (dl *diskLayer) markStale() {
	dl.lock.Lock()
	defer dl.lock.Unlock()

	if dl.stale {
		panic("triedb disk layer is stale") // we've committed into the same base from two children, boom
	}
	dl.stale = true
}

// Node retrieves the trie node with the provided node info. No error will be
// returned if the node is not found.
func (dl *diskLayer) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	if dl.stale {
		return nil, errSnapshotStale
	}
	// Try to retrieve the trie node from the not-yet-written
	// node buffer first. Note the buffer is lock free since
	// it's impossible to mutate the buffer before tagging the
	// layer as stale.
	n, err := dl.buffer.node(owner, path, hash)
	if err != nil {
		return nil, err
	}
	if n != nil {
		// Hit node in disk cache which resides in disk layer
		dirtyHitMeter.Mark(1)
		dirtyReadMeter.Mark(int64(len(n.Blob)))
		return n.Blob, nil
	}
	// If we're in the disk layer, all diff layers missed
	dirtyMissMeter.Mark(1)

	// Try to retrieve the trie node from the clean memory cache
	if dl.db.cleans != nil {
		if blob := dl.db.cleans.Get(nil, hash.Bytes()); len(blob) > 0 {
			cleanHitMeter.Mark(1)
			cleanReadMeter.Mark(int64(len(blob)))
			return blob, nil
		}
		cleanMissMeter.Mark(1)
	}
	// Try to retrieve the trie node from the disk.
	var (
		nBlob []byte
		nHash common.Hash
	)
	if owner == (common.Hash{}) {
		nBlob, nHash = rawdb.ReadAccountTrieNode(dl.db.diskdb, path)
	} else {
		nBlob, nHash = rawdb.ReadStorageTrieNode(dl.db.diskdb, owner, path)
	}
	if nHash != hash {
		return nil, &UnexpectedNodeError{
			typ:      "disk",
			expected: hash,
			hash:     nHash,
			owner:    owner,
			path:     path,
		}
	}
	if dl.db.cleans != nil && len(nBlob) > 0 {
		dl.db.cleans.Set(hash.Bytes(), nBlob)
		cleanWriteMeter.Mark(int64(len(nBlob)))
	}
	return nBlob, nil
}

// update returns a new diff layer on top with the given dirty node set.
func (dl *diskLayer) update(stateRoot common.Hash, id uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	return newDiffLayer(dl, stateRoot, id, nodes, states)
}

// commit merges the given bottom-most diff layer into the node buffer
// and returns a newly constructed disk layer. Note the current disk
// layer must be tagged as stale first to prevent re-access.
func (dl *diskLayer) commit(bottom *diffLayer, force bool) (*diskLayer, error) {
	dl.lock.Lock()
	defer dl.lock.Unlock()

	// Construct and store the state history first. If crash happens
	// after storing the state history but without flushing the
	// corresponding states(journal), the stored state history will
	// be truncated in the next restart.
	if dl.db.freezer != nil {
		err := writeStateHistory(dl.db.freezer, bottom, dl.db.config.StateLimit)
		if err != nil {
			return nil, err
		}
	}
	// Mark the diskLayer as stale before applying any mutations on top.
	dl.stale = true

	// Store the root->id lookup afterwards. All stored lookups are
	// identified by the **unique** state root. It's impossible that
	// in the same chain blocks which are not adjacent have the same
	// root.
	if dl.id == 0 {
		rawdb.WriteStateID(dl.db.diskdb, dl.rootHash, 0)
	}
	rawdb.WriteStateID(dl.db.diskdb, bottom.root(), bottom.stateID())

	// Persist the content in disk layer if there are too many nodes cached.
	ndl := newDiskLayer(bottom.rootHash, bottom.id, dl.db, dl.buffer.commit(bottom.nodes))
	err := ndl.buffer.flush(ndl.db.diskdb, ndl.db.cleans, ndl.id, force)
	if err != nil {
		return nil, err
	}
	return ndl, nil
}

// revert applies the given reverse diff by reverting the disk layer
// and return a newly constructed disk layer.
func (dl *diskLayer) revert(h *history, loader triestate.TrieLoader) (*diskLayer, error) {
	if h.meta.root != dl.root() {
		return nil, errUnexpectedHistory
	}
	if len(h.meta.incomplete) > 0 {
		return nil, errors.New("incomplete state history")
	}
	if dl.id == 0 {
		return nil, fmt.Errorf("%w: zero state id", errStateUnrecoverable)
	}
	// Apply the reverse state changes upon the current state. This must
	// be done before holding the lock in order to access state in "this"
	// layer.
	nodes, err := triestate.Apply(h.meta.parent, h.meta.root, h.accounts, h.storages, loader)
	if err != nil {
		return nil, err
	}
	// Mark the diskLayer as stale before applying any mutations on top.
	dl.lock.Lock()
	defer dl.lock.Unlock()

	dl.stale = true

	// Revert embedded states in the node buffer first in case
	// it's not empty.
	if !dl.buffer.empty() {
		err := dl.buffer.revert(nodes)
		if err != nil {
			return nil, err
		}
	} else {
		// The node buffer is empty, applies the state changes in
		// disk state directly.
		batch := dl.db.diskdb.NewBatch()
		writeNodes(batch, nodes, nil)
		rawdb.WritePersistentStateID(batch, dl.id-1)
		if err := batch.Write(); err != nil {
			log.Crit("Failed to write states", "err", err)
		}
		// Reset the clean cache in case disk state is mutated.
		if dl.db.cleans != nil {
			dl.db.cleans.Reset()
		}
	}
	return newDiskLayer(h.meta.parent, dl.id-1, dl.db, dl.buffer), nil
}

// setCacheSize sets the dirty cache size to the provided value.
func (dl *diskLayer) setCacheSize(size int) error {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	if dl.stale {
		return errSnapshotStale
	}
	return dl.buffer.setSize(size, dl.db.diskdb, dl.db.cleans, dl.id)
}

// size returns the approximate size of cached nodes in the disk layer.
func (dl *diskLayer) size() common.StorageSize {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	if dl.stale {
		return 0
	}
	return common.StorageSize(dl.buffer.size)
}