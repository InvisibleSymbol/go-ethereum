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
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

var (
	// defaultBufferSize is the default memory limitation of the node buffer
	// that aggregates the writes from above until it's flushed into the disk.
	// Do not increase the buffer size arbitrarily, otherwise the system pause
	// time will increase when the database writes happen.
	defaultBufferSize = 128 * 1024 * 1024
)

// nodebuffer is a collection of modified trie nodes to aggregate the disk
// write. The content of the nodebuffer must be checked before diving into
// disk (since it basically is not-yet-written data).
type nodebuffer struct {
	layers uint64                                    // The number of diff layers aggregated inside
	size   uint64                                    // The size of aggregated writes
	limit  uint64                                    // The maximum memory allowance in bytes
	nodes  map[common.Hash]map[string]*trienode.Node // The dirty node set, mapped by owner and path
}

// newNodeBuffer initializes the node buffer with the provided nodes.
func newNodeBuffer(limit int, nodes map[common.Hash]map[string]*trienode.Node, layers uint64) *nodebuffer {
	if nodes == nil {
		nodes = make(map[common.Hash]map[string]*trienode.Node)
	}
	var size uint64
	for _, subset := range nodes {
		for path, n := range subset {
			size += uint64(len(n.Blob) + len(path))
		}
	}
	return &nodebuffer{
		layers: layers,
		nodes:  nodes,
		size:   size,
		limit:  uint64(limit),
	}
}

// node retrieves the trie node with given node info.
func (b *nodebuffer) node(owner common.Hash, path []byte, hash common.Hash) (*trienode.Node, error) {
	subset, ok := b.nodes[owner]
	if !ok {
		return nil, nil
	}
	n, ok := subset[string(path)]
	if !ok {
		return nil, nil
	}
	if n.Hash != hash {
		return nil, &UnexpectedNodeError{
			typ:      "cache",
			expected: hash,
			hash:     n.Hash,
			owner:    owner,
			path:     path,
		}
	}
	return n, nil
}

// commit merges the dirty nodes into the nodebuffer. This operation won't take
// the ownership of the nodes map which belongs to the bottom-most diff layer.
// It will just hold the node references from the given map which are safe to
// copy.
func (b *nodebuffer) commit(nodes map[common.Hash]map[string]*trienode.Node) *nodebuffer {
	var (
		delta         int64
		overwrite     int64
		overwriteSize int64
	)
	for owner, subset := range nodes {
		current, exist := b.nodes[owner]
		if !exist {
			// Allocate a new map for the subset instead of claiming it directly
			// from the passed one to avoid potential concurrent map read/write.
			// The nodes belong to original diff layer are still accessible even
			// after merging, thus the ownership of nodes map should still belong
			// to original layer and any mutation on it should be prevented.
			b.nodes[owner] = make(map[string]*trienode.Node)
			for path, n := range subset {
				b.nodes[owner][path] = n
				delta += int64(len(n.Blob) + len(path))
			}
			continue
		}
		for path, n := range subset {
			if orig, exist := current[path]; !exist {
				delta += int64(len(n.Blob) + len(path))
			} else {
				delta += int64(len(n.Blob) - len(orig.Blob))
				overwrite += 1
				overwriteSize += int64(len(orig.Blob) + len(path))
			}
			b.nodes[owner][path] = n
		}
	}
	b.updateSize(delta)
	b.layers += 1
	gcNodesMeter.Mark(overwrite)
	gcSizeMeter.Mark(overwriteSize)
	return b
}

// revert is the reverse operation of commit. It also merges the provided nodes
// into the nodebuffer, but difference is it decrements the layers counter. The
// provided nodes don't belong to any live layer but generated inflight, safe to
// take the ownership if necessary.
func (b *nodebuffer) revert(nodes map[common.Hash]map[string]*trienode.Node) error {
	if b.layers == 0 {
		return errStateUnrecoverable
	}
	b.layers -= 1
	if b.layers == 0 {
		b.reset()
		return nil
	}
	var delta int64
	for owner, subset := range nodes {
		current, ok := b.nodes[owner]
		if !ok {
			panic(fmt.Sprintf("non-existent subset (%x)", owner))
		}
		for path, n := range subset {
			cur, ok := current[path]
			if !ok {
				panic(fmt.Sprintf("non-existent node (%x %v) blob: %v", owner, path, crypto.Keccak256Hash(n.Blob).Hex()))
			}
			if len(n.Blob) == 0 {
				current[path] = trienode.NewDeleted()
				delta -= int64(len(cur.Blob))
			} else {
				current[path] = trienode.New(crypto.Keccak256Hash(n.Blob), n.Blob)
				delta += int64(len(n.Blob)) - int64(len(cur.Blob))
			}
		}
	}
	b.updateSize(delta)
	return nil
}

// updateSize updates the total cache size by the given delta.
func (b *nodebuffer) updateSize(delta int64) {
	size := int64(b.size) + delta
	if size >= 0 {
		b.size = uint64(size)
		return
	}
	s := b.size
	b.size = 0
	log.Error("Invalid buffer size", "prev", common.StorageSize(s), "delta", common.StorageSize(delta))
}

// reset cleans up the disk cache.
func (b *nodebuffer) reset() {
	b.layers = 0
	b.size = 0
	b.nodes = make(map[common.Hash]map[string]*trienode.Node)
}

// empty returns an indicator if nodebuffer contains any state transition inside.
func (b *nodebuffer) empty() bool {
	return b.layers == 0
}

// setSize sets the buffer size to the provided number, and invokes a flush
// operation if the current memory usage exceeds the new limit.
func (b *nodebuffer) setSize(size int, db ethdb.KeyValueStore, clean *fastcache.Cache, id uint64) error {
	b.limit = uint64(size)
	return b.flush(db, clean, id, false)
}

// flush persists the in-memory dirty trie node into the disk if the configured
// memory threshold is reached. Note, all data must be written atomically.
func (b *nodebuffer) flush(db ethdb.KeyValueStore, clean *fastcache.Cache, id uint64, force bool) error {
	if b.size <= b.limit && !force {
		return nil
	}
	// Ensure the given target state id is aligned with the internal counter.
	head := rawdb.ReadPersistentStateID(db)
	if head+b.layers != id {
		return fmt.Errorf("disk has invalid state id %d, want %d", id, head+b.layers)
	}
	var (
		start = time.Now()
		batch = db.NewBatchWithSize(int(b.size))
	)
	writeNodes(batch, b.nodes, clean)
	rawdb.WritePersistentStateID(batch, id)
	if err := batch.Write(); err != nil {
		return err
	}
	commitSizeMeter.Mark(int64(batch.ValueSize()))
	commitNodesMeter.Mark(int64(len(b.nodes)))
	commitTimeTimer.UpdateSince(start)
	log.Debug("Persisted nodes", "number", len(b.nodes), "size", common.StorageSize(batch.ValueSize()), "elapsed", common.PrettyDuration(time.Since(start)))
	b.reset()
	return nil
}

// writeNodes writes the trie nodes into the provided database batch.
func writeNodes(batch ethdb.Batch, nodes map[common.Hash]map[string]*trienode.Node, clean *fastcache.Cache) {
	for owner, subset := range nodes {
		for path, n := range subset {
			if n.IsDeleted() {
				if owner == (common.Hash{}) {
					rawdb.DeleteAccountTrieNode(batch, []byte(path))
				} else {
					rawdb.DeleteStorageTrieNode(batch, owner, []byte(path))
				}
				continue
			}
			if owner == (common.Hash{}) {
				rawdb.WriteAccountTrieNode(batch, []byte(path), n.Blob)
			} else {
				rawdb.WriteStorageTrieNode(batch, owner, []byte(path), n.Blob)
			}
			if clean != nil {
				clean.Set(n.Hash.Bytes(), n.Blob)
			}
		}
	}
}
