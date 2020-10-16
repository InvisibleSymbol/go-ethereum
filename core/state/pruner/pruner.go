// Copyright 2020 The go-ethereum Authors
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

package pruner

import (
	"bytes"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

// stateBloomFileName is the filename of state bloom filter.
const stateBloomFileName = "statebloom.bf.gz"

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyCode is the known hash of the empty EVM bytecode.
	emptyCode = crypto.Keccak256(nil)
)

type Pruner struct {
	db             ethdb.Database
	stateBloom     *StateBloom
	stateBloomPath string
	snaptree       *snapshot.Tree
}

// NewPruner creates the pruner instance.
func NewPruner(db ethdb.Database, root common.Hash, homedir string) (*Pruner, error) {
	snaptree, err := snapshot.New(db, trie.NewDatabase(db), 256, root, false, false, false)
	if err != nil {
		return nil, err // The relevant snapshot(s) might not exist
	}
	// TODO @rjl493456442 don't hardcode here.
	stateBloom, err := NewStateBloom(600*1024*1024 /* 600M */, 0.0005 /* 0.05% */)
	if err != nil {
		return nil, err
	}
	return &Pruner{
		db:             db,
		stateBloom:     stateBloom,
		stateBloomPath: filepath.Join(homedir, stateBloomFileName),
		snaptree:       snaptree,
	}, nil
}

func prune(maindb ethdb.Database, stateBloom *StateBloom, start time.Time) error {
	// Extract all node refs belong to the genesis. We have to keep the
	// genesis all the time.
	marker, err := extractGenesis(maindb)
	if err != nil {
		return err
	}
	// Delete all old trie nodes in the disk(it's safe since we already commit
	// a complete trie to the temporary db, any crash happens we can recover
	// a complete state from it).
	var (
		count  int
		size   common.StorageSize
		pstart = time.Now()
		logged = time.Now()
		batch  = maindb.NewBatch()
		iter   = maindb.NewIterator(nil, nil)

		rangestart, rangelimit []byte
	)
	for iter.Next() {
		key := iter.Key()

		// Note all entries with 32byte length key(trie nodes,
		// contract codes) are deleted here.
		isCode, codeKey := rawdb.IsCodeKey(key)
		if len(key) == common.HashLength || isCode {
			if isCode {
				if _, ok := marker[common.BytesToHash(codeKey)]; ok {
					continue // Genesis contract code
				}
				ok, err := stateBloom.Contain(codeKey)
				if err != nil {
					return err // Something very wrong
				}
				if ok {
					continue
				}
			} else {
				if _, ok := marker[common.BytesToHash(key)]; ok {
					continue // Genesis state trie node or legacy contract code
				}
				ok, err := stateBloom.Contain(key)
				if err != nil {
					return err // Something very wrong
				}
				if ok {
					continue
				}
			}
			size += common.StorageSize(len(key) + len(iter.Value()))
			batch.Delete(key)

			if batch.ValueSize() >= ethdb.IdealBatchSize {
				batch.Write()
				batch.Reset()
			}
			count += 1
			if count%1000 == 0 && time.Since(logged) > 8*time.Second {
				log.Info("Pruning state data", "count", count, "size", size, "elapsed", common.PrettyDuration(time.Since(pstart)))
				logged = time.Now()
			}
			if rangestart == nil || bytes.Compare(rangestart, key) > 0 {
				if rangestart == nil {
					rangestart = make([]byte, common.HashLength)
				}
				copy(rangestart, key)
			}
			if rangelimit == nil || bytes.Compare(rangelimit, key) < 0 {
				if rangelimit == nil {
					rangelimit = make([]byte, common.HashLength)
				}
				copy(rangelimit, key)
			}
		}
	}
	if batch.ValueSize() > 0 {
		batch.Write()
		batch.Reset()
	}
	iter.Release() // Please release the iterator here, otherwise will block the compactor
	log.Info("Pruned state data", "count", count, "size", size, "elapsed", common.PrettyDuration(time.Since(pstart)))

	// Start compactions, will remove the deleted data from the disk immediately.
	cstart := time.Now()
	log.Info("Start compacting the database")
	if err := maindb.Compact(rangestart, rangelimit); err != nil {
		log.Error("Failed to compact the whole database", "error", err)
	}
	log.Info("Compacted the whole database", "elapsed", common.PrettyDuration(time.Since(cstart)))
	log.Info("Successfully prune the state", "pruned", size, "elasped", common.PrettyDuration(time.Since(start)))
	return nil
}

// Prune deletes all historical state nodes except the nodes belong to the
// specified state version. If user doesn't specify the state version, use
// the persisted snapshot disk layer as the target.
func (p *Pruner) Prune(root common.Hash) error {
	// If the target state root is not specified, use the oldest layer
	// (disk layer). Fresh new layer as the target is not recommended,
	// since it might be non-canonical.
	if root == (common.Hash{}) {
		root = rawdb.ReadSnapshotRoot(p.db)
		if root == (common.Hash{}) {
			return errors.New("no target state specified")
		}
	}
	start := time.Now()
	// Traverse the target state, re-construct the whole state trie and
	// commit to the given temporary database.
	if err := snapshot.CommitAndVerifyState(p.snaptree, root, p.db, p.stateBloom); err != nil {
		return err
	}
	type commiter interface {
		Commit(string) error
	}
	if err := p.stateBloom.Commit(p.stateBloomPath); err != nil {
		return err
	}
	if err := prune(p.db, p.stateBloom, start); err != nil {
		return err
	}
	os.RemoveAll(p.stateBloomPath)
	return nil
}

// RecoverTemporaryDatabase migrates all state data from external state database
// to given main db. If the state database is broken(not complete), then interrupt
// the recovery.
//
// Note before the migration, the main db still needs to be pruned, otherwise the
// dangling nodes will be left.
//
// This function is used in this case: user tries to prune state data, but after
// creating the backup for specific version, the system exits(maually or crashed).
// In the next restart, the paused pruning should be continued.
func RecoverTemporaryDatabase(homedir string, db ethdb.Database) error {
	stateBloomPath := filepath.Join(homedir, stateBloomFileName)
	if _, err := os.Stat(stateBloomPath); os.IsNotExist(err) {
		return nil // nothing to recover
	}
	stateBloom, err := NewStateBloomFromDisk(stateBloomPath)
	if err != nil {
		return err
	}
	if err := prune(db, stateBloom, time.Now()); err != nil {
		return err
	}
	os.RemoveAll(stateBloomPath)
	return nil
}

// extractGenesis loads the genesis state and creates the nodes marker.
// So that it can be used as an present indicator for all genesis trie nodes.
func extractGenesis(db ethdb.Database) (map[common.Hash]struct{}, error) {
	genesisHash := rawdb.ReadCanonicalHash(db, 0)
	if genesisHash == (common.Hash{}) {
		return nil, errors.New("missing genesis hash")
	}
	genesis := rawdb.ReadBlock(db, genesisHash, 0)
	if genesis == nil {
		return nil, errors.New("missing genesis block")
	}
	t, err := trie.New(genesis.Root(), trie.NewDatabase(db))
	if err != nil {
		return nil, err
	}
	marker := make(map[common.Hash]struct{})
	accIter := t.NodeIterator(nil)
	for accIter.Next(true) {
		node := accIter.Hash()

		// Embeded nodes don't have hash.
		if node != (common.Hash{}) {
			marker[node] = struct{}{}
		}
		// If it's a leaf node, yes we are touching an account,
		// dig into the storage trie further.
		if accIter.Leaf() {
			var acc struct {
				Nonce    uint64
				Balance  *big.Int
				Root     common.Hash
				CodeHash []byte
			}
			if err := rlp.DecodeBytes(accIter.LeafBlob(), &acc); err != nil {
				return nil, err
			}
			if acc.Root != emptyRoot {
				storageTrie, err := trie.NewSecure(acc.Root, trie.NewDatabase(db))
				if err != nil {
					return nil, err
				}
				storageIter := storageTrie.NodeIterator(nil)
				for storageIter.Next(true) {
					node := storageIter.Hash()
					if node != (common.Hash{}) {
						marker[node] = struct{}{}
					}
				}
				if storageIter.Error() != nil {
					return nil, storageIter.Error()
				}
			}
			if !bytes.Equal(acc.CodeHash, emptyCode) {
				marker[common.BytesToHash(acc.CodeHash)] = struct{}{}
			}
		}
	}
	if accIter.Error() != nil {
		return nil, accIter.Error()
	}
	return marker, nil
}
