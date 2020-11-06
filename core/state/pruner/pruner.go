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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

const (
	// stateBloomFilePrefix is the filename prefix of state bloom filter.
	stateBloomFilePrefix = "statebloom"

	// stateBloomFilePrefix is the filename suffix of state bloom filter.
	stateBloomFileSuffix = "bf.gz"

	// bloomFilterEntries is the estimated value of the number of trie nodes
	// and codes contained in the state. It's designed for mainnet but also
	// suitable for other small testnets.
	bloomFilterEntries = 600 * 1024 * 1024

	// bloomFalsePositiveRate is the acceptable probability of bloom filter
	// false-positive. It's around 0.01%.
	//
	// Check the https://hur.st/bloomfilter/?n=600000000&p=0.0005&m=&k= for
	// more calculation details.
	bloomFalsePositiveRate = 0.0005

	// rangeCompactionThreshold is the minimal deleted entry number for
	// triggering range compaction. It's a quite arbitrary number but just
	// to avoid triggering range compaction because of small deletion.
	rangeCompactionThreshold = 100000
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyCode is the known hash of the empty EVM bytecode.
	emptyCode = crypto.Keccak256(nil)
)

// Pruner is an offline tool to prune the stale state with the
// help of the snapshot. The workflow of pruner is very simple:
//
// - iterate the snapshot, reconstruct the relevant state
// - iterate the database, delete all other state entries which
//   don't belong to the target state and the genesis state
//
// It can take several hours(around 2 hours for mainnet) to finish
// the whole pruning work. It's recommended to run this offline tool
// periodically in order to release the disk usage and improve the
// disk read performance to some extent.
type Pruner struct {
	db            ethdb.Database
	stateBloom    *stateBloom
	datadir       string
	trieCachePath string
	headHeader    *types.Header
	snaptree      *snapshot.Tree
}

// NewPruner creates the pruner instance.
func NewPruner(db ethdb.Database, headHeader *types.Header, datadir, trieCachePath string, bloomSize uint64) (*Pruner, error) {
	snaptree, err := snapshot.New(db, trie.NewDatabase(db), 256, headHeader.Root, false, false, false)
	if err != nil {
		return nil, err // The relevant snapshot(s) might not exist
	}
	// Sanitize the bloom filter size if it's too small.
	if bloomSize < 256 {
		log.Warn("Sanitizing bloomfilter size", "provided(MB)", bloomSize, "updated(MB)", 256)
		bloomSize = 256
	}
	stateBloom, err := newStateBloomWithSize(bloomSize)
	if err != nil {
		return nil, err
	}
	return &Pruner{
		db:            db,
		stateBloom:    stateBloom,
		datadir:       datadir,
		trieCachePath: trieCachePath,
		headHeader:    headHeader,
		snaptree:      snaptree,
	}, nil
}

func prune(maindb ethdb.Database, stateBloom *stateBloom, start time.Time) error {
	// Delete all stale trie nodes in the disk. With the help of state bloom
	// the trie nodes(and codes) belong to the active state will be filtered
	// out. A very small part of stale tries will also be filtered because of
	// the false-positive rate of bloom filter. But the assumption is held here
	// that the false-positive is low enough(~0.05%). The probablity of the
	// dangling node is the state root is super low. So the dangling nodes in
	// theory will never ever be visited again.
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

		// All state entries don't belong to specific state and genesis are deleted here
		// - trie node
		// - legacy contract code
		// - new-scheme contract code
		isCode, codeKey := rawdb.IsCodeKey(key)
		if len(key) == common.HashLength || isCode {
			checkKey := key
			if isCode {
				checkKey = codeKey
			}
			// Filter out the state belongs the pruning target
			if ok, err := stateBloom.Contain(checkKey); err != nil {
				return err
			} else if ok {
				continue
			}
			// Filter out the state belongs to the "blacklist". Usually
			// the root of the "useless" states are contained here.
			size += common.StorageSize(len(key) + len(iter.Value()))
			batch.Delete(key)

			if batch.ValueSize() >= ethdb.IdealBatchSize {
				batch.Write()
				batch.Reset()
			}
			count += 1
			if time.Since(logged) > 8*time.Second {
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
	// Note for small pruning, the compaction is skipped.
	if count >= rangeCompactionThreshold {
		cstart := time.Now()
		log.Info("Start compacting the database")
		if err := maindb.Compact(rangestart, rangelimit); err != nil {
			log.Error("Failed to compact the whole database", "error", err)
		}
		log.Info("Compacted the whole database", "elapsed", common.PrettyDuration(time.Since(cstart)))
	}
	log.Info("Successfully prune the state", "pruned", size, "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

// Prune deletes all historical state nodes except the nodes belong to the
// specified state version. If user doesn't specify the state version, use
// the bottom-most snapshot diff layer as the target.
func (p *Pruner) Prune(root common.Hash) error {
	// If the state bloom filter is already committed previously,
	// reuse it for pruning instead of generating a new one. It's
	// mandatory because a part of state may already be deleted,
	// the recovery procedure is necessary.
	_, stateBloomRoot, err := findBloomFilter(p.datadir)
	if err != nil {
		return err
	}
	if stateBloomRoot != (common.Hash{}) {
		return RecoverPruning(p.datadir, p.db, p.trieCachePath)
	}
	// If the target state root is not specified, use the HEAD-127 as the
	// target. The reason for picking it is:
	// - in most of the normal cases, the related state is available
	// - the probability of this layer being reorg is very low
	var layers []snapshot.Snapshot
	if root == (common.Hash{}) {
		// Retrieve all snapshot layers from the current HEAD.
		// In theory there are 128 difflayers + 1 disk layer present,
		// so 128 diff layers are expected to be returned.
		layers = p.snaptree.Snapshots(p.headHeader.Root, 128, true)
		if len(layers) != 128 {
			// Reject if the accumulated diff layers are less than 128. It
			// means in most of normal cases, there is no associated state
			// with bottom-most diff layer.
			return errors.New("the snapshot difflayers are less than 128")
		}
		// Use the bottom-most diff layer as the target
		root = layers[len(layers)-1].Root()
	}
	// Ensure the root is really present. The weak assumption
	// is the presence of root can indicate the presence of the
	// entire trie.
	if blob := rawdb.ReadTrieNode(p.db, root); len(blob) == 0 {
		// The special case is for clique based networks(rinkeby, goerli
		// and some other private networks), it's possible that two
		// consecutive blocks will have same root. In this case snapshot
		// difflayer won't be created. So HEAD-127 may not paired with
		// head-127 layer. Instead the paired layer is higher than the
		// bottom-most diff layer. Try to find the bottom-most snapshot
		// layer with state available.
		//
		// Note HEAD and HEAD-1 is ignored. Usually there is the associated
		// state available, but we don't want to use the topmost state
		// as the pruning target.
		var found bool
		for i := len(layers) - 2; i >= 2; i-- {
			if blob := rawdb.ReadTrieNode(p.db, layers[i].Root()); len(blob) != 0 {
				root = layers[i].Root()
				found = true
				log.Info("Pick middle-layer as the pruning target", "root", root, "depth", i)
				break
			}
		}
		if !found {
			if len(layers) > 0 {
				return errors.New("no snapshot paired state")
			}
			return fmt.Errorf("associated state[%x] is not present", root)
		}
	} else {
		if len(layers) > 0 {
			log.Info("Pick bottom-most difflayer as the pruning target", "root", root, "height", p.headHeader.Number.Uint64()-127)
		} else {
			log.Info("Pick user-specified state as the pruning target", "root", root)
		}
	}
	// Before start the pruning, delete the clean trie cache first.
	// It's necessary otherwise in the next restart we will hit the
	// deleted state root in the "clean cache" so that the incomplete
	// state is picked for usage.
	deleteCleanTrieCache(p.trieCachePath)

	// Traverse the target state, re-construct the whole state trie and
	// commit to the given bloom filter.
	start := time.Now()
	if err := snapshot.GenerateTrie(p.snaptree, root, p.db, p.stateBloom); err != nil {
		return err
	}
	// Traverse the genesis, put all genesis state entries into the
	// bloom filter too.
	if err := extractGenesis(p.db, p.stateBloom); err != nil {
		return err
	}
	filterName := bloomFilterName(p.datadir, root)
	if err := p.stateBloom.Commit(filterName); err != nil {
		return err
	}
	log.Info("Committed the state bloom filter", "name", filterName)

	if err := prune(p.db, p.stateBloom, start); err != nil {
		return err
	}
	// Pruning is done, now drop the "useless" layers from the snapshot.
	// Firstly, flushing the target layer into the disk. After that all
	// diff layers below the target will all be merged into the disk.
	if err := p.snaptree.Cap(root, 0); err != nil {
		return err
	}
	// Secondly, flushing the snapshot journal into the disk. All diff
	// layers upon the target layer are dropped silently. Eventually the
	// entire snapshot tree is converted into a single disk layer with
	// the pruning target as the root.
	if _, err := p.snaptree.Journal(root); err != nil {
		return err
	}
	// Delete the state bloom, it marks the entire pruning procedure is
	// finished. If any crashes or manual exit happens before this,
	// `RecoverPruning` will pick it up in the next restarts to redo all
	// the things.
	os.RemoveAll(filterName)
	return nil
}

// RecoverPruning will resume the pruning procedure during the system restart.
// This function is used in this case: user tries to prune state data, but the
// system was interrupted midway because of crash or manual-kill. In this case
// if the bloom filter for filtering active state is already constructed, the
// pruning can be resumed. What's more if the bloom filter is constructed, the
// pruning **has to be resumed**. Otherwise a lot of dangling nodes may be left
// in the disk.
func RecoverPruning(datadir string, db ethdb.Database, trieCachePath string) error {
	stateBloomPath, stateBloomRoot, err := findBloomFilter(datadir)
	if err != nil {
		return err
	}
	if stateBloomPath == "" {
		return nil // nothing to recover
	}
	headHeader, err := getHeadHeader(db)
	if err != nil {
		return err
	}
	// Initialize the snapshot tree in recovery mode to handle this special case:
	// - Users run the `prune-state` command multiple times
	// - Neither these `prune-state` running is finished(e.g. interrupted manually)
	// - The state bloom filter is already generated, a part of state is deleted,
	//   so that resuming the pruning here is mandatory
	// - The state HEAD is rewound already because of multiple incomplete `prune-state`
	// In this case, even the state HEAD is not exactly matched with snapshot, it
	// still feasible to recover the pruning correctly.
	snaptree, err := snapshot.New(db, trie.NewDatabase(db), 256, headHeader.Root, false, false, true)
	if err != nil {
		return err // The relevant snapshot(s) might not exist
	}
	stateBloom, err := NewStateBloomFromDisk(stateBloomPath)
	if err != nil {
		return err
	}
	log.Info("Loaded the state bloom filter", "path", stateBloomPath)

	// Before start the pruning, delete the clean trie cache first.
	// It's necessary otherwise in the next restart we will hit the
	// deleted state root in the "clean cache" so that the incomplete
	// state is picked for usage.
	deleteCleanTrieCache(trieCachePath)

	if err := prune(db, stateBloom, time.Now()); err != nil {
		return err
	}
	// Pruning is done, now drop the "useless" layers from the snapshot.
	// Firstly, flushing the target layer into the disk. After that all
	// diff layers below the target will all be merged into the disk.
	if err := snaptree.Cap(stateBloomRoot, 0); err != nil {
		return err
	}
	// Secondly, flushing the snapshot journal into the disk. All diff
	// layers upon are dropped silently. Eventually the entire snapshot
	// tree is converted into a single disk layer with the pruning target
	// as the root.
	if _, err := snaptree.Journal(stateBloomRoot); err != nil {
		return err
	}
	// Delete the state bloom, it marks the entire pruning procedure is
	// finished. If any crashes or manual exit happens before this,
	// `RecoverPruning` will pick it up in the next restarts to redo all
	// the things.
	os.RemoveAll(stateBloomPath)
	return nil
}

// extractGenesis loads the genesis state and commits all the state entries
// into the given bloomfilter.
func extractGenesis(db ethdb.Database, stateBloom *stateBloom) error {
	genesisHash := rawdb.ReadCanonicalHash(db, 0)
	if genesisHash == (common.Hash{}) {
		return errors.New("missing genesis hash")
	}
	genesis := rawdb.ReadBlock(db, genesisHash, 0)
	if genesis == nil {
		return errors.New("missing genesis block")
	}
	t, err := trie.NewSecure(genesis.Root(), trie.NewDatabase(db))
	if err != nil {
		return err
	}
	accIter := t.NodeIterator(nil)
	for accIter.Next(true) {
		hash := accIter.Hash()

		// Embedded nodes don't have hash.
		if hash != (common.Hash{}) {
			stateBloom.Put(hash.Bytes(), nil)
		}
		// If it's a leaf node, yes we are touching an account,
		// dig into the storage trie further.
		if accIter.Leaf() {
			var acc state.Account
			if err := rlp.DecodeBytes(accIter.LeafBlob(), &acc); err != nil {
				return err
			}
			if acc.Root != emptyRoot {
				storageTrie, err := trie.NewSecure(acc.Root, trie.NewDatabase(db))
				if err != nil {
					return err
				}
				storageIter := storageTrie.NodeIterator(nil)
				for storageIter.Next(true) {
					hash := storageIter.Hash()
					if hash != (common.Hash{}) {
						stateBloom.Put(hash.Bytes(), nil)
					}
				}
				if storageIter.Error() != nil {
					return storageIter.Error()
				}
			}
			if !bytes.Equal(acc.CodeHash, emptyCode) {
				stateBloom.Put(acc.CodeHash, nil)
			}
		}
	}
	return accIter.Error()
}

func bloomFilterName(datadir string, hash common.Hash) string {
	return filepath.Join(datadir, fmt.Sprintf("%s.%s.%s", stateBloomFilePrefix, hash.Hex(), stateBloomFileSuffix))
}

func isBloomFilter(filename string) (bool, common.Hash) {
	filename = filepath.Base(filename)
	if strings.HasPrefix(filename, stateBloomFilePrefix) && strings.HasSuffix(filename, stateBloomFileSuffix) {
		return true, common.HexToHash(filename[len(stateBloomFilePrefix)+1 : len(filename)-len(stateBloomFileSuffix)-1])
	}
	return false, common.Hash{}
}

func findBloomFilter(datadir string) (string, common.Hash, error) {
	var (
		stateBloomPath string
		stateBloomRoot common.Hash
	)
	if err := filepath.Walk(datadir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			ok, root := isBloomFilter(path)
			if ok {
				stateBloomPath = path
				stateBloomRoot = root
			}
		}
		return nil
	}); err != nil {
		return "", common.Hash{}, err
	}
	return stateBloomPath, stateBloomRoot, nil
}

func getHeadHeader(db ethdb.Database) (*types.Header, error) {
	headHeaderHash := rawdb.ReadHeadBlockHash(db)
	if headHeaderHash == (common.Hash{}) {
		return nil, errors.New("empty head block hash")
	}
	headHeaderNumber := rawdb.ReadHeaderNumber(db, headHeaderHash)
	if headHeaderNumber == nil {
		return nil, errors.New("empty head block number")
	}
	headHeader := rawdb.ReadHeader(db, headHeaderHash, *headHeaderNumber)
	if headHeader == nil {
		return nil, errors.New("empty head header")
	}
	return headHeader, nil
}

const warningLog = `

WARNING!

The clean trie cache is not found. Please delete it by yourself after the 
pruning. Remember don't start the Geth without deleting the clean trie cache
otherwise the entire database may be damaged!

Check the command description "geth snapshot prune-state --help" for more details.
`

func deleteCleanTrieCache(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Warn(warningLog)
		return
	}
	os.RemoveAll(path)
	log.Info("Deleted trie clean cache", "path", path)
}
