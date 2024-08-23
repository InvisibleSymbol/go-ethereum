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
	"maps"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/database"
)

var (
	readerAccountTimer       = metrics.NewRegisteredResettingTimer("statedb/reader/account/total/read/time", nil)
	readerAccountHashTimer   = metrics.NewRegisteredResettingTimer("statedb/reader/account/hash/read/time", nil)
	readerAccountBlobTimer   = metrics.NewRegisteredResettingTimer("statedb/reader/account/blob/read/time", nil)
	readerAccountDecodeTimer = metrics.NewRegisteredResettingTimer("statedb/reader/account/decode/read/time", nil)

	readerStorageTimer       = metrics.NewRegisteredResettingTimer("statedb/reader/storage/total/read/time", nil)
	readerStorageHashTimer   = metrics.NewRegisteredResettingTimer("statedb/reader/storage/hash/read/time", nil)
	readerStorageBlobTimer   = metrics.NewRegisteredResettingTimer("statedb/reader/storage/blob/read/time", nil)
	readerStorageDecodeTimer = metrics.NewRegisteredResettingTimer("statedb/reader/storage/decode/read/time", nil)
)

// Reader defines the interface for accessing accounts and storage slots
// associated with a specific state.
type Reader interface {
	// Account retrieves the account associated with a particular address.
	//
	// - Returns a nil account if it does not exist
	// - Returns an error only if an unexpected issue occurs
	// - The returned account is safe to modify after the call
	Account(addr common.Address) (*types.StateAccount, error)

	// Storage retrieves the storage slot associated with a particular account
	// address and slot key.
	//
	// - Returns an empty slot if it does not exist
	// - Returns an error only if an unexpected issue occurs
	// - The returned storage slot is safe to modify after the call
	Storage(addr common.Address, slot common.Hash) (common.Hash, error)

	// Stats returns the statistics of the reader, specifically detailing the time
	// spent on account reading and storage reading.
	Stats() (time.Duration, time.Duration, int, int)

	// Copy returns a deep-copied state reader.
	Copy() Reader
}

// stateReader wraps a database state reader.
type stateReader struct {
	reader database.StateReader
	buff   crypto.KeccakState

	accountRead int // Number of accounts have been resolved from the reader
	storageRead int // Number of storages have been resolved from the reader
	accountTime time.Duration
	storageTime time.Duration
}

// newStateReader constructs a state reader with on the given state root.
func newStateReader(reader database.StateReader) *stateReader {
	return &stateReader{
		reader: reader,
		buff:   crypto.NewKeccakState(),
	}
}

// Account implements Reader, retrieving the account specified by the address.
//
// An error will be returned if the associated snapshot is already stale or
// the requested account is not yet covered by the snapshot.
//
// The returned account might be nil if it's not existent.
func (r *stateReader) Account(addr common.Address) (*types.StateAccount, error) {
	defer func(start time.Time) {
		r.accountTime += time.Since(start)
		readerAccountTimer.UpdateSince(start)
	}(time.Now())

	ss := time.Now()
	hash := crypto.HashData(r.buff, addr.Bytes())
	readerAccountHashTimer.UpdateSince(ss)

	ss = time.Now()
	account, err := r.reader.Account(hash)
	if err != nil {
		return nil, err
	}
	readerAccountBlobTimer.UpdateSince(ss)
	if account == nil {
		r.accountRead++
		return nil, nil
	}
	ss = time.Now()
	acct := &types.StateAccount{
		Nonce:    account.Nonce,
		Balance:  account.Balance,
		CodeHash: account.CodeHash,
		Root:     common.BytesToHash(account.Root),
	}
	if len(acct.CodeHash) == 0 {
		acct.CodeHash = types.EmptyCodeHash.Bytes()
	}
	if acct.Root == (common.Hash{}) {
		acct.Root = types.EmptyRootHash
	}
	readerAccountDecodeTimer.UpdateSince(ss)
	r.accountRead++
	return acct, nil
}

// Storage implements Reader, retrieving the storage slot specified by the
// address and slot key.
//
// An error will be returned if the associated snapshot is already stale or
// the requested storage slot is not yet covered by the snapshot.
//
// The returned storage slot might be empty if it's not existent.
func (r *stateReader) Storage(addr common.Address, key common.Hash) (common.Hash, error) {
	defer func(start time.Time) {
		r.storageTime += time.Since(start)
		readerStorageTimer.UpdateSince(start)
	}(time.Now())

	ss := time.Now()
	addrHash := crypto.HashData(r.buff, addr.Bytes())
	slotHash := crypto.HashData(r.buff, key.Bytes())
	readerStorageHashTimer.UpdateSince(ss)

	ss = time.Now()
	ret, err := r.reader.Storage(addrHash, slotHash)
	if err != nil {
		return common.Hash{}, err
	}
	readerStorageBlobTimer.UpdateSince(ss)
	if len(ret) == 0 {
		r.storageRead++
		return common.Hash{}, nil
	}
	ss = time.Now()
	_, content, _, err := rlp.Split(ret)
	if err != nil {
		return common.Hash{}, err
	}
	var value common.Hash
	value.SetBytes(content)
	readerStorageDecodeTimer.UpdateSince(ss)
	r.storageRead++
	return value, nil
}

// Stats implements Reader, returning the time spent on account reading and
// storage reading from the snapshot.
func (r *stateReader) Stats() (time.Duration, time.Duration, int, int) {
	return r.accountTime, r.storageTime, r.accountRead, r.storageRead
}

// Copy implements Reader, returning a deep-copied snap reader.
func (r *stateReader) Copy() Reader {
	return &stateReader{
		reader: r.reader,
		buff:   crypto.NewKeccakState(),

		// statistics (accountTime and storageTime) are not copied, as they
		// only belong to current reader instance.
	}
}

// trieReader implements the Reader interface, providing functions to access
// state from the referenced trie.
type trieReader struct {
	root     common.Hash                    // State root which uniquely represent a state
	db       *triedb.Database               // Database for loading trie
	buff     crypto.KeccakState             // Buffer for keccak256 hashing
	mainTrie Trie                           // Main trie, resolved in constructor
	subRoots map[common.Address]common.Hash // Set of storage roots, cached when the account is resolved
	subTries map[common.Address]Trie        // Group of storage tries, cached when it's resolved

	accountRead int           // Number of accounts have been resolved from the reader
	storageRead int           // Number of storages have been resolved from the reader
	accountTime time.Duration // Time spent on the account reading
	storageTime time.Duration // Time spent on the storage reading
}

// trieReader constructs a trie reader of the specific state. An error will be
// returned if the associated trie specified by root is not existent.
func newTrieReader(root common.Hash, db *triedb.Database, cache *utils.PointCache) (*trieReader, error) {
	var (
		tr  Trie
		err error
	)
	if !db.IsVerkle() {
		tr, err = trie.NewStateTrie(trie.StateTrieID(root), db)
	} else {
		tr, err = trie.NewVerkleTrie(root, db, cache)
	}
	if err != nil {
		return nil, err
	}
	return &trieReader{
		root:     root,
		db:       db,
		buff:     crypto.NewKeccakState(),
		mainTrie: tr,
		subRoots: make(map[common.Address]common.Hash),
		subTries: make(map[common.Address]Trie),
	}, nil
}

// Account implements Reader, retrieving the account specified by the address.
//
// An error will be returned if the trie state is corrupted. An nil account
// will be returned if it's not existent in the trie.
func (r *trieReader) Account(addr common.Address) (*types.StateAccount, error) {
	defer func(start time.Time) {
		r.accountTime += time.Since(start)
	}(time.Now())

	account, err := r.mainTrie.GetAccount(addr)
	if err != nil {
		return nil, err
	}
	if account == nil {
		r.subRoots[addr] = types.EmptyRootHash
	} else {
		r.subRoots[addr] = account.Root
	}
	r.accountRead++
	return account, nil
}

// Storage implements Reader, retrieving the storage slot specified by the
// address and slot key.
//
// An error will be returned if the trie state is corrupted. An empty storage
// slot will be returned if it's not existent in the trie.
func (r *trieReader) Storage(addr common.Address, key common.Hash) (common.Hash, error) {
	defer func(start time.Time) {
		r.storageTime += time.Since(start)
	}(time.Now())

	var (
		tr    Trie
		found bool
		value common.Hash
	)
	if r.db.IsVerkle() {
		tr = r.mainTrie
	} else {
		tr, found = r.subTries[addr]
		if !found {
			root, ok := r.subRoots[addr]

			// The storage slot is accessed without account caching. It's unexpected
			// behavior but try to resolve the account first anyway.
			if !ok {
				_, err := r.Account(addr)
				if err != nil {
					return common.Hash{}, err
				}
				root = r.subRoots[addr]
			}
			var err error
			tr, err = trie.NewStateTrie(trie.StorageTrieID(r.root, crypto.HashData(r.buff, addr.Bytes()), root), r.db)
			if err != nil {
				return common.Hash{}, err
			}
			r.subTries[addr] = tr
		}
	}
	ret, err := tr.GetStorage(addr, key.Bytes())
	if err != nil {
		return common.Hash{}, err
	}
	value.SetBytes(ret)
	r.storageRead++
	return value, nil
}

// Stats implements Reader, returning the statistics of reader.
func (r *trieReader) Stats() (time.Duration, time.Duration, int, int) {
	return r.accountTime, r.storageTime, r.accountRead, r.storageRead
}

// Copy implements Reader, returning a deep-copied trie reader.
func (r *trieReader) Copy() Reader {
	tries := make(map[common.Address]Trie)
	for addr, tr := range r.subTries {
		tries[addr] = mustCopyTrie(tr)
	}
	return &trieReader{
		root:     r.root,
		db:       r.db,
		buff:     crypto.NewKeccakState(),
		mainTrie: mustCopyTrie(r.mainTrie),
		subRoots: maps.Clone(r.subRoots),
		subTries: tries,

		// statistics are not copied, as they only belong to current reader instance.
	}
}

// multiReader is the aggregation of a list of Reader interface, providing state
// access by leveraging all readers. The checking priority is determined by the
// position in the reader list.
type multiReader struct {
	readers []Reader // List of readers, sorted by checking priority
}

// newMultiReader constructs a multiReader instance with the given readers. The
// priority among readers is assumed to be sorted. Note, it must contain at least
// one reader for constructing a multiReader.
func newMultiReader(readers ...Reader) (*multiReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("empty reader set")
	}
	return &multiReader{
		readers: readers,
	}, nil
}

// Account implementing Reader interface, retrieving the account associated with
// a particular address.
//
// - Returns a nil account if it does not exist
// - Returns an error only if an unexpected issue occurs
// - The returned account is safe to modify after the call
func (r *multiReader) Account(addr common.Address) (*types.StateAccount, error) {
	var errs []error
	for _, reader := range r.readers {
		acct, err := reader.Account(addr)
		if err == nil {
			return acct, nil
		}
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}

// Storage implementing Reader interface, retrieving the storage slot associated
// with a particular account address and slot key.
//
// - Returns an empty slot if it does not exist
// - Returns an error only if an unexpected issue occurs
// - The returned storage slot is safe to modify after the call
func (r *multiReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	var errs []error
	for _, reader := range r.readers {
		slot, err := reader.Storage(addr, slot)
		if err == nil {
			return slot, nil
		}
		errs = append(errs, err)
	}
	return common.Hash{}, errors.Join(errs...)
}

// Stats implements Reader, returning the time spent on account reading and
// storage reading from the reader.
func (r *multiReader) Stats() (time.Duration, time.Duration, int, int) {
	var (
		accountRead int
		storageRead int
		accountTime time.Duration
		storageTime time.Duration
	)
	for _, reader := range r.readers {
		aTime, sTime, aRead, sRead := reader.Stats()
		accountTime += aTime
		storageTime += sTime
		accountRead += aRead
		storageRead += sRead
	}
	return accountTime, storageTime, accountRead, storageRead
}

// Copy implementing Reader interface, returning a deep-copied state reader.
func (r *multiReader) Copy() Reader {
	var readers []Reader
	for _, reader := range r.readers {
		readers = append(readers, reader.Copy())
	}
	return &multiReader{readers: readers}
}
