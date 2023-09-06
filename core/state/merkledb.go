// Copyright 2023 The go-ethereum Authors
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
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

// merkleReader implements the StateReader interface, offering methods to access
// accounts and storage slots in the Merkle-Patricia-Tree manner.
type merkleReader struct {
	// The associated state root, intended to uniquely represent a state.
	root common.Hash

	// The associated Merkle database used for reading states from the merkle
	// tree if the snapshot does not contain the requested state.
	db *merkleDB

	// The associated state snapshot, which may be nil if the snapshot is not
	// enabled, or partial functional which is still generating in the background.
	snap snapshot.Snapshot

	// The reusable hasher for keccak256 hashing.
	hasher crypto.KeccakState

	// The associated account trie, opened by default in the constructor, serves
	// as a fallback for accessing states if the snapshot is unavailable.
	accountTrie Trie

	// The map of storage roots, filled up when resolving accounts.
	storageRoots map[common.Address]common.Hash

	// The group of storage tries, loaded only when needed. It serves as a
	// fallback for accessing storage slots if the snapshot is unavailable.
	storageTries map[common.Address]Trie
}

// newMerkleReader constructs a merkle reader with specific state root.
func newMerkleReader(root common.Hash, db *merkleDB) (*merkleReader, error) {
	// Open the account trie, bail out if it's not available.
	t, err := db.OpenTrie(root)
	if err != nil {
		return nil, err
	}
	// Opens the optional state snapshot, which can significantly improve
	// state read efficiency but may have limited functionality(not fully
	// generated).
	var snap snapshot.Snapshot
	if db.snaps != nil {
		snap = db.snaps.Snapshot(root)
	}
	return &merkleReader{
		root:         root,
		db:           db,
		snap:         snap,
		hasher:       crypto.NewKeccakState(),
		accountTrie:  t,
		storageRoots: make(map[common.Address]common.Hash),
		storageTries: make(map[common.Address]Trie),
	}, nil
}

// Account implements StateReader, retrieving the account specified by the address
// from the associated state.
func (r *merkleReader) Account(addr common.Address) (acct *types.StateAccount, err error) {
	// Cache the storage root of resolved account at the end.
	defer func() {
		if acct == nil {
			r.storageRoots[addr] = types.EmptyRootHash
		} else {
			r.storageRoots[addr] = acct.Root
		}
	}()
	// Try to read account from snapshot, which is more read-efficient.
	if r.snap != nil {
		ret, err := r.snap.Account(crypto.HashData(r.hasher, addr.Bytes()))
		if err == nil {
			if ret == nil {
				return nil, nil
			}
			acct = &types.StateAccount{
				Nonce:    ret.Nonce,
				Balance:  ret.Balance,
				CodeHash: ret.CodeHash,
				Root:     common.BytesToHash(ret.Root),
			}
			if len(acct.CodeHash) == 0 {
				acct.CodeHash = types.EmptyCodeHash.Bytes()
			}
			if acct.Root == (common.Hash{}) {
				acct.Root = types.EmptyRootHash
			}
			return acct, nil
		}
	}
	// If snapshot unavailable or reading from it failed, read account
	// from merkle tree as fallback.
	return r.accountTrie.GetAccount(addr)
}

// storageTrie returns the associated storage trie with the provided account
// address. The trie will be opened and cached locally if it's not loaded yet.
func (r *merkleReader) storageTrie(addr common.Address) (Trie, error) {
	// Short circuit if the storage trie is already cached.
	if t, ok := r.storageTries[addr]; ok {
		return t, nil
	}
	// Open the storage trie with the associated storage root of the account.
	var storageRoot common.Hash
	if root, ok := r.storageRoots[addr]; ok {
		storageRoot = root
	} else {
		account, err := r.Account(addr)
		if err != nil {
			return nil, err
		}
		if account == nil {
			storageRoot = types.EmptyRootHash
		} else {
			storageRoot = account.Root
		}
	}
	t, err := r.db.OpenStorageTrie(r.root, addr, storageRoot)
	if err != nil {
		return nil, err
	}
	r.storageTries[addr] = t
	return t, nil
}

// Storage implements StateReader, retrieving the storage slot specified by the
// address and slot key from the associated state.
func (r *merkleReader) Storage(addr common.Address, key common.Hash) (common.Hash, error) {
	// Try to read storage slot from snapshot first, which is more read-efficient.
	if r.snap != nil {
		ret, err := r.snap.Storage(crypto.Keccak256Hash(addr.Bytes()), crypto.Keccak256Hash(key.Bytes()))
		if err == nil {
			if len(ret) == 0 {
				return common.Hash{}, nil
			}
			_, content, _, err := rlp.Split(ret)
			if err != nil {
				return common.Hash{}, err
			}
			var slot common.Hash
			slot.SetBytes(content)
			return slot, nil
		}
	}
	// If snapshot unavailable or reading from it failed, read storage slot
	// from merkle tree as fallback.
	t, err := r.storageTrie(addr)
	if err != nil {
		return common.Hash{}, err
	}
	ret, err := t.GetStorage(addr, key.Bytes())
	if err != nil {
		return common.Hash{}, err
	}
	var slot common.Hash
	slot.SetBytes(ret)
	return slot, nil
}

// NewDatabase creates a merkleDB instance with provided components.
func NewDatabase(codeDB CodeStore, trieDB *trie.Database, snaps *snapshot.Tree) Database {
	return &merkleDB{
		codeDB: codeDB,
		trieDB: trieDB,
		snaps:  snaps,
	}
}

// NewDatabaseForTesting is similar to NewDatabase, but it sets up a local code
// store and trie database with default config by using the provided database,
// specifically intended for testing.
func NewDatabaseForTesting(db ethdb.Database) Database {
	return NewDatabase(NewCodeDB(db), trie.NewDatabase(db, nil), nil)
}

// merkleDB is the implementation of Database interface, designed for providing
// functionalities to read and write states.
type merkleDB struct {
	snaps  *snapshot.Tree
	codeDB CodeStore
	trieDB *trie.Database
}

// StateScheme returns the state scheme used by the database.
func (db *merkleDB) StateScheme() string {
	return db.trieDB.Scheme()
}

// TreeScheme returns the tree scheme used by the database.
func (db *merkleDB) TreeScheme() string {
	return rawdb.MerkleTree
}

// StateReader constructs a reader for the specific state.
func (db *merkleDB) StateReader(stateRoot common.Hash) (StateReader, error) {
	return newMerkleReader(stateRoot, db)
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *merkleDB) OpenTrie(root common.Hash) (Trie, error) {
	return trie.NewStateTrie(trie.StateTrieID(root), db.trieDB)
}

// OpenStorageTrie opens the storage trie of an account.
func (db *merkleDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash) (Trie, error) {
	return trie.NewStateTrie(trie.StorageTrieID(stateRoot, crypto.Keccak256Hash(address.Bytes()), root), db.trieDB)
}

// CopyTrie returns an independent copy of the given trie.
func (db *merkleDB) CopyTrie(t Trie) Trie {
	switch t := t.(type) {
	case *trie.StateTrie:
		return t.Copy()
	default:
		panic(fmt.Errorf("unknown trie type %T", t))
	}
}

// ReadCode implements CodeReader, retrieving a particular contract's code.
func (db *merkleDB) ReadCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	return db.codeDB.ReadCode(address, codeHash)
}

// ReadCodeSize implements CodeReader, retrieving a particular contracts
// code's size.
func (db *merkleDB) ReadCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	return db.codeDB.ReadCodeSize(addr, codeHash)
}

// WriteCodes implements CodeWriter, writing the provided a list of contract
// codes into database.
func (db *merkleDB) WriteCodes(addresses []common.Address, hashes []common.Hash, codes [][]byte) error {
	return db.codeDB.WriteCodes(addresses, hashes, codes)
}

// TrieDB returns the associated trie database.
func (db *merkleDB) TrieDB() *trie.Database {
	return db.trieDB
}

// Snapshot returns the associated state snapshot, it may be nil if not configured.
func (db *merkleDB) Snapshot() *snapshot.Tree {
	return db.snaps
}
