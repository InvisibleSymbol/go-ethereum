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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package pathdb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/testutil"
	"github.com/ethereum/go-ethereum/trie/triestate"
)

// randomStateSet generates a random state change set.
func randomStateSet(parent common.Hash, n int) *triestate.Set {
	var (
		accounts = make(map[common.Hash][]byte)
		storages = make(map[common.Hash]map[common.Hash][]byte)
	)
	for i := 0; i < n; i++ {
		addrHash := testutil.RandomHash()
		storages[addrHash] = make(map[common.Hash][]byte)
		for j := 0; j < 3; j++ {
			v, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(testutil.RandBytes(32)))
			storages[addrHash][testutil.RandomHash()] = v
		}
		account := generateAccount(types.EmptyRootHash)
		accounts[addrHash] = types.SlimAccountRLP(account)
	}
	return triestate.New(parent, testutil.RandomHash(), accounts, storages)
}

func makeHistory() *history {
	return newHistory(randomStateSet(types.EmptyRootHash, 3))
}

func makeHistories(n int) []*history {
	var (
		parent = types.EmptyRootHash
		result []*history
	)
	for i := 0; i < n; i++ {
		h := newHistory(randomStateSet(parent, 3))
		parent = h.meta.root
		result = append(result, h)
	}
	return result
}

func TestEncodeDecodeHistory(t *testing.T) {
	var (
		dec history
		obj = makeHistory()
	)
	accountData, storageData, accountIndexes, storageIndexes := obj.encode()
	if err := dec.decode(accountData, storageData, accountIndexes, storageIndexes); err != nil {
		t.Fatalf("Failed to decode, err: %v", err)
	}
	if !compareAccounts(dec.accounts, obj.accounts) {
		t.Fatal("account data is mismatched")
	}
	if !compareStorages(dec.storages, obj.storages) {
		t.Fatal("storage data is mismatched")
	}
	if !compareAccountList(dec.accountList, obj.accountList) {
		t.Fatal("account list is mismatched")
	}
	if !compareStorageList(dec.storageList, obj.storageList) {
		t.Fatal("storage list is mismatched")
	}
}

func TestEncodeDecodeMeta(t *testing.T) {
	var (
		dec meta
		h   = makeHistory()
	)
	blob := h.meta.encode()
	if err := dec.decode(blob); err != nil {
		t.Fatalf("Failed to decode %v", err)
	}
	if !reflect.DeepEqual(&dec, h.meta) {
		t.Fatal("meta is mismatched")
	}
}

func checkHistory(t *testing.T, freezer *rawdb.ResettableFreezer, id uint64, exist bool) {
	blob := rawdb.ReadStateHistoryMeta(freezer, id)
	if exist && len(blob) == 0 {
		t.Errorf("Failed to load trie history, %d", id)
	}
	if !exist && len(blob) != 0 {
		t.Errorf("Unexpected trie history, %d", id)
	}
}

func checkHistories(t *testing.T, freezer *rawdb.ResettableFreezer, from, to uint64, exist bool) {
	for i := from; i <= to; i += 1 {
		checkHistory(t, freezer, i, exist)
	}
}

func TestTruncateHeadHistory(t *testing.T) {
	var (
		hs         = makeHistories(10)
		freezer, _ = openFreezer(t.TempDir(), false)
	)
	for i := 0; i < len(hs); i++ {
		accountData, storageData, accountIndex, storageIndex := hs[i].encode()
		rawdb.WriteStateHistory(freezer, uint64(i+1), hs[i].meta.encode(), accountIndex, storageIndex, accountData, storageData)
	}
	for size := len(hs); size > 0; size-- {
		pruned, err := truncateFromHead(freezer, uint64(size-1))
		if err != nil {
			t.Fatalf("Failed to truncate from head %v", err)
		}
		if pruned != 1 {
			t.Error("Unexpected pruned items", "want", 1, "got", pruned)
		}
		checkHistories(t, freezer, uint64(size), uint64(10), false)
		checkHistories(t, freezer, uint64(1), uint64(size-1), true)
	}
}

func TestTruncateTailHistory(t *testing.T) {
	var (
		hs         = makeHistories(10)
		freezer, _ = openFreezer(t.TempDir(), false)
	)
	for i := 0; i < len(hs); i++ {
		accountData, storageData, accountIndex, storageIndex := hs[i].encode()
		rawdb.WriteStateHistory(freezer, uint64(i+1), hs[i].meta.encode(), accountIndex, storageIndex, accountData, storageData)
	}
	for newTail := 1; newTail < len(hs); newTail++ {
		pruned, _ := truncateFromTail(freezer, uint64(newTail))
		if pruned != 1 {
			t.Error("Unexpected pruned items", "want", 1, "got", pruned)
		}
		checkHistories(t, freezer, uint64(1), uint64(newTail), false)
		checkHistories(t, freezer, uint64(newTail+1), uint64(10), true)
	}
}

func TestTruncateTailHistories(t *testing.T) {
	var cases = []struct {
		limit       uint64
		expPruned   int
		maxPruned   uint64
		minUnpruned uint64
		empty       bool
	}{
		{
			1, 9, 9, 10, false,
		},
		{
			0, 10, 10, 0 /* no meaning */, true,
		},
		{
			10, 0, 0, 1, false,
		},
	}
	for _, c := range cases {
		var (
			hs         = makeHistories(10)
			freezer, _ = openFreezer(t.TempDir(), false)
		)
		for i := 0; i < len(hs); i++ {
			accountData, storageData, accountIndex, storageIndex := hs[i].encode()
			rawdb.WriteStateHistory(freezer, uint64(i+1), hs[i].meta.encode(), accountIndex, storageIndex, accountData, storageData)
		}
		pruned, _ := truncateFromTail(freezer, uint64(10)-c.limit)
		if pruned != c.expPruned {
			t.Error("Unexpected pruned items", "want", c.expPruned, "got", pruned)
		}
		if c.empty {
			checkHistories(t, freezer, uint64(1), uint64(10), false)
		} else {
			checkHistories(t, freezer, uint64(1), c.maxPruned, false)
			checkHistory(t, freezer, c.minUnpruned, true)
		}
	}
}

// openFreezer initializes the freezer instance for storing state histories.
func openFreezer(datadir string, readOnly bool) (*rawdb.ResettableFreezer, error) {
	return rawdb.NewStateHistoryFreezer(datadir, readOnly)
}

func compareAccounts(a, b map[common.Hash][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for key, valA := range a {
		valB, ok := b[key]
		if !ok {
			return false
		}
		if !bytes.Equal(valA, valB) {
			return false
		}
	}
	return true
}

func compareStorages(a, b map[common.Hash]map[common.Hash][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for h, subA := range a {
		subB, ok := b[h]
		if !ok {
			return false
		}
		if !compareAccounts(subA, subB) {
			return false
		}
	}
	return true
}

func compareAccountList(a, b []common.Hash) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func compareStorageList(a, b map[common.Hash][]common.Hash) bool {
	if len(a) != len(b) {
		return false
	}
	for h, la := range a {
		lb, ok := b[h]
		if !ok {
			return false
		}
		if !compareAccountList(la, lb) {
			return false
		}
	}
	return true
}
