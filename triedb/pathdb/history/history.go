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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package history

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/exp/slices"
)

// State history records the state changes involved in executing a block. The
// state can be reverted to the previous version by applying the associated
// history object (state reverse diff). State history objects are kept to
// guarantee that the system can perform state rollbacks in case of deep reorg.
//
// Each state transition will generate a state history object. Note that not
// every block has a corresponding state history object. If a block performs
// no state changes whatsoever, no state is created for it. Each state history
// will have a sequentially increasing number acting as its unique identifier.
//
// The state history is written to disk (ancient store) when the corresponding
// diff layer is merged into the disk layer. At the same time, system can prune
// the oldest histories according to config.
//
//                                                        Disk State
//                                                            ^
//                                                            |
//   +------------+     +---------+     +---------+     +---------+
//   | Init State |---->| State 1 |---->|   ...   |---->| State n |
//   +------------+     +---------+     +---------+     +---------+
//
//                     +-----------+      +------+     +-----------+
//                     | History 1 |----> | ...  |---->| History n |
//                     +-----------+      +------+     +-----------+
//
// # Rollback
//
// If the system wants to roll back to a previous state n, it needs to ensure
// all history objects from n+1 up to the current disk layer are existent. The
// history objects are applied to the state in reverse order, starting from the
// current disk layer.
//
// Each state history entry is consisted of five elements:
//
// # metadata
//  This object contains a few meta fields, such as the associated state root,
//  block number, version tag and so on. This object may contain an extra
//  accountHash list which means the storage changes belong to these accounts
//  are not complete due to large contract destruction. The incomplete history
//  can not be used for rollback and serving archive state request.
//
// # account index
//  This object contains some index information of account. For example, offset
//  and length indicate the location of the data belonging to the account. Besides,
//  storageOffset and storageSlots indicate the storage modification location
//  belonging to the account.
//
//  The size of each account index is *fixed*, and all indexes are sorted
//  lexicographically. Thus binary search can be performed to quickly locate a
//  specific account.
//
// # account data
//  Account data is a concatenated byte stream composed of all account data.
//  The account data can be solved by the offset and length info indicated
//  by corresponding account index.
//
//            fixed size
//         ^             ^
//        /               \
//        +-----------------+-----------------+----------------+-----------------+
//        | Account index 1 | Account index 2 |       ...      | Account index N |
//        +-----------------+-----------------+----------------+-----------------+
//        |
//        |     length
// offset |----------------+
//        v                v
//        +----------------+----------------+----------------+----------------+
//        | Account data 1 | Account data 2 |       ...      | Account data N |
//        +----------------+----------------+----------------+----------------+
//
// # storage index
//  This object is similar with account index. It's also fixed size and contains
//  the location info of storage slot data.
//
// # storage data
//  Storage data is a concatenated byte stream composed of all storage slot data.
//  The storage slot data can be solved by the location info indicated by
//  corresponding account index and storage slot index.
//
//                    fixed size
//                 ^             ^
//                /               \
//                +-----------------+-----------------+----------------+-----------------+
//                | Account index 1 | Account index 2 |       ...      | Account index N |
//                +-----------------+-----------------+----------------+-----------------+
//                |
//                |                    storage slots
// storage offset |-----------------------------------------------------+
//                v                                                     v
//                +-----------------+-----------------+-----------------+
//                | storage index 1 | storage index 2 | storage index 3 |
//                +-----------------+-----------------+-----------------+
//                |     length
//         offset |-------------+
//                v             v
//                +-------------+
//                | slot data 1 |
//                +-------------+

// History represents a set of state changes belong to a block along with
// the metadata including the state roots involved in the state transition.
// State history objects in disk are linked with each other by a unique id
// (8-bytes integer), the oldest state history object can be pruned on demand
// in order to control the storage size.
type History struct {
	meta        *meta                                     // Meta data of history
	accounts    map[common.Address][]byte                 // Account data keyed by its address hash
	accountList []common.Address                          // Sorted account hash list
	storages    map[common.Address]map[common.Hash][]byte // Storage data keyed by its address hash and slot hash
	storageList map[common.Address][]common.Hash          // Sorted slot hash list
}

// New initializes the state history object.
func New(root common.Hash, parent common.Hash, block uint64, accounts map[common.Address][]byte, storages map[common.Address]map[common.Hash][]byte) *History {
	var (
		accountList []common.Address
		storageList = make(map[common.Address][]common.Hash)
	)
	for addr := range accounts {
		accountList = append(accountList, addr)
	}
	slices.SortFunc(accountList, common.Address.Cmp)

	for addr, slots := range storages {
		slist := make([]common.Hash, 0, len(slots))
		for slotHash := range slots {
			slist = append(slist, slotHash)
		}
		slices.SortFunc(slist, common.Hash.Cmp)
		storageList[addr] = slist
	}
	return &History{
		meta: &meta{
			version: version,
			parent:  parent,
			root:    root,
			block:   block,
		},
		accounts:    accounts,
		accountList: accountList,
		storages:    storages,
		storageList: storageList,
	}
}

// encode serializes the state history and returns four byte streams represent
// concatenated account/storage data, account/storage indexes respectively.
func (h *History) encode() ([]byte, []byte, []byte, []byte) {
	var (
		slotNumber     uint32 // the number of processed slots
		accountData    []byte // the buffer for concatenated account data
		storageData    []byte // the buffer for concatenated storage data
		accountIndexes []byte // the buffer for concatenated account index
		storageIndexes []byte // the buffer for concatenated storage index
	)
	for _, addr := range h.accountList {
		accIndex := accountIndex{
			address: addr,
			length:  uint8(len(h.accounts[addr])),
			offset:  uint32(len(accountData)),
		}
		slots, exist := h.storages[addr]
		if exist {
			// Encode storage slots in order
			for _, slotHash := range h.storageList[addr] {
				sIndex := slotIndex{
					hash:   slotHash,
					length: uint8(len(slots[slotHash])),
					offset: uint32(len(storageData)),
				}
				storageData = append(storageData, slots[slotHash]...)
				storageIndexes = append(storageIndexes, sIndex.encode()...)
			}
			// Fill up the storage meta in account index
			accIndex.storageOffset = slotNumber
			accIndex.storageSlots = uint32(len(slots))
			slotNumber += uint32(len(slots))
		}
		accountData = append(accountData, h.accounts[addr]...)
		accountIndexes = append(accountIndexes, accIndex.encode()...)
	}
	return accountData, storageData, accountIndexes, storageIndexes
}

// decoder wraps the byte streams for decoding with extra meta fields.
type decoder struct {
	accountData    []byte // the buffer for concatenated account data
	storageData    []byte // the buffer for concatenated storage data
	accountIndexes []byte // the buffer for concatenated account index
	storageIndexes []byte // the buffer for concatenated storage index

	lastAccount       *common.Address // the address of last resolved account
	lastAccountRead   uint32          // the read-cursor position of account data
	lastSlotIndexRead uint32          // the read-cursor position of storage slot index
	lastSlotDataRead  uint32          // the read-cursor position of storage slot data
}

// verify validates the provided byte streams for decoding state history. A few
// checks will be performed to quickly detect data corruption. The byte stream
// is regarded as corrupted if:
//
// - account indexes buffer is empty(empty state set is invalid)
// - account indexes/storage indexer buffer is not aligned
//
// note, these situations are allowed:
//
// - empty account data: all accounts were not present
// - empty storage set: no slots are modified
func (r *decoder) verify() error {
	if len(r.accountIndexes)%accountIndexSize != 0 || len(r.accountIndexes) == 0 {
		return fmt.Errorf("invalid account index, len: %d", len(r.accountIndexes))
	}
	if len(r.storageIndexes)%slotIndexSize != 0 {
		return fmt.Errorf("invalid storage index, len: %d", len(r.storageIndexes))
	}
	return nil
}

// readAccount parses the account from the byte stream with specified position.
func (r *decoder) readAccount(pos int) (accountIndex, []byte, error) {
	// Decode account index from the index byte stream.
	var index accountIndex
	if (pos+1)*accountIndexSize > len(r.accountIndexes) {
		return accountIndex{}, nil, errors.New("account data buffer is corrupted")
	}
	index.decode(r.accountIndexes[pos*accountIndexSize : (pos+1)*accountIndexSize])

	// Perform validation before parsing account data, ensure
	// - account is sorted in order in byte stream
	// - account data is strictly encoded with no gap inside
	// - account data is not out-of-slice
	if r.lastAccount != nil { // zero address is possible
		if bytes.Compare(r.lastAccount.Bytes(), index.address.Bytes()) >= 0 {
			return accountIndex{}, nil, errors.New("account is not in order")
		}
	}
	if index.offset != r.lastAccountRead {
		return accountIndex{}, nil, errors.New("account data buffer is gaped")
	}
	last := index.offset + uint32(index.length)
	if uint32(len(r.accountData)) < last {
		return accountIndex{}, nil, errors.New("account data buffer is corrupted")
	}
	data := r.accountData[index.offset:last]

	r.lastAccount = &index.address
	r.lastAccountRead = last

	return index, data, nil
}

// readStorage parses the storage slots from the byte stream with specified account.
func (r *decoder) readStorage(accIndex accountIndex) ([]common.Hash, map[common.Hash][]byte, error) {
	var (
		last    common.Hash
		list    []common.Hash
		storage = make(map[common.Hash][]byte)
	)
	for j := 0; j < int(accIndex.storageSlots); j++ {
		var (
			index slotIndex
			start = (accIndex.storageOffset + uint32(j)) * uint32(slotIndexSize)
			end   = (accIndex.storageOffset + uint32(j+1)) * uint32(slotIndexSize)
		)
		// Perform validation before parsing storage slot data, ensure
		// - slot index is not out-of-slice
		// - slot data is not out-of-slice
		// - slot is sorted in order in byte stream
		// - slot indexes is strictly encoded with no gap inside
		// - slot data is strictly encoded with no gap inside
		if start != r.lastSlotIndexRead {
			return nil, nil, errors.New("storage index buffer is gapped")
		}
		if uint32(len(r.storageIndexes)) < end {
			return nil, nil, errors.New("storage index buffer is corrupted")
		}
		index.decode(r.storageIndexes[start:end])

		if bytes.Compare(last.Bytes(), index.hash.Bytes()) >= 0 {
			return nil, nil, errors.New("storage slot is not in order")
		}
		if index.offset != r.lastSlotDataRead {
			return nil, nil, errors.New("storage data buffer is gapped")
		}
		sEnd := index.offset + uint32(index.length)
		if uint32(len(r.storageData)) < sEnd {
			return nil, nil, errors.New("storage data buffer is corrupted")
		}
		storage[index.hash] = r.storageData[r.lastSlotDataRead:sEnd]
		list = append(list, index.hash)

		last = index.hash
		r.lastSlotIndexRead = end
		r.lastSlotDataRead = sEnd
	}
	return list, storage, nil
}

// decode deserializes the account and storage data from the provided byte stream.
func (h *History) decode(accountData, storageData, accountIndexes, storageIndexes []byte) error {
	var (
		accounts    = make(map[common.Address][]byte)
		storages    = make(map[common.Address]map[common.Hash][]byte)
		accountList []common.Address
		storageList = make(map[common.Address][]common.Hash)

		r = &decoder{
			accountData:    accountData,
			storageData:    storageData,
			accountIndexes: accountIndexes,
			storageIndexes: storageIndexes,
		}
	)
	if err := r.verify(); err != nil {
		return err
	}
	for i := 0; i < len(accountIndexes)/accountIndexSize; i++ {
		// Resolve account first
		accIndex, accData, err := r.readAccount(i)
		if err != nil {
			return err
		}
		accounts[accIndex.address] = accData
		accountList = append(accountList, accIndex.address)

		// Resolve storage slots
		slotList, slotData, err := r.readStorage(accIndex)
		if err != nil {
			return err
		}
		if len(slotList) > 0 {
			storageList[accIndex.address] = slotList
			storages[accIndex.address] = slotData
		}
	}
	h.accounts = accounts
	h.accountList = accountList
	h.storages = storages
	h.storageList = storageList
	return nil
}

// Parent returns the associated parent state root.
func (h *History) Parent() common.Hash {
	return h.meta.parent
}

// Root returns the associated state root.
func (h *History) Root() common.Hash {
	return h.meta.root
}

// Block returns the associated block number.
func (h *History) Block() uint64 {
	return h.meta.block
}

// AccountData returns the associated account data.
func (h *History) AccountData() map[common.Address][]byte {
	return h.accounts
}

// StorageData returns the associated storage data.
func (h *History) StorageData() map[common.Address]map[common.Hash][]byte {
	return h.storages
}

// Read retrieves the state history object by the given id.
func Read(freezer *rawdb.ResettableFreezer, id uint64) (*History, error) {
	blob := rawdb.ReadStateHistoryMeta(freezer, id)
	if len(blob) == 0 {
		return nil, fmt.Errorf("state history not found %d", id)
	}
	var m meta
	if err := m.decode(blob); err != nil {
		return nil, err
	}
	var (
		dec            = History{meta: &m}
		accountData    = rawdb.ReadStateAccountHistory(freezer, id)
		storageData    = rawdb.ReadStateStorageHistory(freezer, id)
		accountIndexes = rawdb.ReadStateAccountIndex(freezer, id)
		storageIndexes = rawdb.ReadStateStorageIndex(freezer, id)
	)
	if err := dec.decode(accountData, storageData, accountIndexes, storageIndexes); err != nil {
		return nil, err
	}
	return &dec, nil
}

func ReadBatch(freezer *rawdb.ResettableFreezer, start uint64, count uint64) ([]*History, error) {
	metaList, aIndexList, sIndexList, aDataList, sDataList, err := rawdb.ReadStateHistoryList(freezer, start, count)
	if err != nil {
		return nil, err
	}
	number := len(metaList)
	if number != len(aIndexList) || number != len(sIndexList) || number != len(aDataList) || number != len(sDataList) {
		return nil, errors.New("corrupted state history")
	}
	var result []*History
	for i := 0; i < number; i++ {
		var m meta
		if err := m.decode(metaList[i]); err != nil {
			return nil, err
		}
		dec := History{meta: &m}
		if err := dec.decode(aDataList[i], sDataList[i], aIndexList[i], sIndexList[i]); err != nil {
			return nil, err
		}
		result = append(result, &dec)
	}
	return result, nil
}

// ReadMetaBatch retrieves a batch of meta objects with the specified range
// and performs the callback on each item.
func ReadMetaBatch(freezer *rawdb.ResettableFreezer, start, count uint64, check func(parent common.Hash, root common.Hash) error) error {
	for count > 0 {
		number := count
		if number > 10000 {
			number = 10000 // split the big read into small chunks
		}
		blobs, err := rawdb.ReadStateHistoryMetaList(freezer, start, number)
		if err != nil {
			return err
		}
		for _, blob := range blobs {
			var dec meta
			if err := dec.decode(blob); err != nil {
				return err
			}
			if err := check(dec.parent, dec.root); err != nil {
				return err
			}
		}
		count -= uint64(len(blobs))
		start += uint64(len(blobs))
	}
	return nil
}

// Write persists the state history with the provided state set.
func Write(freezer *rawdb.ResettableFreezer, id uint64, history *History) error {
	accountData, storageData, accountIndex, storageIndex := history.encode()
	dataSize := common.StorageSize(len(accountData) + len(storageData))
	indexSize := common.StorageSize(len(accountIndex) + len(storageIndex))

	// Write history data into five freezer table respectively.
	rawdb.WriteStateHistory(freezer, id, history.meta.encode(), accountIndex, storageIndex, accountData, storageData)

	historyDataBytesMeter.Mark(int64(dataSize))
	historyIndexBytesMeter.Mark(int64(indexSize))
	log.Debug("Stored state history", "id", id, "block", history.Block(), "data", dataSize, "index", indexSize)
	return nil
}

// TruncateHead removes the extra state histories from the head with the given
// parameters. It returns the number of items removed from the head.
func TruncateHead(db ethdb.Batcher, freezer *rawdb.ResettableFreezer, nhead uint64) (int, error) {
	ohead, err := freezer.Ancients()
	if err != nil {
		return 0, err
	}
	otail, err := freezer.Tail()
	if err != nil {
		return 0, err
	}
	// Ensure that the truncation target falls within the specified range.
	if ohead < nhead || nhead < otail {
		return 0, fmt.Errorf("out of range, tail: %d, head: %d, target: %d", otail, ohead, nhead)
	}
	// Short circuit if nothing to truncate.
	if ohead == nhead {
		return 0, nil
	}
	// Load the meta objects in range [nhead+1, ohead]
	blobs, err := rawdb.ReadStateHistoryMetaList(freezer, nhead+1, ohead-nhead)
	if err != nil {
		return 0, err
	}
	batch := db.NewBatch()
	for _, blob := range blobs {
		var m meta
		if err := m.decode(blob); err != nil {
			return 0, err
		}
		rawdb.DeleteStateID(batch, m.root)
	}
	if err := batch.Write(); err != nil {
		return 0, err
	}
	ohead, err = freezer.TruncateHead(nhead)
	if err != nil {
		return 0, err
	}
	return int(ohead - nhead), nil
}

// TruncateTail removes the extra state histories from the tail with the given
// parameters. It returns the number of items removed from the tail.
func TruncateTail(db ethdb.Batcher, freezer *rawdb.ResettableFreezer, ntail uint64) (int, error) {
	ohead, err := freezer.Ancients()
	if err != nil {
		return 0, err
	}
	otail, err := freezer.Tail()
	if err != nil {
		return 0, err
	}
	// Ensure that the truncation target falls within the specified range.
	if otail > ntail || ntail > ohead {
		return 0, fmt.Errorf("out of range, tail: %d, head: %d, target: %d", otail, ohead, ntail)
	}
	// Short circuit if nothing to truncate.
	if otail == ntail {
		return 0, nil
	}
	// Load the meta objects in range [otail+1, ntail]
	blobs, err := rawdb.ReadStateHistoryMetaList(freezer, otail+1, ntail-otail)
	if err != nil {
		return 0, err
	}
	batch := db.NewBatch()
	for _, blob := range blobs {
		var m meta
		if err := m.decode(blob); err != nil {
			return 0, err
		}
		rawdb.DeleteStateID(batch, m.root)
	}
	if err := batch.Write(); err != nil {
		return 0, err
	}
	otail, err = freezer.TruncateTail(ntail)
	if err != nil {
		return 0, err
	}
	return int(ntail - otail), nil
}
