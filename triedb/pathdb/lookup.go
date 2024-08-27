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

package pathdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/sync/errgroup"
)

type lookup struct {
	total      atomic.Int64
	nodes      map[common.Hash]map[string][]common.Hash
	descendant func(state common.Hash, ancestor common.Hash) bool
}

func newLookup(head layer, descendant func(state common.Hash, ancestor common.Hash) bool) *lookup {
	l := new(lookup)
	l.reset(head)
	l.descendant = descendant
	return l
}

func (l *lookup) reset(head layer) {
	var (
		current = head
		layers  []layer
	)
	for current != nil {
		layers = append(layers, current)
		current = current.parentLayer()
	}
	l.nodes = make(map[common.Hash]map[string][]common.Hash)

	for i := len(layers) - 1; i >= 0; i-- {
		switch diff := layers[i].(type) {
		case *diskLayer:
			continue
		case *diffLayer:
			l.addLayer(diff)
		}
	}
}

func (l *lookup) nodeTip(owner common.Hash, path []byte, head common.Hash) common.Hash {
	defer func(now time.Time) {
		lookupNodeTimer.UpdateSince(now)
	}(time.Now())

	subset, exists := l.nodes[owner]
	if !exists {
		return common.Hash{}
	}
	list := subset[string(path)]

	for i := len(list) - 1; i >= 0; i-- {
		if list[i] == head || l.descendant(head, list[i]) {
			lookupNodeStepMeter.Mark(int64(len(list) - i))
			return list[i]
		}
	}
	return common.Hash{}
}

func (l *lookup) addLayer(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddLayerTimer.UpdateSince(now)
	}(time.Now())

	var (
		state   = diff.rootHash()
		lock    sync.Mutex
		workers errgroup.Group
	)
	for accountHash, nodes := range diff.nodes {
		hash, nodes := accountHash, nodes // closure
		workers.Go(func() error {
			lock.Lock()
			subset := l.nodes[hash]
			if subset == nil {
				subset = make(map[string][]common.Hash)
				l.nodes[hash] = subset
			}
			lock.Unlock()

			for path := range nodes {
				subset[path] = append(subset[path], state)
			}
			l.total.Add(int64(len(nodes)))
			return nil
		})
	}
	workers.Wait()
	lookupItemGauge.Update(l.total.Load())
}

func (l *lookup) removeLayer(diff *diffLayer) error {
	defer func(now time.Time) {
		lookupRemoveLayerTimer.UpdateSince(now)
	}(time.Now())

	var (
		state   = diff.rootHash()
		lock    sync.RWMutex
		workers errgroup.Group
	)
	for accountHash, nodes := range diff.nodes {
		hash, nodes := accountHash, nodes // closure

		workers.Go(func() error {
			lock.RLock()
			subset := l.nodes[hash]
			if subset == nil {
				lock.RUnlock()
				return fmt.Errorf("unknown node owner %x", hash)
			}
			lock.RUnlock()

			for path := range nodes {
				var found bool
				for j := 0; j < len(subset[path]); j++ {
					if subset[path][j] == state {
						if j == 0 {
							subset[path] = subset[path][1:]
						} else {
							subset[path] = append(subset[path][:j], subset[path][j+1:]...)
						}
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("failed to delete lookup %x %v", hash, []byte(path))
				}
			}
			if len(subset) == 0 {
				lock.Lock()
				delete(l.nodes, hash)
				lock.Unlock()
			}
			l.total.Add(-1 * int64(len(nodes)))
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return err
	}
	lookupItemGauge.Update(l.total.Load())
	return nil
}
