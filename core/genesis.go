// Copyright 2014 The go-ethereum Authors
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

package core

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

//go:generate go run github.com/fjl/gencodec -type Genesis -field-override genesisSpecMarshaling -out gen_genesis.go
//go:generate go run github.com/fjl/gencodec -type GenesisAccount -field-override genesisAccountMarshaling -out gen_genesis_account.go

var errGenesisNoConfig = errors.New("genesis has no chain configuration")

// Genesis specifies the header fields, state of a genesis block. It also defines hard
// fork switch-over blocks through the chain configuration.
type Genesis struct {
	Config     *params.ChainConfig `json:"config"`
	Nonce      uint64              `json:"nonce"`
	Timestamp  uint64              `json:"timestamp"`
	ExtraData  []byte              `json:"extraData"`
	GasLimit   uint64              `json:"gasLimit"   gencodec:"required"`
	Difficulty *big.Int            `json:"difficulty" gencodec:"required"`
	Mixhash    common.Hash         `json:"mixHash"`
	Coinbase   common.Address      `json:"coinbase"`
	Alloc      GenesisAlloc        `json:"alloc"      gencodec:"required"`

	// These fields are used for consensus tests. Please don't use them
	// in actual genesis blocks.
	Number     uint64      `json:"number"`
	GasUsed    uint64      `json:"gasUsed"`
	ParentHash common.Hash `json:"parentHash"`
	BaseFee    *big.Int    `json:"baseFeePerGas"`
}

// GenesisAlloc specifies the initial state that is part of the genesis block.
type GenesisAlloc map[common.Address]GenesisAccount

func (ga *GenesisAlloc) UnmarshalJSON(data []byte) error {
	m := make(map[common.UnprefixedAddress]GenesisAccount)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*ga = make(GenesisAlloc)
	for addr, a := range m {
		(*ga)[common.Address(addr)] = a
	}
	return nil
}

// deriveHash computes the state root according to the genesis specification.
func (ga *GenesisAlloc) deriveHash() (common.Hash, error) {
	// Create an ephemeral in-memory database for computing hash,
	// all the derived states will be discarded to not pollute disk.
	db := state.NewDatabase(rawdb.NewMemoryDatabase())
	statedb, err := state.New(common.Hash{}, db, nil)
	if err != nil {
		return common.Hash{}, err
	}
	for addr, account := range *ga {
		statedb.AddBalance(addr, account.Balance)
		statedb.SetCode(addr, account.Code)
		statedb.SetNonce(addr, account.Nonce)
		for key, value := range account.Storage {
			statedb.SetState(addr, key, value)
		}
	}
	return statedb.Commit(false)
}

// flush is very similar with deriveHash, but the main difference is
// all the generated states will be persisted into the given database.
// Also, the genesis state specification will be flushed as well.
func (ga *GenesisAlloc) flush(db ethdb.Database) error {
	statedb, err := state.New(common.Hash{}, state.NewDatabase(db), nil)
	if err != nil {
		return err
	}
	for addr, account := range *ga {
		statedb.AddBalance(addr, account.Balance)
		statedb.SetCode(addr, account.Code)
		statedb.SetNonce(addr, account.Nonce)
		for key, value := range account.Storage {
			statedb.SetState(addr, key, value)
		}
	}
	root, err := statedb.Commit(false)
	if err != nil {
		return err
	}
	err = statedb.Database().TrieDB().Commit(root, true, nil)
	if err != nil {
		return err
	}
	// Marshal the genesis state specification and persist.
	blob, err := json.Marshal(ga)
	if err != nil {
		return err
	}
	rawdb.WriteGenesisStateSpec(db, root, blob)
	return nil
}

// CommitGenesisState loads the stored genesis state with the given block
// hash and commits them into the given database handler.
func CommitGenesisState(db ethdb.Database, hash common.Hash) error {
	var alloc GenesisAlloc
	blob := rawdb.ReadGenesisStateSpec(db, hash)
	if len(blob) != 0 {
		if err := alloc.UnmarshalJSON(blob); err != nil {
			return err
		}
	} else {
		// Genesis allocation is missing and there are several possibilities:
		// the node is legacy which doesn't persist the genesis allocation or
		// the persisted allocation is just lost.
		// - supported networks(mainnet, testnets), recover with defined allocations
		// - private network, can't recover
		var genesis *Genesis
		switch hash {
		case params.MainnetGenesisHash:
			genesis = DefaultGenesisBlock()
		case params.RopstenGenesisHash:
			genesis = DefaultRopstenGenesisBlock()
		case params.RinkebyGenesisHash:
			genesis = DefaultRinkebyGenesisBlock()
		case params.GoerliGenesisHash:
			genesis = DefaultGoerliGenesisBlock()
		case params.SepoliaGenesisHash:
			genesis = DefaultSepoliaGenesisBlock()
		}
		if genesis != nil {
			alloc = genesis.Alloc
		} else {
			return errors.New("not found")
		}
	}
	return alloc.flush(db)
}

// GenesisAccount is an account in the state of the genesis block.
type GenesisAccount struct {
	Code       []byte                      `json:"code,omitempty"`
	Storage    map[common.Hash]common.Hash `json:"storage,omitempty"`
	Balance    *big.Int                    `json:"balance" gencodec:"required"`
	Nonce      uint64                      `json:"nonce,omitempty"`
	PrivateKey []byte                      `json:"secretKey,omitempty"` // for tests
}

// field type overrides for gencodec
type genesisSpecMarshaling struct {
	Nonce      math.HexOrDecimal64
	Timestamp  math.HexOrDecimal64
	ExtraData  hexutil.Bytes
	GasLimit   math.HexOrDecimal64
	GasUsed    math.HexOrDecimal64
	Number     math.HexOrDecimal64
	Difficulty *math.HexOrDecimal256
	BaseFee    *math.HexOrDecimal256
	Alloc      map[common.UnprefixedAddress]GenesisAccount
}

type genesisAccountMarshaling struct {
	Code       hexutil.Bytes
	Balance    *math.HexOrDecimal256
	Nonce      math.HexOrDecimal64
	Storage    map[storageJSON]storageJSON
	PrivateKey hexutil.Bytes
}

// storageJSON represents a 256 bit byte array, but allows less than 256 bits when
// unmarshaling from hex.
type storageJSON common.Hash

func (h *storageJSON) UnmarshalText(text []byte) error {
	text = bytes.TrimPrefix(text, []byte("0x"))
	if len(text) > 64 {
		return fmt.Errorf("too many hex characters in storage key/value %q", text)
	}
	offset := len(h) - len(text)/2 // pad on the left
	if _, err := hex.Decode(h[offset:], text); err != nil {
		fmt.Println(err)
		return fmt.Errorf("invalid hex storage key/value %q", text)
	}
	return nil
}

func (h storageJSON) MarshalText() ([]byte, error) {
	return hexutil.Bytes(h[:]).MarshalText()
}

// GenesisMismatchError is raised when trying to overwrite an existing
// genesis block with an incompatible one.
type GenesisMismatchError struct {
	Stored, New common.Hash
}

func (e *GenesisMismatchError) Error() string {
	return fmt.Sprintf("database contains incompatible genesis (have %x, new %x)", e.Stored, e.New)
}

func LoadCliqueConfig(db ethdb.Database, genesis *Genesis) (*params.CliqueConfig, error) {
	// Load the stored chain config from the database. It can be nil
	// in case the database is empty. Note we only care about the
	// chain config corresponds to the canonical chain.
	var (
		storedcfg *params.ChainConfig // stored chain config
		newcfg    *params.ChainConfig // newly passed config
	)
	stored := rawdb.ReadCanonicalHash(db, 0)
	if stored != (common.Hash{}) {
		storedcfg = rawdb.ReadChainConfig(db, stored)
	}
	if genesis != nil {
		// Reject invalid genesis spec without valid chain config
		if genesis.Config == nil {
			return nil, errGenesisNoConfig
		}
		newcfg = genesis.Config
	}
	switch {
	case storedcfg == nil && newcfg == nil:
		// There is no stored chain config and no new config provided,
		// In this case the default chain config(mainnet) will be used,
		// namely ethash is the specified consensus engine, return nil.
		return nil, nil

	case storedcfg != nil && newcfg == nil:
		// No new chain config provided, use the stored one.
		return storedcfg.Clique, nil

	case storedcfg == nil && newcfg != nil:
		// No chain config stored in database, use the provided one.
		return newcfg.Clique, nil

	default:
		// There is a stored chain config in database and also a new
		// config provided via genesis spec.
		return newcfg.Clique, nil
	}
}

// SetupChainConfig writes or updates the chain config in db.
// The chain config that will be used is:
//
//                               genesis == nil       genesis != nil
//                            +------------------------------------------
//     db has no chain config |  mainnet.config    |  genesis.config
//     db has chain config    |  from DB           |  genesis.config (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
func SetupChainConfig(db ethdb.Database, genesis *Genesis) (*params.ChainConfig, error) {
	return SetupChainConfigWithOverride(db, genesis, nil, nil)
}

func SetupChainConfigWithOverride(db ethdb.Database, genesis *Genesis, overrideTerminalTotalDifficulty *big.Int, overrideTerminalTotalDifficultyPassed *bool) (*params.ChainConfig, error) {
	// Load the stored chain config from the database. It can be nil
	// in case the database is empty. Note we only care about the
	// chain config corresponds to the canonical chain.
	var (
		storedcfg *params.ChainConfig // stored chain config
		newcfg    *params.ChainConfig // newly passed config
	)
	stored := rawdb.ReadCanonicalHash(db, 0)
	if stored != (common.Hash{}) {
		storedcfg = rawdb.ReadChainConfig(db, stored)
	}
	if genesis != nil {
		// Reject invalid genesis spec without valid chain config
		if genesis.Config == nil {
			return nil, errGenesisNoConfig
		}
		newcfg = genesis.Config
	}
	// applyAndCheck overrides the chain config with the provided settings
	// and also checks the config validity.
	applyAndCheck := func(config *params.ChainConfig) error {
		if overrideTerminalTotalDifficulty != nil {
			config.TerminalTotalDifficulty = overrideTerminalTotalDifficulty
		}
		if overrideTerminalTotalDifficultyPassed != nil {
			config.TerminalTotalDifficultyPassed = *overrideTerminalTotalDifficultyPassed
		}
		//if err := config.CheckConfigForkOrder(); err != nil {
		//	return nil, err
		//}
		//if config.Clique != nil && len(block.Extra()) < 32+crypto.SignatureLength {
		//	return nil, errors.New("can't start clique chain without signers")
		//}
		return config.CheckConfigForkOrder()
	}
	// Perform a series of checks based on the obtained chain config
	switch {
	case storedcfg == nil && newcfg == nil:
		// There is no stored chain config and no new config provided,
		// use the default chain config(mainnet) for initialization
		block := DefaultGenesisBlock()
		config := block.Config
		if err := applyAndCheck(config); err != nil {
			return nil, err
		}
		rawdb.WriteChainConfig(db, block.ToBlock().Hash(), config)
		return config, nil

	case storedcfg != nil && newcfg == nil:
		// No new chain config provided, use the stored one. Apply the
		// overrides if necessary and return.
		if err := applyAndCheck(storedcfg); err != nil {
			return nil, err
		}
		rawdb.WriteChainConfig(db, stored, storedcfg) // stored must be non-empty
		return storedcfg, nil

	case storedcfg == nil && newcfg != nil:
		// No chain config stored in database, persist the provided
		// one into database. Apply the overrides if necessary and
		// return.
		if err := applyAndCheck(newcfg); err != nil {
			return nil, err
		}
		rawdb.WriteChainConfig(db, genesis.ToBlock().Hash(), newcfg) // genesis must be non-nil
		return newcfg, nil

	default:
		// There is a stored chain config in database and also a new
		// config provided via genesis spec. Check the compatibility
		// of them and use the new config if all checks can be passed.
		if err := applyAndCheck(newcfg); err != nil {
			return nil, err
		}
		// Check config compatibility and write the config. Compatibility errors
		// are returned to the caller unless we're already at block zero.
		height := rawdb.ReadHeaderNumber(db, stored) // stored must be non-empty
		if height == nil {
			return nil, fmt.Errorf("missing block number for head header hash")
		}
		compatErr := storedcfg.CheckCompatible(newcfg, *height)
		if compatErr != nil && *height != 0 && compatErr.RewindTo != 0 {
			return newcfg, compatErr
		}
		rawdb.WriteChainConfig(db, stored, newcfg)
		return newcfg, nil
	}
}

// SetupGenesisState writes the genesis block into database if it's missing.
// The genesis block that will be used is:
//
//                          genesis == nil       genesis != nil
//                       +------------------------------------------
//     db has no genesis |  mainnet           |  genesis
//     db has genesis    |  from DB           |  from DB(ignore provided)
func SetupGenesisState(db ethdb.Database, genesis *Genesis) (common.Hash, error) {
	// Commit the new block if there is no stored genesis block.
	stored := rawdb.ReadCanonicalHash(db, 0)
	if (stored == common.Hash{}) {
		if genesis == nil {
			log.Info("Writing default main-net genesis block")
			genesis = DefaultGenesisBlock()
		} else {
			log.Info("Writing custom genesis block")
		}
		block, err := genesis.Commit(db)
		if err != nil {
			return common.Hash{}, err
		}
		return block.Hash(), nil
	}
	// Both genesis block and genesis state are present, nothing to do.
	header := rawdb.ReadHeader(db, stored, 0)
	if header == nil {
		return common.Hash{}, errors.New("missing genesis block header")
	}
	if _, err := state.New(header.Root, state.NewDatabaseWithConfig(db, nil), nil); err == nil {
		return header.Hash(), nil
	}
	// Genesis block is present(perhaps in ancient database) while genesis
	// state is not, it can happen users are trying to initialize Geth with
	// an external ancient chain segments. Commit genesis block and state
	// to database for initializing the missing parts.
	if genesis == nil {
		genesis = DefaultGenesisBlock()
	}
	// Ensure the stored genesis matches with the given one.
	hash := genesis.ToBlock().Hash()
	if hash != stored {
		return common.Hash{}, &GenesisMismatchError{stored, hash}
	}
	block, err := genesis.Commit(db)
	if err != nil {
		return common.Hash{}, err
	}
	return block.Hash(), nil
}

// SetupGenesisBlock writes or updates the chain configuration and genesis block in db.
// The block that will be used is:
//
//                          genesis == nil       genesis != nil
//                       +------------------------------------------
//     db has no genesis |  main-net default  |  genesis
//     db has genesis    |  from DB           |  genesis (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
//
// The returned chain configuration can be non-nil if the encountered error is
// *params.ConfigCompatError.
func SetupGenesisBlock(db ethdb.Database, genesis *Genesis) (*params.ChainConfig, common.Hash, error) {
	return SetupGenesisBlockWithOverride(db, genesis, nil, nil)
}

func SetupGenesisBlockWithOverride(db ethdb.Database, genesis *Genesis, overrideTerminalTotalDifficulty *big.Int, overrideTerminalTotalDifficultyPassed *bool) (*params.ChainConfig, common.Hash, error) {
	chainConfig, err := SetupChainConfigWithOverride(db, genesis, overrideTerminalTotalDifficulty, overrideTerminalTotalDifficultyPassed)
	if err != nil {
		return nil, common.Hash{}, err
	}
	hash, err := SetupGenesisState(db, genesis)
	if err != nil {
		return nil, common.Hash{}, err
	}
	return chainConfig, hash, nil
}

// ToBlock returns the genesis block according to genesis specification.
func (g *Genesis) ToBlock() *types.Block {
	root, err := g.Alloc.deriveHash()
	if err != nil {
		panic(err)
	}
	head := &types.Header{
		Number:     new(big.Int).SetUint64(g.Number),
		Nonce:      types.EncodeNonce(g.Nonce),
		Time:       g.Timestamp,
		ParentHash: g.ParentHash,
		Extra:      g.ExtraData,
		GasLimit:   g.GasLimit,
		GasUsed:    g.GasUsed,
		BaseFee:    g.BaseFee,
		Difficulty: g.Difficulty,
		MixDigest:  g.Mixhash,
		Coinbase:   g.Coinbase,
		Root:       root,
	}
	if g.GasLimit == 0 {
		head.GasLimit = params.GenesisGasLimit
	}
	if g.Difficulty == nil && g.Mixhash == (common.Hash{}) {
		head.Difficulty = params.GenesisDifficulty
	}
	if g.Config != nil && g.Config.IsLondon(common.Big0) {
		if g.BaseFee != nil {
			head.BaseFee = g.BaseFee
		} else {
			head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
		}
	}
	return types.NewBlock(head, nil, nil, nil, trie.NewStackTrie(nil))
}

// Commit writes the block and state of a genesis specification to the database.
// The block is committed as the canonical head block.
func (g *Genesis) Commit(db ethdb.Database) (*types.Block, error) {
	block := g.ToBlock()
	if block.Number().Sign() != 0 {
		return nil, errors.New("can't commit genesis block with number > 0")
	}
	// All the checks has passed, flush the states derived from the genesis
	// specification as well as the specification itself into the provided
	// database.
	if err := g.Alloc.flush(db); err != nil {
		return nil, err
	}
	rawdb.WriteTd(db, block.Hash(), block.NumberU64(), block.Difficulty())
	rawdb.WriteBlock(db, block)
	rawdb.WriteReceipts(db, block.Hash(), block.NumberU64(), nil)
	rawdb.WriteCanonicalHash(db, block.Hash(), block.NumberU64())
	rawdb.WriteHeadBlockHash(db, block.Hash())
	rawdb.WriteHeadFastBlockHash(db, block.Hash())
	rawdb.WriteHeadHeaderHash(db, block.Hash())
	return block, nil
}

// MustCommit writes the genesis block and state to db, panicking on error.
// The block is committed as the canonical head block.
func (g *Genesis) MustCommit(db ethdb.Database) *types.Block {
	block, err := g.Commit(db)
	if err != nil {
		panic(err)
	}
	return block
}

// DefaultGenesisBlock returns the Ethereum main net genesis block.
func DefaultGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.MainnetChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa"),
		GasLimit:   5000,
		Difficulty: big.NewInt(17179869184),
		Alloc:      decodePrealloc(mainnetAllocData),
	}
}

// DefaultRopstenGenesisBlock returns the Ropsten network genesis block.
func DefaultRopstenGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.RopstenChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x3535353535353535353535353535353535353535353535353535353535353535"),
		GasLimit:   16777216,
		Difficulty: big.NewInt(1048576),
		Alloc:      decodePrealloc(ropstenAllocData),
	}
}

// DefaultRinkebyGenesisBlock returns the Rinkeby network genesis block.
func DefaultRinkebyGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.RinkebyChainConfig,
		Timestamp:  1492009146,
		ExtraData:  hexutil.MustDecode("0x52657370656374206d7920617574686f7269746168207e452e436172746d616e42eb768f2244c8811c63729a21a3569731535f067ffc57839b00206d1ad20c69a1981b489f772031b279182d99e65703f0076e4812653aab85fca0f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   4700000,
		Difficulty: big.NewInt(1),
		Alloc:      decodePrealloc(rinkebyAllocData),
	}
}

// DefaultGoerliGenesisBlock returns the Görli network genesis block.
func DefaultGoerliGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.GoerliChainConfig,
		Timestamp:  1548854791,
		ExtraData:  hexutil.MustDecode("0x22466c6578692069732061207468696e6722202d204166726900000000000000e0a2bd4258d2768837baa26a28fe71dc079f84c70000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   10485760,
		Difficulty: big.NewInt(1),
		Alloc:      decodePrealloc(goerliAllocData),
	}
}

// DefaultSepoliaGenesisBlock returns the Sepolia network genesis block.
func DefaultSepoliaGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.SepoliaChainConfig,
		Nonce:      0,
		ExtraData:  []byte("Sepolia, Athens, Attica, Greece!"),
		GasLimit:   0x1c9c380,
		Difficulty: big.NewInt(0x20000),
		Timestamp:  1633267481,
		Alloc:      decodePrealloc(sepoliaAllocData),
	}
}

// DefaultKilnGenesisBlock returns the kiln network genesis block.
func DefaultKilnGenesisBlock() *Genesis {
	g := new(Genesis)
	reader := strings.NewReader(KilnAllocData)
	if err := json.NewDecoder(reader).Decode(g); err != nil {
		panic(err)
	}
	return g
}

// DeveloperGenesisBlock returns the 'geth --dev' genesis block.
func DeveloperGenesisBlock(period uint64, gasLimit uint64, faucet common.Address) *Genesis {
	// Override the default period to the user requested one
	config := *params.AllCliqueProtocolChanges
	config.Clique = &params.CliqueConfig{
		Period: period,
		Epoch:  config.Clique.Epoch,
	}

	// Assemble and return the genesis with the precompiles and faucet pre-funded
	return &Genesis{
		Config:     &config,
		ExtraData:  append(append(make([]byte, 32), faucet[:]...), make([]byte, crypto.SignatureLength)...),
		GasLimit:   gasLimit,
		BaseFee:    big.NewInt(params.InitialBaseFee),
		Difficulty: big.NewInt(1),
		Alloc: map[common.Address]GenesisAccount{
			common.BytesToAddress([]byte{1}): {Balance: big.NewInt(1)}, // ECRecover
			common.BytesToAddress([]byte{2}): {Balance: big.NewInt(1)}, // SHA256
			common.BytesToAddress([]byte{3}): {Balance: big.NewInt(1)}, // RIPEMD
			common.BytesToAddress([]byte{4}): {Balance: big.NewInt(1)}, // Identity
			common.BytesToAddress([]byte{5}): {Balance: big.NewInt(1)}, // ModExp
			common.BytesToAddress([]byte{6}): {Balance: big.NewInt(1)}, // ECAdd
			common.BytesToAddress([]byte{7}): {Balance: big.NewInt(1)}, // ECScalarMul
			common.BytesToAddress([]byte{8}): {Balance: big.NewInt(1)}, // ECPairing
			common.BytesToAddress([]byte{9}): {Balance: big.NewInt(1)}, // BLAKE2b
			faucet:                           {Balance: new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(9))},
		},
	}
}

func decodePrealloc(data string) GenesisAlloc {
	var p []struct{ Addr, Balance *big.Int }
	if err := rlp.NewStream(strings.NewReader(data), 0).Decode(&p); err != nil {
		panic(err)
	}
	ga := make(GenesisAlloc, len(p))
	for _, account := range p {
		ga[common.BigToAddress(account.Addr)] = GenesisAccount{Balance: account.Balance}
	}
	return ga
}
