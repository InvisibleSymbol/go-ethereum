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

package encoding

// Trie keys are dealt with in three distinct encodings:
//
// KEYBYTES encoding contains the actual key and nothing else. This encoding is the
// input to most API functions.
//
// HEX encoding contains one byte for each nibble of the key and an optional trailing
// 'terminator' byte of value 0x10 which indicates whether or not the node at the key
// contains a value. Hex key encoding is used for nodes loaded in memory because it's
// convenient to access.
//
// COMPACT encoding is defined by the Ethereum Yellow Paper (it's called "hex prefix
// encoding" there) and contains the bytes of the key and a flag. The high nibble of the
// first byte contains the flag; the lowest bit encoding the oddness of the length and
// the second-lowest encoding whether the node at the key is a value node. The low nibble
// of the first byte is zero in the case of an even number of nibbles and the first nibble
// in the case of an odd number. All remaining nibbles (now an even number) fit properly
// into the remaining bytes. Compact encoding is used for nodes stored on disk.

func HexToCompact(hex []byte) []byte {
	terminator := byte(0)
	if HasTerm(hex) {
		terminator = 1
		hex = hex[:len(hex)-1]
	}
	buf := make([]byte, len(hex)/2+1)
	buf[0] = terminator << 5 // the flag byte
	if len(hex)&1 == 1 {
		buf[0] |= 1 << 4 // odd flag
		buf[0] |= hex[0] // first nibble is contained in the first byte
		hex = hex[1:]
	}
	decodeNibbles(hex, buf[1:])
	return buf
}

// HexToCompactInPlace places the compact key in input buffer, returning the length
// needed for the representation
func HexToCompactInPlace(hex []byte) int {
	var (
		hexLen    = len(hex) // length of the hex input
		firstByte = byte(0)
	)
	// Check if we have a terminator there
	if hexLen > 0 && hex[hexLen-1] == 16 {
		firstByte = 1 << 5
		hexLen-- // last part was the terminator, ignore that
	}
	var (
		binLen = hexLen/2 + 1
		ni     = 0 // index in hex
		bi     = 1 // index in bin (compact)
	)
	if hexLen&1 == 1 {
		firstByte |= 1 << 4 // odd flag
		firstByte |= hex[0] // first nibble is contained in the first byte
		ni++
	}
	for ; ni < hexLen; bi, ni = bi+1, ni+2 {
		hex[bi] = hex[ni]<<4 | hex[ni+1]
	}
	hex[0] = firstByte
	return binLen
}

func CompactToHex(compact []byte) []byte {
	if len(compact) == 0 {
		return compact
	}
	base := KeybytesToHex(compact)
	// delete terminator flag
	if base[0] < 2 {
		base = base[:len(base)-1]
	}
	// apply odd flag
	chop := 2 - base[0]&1
	return base[chop:]
}

// REVERSE-COMAPCT encoding is used for encoding trie node path in the trie node
// storage key. The main difference with COMPACT encoding is that the key flag
// is put in the end of the key.
//
// e.g.
// - the key [] is encoded as [0x00]
// - the key [0x1, 0x2, 0x3] is encoded as [0x12, 0x31]
// - the key [0x1, 0x2, 0x3, 0x0] is encoded as [0x12, 0x30, 0x00]
//
// The main benefit of this format is the continuous paths can retain the shared
// path prefix after encoding.

func HexToReverseCompact(hex []byte) []byte {
	terminator := byte(0)
	if HasTerm(hex) {
		terminator = 1
		hex = hex[:len(hex)-1]
	}
	buf := make([]byte, len(hex)/2+1)
	buf[len(buf)-1] = terminator << 1 // the flag byte
	if len(hex)&1 == 1 {
		buf[len(buf)-1] |= 1                    // odd flag
		buf[len(buf)-1] |= hex[len(hex)-1] << 4 // last nibble is contained in the last byte
		hex = hex[:len(hex)-1]
	}
	decodeNibbles(hex, buf[:len(buf)-1])
	return buf
}

func ReverseCompactToHex(compact []byte) []byte {
	if len(compact) == 0 {
		return compact
	}
	// delete terminator flag
	base := KeybytesToHex(compact)
	base = base[:len(base)-1]

	// apply odd flag
	flag := base[len(base)-1]
	chop := 2 - flag&1
	base = base[:len(base)-int(chop)]

	// apply terminator flag
	if flag >= 2 {
		base = append(base, 16)
	}
	return base
}

func KeybytesToHex(str []byte) []byte {
	l := len(str)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return nibbles
}

// HexToKeybytes turns hex nibbles into key bytes.
// This can only be used for keys of even length.
func HexToKeybytes(hex []byte) []byte {
	if HasTerm(hex) {
		hex = hex[:len(hex)-1]
	}
	if len(hex)&1 != 0 {
		panic("can't convert hex key of odd length")
	}
	key := make([]byte, len(hex)/2)
	decodeNibbles(hex, key)
	return key
}

func decodeNibbles(nibbles []byte, bytes []byte) {
	for bi, ni := 0, 0; ni < len(nibbles); bi, ni = bi+1, ni+2 {
		bytes[bi] = nibbles[ni]<<4 | nibbles[ni+1]
	}
}

// PrefixLen returns the length of the common prefix of a and b.
func PrefixLen(a, b []byte) int {
	var i, length = 0, len(a)
	if len(b) < length {
		length = len(b)
	}
	for ; i < length; i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}

// HasTerm returns whether a hex key has the terminator flag.
func HasTerm(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 16
}