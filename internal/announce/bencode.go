// Package announce implements BEP 3 bencode helpers and request/response types.
package announce

import (
	"fmt"
	"strconv"
	"strings"
)

// ── Minimal bencode encoder ──────────────────────────────────────────────────

// BencodeDict encodes a map to a bencoded byte slice.
// Values may be string, []byte, int, int32, int64, []interface{}, or map[string]interface{}.
func BencodeDict(d map[string]interface{}) []byte {
	keys := sortedKeys(d)
	var b strings.Builder
	b.WriteByte('d')
	for _, k := range keys {
		b.Write(BencodeString([]byte(k)))
		b.Write(BencodeValue(d[k]))
	}
	b.WriteByte('e')
	return []byte(b.String())
}

func BencodeValue(v interface{}) []byte {
	switch val := v.(type) {
	case string:
		return BencodeString([]byte(val))
	case []byte:
		return BencodeString(val)
	case int:
		return BencodeInt(int64(val))
	case int32:
		return BencodeInt(int64(val))
	case int64:
		return BencodeInt(val)
	case map[string]interface{}:
		return BencodeDict(val)
	case []interface{}:
		return BencodeList(val)
	default:
		return []byte("0:")
	}
}

func BencodeString(b []byte) []byte {
	return []byte(strconv.Itoa(len(b)) + ":" + string(b))
}

func BencodeInt(n int64) []byte {
	return []byte("i" + strconv.FormatInt(n, 10) + "e")
}

func BencodeList(items []interface{}) []byte {
	var b strings.Builder
	b.WriteByte('l')
	for _, item := range items {
		b.Write(BencodeValue(item))
	}
	b.WriteByte('e')
	return []byte(b.String())
}

func BencodeError(reason string) []byte {
	return BencodeDict(map[string]interface{}{
		"failure reason": reason,
	})
}

// sortedKeys returns sorted keys for deterministic bencode output.
func sortedKeys(d map[string]interface{}) []string {
	keys := make([]string, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}
	// insertion sort (small maps)
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// ── Bencode decoder (minimal, for upstream HTTP responses) ───────────────────

// DecodeBencodeDict decodes a flat bencoded dictionary into a string map.
// Values are returned as strings (integers converted) or raw []byte for binary
// fields. This is intentionally minimal — only what we need.
func DecodeBencodeDict(data []byte) (map[string]interface{}, error) {
	if len(data) == 0 || data[0] != 'd' {
		return nil, fmt.Errorf("not a bencoded dict")
	}
	m := make(map[string]interface{})
	pos := 1
	for pos < len(data) && data[pos] != 'e' {
		// key
		key, n, err := decodeString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n
		// value
		val, n2, err := decodeValue(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n2
		m[string(key)] = val
	}
	return m, nil
}

func decodeValue(data []byte, pos int) (interface{}, int, error) {
	if pos >= len(data) {
		return nil, 0, fmt.Errorf("unexpected end")
	}
	switch {
	case data[pos] == 'i':
		return decodeInt(data, pos)
	case data[pos] == 'l':
		return decodeList(data, pos)
	case data[pos] == 'd':
		sub, n, err := decodeDict(data, pos)
		return sub, n, err
	case data[pos] >= '0' && data[pos] <= '9':
		b, n, err := decodeString(data, pos)
		return b, n, err
	default:
		return nil, 0, fmt.Errorf("unknown type byte %q at %d", data[pos], pos)
	}
}

func decodeString(data []byte, pos int) ([]byte, int, error) {
	colon := pos
	for colon < len(data) && data[colon] != ':' {
		colon++
	}
	if colon >= len(data) {
		return nil, 0, fmt.Errorf("missing colon in string at %d", pos)
	}
	length, err := strconv.Atoi(string(data[pos:colon]))
	if err != nil {
		return nil, 0, err
	}
	start := colon + 1
	end := start + length
	if end > len(data) {
		return nil, 0, fmt.Errorf("string too short")
	}
	return data[start:end], end - pos, nil
}

func decodeInt(data []byte, pos int) (int64, int, error) {
	end := pos + 1
	for end < len(data) && data[end] != 'e' {
		end++
	}
	n, err := strconv.ParseInt(string(data[pos+1:end]), 10, 64)
	return n, end - pos + 1, err
}

func decodeList(data []byte, pos int) ([]interface{}, int, error) {
	var out []interface{}
	i := pos + 1
	for i < len(data) && data[i] != 'e' {
		v, n, err := decodeValue(data, i)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, v)
		i += n
	}
	return out, i - pos + 1, nil
}

func decodeDict(data []byte, pos int) (map[string]interface{}, int, error) {
	m := make(map[string]interface{})
	i := pos + 1
	for i < len(data) && data[i] != 'e' {
		key, n, err := decodeString(data, i)
		if err != nil {
			return nil, 0, err
		}
		i += n
		val, n2, err := decodeValue(data, i)
		if err != nil {
			return nil, 0, err
		}
		i += n2
		m[string(key)] = val
	}
	return m, i - pos + 1, nil
}
