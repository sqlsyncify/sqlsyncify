package utils

import "strconv"

// maxPHPSerializeBytes caps attacker-controlled PHP serialized blobs before they
// reach gophp. That library allocates a buffer from the declared string length,
// which can be gigabytes for a few dozen bytes of input. Object types are rejected;
// gophp does not instantiate them either.
const maxPHPSerializeBytes = 1 << 20

// PHPSerializeBounded reports whether s is a PHP serialize payload whose declared
// lengths fit inside the input and inside maxPHPSerializeBytes.
func PHPSerializeBounded(s string) bool {
	if len(s) == 0 || len(s) > maxPHPSerializeBytes {
		return false
	}
	next, ok := walkPHP(s, 0, 0)
	return ok && next == len(s)
}

func walkPHP(s string, i, depth int) (int, bool) {
	if depth > 32 || i >= len(s) {
		return 0, false
	}
	switch s[i] {
	case 'N':
		if i+1 < len(s) && s[i+1] == ';' {
			return i + 2, true
		}
		return 0, false
	case 'b':
		if i+3 < len(s) && s[i+1] == ':' && (s[i+2] == '0' || s[i+2] == '1') && s[i+3] == ';' {
			return i + 4, true
		}
		return 0, false
	case 'i', 'd':
		j := i + 1
		if j >= len(s) || s[j] != ':' {
			return 0, false
		}
		j++
		start := j
		if j < len(s) && (s[j] == '-' || s[j] == '+') {
			j++
		}
		for j < len(s) && s[j] != ';' {
			c := s[j]
			if (c < '0' || c > '9') && c != '.' && c != 'e' && c != 'E' && c != '+' && c != '-' {
				return 0, false
			}
			j++
			if j-start > 64 {
				return 0, false
			}
		}
		if j >= len(s) || j == start {
			return 0, false
		}
		return j + 1, true
	case 's':
		n, j, ok := readPHPLen(s, i+1)
		if !ok || j >= len(s) || s[j] != '"' {
			return 0, false
		}
		end := j + 1 + n
		if end+1 >= len(s) || s[end] != '"' || s[end+1] != ';' {
			return 0, false
		}
		return end + 2, true
	case 'a':
		n, j, ok := readPHPLen(s, i+1)
		if !ok || n > maxPHPSerializeBytes || j >= len(s) || s[j] != '{' {
			return 0, false
		}
		j++
		for k := 0; k < n; k++ {
			var okKey bool
			j, okKey = walkPHP(s, j, depth+1)
			if !okKey {
				return 0, false
			}
			j, okKey = walkPHP(s, j, depth+1)
			if !okKey {
				return 0, false
			}
		}
		if j >= len(s) || s[j] != '}' {
			return 0, false
		}
		return j + 1, true
	default:
		// 'O' and 'C' objects are rejected.
		return 0, false
	}
}

func readPHPLen(s string, i int) (int, int, bool) {
	if i >= len(s) || s[i] != ':' {
		return 0, 0, false
	}
	i++
	start := i
	if i < len(s) && s[i] == '-' {
		return 0, 0, false
	}
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
		if i-start > 7 {
			return 0, 0, false
		}
	}
	if i == start || i >= len(s) || s[i] != ':' {
		return 0, 0, false
	}
	n, err := strconv.Atoi(s[start:i])
	if err != nil || n < 0 || n > maxPHPSerializeBytes {
		return 0, 0, false
	}
	return n, i + 1, true
}
