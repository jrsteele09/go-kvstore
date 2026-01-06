package kvstore

import (
	"unicode"
)

// Prepopulated map for valid special runes.
var validRunesMap = map[rune]bool{
	':': true,
	'@': true,
	'#': true,
	'+': true,
	'-': true,
	'_': true,
	'/': true,
}

// KeyValid returns true if the key contains valid characters.
// The valid characters include Unicode letters, digits, and specific special characters: ':', '@', '#', '+', '-', '_', '/'.
func KeyValid(key string) bool {
	if key == "" {
		return false
	}
	for _, r := range key {
		if !isValidRune(r) {
			return false
		}
	}
	return true
}

// isValidRune returns true if the rune is a valid character for a key.
// Valid characters include Unicode letters, digits, or special characters listed in the validRunesMap.
func isValidRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || validRunesMap[r]
}
