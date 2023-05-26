package kvstore

import "unicode"

// KeyValid returns true if the key contains valid characters - unicode letters, digits and or valid characters: ':', '@', '#', '+', '-', '_'
func KeyValid(key string) bool {
	validRunes := []rune{':', '@', '#', '+', '-', '_'}

	for _, r := range key {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !containsRune(validRunes, r) {
			return false
		}
	}
	return true
}

func containsRune(runes []rune, r rune) bool {
	for _, r2 := range runes {
		if r2 == r {
			return true
		}
	}
	return false
}
