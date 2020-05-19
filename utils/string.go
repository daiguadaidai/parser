package utils

import "strings"

func GetIndent(level, indent int64, char string) string {
	return strings.Repeat(char, int(level*indent))
}
