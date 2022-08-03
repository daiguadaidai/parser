package utils

import (
	"fmt"
	"strings"
)

/*
data: 源数据
warpStr: 最后元数据需要使用什么包括
如: data: aabb, wrapStr: '
最后: 'aabb'
*/
func GetSqlStrValue(data string, wrapStr string) (string, error) {
	oriStrRunes := []rune(data)
	fmt.Println(oriStrRunes)

	var sb strings.Builder
	// 添加开头单引号
	_, err := fmt.Fprint(&sb, wrapStr)
	if err != nil {
		return "", err
	}
	for _, oriStrRune := range oriStrRunes {
		var s string

		switch oriStrRune {
		case 34: // " 双引号
			s = "\\\""
		case 39: // ' 单引号
			s = "\\'"
		case 92: // \ 反斜杠
			s = "\\\\"
		default:
			s = string(oriStrRune)
		}

		_, err := fmt.Fprint(&sb, s)
		if err != nil {
			return "", err
		}
	}

	// 添加结尾单引号
	_, err = fmt.Fprint(&sb, wrapStr)
	if err != nil {
		return "", err
	}

	return sb.String(), nil
}
