package tests

import (
	"fmt"
	"github.com/daiguadaidai/parser/ast"
	"github.com/daiguadaidai/parser/format"
	"strings"
)

func RestoreSql(node ast.Node) (string, error) {
	// 重写sql
	var sb strings.Builder
	if err := node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", fmt.Errorf("重写SQL出错. %s", err.Error())
	}

	return sb.String(), nil
}
