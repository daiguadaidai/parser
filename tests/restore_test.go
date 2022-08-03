package tests

import (
	"fmt"
	"github.com/daiguadaidai/parser"
	_ "github.com/daiguadaidai/parser/test_driver"
	"testing"
)

func TestRestoreInsertStr_01(t *testing.T) {
	query := `INSERT INTO t_test(col1, col2) VALUES (852209,"{\"acol\":\"💁👌🎍😍690\\1586105001\",\"bcol\":\"500g+420g \"}")`

	sqlParser := parser.New()
	stmtNodes, _, err := sqlParser.Parse(query, "", "")
	if err != nil {
		fmt.Printf("Syntax Error: %v\n", err)
	}

	for _, stmtNode := range stmtNodes {
		vst := NewInsertVisitor()
		stmtNode.Accept(vst)

		restoreSql, err := RestoreSql(stmtNode)
		if err != nil {
			t.Fatal(err.Error())
		}

		fmt.Println(restoreSql)
	}
}
