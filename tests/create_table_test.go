package tests

import (
	"fmt"
	"github.com/daiguadaidai/parser"
	"github.com/daiguadaidai/parser/ast"
	_ "github.com/daiguadaidai/parser/test_driver"
	"testing"
)

func Test_TDSQLShard_01(t *testing.T) {
	query := `
create table test1 (
    a int,
    b int,
    c char(20),
    primary key (a,b),
    unique key u_1(a,c)
) shardkey=a comment='测试';
`
	sqlParser := parser.New()
	stmtNodes, _, err := sqlParser.Parse(query, "", "")
	if err != nil {
		fmt.Printf("Syntax Error: %v", err)
	}

	for _, stmtNode := range stmtNodes {
		fmt.Println(stmtNode.Text())

		createTableStmtNode := stmtNode.(*ast.CreateTableStmt)
		for _, ops := range createTableStmtNode.Options {
			fmt.Printf("%v=%v\n", ops.Tp, ops.StrValue)
		}

		createSql, err := RestoreSql(stmtNode)
		if err != nil {
			t.Fatal(err.Error())
		}

		fmt.Println(createSql)
	}
}
