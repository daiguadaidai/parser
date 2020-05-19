package parser

import (
	"fmt"
	format1 "github.com/daiguadaidai/parser/format"
	"strings"
	"testing"
)

func Test_Degester_01(t *testing.T) {
	query := "select * from b where id = 1"
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())
	normalized, digest := NormalizeDigest(stmt.Text())
	fmt.Println(normalized)
	fmt.Println(digest)

	var sb strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb), 0, 0, ""); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}

	fmt.Println("Restore语句:", sb.String())
}
