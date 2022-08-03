package tests

import (
	"fmt"
	"github.com/daiguadaidai/parser/ast"
	"strings"
)

type InsertVisitor struct {
	Deep int
}

func NewInsertVisitor() *InsertVisitor {
	vst := new(InsertVisitor)

	return vst
}

func (this *InsertVisitor) GetIntend() string {
	return strings.Repeat(" ", this.Deep*4)
}

func (this *InsertVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	this.Deep++
	fmt.Printf("Enter: %v%T\n", this.GetIntend(), in)

	return in, false
}

func (this *InsertVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	defer func() {
		this.Deep--
	}()

	fmt.Printf("Leave: %v%T\n", this.GetIntend(), in)
	return in, true
}
