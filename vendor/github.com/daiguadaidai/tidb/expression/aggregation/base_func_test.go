package aggregation

import (
	"github.com/daiguadaidai/parser/ast"
	"github.com/daiguadaidai/parser/mysql"
	"github.com/daiguadaidai/tidb/expression"
	"github.com/daiguadaidai/tidb/sessionctx"
	"github.com/daiguadaidai/tidb/types"
	"github.com/daiguadaidai/tidb/util/mock"
	"github.com/pingcap/check"
)

var _ = check.Suite(&testBaseFuncSuite{})

type testBaseFuncSuite struct {
	ctx sessionctx.Context
}

func (s *testBaseFuncSuite) SetUpSuite(c *check.C) {
	s.ctx = mock.NewContext()
}

func (s *testBaseFuncSuite) TestClone(c *check.C) {
	col := &expression.Column{
		UniqueID: 0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	desc := newBaseFuncDesc(s.ctx, ast.AggFuncFirstRow, []expression.Expression{col})
	cloned := desc.clone()
	c.Assert(desc.equal(s.ctx, cloned), check.IsTrue)

	col1 := &expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeVarchar),
	}
	cloned.Args[0] = col1

	c.Assert(desc.Args[0], check.Equals, col)
	c.Assert(desc.equal(s.ctx, cloned), check.IsFalse)
}
