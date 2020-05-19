// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"github.com/daiguadaidai/parser/format"
	"github.com/daiguadaidai/parser/opcode"
	"github.com/pingcap/errors"
)

func (n *BetweenExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore BetweenExpr.Expr")
	}
	if n.Not {
		ctx.WriteKeyWord(" NOT BETWEEN ")
	} else {
		ctx.WriteKeyWord(" BETWEEN ")
	}
	if err := n.Left.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore BetweenExpr.Left")
	}
	ctx.WriteKeyWord(" AND ")
	if err := n.Right.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore BetweenExpr.Right ")
	}
	return nil
}

func prettyBinaryOpWithSpacesAround(ctx *format.RestoreCtx, op opcode.Op, level, indent int64) error {
	shouldInsertSpace := ctx.Flags.HasSpacesAroundBinaryOperationFlag() || op.IsKeyword()
	if shouldInsertSpace {
		ctx.WritePlain(" ")
	}
	if err := op.Restore(ctx); err != nil {
		return err // no need to annotate, the caller will annotate.
	}
	if shouldInsertSpace {
		ctx.WritePlain(" ")
	}
	return nil
}

func (n *BinaryOperationExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.L.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred when restore BinaryOperationExpr.L")
	}
	if err := restoreBinaryOpWithSpacesAround(ctx, n.Op); err != nil {
		return errors.Annotate(err, "An error occurred when restore BinaryOperationExpr.Op")
	}
	if err := n.R.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred when restore BinaryOperationExpr.R")
	}

	return nil
}

func (n *WhenClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("WHEN ")
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore WhenClauses.Expr")
	}
	ctx.WriteKeyWord(" THEN ")
	if err := n.Result.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore WhenClauses.Result")
	}
	return nil
}

func (n *CaseExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("CASE")
	if n.Value != nil {
		ctx.WritePlain(" ")
		if err := n.Value.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CaseExpr.Value")
		}
	}
	for _, clause := range n.WhenClauses {
		ctx.WritePlain(" ")
		if err := clause.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CaseExpr.WhenClauses")
		}
	}
	if n.ElseClause != nil {
		ctx.WriteKeyWord(" ELSE ")
		if err := n.ElseClause.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CaseExpr.ElseClause")
		}
	}
	ctx.WriteKeyWord(" END")

	return nil
}

func (n *SubqueryExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WritePlain("(")
	if err := n.Query.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore SubqueryExpr.Query")
	}
	ctx.WritePlain(")")
	return nil
}

func (n *CompareSubqueryExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.L.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CompareSubqueryExpr.L")
	}
	if err := restoreBinaryOpWithSpacesAround(ctx, n.Op); err != nil {
		return errors.Annotate(err, "An error occurred while restore CompareSubqueryExpr.Op")
	}
	if n.All {
		ctx.WriteKeyWord("ALL ")
	} else {
		ctx.WriteKeyWord("ANY ")
	}
	if err := n.R.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CompareSubqueryExpr.R")
	}
	return nil
}

func (n *TableNameExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (n *ColumnName) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Schema.O != "" {
		ctx.WriteName(n.Schema.O)
		ctx.WritePlain(".")
	}
	if n.Table.O != "" {
		ctx.WriteName(n.Table.O)
		ctx.WritePlain(".")
	}
	ctx.WriteName(n.Name.O)
	return nil
}

func (n *ColumnNameExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (n *DefaultExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("DEFAULT")
	if n.Name != nil {
		ctx.WritePlain("(")
		if err := n.Name.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore DefaultExpr.Name")
		}
		ctx.WritePlain(")")
	}
	return nil
}

func (n *ExistsSubqueryExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Not {
		ctx.WriteKeyWord("NOT EXISTS ")
	} else {
		ctx.WriteKeyWord("EXISTS ")
	}
	if err := n.Sel.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore ExistsSubqueryExpr.Sel")
	}
	return nil
}

func (n *PatternInExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PatternInExpr.Expr")
	}
	if n.Not {
		ctx.WriteKeyWord(" NOT IN ")
	} else {
		ctx.WriteKeyWord(" IN ")
	}
	if n.Sel != nil {
		if err := n.Sel.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PatternInExpr.Sel")
		}
	} else {
		ctx.WritePlain("(")
		for i, expr := range n.List {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := expr.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PatternInExpr.List[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

func (n *IsNullExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	if n.Not {
		ctx.WriteKeyWord(" IS NOT NULL")
	} else {
		ctx.WriteKeyWord(" IS NULL")
	}
	return nil
}

func (n *IsTruthExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	if n.Not {
		ctx.WriteKeyWord(" IS NOT")
	} else {
		ctx.WriteKeyWord(" IS")
	}
	if n.True > 0 {
		ctx.WriteKeyWord(" TRUE")
	} else {
		ctx.WriteKeyWord(" FALSE")
	}
	return nil
}

func (n *PatternLikeExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PatternLikeExpr.Expr")
	}

	if n.Not {
		ctx.WriteKeyWord(" NOT LIKE ")
	} else {
		ctx.WriteKeyWord(" LIKE ")
	}

	if err := n.Pattern.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PatternLikeExpr.Pattern")
	}

	escape := string(n.Escape)
	if escape != "\\" {
		ctx.WriteKeyWord(" ESCAPE ")
		ctx.WriteString(escape)

	}
	return nil
}

func (n *ParenthesesExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WritePlain("(")
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred when restore ParenthesesExpr.Expr")
	}
	ctx.WritePlain(")")
	return nil
}

func (n *PositionExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WritePlainf("%d", n.N)
	return nil
}

func (n *PatternRegexpExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PatternRegexpExpr.Expr")
	}

	if n.Not {
		ctx.WriteKeyWord(" NOT REGEXP ")
	} else {
		ctx.WriteKeyWord(" REGEXP ")
	}

	if err := n.Pattern.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PatternRegexpExpr.Pattern")
	}

	return nil
}

func (n *RowExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ROW")
	ctx.WritePlain("(")
	for i, v := range n.Values {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred when restore RowExpr.Values[%v]", i)
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *UnaryOperationExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Op.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := n.V.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (n *ValuesExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("VALUES")
	ctx.WritePlain("(")
	if err := n.Column.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore ValuesExpr.Column")
	}
	ctx.WritePlain(")")

	return nil
}

func (n *VariableExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.IsSystem {
		ctx.WritePlain("@@")
		if n.ExplicitScope {
			if n.IsGlobal {
				ctx.WriteKeyWord("GLOBAL")
			} else {
				ctx.WriteKeyWord("SESSION")
			}
			ctx.WritePlain(".")
		}
	} else {
		ctx.WritePlain("@")
	}
	ctx.WriteName(n.Name)

	if n.Value != nil {
		ctx.WritePlain(":=")
		if err := n.Value.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore VariableExpr.Value")
		}
	}

	return nil
}

func (n *MaxValueExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("MAXVALUE")
	return nil
}

func (n *MatchAgainst) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("MATCH")
	ctx.WritePlain(" (")
	for i, v := range n.ColumnNames {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore MatchAgainst.ColumnNames[%d]", i)
		}
	}
	ctx.WritePlain(") ")
	ctx.WriteKeyWord("AGAINST")
	ctx.WritePlain(" (")
	if err := n.Against.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore MatchAgainst.Against")
	}
	if n.Modifier.IsBooleanMode() {
		ctx.WritePlain(" IN BOOLEAN MODE")
		if n.Modifier.WithQueryExpansion() {
			return errors.New("BOOLEAN MODE doesn't support QUERY EXPANSION")
		}
	} else if n.Modifier.WithQueryExpansion() {
		ctx.WritePlain(" WITH QUERY EXPANSION")
	}
	ctx.WritePlain(")")
	return nil
}

func (n *SetCollationExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	ctx.WriteKeyWord(" COLLATE ")
	ctx.WritePlain(n.Collate)
	return nil
}
