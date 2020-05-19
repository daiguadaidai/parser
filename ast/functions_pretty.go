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
	"github.com/pingcap/errors"
	"strings"
)

func (n *FuncCallExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	var specialLiteral string
	switch n.FnName.L {
	case DateLiteral:
		specialLiteral = "DATE "
	case TimeLiteral:
		specialLiteral = "TIME "
	case TimestampLiteral:
		specialLiteral = "TIMESTAMP "
	}
	if specialLiteral != "" {
		ctx.WritePlain(specialLiteral)
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		return nil
	}

	ctx.WriteKeyWord(n.FnName.O)
	ctx.WritePlain("(")
	switch n.FnName.L {
	case "convert":
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		ctx.WriteKeyWord(" USING ")
		ctx.WriteKeyWord(n.Args[1].GetType().Charset)
	case "adddate", "subdate", "date_add", "date_sub":
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[0]")
		}
		ctx.WritePlain(", ")
		ctx.WriteKeyWord("INTERVAL ")
		if err := n.Args[1].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[1]")
		}
		ctx.WritePlain(" ")
		if err := n.Args[2].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[2]")
		}
	case "extract":
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[0]")
		}
		ctx.WriteKeyWord(" FROM ")
		if err := n.Args[1].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[1]")
		}
	case "position":
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr")
		}
		ctx.WriteKeyWord(" IN ")
		if err := n.Args[1].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr")
		}
	case "trim":
		switch len(n.Args) {
		case 3:
			if err := n.Args[2].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[2]")
			}
			ctx.WritePlain(" ")
			fallthrough
		case 2:
			if n.Args[1].(ValueExpr).GetValue() != nil {
				if err := n.Args[1].Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[1]")
				}
				ctx.WritePlain(" ")
			}
			ctx.WriteKeyWord("FROM ")
			fallthrough
		case 1:
			if err := n.Args[0].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[0]")
			}
		}
	case WeightString:
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.(WEIGHT_STRING).Args[0]")
		}
		if len(n.Args) == 3 {
			ctx.WriteKeyWord(" AS ")
			ctx.WriteKeyWord(n.Args[1].(ValueExpr).GetValue().(string))
			ctx.WritePlain("(")
			if err := n.Args[2].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.(WEIGHT_STRING).Args[2]")
			}
			ctx.WritePlain(")")
		}
	default:
		for i, argv := range n.Args {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := argv.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args %d", i)
			}
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *FuncCastExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.FunctionType {
	case CastFunction:
		ctx.WriteKeyWord("CAST")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		ctx.WriteKeyWord(" AS ")
		n.Tp.RestoreAsCastType(ctx)
		ctx.WritePlain(")")
	case CastConvertFunction:
		ctx.WriteKeyWord("CONVERT")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		ctx.WritePlain(", ")
		n.Tp.RestoreAsCastType(ctx)
		ctx.WritePlain(")")
	case CastBinaryOperator:
		ctx.WriteKeyWord("BINARY ")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
	}
	return nil
}

func (n *TrimDirectionExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord(n.Direction.String())
	return nil
}

func (n *AggregateFuncExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord(n.F)
	ctx.WritePlain("(")
	if n.Distinct {
		ctx.WriteKeyWord("DISTINCT ")
	}
	switch strings.ToLower(n.F) {
	case "group_concat":
		for i := 0; i < len(n.Args)-1; i++ {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := n.Args[i].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AggregateFuncExpr.Args[%d]", i)
			}
		}
		if n.Order != nil {
			ctx.WritePlain(" ")
			if err := n.Order.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occur while restore AggregateFuncExpr.Args Order")
			}
		}
		ctx.WriteKeyWord(" SEPARATOR ")
		if err := n.Args[len(n.Args)-1].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AggregateFuncExpr.Args SEPARATOR")
		}
	default:
		for i, argv := range n.Args {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := argv.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AggregateFuncExpr.Args[%d]", i)
			}
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *WindowFuncExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord(n.F)
	ctx.WritePlain("(")
	for i, v := range n.Args {
		if i != 0 {
			ctx.WritePlain(", ")
		} else if n.Distinct {
			ctx.WriteKeyWord("DISTINCT ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore WindowFuncExpr.Args[%d]", i)
		}
	}
	ctx.WritePlain(")")
	if n.FromLast {
		ctx.WriteKeyWord(" FROM LAST")
	}
	if n.IgnoreNull {
		ctx.WriteKeyWord(" IGNORE NULLS")
	}
	ctx.WriteKeyWord(" OVER ")
	if err := n.Spec.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore WindowFuncExpr.Spec")
	}

	return nil
}

func (n *TimeUnitExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord(n.Unit.String())
	return nil
}

func (n *GetFormatSelectorExpr) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord(n.Selector.String())
	return nil
}
