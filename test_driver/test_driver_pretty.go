// Copyright 2019 PingCAP, Inc.
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

//+build !codes

package test_driver

import (
	"fmt"
	"strconv"

	"github.com/daiguadaidai/parser/format"
	"github.com/daiguadaidai/parser/mysql"
)

func (n *ValueExpr) Pretty(ctx *format.RestoreCtx, level, indent int64) error {
	switch n.Kind() {
	case KindNull:
		ctx.WriteKeyWord("NULL")
	case KindInt64:
		if n.Type.Flag&mysql.IsBooleanFlag != 0 {
			if n.GetInt64() > 0 {
				ctx.WriteKeyWord("TRUE")
			} else {
				ctx.WriteKeyWord("FALSE")
			}
		} else {
			ctx.WritePlain(strconv.FormatInt(n.GetInt64(), 10))
		}
	case KindUint64:
		ctx.WritePlain(strconv.FormatUint(n.GetUint64(), 10))
	case KindFloat32:
		ctx.WritePlain(strconv.FormatFloat(n.GetFloat64(), 'e', -1, 32))
	case KindFloat64:
		ctx.WritePlain(strconv.FormatFloat(n.GetFloat64(), 'e', -1, 64))
	case KindString:
		if n.Type.Charset != "" && n.Type.Charset != mysql.DefaultCharset {
			ctx.WritePlain("_")
			ctx.WriteKeyWord(n.Type.Charset)
		}
		ctx.WriteString(n.GetString())
	case KindBytes:
		ctx.WriteString(n.GetString())
	case KindMysqlDecimal:
		ctx.WritePlain(n.GetMysqlDecimal().String())
	case KindBinaryLiteral:
		if n.Type.Flag&mysql.UnsignedFlag != 0 {
			ctx.WritePlainf("x'%x'", n.GetBytes())
		} else {
			ctx.WritePlain(n.GetBinaryLiteral().ToBitLiteralString(true))
		}
	case KindMysqlDuration, KindMysqlEnum,
		KindMysqlBit, KindMysqlSet, KindMysqlTime,
		KindInterface, KindMinNotNull, KindMaxValue,
		KindRaw, KindMysqlJSON:
		// TODO implement Restore function
		return fmt.Errorf("not implemented")
	default:
		return fmt.Errorf("can't format to string")
	}
	return nil
}

func (n *ParamMarkerExpr) Pretty(ctx *format.RestoreCtx, level, indent int64) error {
	ctx.WritePlain("?")
	return nil
}
