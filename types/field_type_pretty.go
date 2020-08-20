package types

import (
	"github.com/daiguadaidai/parser/charset"
	"github.com/daiguadaidai/parser/format"
	"github.com/daiguadaidai/parser/mysql"
)

// Pretty implements Node interface.
func (ft *FieldType) Pretty(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(TypeToStr(ft.Tp, ft.Charset))

	precision := UnspecifiedLength
	scale := UnspecifiedLength

	switch ft.Tp {
	case mysql.TypeEnum, mysql.TypeSet:
		ctx.WritePlain("(")
		for i, e := range ft.Elems {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteString(e)
		}
		ctx.WritePlain(")")
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		precision = ft.Decimal
	case mysql.TypeUnspecified, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		precision = ft.Flen
		scale = ft.Decimal
	default:
		precision = ft.Flen
	}

	if precision != UnspecifiedLength {
		ctx.WritePlainf("(%d", precision)
		if scale != UnspecifiedLength {
			ctx.WritePlainf(",%d", scale)
		}
		ctx.WritePlain(")")
	}

	if mysql.HasUnsignedFlag(ft.Flag) {
		ctx.WriteKeyWord(" UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.Flag) {
		ctx.WriteKeyWord(" ZEROFILL")
	}
	if mysql.HasBinaryFlag(ft.Flag) && ft.Charset != charset.CharsetBin {
		ctx.WriteKeyWord(" BINARY")
	}

	if IsTypeChar(ft.Tp) || IsTypeBlob(ft.Tp) {
		if ft.Charset != "" && ft.Charset != charset.CharsetBin {
			ctx.WriteKeyWord(" CHARACTER SET " + ft.Charset)
		}
		if ft.Collate != "" && ft.Collate != charset.CharsetBin {
			ctx.WriteKeyWord(" COLLATE ")
			ctx.WritePlain(ft.Collate)
		}
	}

	return nil
}
