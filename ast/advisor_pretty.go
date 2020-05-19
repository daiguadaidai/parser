package ast

import "github.com/daiguadaidai/parser/format"

func (n *IndexAdviseStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("INDEX ADVISE ")
	if n.IsLocal {
		ctx.WriteKeyWord("LOCAL ")
	}
	ctx.WriteKeyWord("INFILE ")
	ctx.WriteString(n.Path)
	if n.MaxMinutes != UnspecifiedSize {
		ctx.WriteKeyWord(" MAX_MINUTES ")
		ctx.WritePlainf("%d", n.MaxMinutes)
	}
	if n.MaxIndexNum != nil {
		n.MaxIndexNum.Pretty(ctx, level, indent, char)
	}
	n.LinesInfo.Pretty(ctx, level, indent, char)
	return nil
}

func (n *MaxIndexNumClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord(" MAX_IDXNUM")
	if n.PerTable != UnspecifiedSize {
		ctx.WriteKeyWord(" PER_TABLE ")
		ctx.WritePlainf("%d", n.PerTable)
	}
	if n.PerDB != UnspecifiedSize {
		ctx.WriteKeyWord(" PER_DB ")
		ctx.WritePlainf("%d", n.PerDB)
	}
	return nil
}
