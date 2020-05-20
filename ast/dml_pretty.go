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
	"github.com/daiguadaidai/parser/mysql"
	"github.com/daiguadaidai/parser/utils"
	"github.com/pingcap/errors"
)

func (n *Join) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.JoinLevel++
	if err := n.Left.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore Join.Left")
	}
	ctx.JoinLevel--
	if n.Right == nil {
		return nil
	}
	if n.NaturalJoin {
		ctx.WritePlain("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		ctx.WriteKeyWord("NATURAL")
	}
	switch n.Tp {
	case LeftJoin:
		ctx.WritePlain("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		ctx.WriteKeyWord("LEFT")
	case RightJoin:
		ctx.WritePlain("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		ctx.WriteKeyWord("RIGHT")
	}
	if n.StraightJoin {
		ctx.WriteKeyWord(" STRAIGHT_JOIN ")
	} else {
		ctx.WriteKeyWord(" JOIN ")
	}
	ctx.JoinLevel++
	if err := n.Right.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore Join.Right")
	}
	ctx.JoinLevel--

	if n.On != nil {
		ctx.WritePlain("\n")
		ctx.WritePlain(utils.GetIndent(level, indent, char))
		if err := n.On.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore Join.On")
		}
	}
	if len(n.Using) != 0 {
		ctx.WriteKeyWord(" USING ")
		ctx.WritePlain("(")
		for i, v := range n.Using {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore Join.Using")
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

func (n *TableName) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	n.prettyName(ctx, level, indent, char)
	n.prettyPartitions(ctx, level, indent, char)
	return n.prettyIndexHints(ctx, level, indent, char)
}

// Restore implements Node interface.
func (n *TableName) prettyName(ctx *format.RestoreCtx, level, indent int64, char string) {
	if n.Schema.String() != "" {
		ctx.WriteName(n.Schema.String())
		ctx.WritePlain(".")
	}
	ctx.WriteName(n.Name.String())
}

func (n *TableName) prettyPartitions(ctx *format.RestoreCtx, level, indent int64, char string) {
	if len(n.PartitionNames) > 0 {
		ctx.WriteKeyWord(" PARTITION")
		ctx.WritePlain("(")
		for i, v := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(v.String())
		}
		ctx.WritePlain(")")
	}
}

func (n *TableName) prettyIndexHints(ctx *format.RestoreCtx, level, indent int64, char string) error {
	for _, value := range n.IndexHints {
		ctx.WritePlain(" ")
		if err := value.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing IndexHints")
		}
	}

	return nil
}

func (n *IndexHint) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	indexHintType := ""
	switch n.HintType {
	case 1:
		indexHintType = "USE INDEX"
	case 2:
		indexHintType = "IGNORE INDEX"
	case 3:
		indexHintType = "FORCE INDEX"
	default: // Prevent accidents
		return errors.New("IndexHintType has an error while matching")
	}

	indexHintScope := ""
	switch n.HintScope {
	case 1:
		indexHintScope = ""
	case 2:
		indexHintScope = " FOR JOIN"
	case 3:
		indexHintScope = " FOR ORDER BY"
	case 4:
		indexHintScope = " FOR GROUP BY"
	default: // Prevent accidents
		return errors.New("IndexHintScope has an error while matching")
	}
	ctx.WriteKeyWord(indexHintType)
	ctx.WriteKeyWord(indexHintScope)
	ctx.WritePlain(" (")
	for i, value := range n.IndexNames {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteName(value.O)
	}
	ctx.WritePlain(")")

	return nil
}

func (n *DeleteTableList) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	for i, t := range n.Tables {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := t.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DeleteTableList.Tables[%v]", i)
		}
	}
	return nil
}

func (n *OnCondition) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ON ")
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore OnCondition.Expr")
	}
	return nil
}

func (n *TableSource) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	needParen := false
	switch n.Source.(type) {
	case *SelectStmt, *UnionStmt:
		needParen = true
	}

	if tn, tnCase := n.Source.(*TableName); tnCase {
		if needParen {
			ctx.WritePlain("(")
		}

		tn.prettyName(ctx, level, indent, char)
		tn.prettyPartitions(ctx, level, indent, char)

		if asName := n.AsName.String(); asName != "" {
			ctx.WriteKeyWord(" AS ")
			ctx.WriteName(asName)
		}
		if err := tn.prettyIndexHints(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore TableSource.Source.(*TableName).IndexHints")
		}

		if needParen {
			ctx.WritePlain(")")
		}
	} else {
		if needParen {
			ctx.WritePlain("(\n")
		}
		if err := n.Source.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore TableSource.Source")
		}
		if needParen {
			ctx.WritePlain("\n")
			ctx.WritePlain(utils.GetIndent(level-1, indent, char))
			ctx.WritePlain(")")
		}
		if asName := n.AsName.String(); asName != "" {
			ctx.WriteKeyWord(" AS ")
			ctx.WriteName(asName)
		}
	}

	return nil
}

func (n *WildCardField) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if schema := n.Schema.String(); schema != "" {
		ctx.WriteName(schema)
		ctx.WritePlain(".")
	}
	if table := n.Table.String(); table != "" {
		ctx.WriteName(table)
		ctx.WritePlain(".")
	}
	ctx.WritePlain("*")
	return nil
}

func (n *SelectField) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.WildCard != nil {
		if err := n.WildCard.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectField.WildCard")
		}
	}
	if n.Expr != nil {
		if err := n.Expr.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectField.Expr")
		}
	}
	if asName := n.AsName.String(); asName != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(asName)
	}
	return nil
}

func (n *FieldList) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	for i, v := range n.Fields {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FieldList.Fields[%d]", i)
		}
	}
	return nil
}

func (n *TableRefsClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.TableRefs.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore TableRefsClause.TableRefs")
	}
	return nil
}

func (n *ByItem) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore ByItem.Expr")
	}
	if n.Desc {
		ctx.WriteKeyWord(" DESC")
	}
	return nil
}

func (n *GroupByClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("GROUP BY ")
	for i, v := range n.Items {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore GroupByClause.Items[%d]", i)
		}
	}
	return nil
}

func (n *HavingClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("HAVING ")
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore HavingClause.Expr")
	}
	return nil
}

func (n *OrderByClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ORDER BY ")
	for i, item := range n.Items {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := item.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore OrderByClause.Items[%d]", i)
		}
	}
	return nil
}

func (n *SelectStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	level += 1
	ctx.WritePlain(utils.GetIndent(level-1, indent, char))
	ctx.WriteKeyWord("SELECT ")

	if n.SelectStmtOpts.Priority > 0 {
		ctx.WriteKeyWord(mysql.Priority2Str[n.SelectStmtOpts.Priority])
		ctx.WritePlain(" ")
	}

	if n.SelectStmtOpts.SQLSmallResult {
		ctx.WriteKeyWord("SQL_SMALL_RESULT ")
	}

	if n.SelectStmtOpts.SQLBigResult {
		ctx.WriteKeyWord("SQL_BIG_RESULT ")
	}

	if n.SelectStmtOpts.SQLBufferResult {
		ctx.WriteKeyWord("SQL_BUFFER_RESULT ")
	}

	if !n.SelectStmtOpts.SQLCache {
		ctx.WriteKeyWord("SQL_NO_CACHE ")
	}

	if n.TableHints != nil && len(n.TableHints) != 0 {
		ctx.WritePlain("/*+ ")
		for i, tableHint := range n.TableHints {
			if err := tableHint.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore SelectStmt.TableHints[%d]", i)
			}
		}
		ctx.WritePlain("*/ ")
	}

	ctx.WritePlain("\n")
	ctx.WritePlain(utils.GetIndent(level, indent, char))
	if n.Distinct {
		ctx.WriteKeyWord("DISTINCT ")
	}
	if n.SelectStmtOpts.StraightJoin {
		ctx.WriteKeyWord("STRAIGHT_JOIN ")
	}
	if n.Fields != nil {
		for i, field := range n.Fields.Fields {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if i != 0 && i%5 == 0 && len(n.Fields.Fields) != i {
				ctx.WritePlain("\n")
				ctx.WritePlain(utils.GetIndent(level, indent, char))
			}
			if err := field.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore SelectStmt.Fields[%d]", i)
			}
		}
	}

	if n.From != nil {
		ctx.WriteKeyWord("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		ctx.WriteKeyWord("FROM ")
		if err := n.From.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectStmt.From")
		}
	}

	if n.From == nil && n.Where != nil {
		ctx.WriteKeyWord("FROM DUAL")
	}
	if n.Where != nil {
		ctx.WriteKeyWord("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		ctx.WriteKeyWord("WHERE ")
		if err := n.Where.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectStmt.Where")
		}
	}

	if n.GroupBy != nil {
		ctx.WriteKeyWord("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		if err := n.GroupBy.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectStmt.GroupBy")
		}
	}

	if n.Having != nil {
		ctx.WriteKeyWord("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		if err := n.Having.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectStmt.Having")
		}
	}

	if n.WindowSpecs != nil {
		ctx.WriteKeyWord("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		ctx.WriteKeyWord("WINDOW ")
		for i, windowsSpec := range n.WindowSpecs {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := windowsSpec.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore SelectStmt.WindowSpec[%d]", i)
			}
		}
	}

	if n.OrderBy != nil {
		ctx.WriteKeyWord("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		if err := n.OrderBy.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectStmt.OrderBy")
		}
	}

	if n.Limit != nil {
		ctx.WriteKeyWord("\n")
		ctx.WritePlain(utils.GetIndent(level-1, indent, char))
		if err := n.Limit.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectStmt.Limit")
		}
	}

	switch n.LockTp {
	case SelectLockInShareMode:
		ctx.WriteKeyWord(" LOCK ")
		ctx.WriteKeyWord(n.LockTp.String())
	case SelectLockForUpdate, SelectLockForUpdateNoWait:
		ctx.WritePlain(" ")
		ctx.WriteKeyWord(n.LockTp.String())
	}

	if n.SelectIntoOpt != nil {
		ctx.WritePlain(" ")
		if err := n.SelectIntoOpt.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectStmt.SelectIntoOpt")
		}
	}
	return nil
}

func (n *UnionSelectList) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	for i, selectStmt := range n.Selects {
		if i != 0 {
			ctx.WriteKeyWord("\n")
			ctx.WriteKeyWord("UNION ")
			if !selectStmt.IsAfterUnionDistinct {
				ctx.WriteKeyWord("ALL ")
			}
			ctx.WriteKeyWord("\n")
		}
		if selectStmt.IsInBraces {
			ctx.WritePlain("(")
		}
		if err := selectStmt.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore UnionSelectList.SelectStmt")
		}
		if selectStmt.IsInBraces {
			ctx.WritePlain(")")
		}
	}
	return nil
}

func (n *UnionStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.SelectList.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore UnionStmt.SelectList")
	}

	if n.OrderBy != nil {
		ctx.WritePlain(" ")
		if err := n.OrderBy.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore UnionStmt.OrderBy")
		}
	}

	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore UnionStmt.Limit")
		}
	}
	return nil
}

func (n *Assignment) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Column.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore Assignment.Column")
	}
	ctx.WritePlain("=")
	if err := n.Expr.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore Assignment.Expr")
	}
	return nil
}

func (n *LoadDataStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("LOAD DATA ")
	if n.IsLocal {
		ctx.WriteKeyWord("LOCAL ")
	}
	ctx.WriteKeyWord("INFILE ")
	ctx.WriteString(n.Path)
	if n.OnDuplicate == OnDuplicateKeyHandlingReplace {
		ctx.WriteKeyWord(" REPLACE")
	} else if n.OnDuplicate == OnDuplicateKeyHandlingIgnore {
		ctx.WriteKeyWord(" IGNORE")
	}
	ctx.WriteKeyWord(" INTO TABLE ")
	if err := n.Table.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore LoadDataStmt.Table")
	}
	n.FieldsInfo.Pretty(ctx, level, indent, char)
	n.LinesInfo.Pretty(ctx, level, indent, char)
	if n.IgnoreLines != 0 {
		ctx.WriteKeyWord(" IGNORE ")
		ctx.WritePlainf("%d", n.IgnoreLines)
		ctx.WriteKeyWord(" LINES")
	}
	if len(n.ColumnsAndUserVars) != 0 {
		ctx.WritePlain(" (")
		for i, c := range n.ColumnsAndUserVars {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if c.ColumnName != nil {
				if err := c.ColumnName.Pretty(ctx, level, indent, char); err != nil {
					return errors.Annotate(err, "An error occurred while restore LoadDataStmt.ColumnsAndUserVars")
				}
			}
			if c.UserVar != nil {
				if err := c.UserVar.Pretty(ctx, level, indent, char); err != nil {
					return errors.Annotate(err, "An error occurred while restore LoadDataStmt.ColumnsAndUserVars")
				}
			}

		}
		ctx.WritePlain(")")
	}

	if n.ColumnAssignments != nil {
		ctx.WriteKeyWord(" SET")
		for i, assign := range n.ColumnAssignments {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WritePlain(" ")
			if err := assign.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore LoadDataStmt.ColumnAssignments")
			}
		}
	}
	return nil
}

func (n *FieldsClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Terminated != "\t" || n.Escaped != '\\' {
		ctx.WriteKeyWord(" FIELDS")
		if n.Terminated != "\t" {
			ctx.WriteKeyWord(" TERMINATED BY ")
			ctx.WriteString(n.Terminated)
		}
		if n.Enclosed != 0 {
			if n.OptEnclosed {
				ctx.WriteKeyWord(" OPTIONALLY")
			}
			ctx.WriteKeyWord(" ENCLOSED BY ")
			ctx.WriteString(string(n.Enclosed))
		}
		if n.Escaped != '\\' {
			ctx.WriteKeyWord(" ESCAPED BY ")
			if n.Escaped == 0 {
				ctx.WritePlain("''")
			} else {
				ctx.WriteString(string(n.Escaped))
			}
		}
	}
	return nil
}

func (n *LinesClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Starting != "" || n.Terminated != "\n" {
		ctx.WriteKeyWord(" LINES")
		if n.Starting != "" {
			ctx.WriteKeyWord(" STARTING BY ")
			ctx.WriteString(n.Starting)
		}
		if n.Terminated != "\n" {
			ctx.WriteKeyWord(" TERMINATED BY ")
			ctx.WriteString(n.Terminated)
		}
	}
	return nil
}

func (n *InsertStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.IsReplace {
		ctx.WriteKeyWord("REPLACE ")
	} else {
		ctx.WriteKeyWord("INSERT ")
	}

	if n.TableHints != nil && len(n.TableHints) != 0 {
		ctx.WritePlain("/*+ ")
		for i, tableHint := range n.TableHints {
			if err := tableHint.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore InsertStmt.TableHints[%d]", i)
			}
		}
		ctx.WritePlain("*/ ")
	}

	if err := n.Priority.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	if n.Priority != mysql.NoPriority {
		ctx.WritePlain(" ")
	}
	if n.IgnoreErr {
		ctx.WriteKeyWord("IGNORE ")
	}
	ctx.WriteKeyWord("INTO ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore InsertStmt.Table")
	}
	if len(n.PartitionNames) != 0 {
		ctx.WriteKeyWord(" PARTITION")
		ctx.WritePlain("(")
		for i := 0; i < len(n.PartitionNames); i++ {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(n.PartitionNames[i].String())
		}
		ctx.WritePlain(")")
	}
	if n.Columns != nil {
		ctx.WritePlain(" (")
		for i, v := range n.Columns {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore InsertStmt.Columns[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}
	if n.Lists != nil {
		ctx.WriteKeyWord(" VALUES\n")
		for i, row := range n.Lists {
			if i != 0 {
				ctx.WritePlain(",\n")
			}
			ctx.WritePlain("(")
			for j, v := range row {
				if j != 0 {
					ctx.WritePlain(",")
				}
				if err := v.Pretty(ctx, level, indent, char); err != nil {
					return errors.Annotatef(err, "An error occurred while restore InsertStmt.Lists[%d][%d]", i, j)
				}
			}
			ctx.WritePlain(")")
		}
	}
	if n.Select != nil {
		ctx.WritePlain("\n")
		switch v := n.Select.(type) {
		case *SelectStmt, *UnionStmt:
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore InsertStmt.Select")
			}
		default:
			return errors.Errorf("Incorrect type for InsertStmt.Select: %T", v)
		}
	}
	if n.Setlist != nil {
		ctx.WriteKeyWord("\nSET\n")
		for i, v := range n.Setlist {
			if i != 0 {
				ctx.WritePlain(",\n")
			}
			ctx.WritePlain(utils.GetIndent(level+1, indent, char))
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore InsertStmt.Setlist[%d]", i)
			}
		}
	}
	if n.OnDuplicate != nil {
		ctx.WriteKeyWord(" ON DUPLICATE KEY UPDATE ")
		for i, v := range n.OnDuplicate {
			if i != 0 {
				ctx.WritePlain(",\n")
			}
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore InsertStmt.OnDuplicate[%d]", i)
			}
		}
	}

	return nil
}

func (n *DeleteStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	level += 1
	ctx.WriteKeyWord("DELETE ")

	if n.TableHints != nil && len(n.TableHints) != 0 {
		ctx.WritePlain("/*+ ")
		for i, tableHint := range n.TableHints {
			if err := tableHint.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore UpdateStmt.TableHints[%d]", i)
			}
		}
		ctx.WritePlain("*/ ")
	}

	if err := n.Priority.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	if n.Priority != mysql.NoPriority {
		ctx.WritePlain(" ")
	}
	if n.Quick {
		ctx.WriteKeyWord("QUICK ")
	}
	if n.IgnoreErr {
		ctx.WriteKeyWord("IGNORE ")
	}

	if n.IsMultiTable { // Multiple-Table Syntax
		if n.BeforeFrom {
			if err := n.Tables.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore DeleteStmt.Tables")
			}

			ctx.WriteKeyWord(" FROM ")
			if err := n.TableRefs.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore DeleteStmt.TableRefs")
			}
		} else {
			ctx.WriteKeyWord("FROM ")
			if err := n.Tables.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore DeleteStmt.Tables")
			}

			ctx.WriteKeyWord(" USING ")
			if err := n.TableRefs.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore DeleteStmt.TableRefs")
			}
		}
	} else { // Single-Table Syntax
		ctx.WriteKeyWord("FROM ")

		if err := n.TableRefs.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore DeleteStmt.TableRefs")
		}
	}

	if n.Where != nil {
		ctx.WriteKeyWord("\n")
		ctx.WriteKeyWord("WHERE ")
		if err := n.Where.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore DeleteStmt.Where")
		}
	}

	if n.Order != nil {
		ctx.WritePlain("\n")
		if err := n.Order.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore DeleteStmt.Order")
		}
	}

	if n.Limit != nil {
		ctx.WritePlain("\n")
		if err := n.Limit.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore DeleteStmt.Limit")
		}
	}

	return nil
}

func (n *UpdateStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	level += 1
	ctx.WriteKeyWord("UPDATE ")

	if n.TableHints != nil && len(n.TableHints) != 0 {
		ctx.WritePlain("/*+ ")
		for i, tableHint := range n.TableHints {
			if err := tableHint.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore UpdateStmt.TableHints[%d]", i)
			}
		}
		ctx.WritePlain("*/ ")
	}

	if err := n.Priority.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	if n.Priority != mysql.NoPriority {
		ctx.WritePlain(" ")
	}
	if n.IgnoreErr {
		ctx.WriteKeyWord("IGNORE ")
	}

	if err := n.TableRefs.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occur while restore UpdateStmt.TableRefs")
	}

	ctx.WriteKeyWord("\nSET\n")
	for i, assignment := range n.List {
		if i != 0 {
			ctx.WritePlain(",\n")
		}
		ctx.WritePlain(utils.GetIndent(level, indent, char))
		if err := assignment.Column.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occur while restore UpdateStmt.List[%d].Column", i)
		}

		ctx.WritePlain("=")

		if err := assignment.Expr.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occur while restore UpdateStmt.List[%d].Expr", i)
		}
	}

	if n.Where != nil {
		ctx.WriteKeyWord("\nWHERE ")
		if err := n.Where.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occur while restore UpdateStmt.Where")
		}
	}

	if n.Order != nil {
		ctx.WritePlain("\n")
		if err := n.Order.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occur while restore UpdateStmt.Order")
		}
	}

	if n.Limit != nil {
		ctx.WritePlain("\n")
		if err := n.Limit.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occur while restore UpdateStmt.Limit")
		}
	}

	return nil
}

func (n *Limit) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("LIMIT ")
	if n.Offset != nil {
		if err := n.Offset.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore Limit.Offset")
		}
		ctx.WritePlain(",")
	}
	if err := n.Count.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore Limit.Count")
	}
	return nil
}

func (n *ShowStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	prettyOptFull := func() {
		if n.Full {
			ctx.WriteKeyWord("FULL ")
		}
	}
	prettyShowDatabaseNameOpt := func() {
		if n.DBName != "" {
			// FROM OR IN
			ctx.WriteKeyWord(" IN ")
			ctx.WriteName(n.DBName)
		}
	}
	prettyGlobalScope := func() {
		if n.GlobalScope {
			ctx.WriteKeyWord("GLOBAL ")
		} else {
			ctx.WriteKeyWord("SESSION ")
		}
	}
	prettyShowLikeOrWhereOpt := func() error {
		if n.Pattern != nil && n.Pattern.Pattern != nil {
			ctx.WriteKeyWord(" LIKE ")
			if err := n.Pattern.Pattern.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore ShowStmt.Pattern")
			}
		} else if n.Where != nil {
			ctx.WriteKeyWord(" WHERE ")
			if err := n.Where.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore ShowStmt.Where")
			}
		}
		return nil
	}

	ctx.WriteKeyWord("SHOW ")
	switch n.Tp {
	case ShowCreateTable:
		ctx.WriteKeyWord("CREATE TABLE ")
		if err := n.Table.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore ShowStmt.Table")
		}
	case ShowCreateView:
		ctx.WriteKeyWord("CREATE VIEW ")
		if err := n.Table.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore ShowStmt.VIEW")
		}
	case ShowCreateDatabase:
		ctx.WriteKeyWord("CREATE DATABASE ")
		if n.IfNotExists {
			ctx.WriteKeyWord("IF NOT EXISTS ")
		}
		ctx.WriteName(n.DBName)
	case ShowCreateSequence:
		ctx.WriteKeyWord("CREATE SEQUENCE ")
		if err := n.Table.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore ShowStmt.SEQUENCE")
		}
	case ShowCreateUser:
		ctx.WriteKeyWord("CREATE USER ")
		if err := n.User.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore ShowStmt.User")
		}
	case ShowGrants:
		ctx.WriteKeyWord("GRANTS")
		if n.User != nil {
			ctx.WriteKeyWord(" FOR ")
			if err := n.User.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore ShowStmt.User")
			}
		}
		if n.Roles != nil {
			ctx.WriteKeyWord(" USING ")
			for i, r := range n.Roles {
				if err := r.Pretty(ctx, level, indent, char); err != nil {
					return errors.Annotate(err, "An error occurred while restore ShowStmt.User")
				}
				if i != len(n.Roles)-1 {
					ctx.WritePlain(", ")
				}
			}
		}
	case ShowMasterStatus:
		ctx.WriteKeyWord("MASTER STATUS")
	case ShowProcessList:
		prettyOptFull()
		ctx.WriteKeyWord("PROCESSLIST")
	case ShowStatsMeta:
		ctx.WriteKeyWord("STATS_META")
		if err := prettyShowLikeOrWhereOpt(); err != nil {
			return err
		}
	case ShowStatsHistograms:
		ctx.WriteKeyWord("STATS_HISTOGRAMS")
		if err := prettyShowLikeOrWhereOpt(); err != nil {
			return err
		}
	case ShowStatsBuckets:
		ctx.WriteKeyWord("STATS_BUCKETS")
		if err := prettyShowLikeOrWhereOpt(); err != nil {
			return err
		}
	case ShowStatsHealthy:
		ctx.WriteKeyWord("STATS_HEALTHY")
		if err := prettyShowLikeOrWhereOpt(); err != nil {
			return err
		}
	case ShowProfiles:
		ctx.WriteKeyWord("PROFILES")
	case ShowProfile:
		ctx.WriteKeyWord("PROFILE")
		if len(n.ShowProfileTypes) > 0 {
			for i, tp := range n.ShowProfileTypes {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WritePlain(" ")
				switch tp {
				case ProfileTypeCPU:
					ctx.WriteKeyWord("CPU")
				case ProfileTypeMemory:
					ctx.WriteKeyWord("MEMORY")
				case ProfileTypeBlockIo:
					ctx.WriteKeyWord("BLOCK IO")
				case ProfileTypeContextSwitch:
					ctx.WriteKeyWord("CONTEXT SWITCHES")
				case ProfileTypeIpc:
					ctx.WriteKeyWord("IPC")
				case ProfileTypePageFaults:
					ctx.WriteKeyWord("PAGE FAULTS")
				case ProfileTypeSource:
					ctx.WriteKeyWord("SOURCE")
				case ProfileTypeSwaps:
					ctx.WriteKeyWord("SWAPS")
				case ProfileTypeAll:
					ctx.WriteKeyWord("ALL")
				}
			}
		}
		if n.ShowProfileArgs != nil {
			ctx.WriteKeyWord(" FOR QUERY ")
			ctx.WritePlainf("%d", *n.ShowProfileArgs)
		}
		if n.ShowProfileLimit != nil {
			ctx.WritePlain(" ")
			if err := n.ShowProfileLimit.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore ShowStmt.WritePlain")
			}
		}

	case ShowPrivileges:
		ctx.WriteKeyWord("PRIVILEGES")
	case ShowBuiltins:
		ctx.WriteKeyWord("BUILTINS")
	// ShowTargetFilterable
	default:
		switch n.Tp {
		case ShowEngines:
			ctx.WriteKeyWord("ENGINES")
		case ShowConfig:
			ctx.WriteKeyWord("CONFIG")
		case ShowDatabases:
			ctx.WriteKeyWord("DATABASES")
		case ShowCharset:
			ctx.WriteKeyWord("CHARSET")
		case ShowTables:
			prettyOptFull()
			ctx.WriteKeyWord("TABLES")
			prettyShowDatabaseNameOpt()
		case ShowOpenTables:
			ctx.WriteKeyWord("OPEN TABLES")
			prettyShowDatabaseNameOpt()
		case ShowTableStatus:
			ctx.WriteKeyWord("TABLE STATUS")
			prettyShowDatabaseNameOpt()
		case ShowIndex:
			// here can be INDEX INDEXES KEYS
			// FROM or IN
			ctx.WriteKeyWord("INDEX IN ")
			if err := n.Table.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while resotre ShowStmt.Table")
			} // TODO: remember to check this case
		case ShowColumns: // equivalent to SHOW FIELDS
			if n.Extended {
				ctx.WriteKeyWord("EXTENDED ")
			}
			prettyOptFull()
			ctx.WriteKeyWord("COLUMNS")
			if n.Table != nil {
				// FROM or IN
				ctx.WriteKeyWord(" IN ")
				if err := n.Table.Restore(ctx); err != nil {
					return errors.Annotate(err, "An error occurred while resotre ShowStmt.Table")
				}
			}
			prettyShowDatabaseNameOpt()
		case ShowWarnings:
			ctx.WriteKeyWord("WARNINGS")
		case ShowErrors:
			ctx.WriteKeyWord("ERRORS")
		case ShowVariables:
			prettyGlobalScope()
			ctx.WriteKeyWord("VARIABLES")
		case ShowStatus:
			prettyGlobalScope()
			ctx.WriteKeyWord("STATUS")
		case ShowCollation:
			ctx.WriteKeyWord("COLLATION")
		case ShowTriggers:
			ctx.WriteKeyWord("TRIGGERS")
			prettyShowDatabaseNameOpt()
		case ShowProcedureStatus:
			ctx.WriteKeyWord("PROCEDURE STATUS")
		case ShowEvents:
			ctx.WriteKeyWord("EVENTS")
			prettyShowDatabaseNameOpt()
		case ShowPlugins:
			ctx.WriteKeyWord("PLUGINS")
		case ShowBindings:
			if n.GlobalScope {
				ctx.WriteKeyWord("GLOBAL ")
			} else {
				ctx.WriteKeyWord("SESSION ")
			}
			ctx.WriteKeyWord("BINDINGS")
		case ShowPumpStatus:
			ctx.WriteKeyWord("PUMP STATUS")
		case ShowDrainerStatus:
			ctx.WriteKeyWord("DRAINER STATUS")
		case ShowAnalyzeStatus:
			ctx.WriteKeyWord("ANALYZE STATUS")
		case ShowRegions:
			ctx.WriteKeyWord("TABLE ")
			if err := n.Table.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore SplitIndexRegionStmt.Table")
			}
			if len(n.IndexName.L) > 0 {
				ctx.WriteKeyWord(" INDEX ")
				ctx.WriteName(n.IndexName.String())
			}
			ctx.WriteKeyWord(" REGIONS")
			if err := prettyShowLikeOrWhereOpt(); err != nil {
				return err
			}
			return nil
		case ShowTableNextRowId:
			ctx.WriteKeyWord("TABLE ")
			if err := n.Table.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore SplitIndexRegionStmt.Table")
			}
			ctx.WriteKeyWord(" NEXT_ROW_ID")
			return nil
		case ShowBackups:
			ctx.WriteKeyWord("BACKUPS")
		case ShowRestores:
			ctx.WriteKeyWord("RESTORES")
		case ShowImports:
			ctx.WriteKeyWord("IMPORTS")
		default:
			return errors.New("Unknown ShowStmt type")
		}
		prettyShowLikeOrWhereOpt()
	}
	return nil
}

func (n *WindowSpec) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if name := n.Name.String(); name != "" {
		ctx.WriteName(name)
		if n.OnlyAlias {
			return nil
		}
		ctx.WriteKeyWord(" AS ")
	}
	ctx.WritePlain("(")
	sep := ""
	if refName := n.Ref.String(); refName != "" {
		ctx.WriteName(refName)
		sep = " "
	}
	if n.PartitionBy != nil {
		ctx.WritePlain(sep)
		if err := n.PartitionBy.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore WindowSpec.PartitionBy")
		}
		sep = " "
	}
	if n.OrderBy != nil {
		ctx.WritePlain(sep)
		if err := n.OrderBy.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore WindowSpec.OrderBy")
		}
		sep = " "
	}
	if n.Frame != nil {
		ctx.WritePlain(sep)
		if err := n.Frame.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore WindowSpec.Frame")
		}
	}
	ctx.WritePlain(")")

	return nil
}

func (n *SelectIntoOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Tp != SelectIntoOutfile {
		// only support SELECT ... INTO OUTFILE now
		return errors.New("Unsupported SelectionInto type")
	}

	ctx.WriteKeyWord("INTO OUTFILE ")
	ctx.WriteString(n.FileName)
	if n.FieldsInfo != nil {
		if err := n.FieldsInfo.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectInto.FieldsInfo")
		}
	}
	if n.LinesInfo != nil {
		if err := n.LinesInfo.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SelectInto.LinesInfo")
		}
	}
	return nil
}

func (n *PartitionByClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("PARTITION BY ")
	for i, v := range n.Items {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore PartitionByClause.Items[%d]", i)
		}
	}
	return nil
}

func (n *FrameClause) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Type {
	case Rows:
		ctx.WriteKeyWord("ROWS")
	case Ranges:
		ctx.WriteKeyWord("RANGE")
	default:
		return errors.New("Unsupported window function frame type")
	}
	ctx.WriteKeyWord(" BETWEEN ")
	if err := n.Extent.Start.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore FrameClause.Extent.Start")
	}
	ctx.WriteKeyWord(" AND ")
	if err := n.Extent.End.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore FrameClause.Extent.End")
	}

	return nil
}

func (n *FrameBound) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.UnBounded {
		ctx.WriteKeyWord("UNBOUNDED")
	}
	switch n.Type {
	case CurrentRow:
		ctx.WriteKeyWord("CURRENT ROW")
	case Preceding, Following:
		if n.Unit != TimeUnitInvalid {
			ctx.WriteKeyWord("INTERVAL ")
		}
		if n.Expr != nil {
			if err := n.Expr.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore FrameBound.Expr")
			}
		}
		if n.Unit != TimeUnitInvalid {
			ctx.WritePlain(" ")
			ctx.WriteKeyWord(n.Unit.String())
		}
		if n.Type == Preceding {
			ctx.WriteKeyWord(" PRECEDING")
		} else {
			ctx.WriteKeyWord(" FOLLOWING")
		}
	}
	return nil
}

func (n *SplitRegionStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("SPLIT ")
	if n.SplitSyntaxOpt != nil {
		if n.SplitSyntaxOpt.HasRegionFor {
			ctx.WriteKeyWord("REGION FOR ")
		}
		if n.SplitSyntaxOpt.HasPartition {
			ctx.WriteKeyWord("PARTITION ")

		}
	}
	ctx.WriteKeyWord("TABLE ")

	if err := n.Table.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore SplitIndexRegionStmt.Table")
	}
	if len(n.PartitionNames) > 0 {
		ctx.WriteKeyWord(" PARTITION")
		ctx.WritePlain("(")
		for i, v := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(v.String())
		}
		ctx.WritePlain(")")
	}

	if len(n.IndexName.L) > 0 {
		ctx.WriteKeyWord(" INDEX ")
		ctx.WriteName(n.IndexName.String())
	}
	ctx.WritePlain(" ")
	err := n.SplitOpt.Pretty(ctx, level, indent, char)
	return err
}

func (n *SplitOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if len(n.ValueLists) == 0 {
		ctx.WriteKeyWord("BETWEEN ")
		ctx.WritePlain("(")
		for j, v := range n.Lower {
			if j != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore SplitOption Lower")
			}
		}
		ctx.WritePlain(")")

		ctx.WriteKeyWord(" AND ")
		ctx.WritePlain("(")
		for j, v := range n.Upper {
			if j != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore SplitOption Upper")
			}
		}
		ctx.WritePlain(")")
		ctx.WriteKeyWord(" REGIONS")
		ctx.WritePlainf(" %d", n.Num)
		return nil
	}
	ctx.WriteKeyWord("BY ")
	for i, row := range n.ValueLists {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WritePlain("(")
		for j, v := range row {
			if j != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore SplitOption.ValueLists[%d][%d]", i, j)
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}
