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
	"github.com/daiguadaidai/parser/model"
	"github.com/daiguadaidai/parser/types"
	"github.com/daiguadaidai/parser/utils"
	"github.com/pingcap/errors"
)

func (n *DatabaseOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Tp {
	case DatabaseOptionCharset:
		ctx.WriteKeyWord("CHARACTER SET")
		ctx.WritePlain(" = ")
		ctx.WritePlain(n.Value)
	case DatabaseOptionCollate:
		ctx.WriteKeyWord("COLLATE")
		ctx.WritePlain(" = ")
		ctx.WritePlain(n.Value)
	case DatabaseOptionEncryption:
		ctx.WriteKeyWord("ENCRYPTION")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.Value)
	default:
		return errors.Errorf("invalid DatabaseOptionType: %d", n.Tp)
	}
	return nil
}

func (n *CreateDatabaseStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("CREATE DATABASE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.Name)
	for i, option := range n.Options {
		ctx.WritePlain(" ")
		err := option.Pretty(ctx, level, indent, char)
		if err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreateDatabaseStmt DatabaseOption: [%v]", i)
		}
	}
	return nil
}

func (n *AlterDatabaseStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ALTER DATABASE")
	if !n.AlterDefaultDatabase {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Name)
	}
	for i, option := range n.Options {
		ctx.WritePlain(" ")
		err := option.Pretty(ctx, level, indent, char)
		if err != nil {
			return errors.Annotatef(err, "An error occurred while splicing AlterDatabaseStmt DatabaseOption: [%v]", i)
		}
	}
	return nil
}

func (n *DropDatabaseStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("DROP DATABASE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.Name)
	return nil
}

func (n *IndexPartSpecification) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Expr != nil {
		ctx.WritePlain("(")
		if err := n.Expr.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing IndexPartSpecifications")
		}
		ctx.WritePlain(")")
		return nil
	}
	if err := n.Column.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while splicing IndexPartSpecifications")
	}
	if n.Length > 0 {
		ctx.WritePlainf("(%d)", n.Length)
	}
	return nil
}

func (n *ReferenceDef) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Table != nil {
		ctx.WriteKeyWord("REFERENCES ")
		if err := n.Table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ReferenceDef")
		}
	}

	if n.IndexPartSpecifications != nil {
		ctx.WritePlain("(")
		for i, indexColNames := range n.IndexPartSpecifications {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			if err := indexColNames.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing IndexPartSpecifications: [%v]", i)
			}
		}
		ctx.WritePlain(")")
	}

	if n.Match != MatchNone {
		ctx.WriteKeyWord(" MATCH ")
		switch n.Match {
		case MatchFull:
			ctx.WriteKeyWord("FULL")
		case MatchPartial:
			ctx.WriteKeyWord("PARTIAL")
		case MatchSimple:
			ctx.WriteKeyWord("SIMPLE")
		}
	}
	if n.OnDelete.ReferOpt != ReferOptionNoOption {
		ctx.WritePlain(" ")
		if err := n.OnDelete.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing OnDelete")
		}
	}
	if n.OnUpdate.ReferOpt != ReferOptionNoOption {
		ctx.WritePlain(" ")
		if err := n.OnUpdate.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing OnUpdate")
		}
	}
	return nil
}

func (n *OnDeleteOpt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.ReferOpt != ReferOptionNoOption {
		ctx.WriteKeyWord("ON DELETE ")
		ctx.WriteKeyWord(n.ReferOpt.String())
	}
	return nil
}

func (n *OnUpdateOpt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.ReferOpt != ReferOptionNoOption {
		ctx.WriteKeyWord("ON UPDATE ")
		ctx.WriteKeyWord(n.ReferOpt.String())
	}
	return nil
}

func (n *ColumnOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Tp {
	case ColumnOptionNoOption:
		return nil
	case ColumnOptionPrimaryKey:
		ctx.WriteKeyWord("PRIMARY KEY")
	case ColumnOptionNotNull:
		ctx.WriteKeyWord("NOT NULL")
	case ColumnOptionAutoIncrement:
		ctx.WriteKeyWord("AUTO_INCREMENT")
	case ColumnOptionDefaultValue:
		ctx.WriteKeyWord("DEFAULT ")
		if err := n.Expr.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption DefaultValue Expr")
		}
	case ColumnOptionUniqKey:
		ctx.WriteKeyWord("UNIQUE KEY")
	case ColumnOptionNull:
		ctx.WriteKeyWord("NULL")
	case ColumnOptionOnUpdate:
		ctx.WriteKeyWord("ON UPDATE ")
		if err := n.Expr.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption ON UPDATE Expr")
		}
	case ColumnOptionFulltext:
		return errors.New("TiDB Parser ignore the `ColumnOptionFulltext` type now")
	case ColumnOptionComment:
		ctx.WriteKeyWord("COMMENT ")
		if err := n.Expr.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption COMMENT Expr")
		}
	case ColumnOptionGenerated:
		ctx.WriteKeyWord("GENERATED ALWAYS AS")
		ctx.WritePlain("(")
		if err := n.Expr.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption GENERATED ALWAYS Expr")
		}
		ctx.WritePlain(")")
		if n.Stored {
			ctx.WriteKeyWord(" STORED")
		} else {
			ctx.WriteKeyWord(" VIRTUAL")
		}
	case ColumnOptionReference:
		if err := n.Refer.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption ReferenceDef")
		}
	case ColumnOptionCollate:
		if n.StrValue == "" {
			return errors.New("Empty ColumnOption COLLATE")
		}
		ctx.WriteKeyWord("COLLATE ")
		ctx.WritePlain(n.StrValue)
	case ColumnOptionCheck:
		ctx.WriteKeyWord("CHECK")
		ctx.WritePlain("(")
		if err := n.Expr.Pretty(ctx, level, indent, char); err != nil {
			return errors.Trace(err)
		}
		ctx.WritePlain(")")
		if n.Enforced {
			ctx.WriteKeyWord(" ENFORCED")
		} else {
			ctx.WriteKeyWord(" NOT ENFORCED")
		}
	case ColumnOptionColumnFormat:
		ctx.WriteKeyWord("COLUMN_FORMAT ")
		ctx.WriteKeyWord(n.StrValue)
	case ColumnOptionStorage:
		ctx.WriteKeyWord("STORAGE ")
		ctx.WriteKeyWord(n.StrValue)
	case ColumnOptionAutoRandom:
		ctx.WriteKeyWord("AUTO_RANDOM")
		if n.AutoRandomBitLength != types.UnspecifiedLength {
			ctx.WritePlainf("(%d)", n.AutoRandomBitLength)
		}
	default:
		return errors.New("An error occurred while splicing ColumnOption")
	}
	return nil
}

func (n *IndexOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	hasPrevOption := false
	if n.KeyBlockSize > 0 {
		ctx.WriteKeyWord("KEY_BLOCK_SIZE")
		ctx.WritePlainf("=%d", n.KeyBlockSize)
		hasPrevOption = true
	}

	if n.Tp != model.IndexTypeInvalid {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("USING ")
		ctx.WritePlain(n.Tp.String())
		hasPrevOption = true
	}

	if len(n.ParserName.O) > 0 {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("WITH PARSER ")
		ctx.WriteName(n.ParserName.O)
		hasPrevOption = true
	}

	if n.Comment != "" {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("COMMENT ")
		ctx.WriteString(n.Comment)
		hasPrevOption = true
	}

	if n.Visibility != IndexVisibilityDefault {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		switch n.Visibility {
		case IndexVisibilityVisible:
			ctx.WriteKeyWord("VISIBLE")
		case IndexVisibilityInvisible:
			ctx.WriteKeyWord("INVISIBLE")
		}
	}
	return nil
}

func (n *Constraint) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Tp {
	case ConstraintNoConstraint:
		return nil
	case ConstraintPrimaryKey:
		ctx.WriteKeyWord("PRIMARY KEY")
	case ConstraintKey:
		ctx.WriteKeyWord("KEY")
		if n.IfNotExists {
			ctx.WriteKeyWord(" IF NOT EXISTS")
		}
	case ConstraintIndex:
		ctx.WriteKeyWord("INDEX")
		if n.IfNotExists {
			ctx.WriteKeyWord(" IF NOT EXISTS")
		}
	case ConstraintUniq:
		ctx.WriteKeyWord("UNIQUE")
	case ConstraintUniqKey:
		ctx.WriteKeyWord("UNIQUE KEY")
	case ConstraintUniqIndex:
		ctx.WriteKeyWord("UNIQUE INDEX")
	case ConstraintFulltext:
		ctx.WriteKeyWord("FULLTEXT")
	case ConstraintCheck:
		if n.Name != "" {
			ctx.WriteKeyWord("CONSTRAINT ")
			ctx.WriteName(n.Name)
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("CHECK")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Trace(err)
		}
		ctx.WritePlain(") ")
		if n.Enforced {
			ctx.WriteKeyWord("ENFORCED")
		} else {
			ctx.WriteKeyWord("NOT ENFORCED")
		}
		return nil
	}

	if n.Tp == ConstraintForeignKey {
		ctx.WriteKeyWord("CONSTRAINT ")
		if n.Name != "" {
			ctx.WriteName(n.Name)
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("FOREIGN KEY ")
		if n.IfNotExists {
			ctx.WriteKeyWord("IF NOT EXISTS ")
		}
	} else if n.Name != "" {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Name)
	}

	ctx.WritePlain("(")
	for i, keys := range n.Keys {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if err := keys.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing Constraint Keys: [%v]", i)
		}
	}
	ctx.WritePlain(")")

	if n.Refer != nil {
		ctx.WritePlain(" ")
		if err := n.Refer.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing Constraint Refer")
		}
	}

	if n.Option != nil {
		ctx.WritePlain(" ")
		if err := n.Option.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing Constraint Option")
		}
	}

	return nil
}

func (n *ColumnDef) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Name.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while splicing ColumnDef Name")
	}
	if n.Tp != nil {
		ctx.WritePlain(" ")
		if err := n.Tp.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnDef Type")
		}
	}
	for i, options := range n.Options {
		ctx.WritePlain(" ")
		if err := options.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing ColumnDef ColumnOption: [%v]", i)
		}
	}
	return nil
}

func (n *CreateTableStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.IsTemporary {
		ctx.WriteKeyWord("CREATE TEMPORARY TABLE ")
	} else {
		ctx.WriteKeyWord("CREATE TABLE ")
	}
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}

	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while splicing CreateTableStmt Table")
	}
	ctx.WritePlain(" ")
	if n.ReferTable != nil {
		ctx.WriteKeyWord("LIKE ")
		if err := n.ReferTable.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while splicing CreateTableStmt ReferTable")
		}
	}
	lenCols := len(n.Cols)
	lenConstraints := len(n.Constraints)
	if lenCols+lenConstraints > 0 {
		ctx.WritePlain("(\n")
		for i, col := range n.Cols {
			ctx.WritePlain(utils.GetIndent(level, indent, " "))
			if i > 0 {
				ctx.WritePlain(",\n")
			}
			if err := col.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt ColumnDef: [%v]", i)
			}
		}
		for i, constraint := range n.Constraints {
			if i > 0 || lenCols >= 1 {
				ctx.WritePlain(",\n")
			}
			if err := constraint.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt Constraints: [%v]", i)
			}
		}
		ctx.WritePlain("\n)")
	}

	for i, option := range n.Options {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt TableOption: [%v]", i)
		}
	}

	if n.Partition != nil {
		ctx.WritePlain(" ")
		if err := n.Partition.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing CreateTableStmt Partition")
		}
	}

	if n.Select != nil {
		switch n.OnDuplicate {
		case OnDuplicateKeyHandlingError:
			ctx.WriteKeyWord(" AS ")
		case OnDuplicateKeyHandlingIgnore:
			ctx.WriteKeyWord(" IGNORE AS ")
		case OnDuplicateKeyHandlingReplace:
			ctx.WriteKeyWord(" REPLACE AS ")
		}

		if err := n.Select.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing CreateTableStmt Select")
		}
	}

	return nil
}

func (n *DropTableStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.IsView {
		ctx.WriteKeyWord("DROP VIEW ")
	} else {
		if n.IsTemporary {
			ctx.WriteKeyWord("DROP TEMPORARY TABLE ")
		} else {
			ctx.WriteKeyWord("DROP TABLE ")
		}
	}
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}

	for index, table := range n.Tables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore DropTableStmt.Tables "+string(index))
		}
	}

	return nil
}

func (n *DropSequenceStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("DROP SEQUENCE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	for i, sequence := range n.Sequences {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := sequence.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DropSequenceStmt.Sequences[%d]", i)
		}
	}

	return nil
}

func (n *RenameTableStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("RENAME TABLE ")
	for index, table2table := range n.TableToTables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table2table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore RenameTableStmt.TableToTables")
		}
	}
	return nil
}

func (n *TableToTable) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.OldTable.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore TableToTable.OldTable")
	}
	ctx.WriteKeyWord(" TO ")
	if err := n.NewTable.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore TableToTable.NewTable")
	}
	return nil
}

func (n *CreateViewStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	ctx.WriteKeyWord("ALGORITHM")
	ctx.WritePlain(" = ")
	ctx.WriteKeyWord(n.Algorithm.String())
	ctx.WriteKeyWord(" DEFINER")
	ctx.WritePlain(" = ")

	// todo Use n.Definer.Restore(ctx) to replace this part
	if n.Definer.CurrentUser {
		ctx.WriteKeyWord("current_user")
	} else {
		ctx.WriteName(n.Definer.Username)
		if n.Definer.Hostname != "" {
			ctx.WritePlain("@")
			ctx.WriteName(n.Definer.Hostname)
		}
	}

	ctx.WriteKeyWord(" SQL SECURITY ")
	ctx.WriteKeyWord(n.Security.String())
	ctx.WriteKeyWord(" VIEW ")

	if err := n.ViewName.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while create CreateViewStmt.ViewName")
	}

	for i, col := range n.Cols {
		if i == 0 {
			ctx.WritePlain(" (")
		} else {
			ctx.WritePlain(",")
		}
		ctx.WriteName(col.O)
		if i == len(n.Cols)-1 {
			ctx.WritePlain(")")
		}
	}

	ctx.WriteKeyWord(" AS ")

	if err := n.Select.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while create CreateViewStmt.Select")
	}

	if n.CheckOption != model.CheckOptionCascaded {
		ctx.WriteKeyWord(" WITH ")
		ctx.WriteKeyWord(n.CheckOption.String())
		ctx.WriteKeyWord(" CHECK OPTION")
	}
	return nil
}

func (n *CreateSequenceStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("CREATE ")
	ctx.WriteKeyWord("SEQUENCE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while create CreateSequenceStmt.Name")
	}
	for i, option := range n.SeqOptions {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreateSequenceStmt SequenceOption: [%v]", i)
		}
	}
	for i, option := range n.TblOptions {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreateSequenceStmt TableOption: [%v]", i)
		}
	}
	return nil
}

func (n *IndexLockAndAlgorithm) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	hasPrevOption := false
	if n.AlgorithmTp != AlgorithmTypeDefault {
		ctx.WriteKeyWord("ALGORITHM")
		ctx.WritePlain(" = ")
		ctx.WriteKeyWord(n.AlgorithmTp.String())
		hasPrevOption = true
	}

	if n.LockTp != LockTypeDefault {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("LOCK")
		ctx.WritePlain(" = ")
		ctx.WriteKeyWord(n.LockTp.String())
	}
	return nil
}

func (n *CreateIndexStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("CREATE ")
	switch n.KeyType {
	case IndexKeyTypeUnique:
		ctx.WriteKeyWord("UNIQUE ")
	case IndexKeyTypeSpatial:
		ctx.WriteKeyWord("SPATIAL ")
	case IndexKeyTypeFullText:
		ctx.WriteKeyWord("FULLTEXT ")
	}
	ctx.WriteKeyWord("INDEX ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.IndexName)
	ctx.WriteKeyWord(" ON ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateIndexStmt.Table")
	}

	ctx.WritePlain(" (")
	for i, indexColName := range n.IndexPartSpecifications {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := indexColName.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateIndexStmt.IndexPartSpecifications: [%v]", i)
		}
	}
	ctx.WritePlain(")")

	if n.IndexOption.Tp != model.IndexTypeInvalid || n.IndexOption.KeyBlockSize > 0 || n.IndexOption.Comment != "" || len(n.IndexOption.ParserName.O) > 0 || n.IndexOption.Visibility != IndexVisibilityDefault {
		ctx.WritePlain(" ")
		if err := n.IndexOption.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CreateIndexStmt.IndexOption")
		}
	}

	if n.LockAlg != nil {
		ctx.WritePlain(" ")
		if err := n.LockAlg.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CreateIndexStmt.LockAlg")
		}
	}

	return nil
}

func (n *DropIndexStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("DROP INDEX ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.IndexName)
	ctx.WriteKeyWord(" ON ")

	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while add index")
	}

	if n.LockAlg != nil {
		ctx.WritePlain(" ")
		if err := n.LockAlg.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CreateIndexStmt.LockAlg")
		}
	}

	return nil
}

func (n *LockTablesStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("LOCK TABLES ")
	for i, tl := range n.TableLocks {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := tl.Table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while add index")
		}
		ctx.WriteKeyWord(" " + tl.Type.String())
	}
	return nil
}

func (n *UnlockTablesStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("UNLOCK TABLES")
	return nil
}

func (n *CleanupTableLockStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ADMIN CLEANUP TABLE LOCK ")
	for i, v := range n.Tables {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CleanupTableLockStmt.Tables[%d]", i)
		}
	}
	return nil
}

func (n *RepairTableStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ADMIN REPAIR TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotatef(err, "An error occurred while restore RepairTableStmt.table : [%v]", n.Table)
	}
	ctx.WritePlain(" ")
	if err := n.CreateStmt.Restore(ctx); err != nil {
		return errors.Annotatef(err, "An error occurred while restore RepairTableStmt.createStmt : [%v]", n.CreateStmt)
	}
	return nil
}

func (n *TableOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Tp {
	case TableOptionEngine:
		ctx.WriteKeyWord("ENGINE ")
		ctx.WritePlain("= ")
		if n.StrValue != "" {
			ctx.WritePlain(n.StrValue)
		} else {
			ctx.WritePlain("''")
		}
	case TableOptionCharset:
		if n.UintValue == TableOptionCharsetWithConvertTo {
			ctx.WriteKeyWord("CONVERT TO ")
		} else {
			ctx.WriteKeyWord("DEFAULT ")
		}
		ctx.WriteKeyWord("CHARACTER SET ")
		if n.UintValue == TableOptionCharsetWithoutConvertTo {
			ctx.WriteKeyWord("= ")
		}
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WriteKeyWord(n.StrValue)
		}
	case TableOptionCollate:
		ctx.WriteKeyWord("DEFAULT COLLATE ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.StrValue)
	case TableOptionAutoIncrement:
		ctx.WriteKeyWord("AUTO_INCREMENT ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionAutoIdCache:
		ctx.WriteKeyWord("AUTO_ID_CACHE ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionAutoRandomBase:
		ctx.WriteKeyWord("AUTO_RANDOM_BASE ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionComment:
		ctx.WriteKeyWord("COMMENT ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionAvgRowLength:
		ctx.WriteKeyWord("AVG_ROW_LENGTH ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionCheckSum:
		ctx.WriteKeyWord("CHECKSUM ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionCompression:
		ctx.WriteKeyWord("COMPRESSION ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionConnection:
		ctx.WriteKeyWord("CONNECTION ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionPassword:
		ctx.WriteKeyWord("PASSWORD ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionKeyBlockSize:
		ctx.WriteKeyWord("KEY_BLOCK_SIZE ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionMaxRows:
		ctx.WriteKeyWord("MAX_ROWS ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionMinRows:
		ctx.WriteKeyWord("MIN_ROWS ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionDelayKeyWrite:
		ctx.WriteKeyWord("DELAY_KEY_WRITE ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionRowFormat:
		ctx.WriteKeyWord("ROW_FORMAT ")
		ctx.WritePlain("= ")
		switch n.UintValue {
		case RowFormatDefault:
			ctx.WriteKeyWord("DEFAULT")
		case RowFormatDynamic:
			ctx.WriteKeyWord("DYNAMIC")
		case RowFormatFixed:
			ctx.WriteKeyWord("FIXED")
		case RowFormatCompressed:
			ctx.WriteKeyWord("COMPRESSED")
		case RowFormatRedundant:
			ctx.WriteKeyWord("REDUNDANT")
		case RowFormatCompact:
			ctx.WriteKeyWord("COMPACT")
		case TokuDBRowFormatDefault:
			ctx.WriteKeyWord("TOKUDB_DEFAULT")
		case TokuDBRowFormatFast:
			ctx.WriteKeyWord("TOKUDB_FAST")
		case TokuDBRowFormatSmall:
			ctx.WriteKeyWord("TOKUDB_SMALL")
		case TokuDBRowFormatZlib:
			ctx.WriteKeyWord("TOKUDB_ZLIB")
		case TokuDBRowFormatQuickLZ:
			ctx.WriteKeyWord("TOKUDB_QUICKLZ")
		case TokuDBRowFormatLzma:
			ctx.WriteKeyWord("TOKUDB_LZMA")
		case TokuDBRowFormatSnappy:
			ctx.WriteKeyWord("TOKUDB_SNAPPY")
		case TokuDBRowFormatUncompressed:
			ctx.WriteKeyWord("TOKUDB_UNCOMPRESSED")
		default:
			return errors.Errorf("invalid TableOption: TableOptionRowFormat: %d", n.UintValue)
		}
	case TableOptionStatsPersistent:
		// TODO: not support
		ctx.WriteKeyWord("STATS_PERSISTENT ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord("DEFAULT")
		ctx.WritePlain(" /* TableOptionStatsPersistent is not supported */ ")
	case TableOptionStatsAutoRecalc:
		ctx.WriteKeyWord("STATS_AUTO_RECALC ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	case TableOptionShardRowID:
		ctx.WriteKeyWord("SHARD_ROW_ID_BITS ")
		ctx.WritePlainf("= %d", n.UintValue)
	case TableOptionPreSplitRegion:
		ctx.WriteKeyWord("PRE_SPLIT_REGIONS ")
		ctx.WritePlainf("= %d", n.UintValue)
	case TableOptionPackKeys:
		// TODO: not support
		ctx.WriteKeyWord("PACK_KEYS ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord("DEFAULT")
		ctx.WritePlain(" /* TableOptionPackKeys is not supported */ ")
	case TableOptionTablespace:
		ctx.WriteKeyWord("TABLESPACE ")
		ctx.WritePlain("= ")
		ctx.WriteName(n.StrValue)
	case TableOptionNodegroup:
		ctx.WriteKeyWord("NODEGROUP ")
		ctx.WritePlainf("= %d", n.UintValue)
	case TableOptionDataDirectory:
		ctx.WriteKeyWord("DATA DIRECTORY ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionIndexDirectory:
		ctx.WriteKeyWord("INDEX DIRECTORY ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionStorageMedia:
		ctx.WriteKeyWord("STORAGE ")
		ctx.WriteKeyWord(n.StrValue)
	case TableOptionStatsSamplePages:
		ctx.WriteKeyWord("STATS_SAMPLE_PAGES ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	case TableOptionSecondaryEngine:
		ctx.WriteKeyWord("SECONDARY_ENGINE ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionSecondaryEngineNull:
		ctx.WriteKeyWord("SECONDARY_ENGINE ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord("NULL")
	case TableOptionInsertMethod:
		ctx.WriteKeyWord("INSERT_METHOD ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionTableCheckSum:
		ctx.WriteKeyWord("TABLE_CHECKSUM ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionUnion:
		ctx.WriteKeyWord("UNION ")
		ctx.WritePlain("= (")
		for i, tableName := range n.TableNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			tableName.Restore(ctx)
		}
		ctx.WritePlain(")")
	case TableOptionEncryption:
		ctx.WriteKeyWord("ENCRYPTION ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	default:
		return errors.Errorf("invalid TableOption: %d", n.Tp)
	}
	return nil
}

func (n *SequenceOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Tp {
	case SequenceOptionIncrementBy:
		ctx.WriteKeyWord("INCREMENT BY ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceStartWith:
		ctx.WriteKeyWord("START WITH ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceNoMinValue:
		ctx.WriteKeyWord("NO MINVALUE")
	case SequenceMinValue:
		ctx.WriteKeyWord("MINVALUE ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceNoMaxValue:
		ctx.WriteKeyWord("NO MAXVALUE")
	case SequenceMaxValue:
		ctx.WriteKeyWord("MAXVALUE ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceNoCache:
		ctx.WriteKeyWord("NOCACHE")
	case SequenceCache:
		ctx.WriteKeyWord("CACHE ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceNoCycle:
		ctx.WriteKeyWord("NOCYCLE")
	case SequenceCycle:
		ctx.WriteKeyWord("CYCLE")
	default:
		return errors.Errorf("invalid SequenceOption: %d", n.Tp)
	}
	return nil
}

func (n *ColumnPosition) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Tp {
	case ColumnPositionNone:
		// do nothing
	case ColumnPositionFirst:
		ctx.WriteKeyWord("FIRST")
	case ColumnPositionAfter:
		ctx.WriteKeyWord("AFTER ")
		if err := n.RelativeColumn.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore ColumnPosition.RelativeColumn")
		}
	default:
		return errors.Errorf("invalid ColumnPositionType: %d", n.Tp)
	}
	return nil
}

func (n *AlterOrderItem) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.Column.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterOrderItem.Column")
	}
	if n.Desc {
		ctx.WriteKeyWord(" DESC")
	}
	return nil
}

func (n *AlterTableSpec) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Tp {
	case AlterTableSetTiFlashReplica:
		ctx.WriteKeyWord("SET TIFLASH REPLICA ")
		ctx.WritePlainf("%d", n.TiFlashReplica.Count)
		if len(n.TiFlashReplica.Labels) == 0 {
			break
		}
		ctx.WriteKeyWord(" LOCATION LABELS ")
		for i, v := range n.TiFlashReplica.Labels {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteString(v)
		}
	case AlterTableOption:
		switch {
		case len(n.Options) == 2 && n.Options[0].Tp == TableOptionCharset && n.Options[1].Tp == TableOptionCollate:
			if n.Options[0].UintValue == TableOptionCharsetWithConvertTo {
				ctx.WriteKeyWord("CONVERT TO ")
			}
			ctx.WriteKeyWord("CHARACTER SET ")
			if n.Options[0].Default {
				ctx.WriteKeyWord("DEFAULT")
			} else {
				ctx.WriteKeyWord(n.Options[0].StrValue)
			}
			ctx.WriteKeyWord(" COLLATE ")
			ctx.WriteKeyWord(n.Options[1].StrValue)
		case n.Options[0].Tp == TableOptionCharset && n.Options[0].Default:
			if n.Options[0].UintValue == TableOptionCharsetWithConvertTo {
				ctx.WriteKeyWord("CONVERT TO ")
			}
			ctx.WriteKeyWord("CHARACTER SET DEFAULT")
		default:
			for i, opt := range n.Options {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := opt.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.Options[%d]", i)
				}
			}
		}
	case AlterTableAddColumns:
		ctx.WriteKeyWord("ADD COLUMN ")
		if n.IfNotExists {
			ctx.WriteKeyWord("IF NOT EXISTS ")
		}
		if n.Position != nil && len(n.NewColumns) == 1 {
			if err := n.NewColumns[0].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.NewColumns[%d]", 0)
			}
			if n.Position.Tp != ColumnPositionNone {
				ctx.WritePlain(" ")
			}
			if err := n.Position.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Position")
			}
		} else {
			lenCols := len(n.NewColumns)
			ctx.WritePlain("(")
			for i, col := range n.NewColumns {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := col.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.NewColumns[%d]", i)
				}
			}
			for i, constraint := range n.NewConstraints {
				if i != 0 || lenCols >= 1 {
					ctx.WritePlain(", ")
				}
				if err := constraint.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.NewConstraints[%d]", i)
				}
			}
			ctx.WritePlain(")")
		}
	case AlterTableAddConstraint:
		ctx.WriteKeyWord("ADD ")
		if err := n.Constraint.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Constraint")
		}
	case AlterTableDropColumn:
		ctx.WriteKeyWord("DROP COLUMN ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.OldColumnName")
		}
	// TODO: RestrictOrCascadeOpt not support
	case AlterTableDropPrimaryKey:
		ctx.WriteKeyWord("DROP PRIMARY KEY")
	case AlterTableDropIndex:
		ctx.WriteKeyWord("DROP INDEX ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		ctx.WriteName(n.Name)
	case AlterTableDropForeignKey:
		ctx.WriteKeyWord("DROP FOREIGN KEY ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		ctx.WriteName(n.Name)
	case AlterTableModifyColumn:
		ctx.WriteKeyWord("MODIFY COLUMN ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		if err := n.NewColumns[0].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0]")
		}
		if n.Position.Tp != ColumnPositionNone {
			ctx.WritePlain(" ")
		}
		if err := n.Position.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Position")
		}
	case AlterTableChangeColumn:
		ctx.WriteKeyWord("CHANGE COLUMN ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.OldColumnName")
		}
		ctx.WritePlain(" ")
		if err := n.NewColumns[0].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0]")
		}
		if n.Position.Tp != ColumnPositionNone {
			ctx.WritePlain(" ")
		}
		if err := n.Position.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Position")
		}
	case AlterTableRenameColumn:
		ctx.WriteKeyWord("RENAME COLUMN ")
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.OldColumnName")
		}
		ctx.WriteKeyWord(" TO ")
		if err := n.NewColumnName.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumnName")
		}
	case AlterTableRenameTable:
		ctx.WriteKeyWord("RENAME AS ")
		if err := n.NewTable.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewTable")
		}
	case AlterTableAlterColumn:
		ctx.WriteKeyWord("ALTER COLUMN ")
		if err := n.NewColumns[0].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0]")
		}
		if len(n.NewColumns[0].Options) == 1 {
			ctx.WriteKeyWord("SET DEFAULT ")
			expr := n.NewColumns[0].Options[0].Expr
			if valueExpr, ok := expr.(ValueExpr); ok {
				if err := valueExpr.Restore(ctx); err != nil {
					return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0].Options[0].Expr")
				}
			} else {
				ctx.WritePlain("(")
				if err := expr.Restore(ctx); err != nil {
					return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0].Options[0].Expr")
				}
				ctx.WritePlain(")")
			}
		} else {
			ctx.WriteKeyWord(" DROP DEFAULT")
		}
	case AlterTableLock:
		ctx.WriteKeyWord("LOCK ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.LockType.String())
	case AlterTableOrderByColumns:
		ctx.WriteKeyWord("ORDER BY ")
		for i, alterOrderItem := range n.OrderByList {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := alterOrderItem.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.OrderByList[%d]", i)
			}
		}
	case AlterTableAlgorithm:
		ctx.WriteKeyWord("ALGORITHM ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.Algorithm.String())
	case AlterTableRenameIndex:
		ctx.WriteKeyWord("RENAME INDEX ")
		ctx.WriteName(n.FromKey.O)
		ctx.WriteKeyWord(" TO ")
		ctx.WriteName(n.ToKey.O)
	case AlterTableForce:
		// TODO: not support
		ctx.WriteKeyWord("FORCE")
		ctx.WritePlain(" /* AlterTableForce is not supported */ ")
	case AlterTableAddPartitions:
		ctx.WriteKeyWord("ADD PARTITION")
		if n.IfNotExists {
			ctx.WriteKeyWord(" IF NOT EXISTS")
		}
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord(" NO_WRITE_TO_BINLOG")
		}
		if n.PartDefinitions != nil {
			ctx.WritePlain(" (")
			for i, def := range n.PartDefinitions {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := def.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.PartDefinitions[%d]", i)
				}
			}
			ctx.WritePlain(")")
		} else if n.Num != 0 {
			ctx.WriteKeyWord(" PARTITIONS ")
			ctx.WritePlainf("%d", n.Num)
		}
	case AlterTableCoalescePartitions:
		ctx.WriteKeyWord("COALESCE PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		ctx.WritePlainf("%d", n.Num)
	case AlterTableDropPartition:
		ctx.WriteKeyWord("DROP PARTITION ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableTruncatePartition:
		ctx.WriteKeyWord("TRUNCATE PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableCheckPartitions:
		ctx.WriteKeyWord("CHECK PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableOptimizePartition:
		ctx.WriteKeyWord("OPTIMIZE PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableRepairPartition:
		ctx.WriteKeyWord("REPAIR PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableImportPartitionTablespace:
		ctx.WriteKeyWord("IMPORT PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
		} else {
			for i, name := range n.PartitionNames {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WriteName(name.O)
			}
		}
		ctx.WriteKeyWord(" TABLESPACE")
	case AlterTableDiscardPartitionTablespace:
		ctx.WriteKeyWord("DISCARD PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
		} else {
			for i, name := range n.PartitionNames {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WriteName(name.O)
			}
		}
		ctx.WriteKeyWord(" TABLESPACE")
	case AlterTablePartition:
		if err := n.Partition.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Partition")
		}
	case AlterTableEnableKeys:
		ctx.WriteKeyWord("ENABLE KEYS")
	case AlterTableDisableKeys:
		ctx.WriteKeyWord("DISABLE KEYS")
	case AlterTableRemovePartitioning:
		ctx.WriteKeyWord("REMOVE PARTITIONING")
	case AlterTableWithValidation:
		ctx.WriteKeyWord("WITH VALIDATION")
	case AlterTableWithoutValidation:
		ctx.WriteKeyWord("WITHOUT VALIDATION")
	case AlterTableRebuildPartition:
		ctx.WriteKeyWord("REBUILD PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableReorganizePartition:
		ctx.WriteKeyWord("REORGANIZE PARTITION")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord(" NO_WRITE_TO_BINLOG")
		}
		if n.OnAllPartitions {
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			} else {
				ctx.WritePlain(" ")
			}
			ctx.WriteName(name.O)
		}
		ctx.WriteKeyWord(" INTO ")
		if n.PartDefinitions != nil {
			ctx.WritePlain("(")
			for i, def := range n.PartDefinitions {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := def.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.PartDefinitions[%d]", i)
				}
			}
			ctx.WritePlain(")")
		}
	case AlterTableExchangePartition:
		ctx.WriteKeyWord("EXCHANGE PARTITION ")
		ctx.WriteName(n.PartitionNames[0].O)
		ctx.WriteKeyWord(" WITH TABLE ")
		n.NewTable.Restore(ctx)
		if !n.WithValidation {
			ctx.WriteKeyWord(" WITHOUT VALIDATION")
		}
	case AlterTableSecondaryLoad:
		ctx.WriteKeyWord("SECONDARY_LOAD")
	case AlterTableSecondaryUnload:
		ctx.WriteKeyWord("SECONDARY_UNLOAD")
	case AlterTableAlterCheck:
		ctx.WriteKeyWord("ALTER CHECK ")
		ctx.WriteName(n.Constraint.Name)
		if !n.Constraint.Enforced {
			ctx.WriteKeyWord(" NOT")
		}
		ctx.WriteKeyWord(" ENFORCED")
	case AlterTableDropCheck:
		ctx.WriteKeyWord("DROP CHECK ")
		ctx.WriteName(n.Constraint.Name)
	case AlterTableImportTablespace:
		ctx.WriteKeyWord("IMPORT TABLESPACE")
	case AlterTableDiscardTablespace:
		ctx.WriteKeyWord("DISCARD TABLESPACE")
	case AlterTableIndexInvisible:
		ctx.WriteKeyWord("ALTER INDEX ")
		ctx.WriteName(n.IndexName.O)
		switch n.Visibility {
		case IndexVisibilityVisible:
			ctx.WriteKeyWord(" VISIBLE")
		case IndexVisibilityInvisible:
			ctx.WriteKeyWord(" INVISIBLE")
		}
	default:
		// TODO: not support
		ctx.WritePlainf(" /* AlterTableType(%d) is not supported */ ", n.Tp)
	}
	return nil
}

func (n *AlterTableStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ALTER TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterTableStmt.Table")
	}
	for i, spec := range n.Specs {
		if i == 0 || spec.Tp == AlterTablePartition || spec.Tp == AlterTableRemovePartitioning || spec.Tp == AlterTableImportTablespace || spec.Tp == AlterTableDiscardTablespace {
			ctx.WritePlain(" ")
		} else {
			ctx.WritePlain(", ")
		}
		if err := spec.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterTableStmt.Specs[%d]", i)
		}
	}
	return nil
}

func (n *TruncateTableStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("TRUNCATE TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore TruncateTableStmt.Table")
	}
	return nil
}

func (spd *SubPartitionDefinition) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("SUBPARTITION ")
	ctx.WriteName(spd.Name.O)
	for i, opt := range spd.Options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore SubPartitionDefinition.Options[%d]", i)
		}
	}
	return nil
}

func (n *PartitionDefinitionClauseNone) pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	return nil
}

func (n *PartitionDefinitionClauseLessThan) pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord(" VALUES LESS THAN ")
	ctx.WritePlain("(")
	for i, expr := range n.Exprs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore PartitionDefinitionClauseLessThan.Exprs[%d]", i)
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *PartitionDefinitionClauseIn) pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	// we special-case an empty list of values to mean MariaDB's "DEFAULT" clause.
	if len(n.Values) == 0 {
		ctx.WriteKeyWord(" DEFAULT")
		return nil
	}

	ctx.WriteKeyWord(" VALUES IN ")
	ctx.WritePlain("(")
	for i, valList := range n.Values {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if len(valList) == 1 {
			if err := valList[0].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PartitionDefinitionClauseIn.Values[%d][0]", i)
			}
		} else {
			ctx.WritePlain("(")
			for j, val := range valList {
				if j != 0 {
					ctx.WritePlain(", ")
				}
				if err := val.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore PartitionDefinitionClauseIn.Values[%d][%d]", i, j)
				}
			}
			ctx.WritePlain(")")
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *PartitionDefinitionClauseHistory) pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Current {
		ctx.WriteKeyWord(" CURRENT")
	} else {
		ctx.WriteKeyWord(" HISTORY")
	}
	return nil
}

func (n *PartitionDefinition) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("PARTITION ")
	ctx.WriteName(n.Name.O)

	if err := n.Clause.restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PartitionDefinition.Clause")
	}

	for i, opt := range n.Options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore PartitionDefinition.Options[%d]", i)
		}
	}

	if len(n.Sub) > 0 {
		ctx.WritePlain(" (")
		for i, spd := range n.Sub {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := spd.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PartitionDefinition.Sub[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

func (n *PartitionMethod) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Linear {
		ctx.WriteKeyWord("LINEAR ")
	}
	ctx.WriteKeyWord(n.Tp.String())

	switch {
	case n.Tp == model.PartitionTypeSystemTime:
		if n.Expr != nil && n.Unit != TimeUnitInvalid {
			ctx.WriteKeyWord(" INTERVAL ")
			if err := n.Expr.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore PartitionMethod.Expr")
			}
			ctx.WritePlain(" ")
			ctx.WriteKeyWord(n.Unit.String())
		}

	case n.Expr != nil:
		ctx.WritePlain(" (")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PartitionMethod.Expr")
		}
		ctx.WritePlain(")")

	default:
		if n.Tp == model.PartitionTypeRange || n.Tp == model.PartitionTypeList {
			ctx.WriteKeyWord(" COLUMNS")
		}
		ctx.WritePlain(" (")
		for i, col := range n.ColumnNames {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := col.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing PartitionMethod.ColumnName[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}

	if n.Limit > 0 {
		ctx.WriteKeyWord(" LIMIT ")
		ctx.WritePlainf("%d", n.Limit)
	}

	return nil
}

func (n *PartitionOptions) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("PARTITION BY ")
	if err := n.PartitionMethod.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PartitionOptions.PartitionMethod")
	}

	if n.Num > 0 && len(n.Definitions) == 0 {
		ctx.WriteKeyWord(" PARTITIONS ")
		ctx.WritePlainf("%d", n.Num)
	}

	if n.Sub != nil {
		ctx.WriteKeyWord(" SUBPARTITION BY ")
		if err := n.Sub.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PartitionOptions.Sub")
		}
		if n.Sub.Num > 0 {
			ctx.WriteKeyWord(" SUBPARTITIONS ")
			ctx.WritePlainf("%d", n.Sub.Num)
		}
	}

	if len(n.Definitions) > 0 {
		ctx.WritePlain(" (")
		for i, def := range n.Definitions {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := def.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PartitionOptions.Definitions[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

func (n *RecoverTableStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("RECOVER TABLE ")
	if n.JobID != 0 {
		ctx.WriteKeyWord("BY JOB ")
		ctx.WritePlainf("%d", n.JobID)
	} else {
		if err := n.Table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing RecoverTableStmt Table")
		}
		if n.JobNum > 0 {
			ctx.WritePlainf(" %d", n.JobNum)
		}
	}
	return nil
}

func (n *FlashBackTableStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("FLASHBACK TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while splicing RecoverTableStmt Table")
	}
	if len(n.NewName) > 0 {
		ctx.WriteKeyWord(" TO ")
		ctx.WriteName(n.NewName)
	}
	return nil
}
