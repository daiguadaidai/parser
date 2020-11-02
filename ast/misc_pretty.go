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
	"github.com/daiguadaidai/parser/mysql"
	"github.com/pingcap/errors"
	"strconv"
)

func (n *AuthOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("IDENTIFIED BY ")
	if n.ByAuthString {
		ctx.WriteString(n.AuthString)
	} else {
		ctx.WriteKeyWord("PASSWORD ")
		ctx.WriteString(n.HashString)
	}
	return nil
}

func (n *TraceStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("TRACE ")
	if n.Format != "json" {
		ctx.WriteKeyWord("FORMAT")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.Format)
		ctx.WritePlain(" ")
	}
	if err := n.Stmt.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore TraceStmt.Stmt")
	}
	return nil
}

func (n *ExplainForStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("EXPLAIN ")
	ctx.WriteKeyWord("FORMAT ")
	ctx.WritePlain("= ")
	ctx.WriteString(n.Format)
	ctx.WritePlain(" ")
	ctx.WriteKeyWord("FOR ")
	ctx.WriteKeyWord("CONNECTION ")
	ctx.WritePlain(strconv.FormatUint(n.ConnectionID, 10))
	return nil
}

func (n *ExplainStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if showStmt, ok := n.Stmt.(*ShowStmt); ok {
		ctx.WriteKeyWord("DESC ")
		if err := showStmt.Table.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore ExplainStmt.ShowStmt.Table")
		}
		if showStmt.Column != nil {
			ctx.WritePlain(" ")
			if err := showStmt.Column.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore ExplainStmt.ShowStmt.Column")
			}
		}
		return nil
	}
	ctx.WriteKeyWord("EXPLAIN ")
	if n.Analyze {
		ctx.WriteKeyWord("ANALYZE ")
	} else {
		ctx.WriteKeyWord("FORMAT ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.Format)
		ctx.WritePlain(" ")
	}
	if err := n.Stmt.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore ExplainStmt.Stmt")
	}
	return nil
}

func (n *PrepareStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("PREPARE ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" FROM ")
	if n.SQLText != "" {
		ctx.WriteString(n.SQLText)
		return nil
	}
	if n.SQLVar != nil {
		if err := n.SQLVar.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore PrepareStmt.SQLVar")
		}
		return nil
	}
	return errors.New("An error occurred while restore PrepareStmt")
}

func (n *DeallocateStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("DEALLOCATE PREPARE ")
	ctx.WriteName(n.Name)
	return nil
}

func (n *ExecuteStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("EXECUTE ")
	ctx.WriteName(n.Name)
	if len(n.UsingVars) > 0 {
		ctx.WriteKeyWord(" USING ")
		for i, val := range n.UsingVars {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := val.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore ExecuteStmt.UsingVars index %d", i)
			}
		}
	}
	return nil
}

func (n *BeginStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Mode == "" {
		if n.ReadOnly {
			ctx.WriteKeyWord("START TRANSACTION READ ONLY")
			if n.Bound != nil {
				switch n.Bound.Mode {
				case TimestampBoundStrong:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND STRONG")
				case TimestampBoundMaxStaleness:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND MAX STALENESS ")
					return n.Bound.Timestamp.Pretty(ctx, level, indent, char)
				case TimestampBoundExactStaleness:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND EXACT STALENESS ")
					return n.Bound.Timestamp.Pretty(ctx, level, indent, char)
				case TimestampBoundReadTimestamp:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND READ TIMESTAMP ")
					return n.Bound.Timestamp.Pretty(ctx, level, indent, char)
				case TimestampBoundMinReadTimestamp:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND MIN READ TIMESTAMP ")
					return n.Bound.Timestamp.Pretty(ctx, level, indent, char)
				}
			}
		} else {
			ctx.WriteKeyWord("START TRANSACTION")
		}
	} else {
		ctx.WriteKeyWord("BEGIN ")
		ctx.WriteKeyWord(n.Mode)
	}
	return nil
}

func (n *BinlogStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("BINLOG ")
	ctx.WriteString(n.Str)
	return nil
}

func (n CompletionType) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n {
	case CompletionTypeDefault:
		break
	case CompletionTypeChain:
		ctx.WriteKeyWord(" AND CHAIN")
	case CompletionTypeRelease:
		ctx.WriteKeyWord(" RELEASE")
	}
	return nil
}

func (n *CommitStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("COMMIT")
	if err := n.CompletionType.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore CommitStmt.CompletionType")
	}
	return nil
}

func (n *RollbackStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ROLLBACK")
	if err := n.CompletionType.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore RollbackStmt.CompletionType")
	}
	return nil
}

func (n *UseStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("USE ")
	ctx.WriteName(n.DBName)
	return nil
}

func (n *VariableAssignment) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.IsSystem {
		ctx.WritePlain("@@")
		if n.IsGlobal {
			ctx.WriteKeyWord("GLOBAL")
		} else {
			ctx.WriteKeyWord("SESSION")
		}
		ctx.WritePlain(".")
	} else if n.Name != SetNames && n.Name != SetCharset {
		ctx.WriteKeyWord("@")
	}
	if n.Name == SetNames {
		ctx.WriteKeyWord("NAMES ")
	} else if n.Name == SetCharset {
		ctx.WriteKeyWord("CHARSET ")
	} else {
		ctx.WriteName(n.Name)
		ctx.WritePlain("=")
	}
	if err := n.Value.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore VariableAssignment.Value")
	}
	if n.ExtendValue != nil {
		ctx.WriteKeyWord(" COLLATE ")
		if err := n.ExtendValue.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore VariableAssignment.ExtendValue")
		}
	}
	return nil
}

func (n *FlushStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("FLUSH ")
	if n.NoWriteToBinLog {
		ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
	}
	switch n.Tp {
	case FlushTables:
		ctx.WriteKeyWord("TABLES")
		for i, v := range n.Tables {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FlushStmt.Tables[%d]", i)
			}
		}
		if n.ReadLock {
			ctx.WriteKeyWord(" WITH READ LOCK")
		}
	case FlushPrivileges:
		ctx.WriteKeyWord("PRIVILEGES")
	case FlushStatus:
		ctx.WriteKeyWord("STATUS")
	case FlushTiDBPlugin:
		ctx.WriteKeyWord("TIDB PLUGINS")
		for i, v := range n.Plugins {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			ctx.WritePlain(v)
		}
	case FlushHosts:
		ctx.WriteKeyWord("HOSTS")
	case FlushLogs:
		var logType string
		switch n.LogType {
		case LogTypeDefault:
			logType = "LOGS"
		case LogTypeBinary:
			logType = "BINARY LOGS"
		case LogTypeEngine:
			logType = "ENGINE LOGS"
		case LogTypeError:
			logType = "ERROR LOGS"
		case LogTypeGeneral:
			logType = "GENERAL LOGS"
		case LogTypeSlow:
			logType = "SLOW LOGS"
		}
		ctx.WriteKeyWord(logType)
	default:
		return errors.New("Unsupported type of FlushStmt")
	}
	return nil
}

func (n *KillStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("KILL")
	if n.TiDBExtension {
		ctx.WriteKeyWord(" TIDB")
	}
	if n.Query {
		ctx.WriteKeyWord(" QUERY")
	}
	ctx.WritePlainf(" %d", n.ConnectionID)
	return nil
}

func (n *SetStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("SET ")
	for i, v := range n.Variables {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore SetStmt.Variables[%d]", i)
		}
	}
	return nil
}

func (n *SetConfigStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("SET CONFIG ")
	if n.Type != "" {
		ctx.WriteKeyWord(n.Type)
	} else {
		ctx.WriteString(n.Instance)
	}
	ctx.WritePlain(" ")
	ctx.WriteKeyWord(n.Name)
	ctx.WritePlain(" = ")
	return n.Value.Pretty(ctx, level, indent, char)
}

func (n *SetPwdStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("SET PASSWORD")
	if n.User != nil {
		ctx.WriteKeyWord(" FOR ")
		if err := n.User.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore SetPwdStmt.User")
		}
	}
	ctx.WritePlain("=")
	ctx.WriteString(n.Password)
	return nil
}

func (n *ChangeStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("CHANGE ")
	ctx.WriteKeyWord(n.NodeType)
	ctx.WriteKeyWord(" TO NODE_STATE ")
	ctx.WritePlain("=")
	ctx.WriteString(n.State)
	ctx.WriteKeyWord(" FOR NODE_ID ")
	ctx.WriteString(n.NodeID)
	return nil
}

func (n *SetRoleStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("SET ROLE")
	switch n.SetRoleOpt {
	case SetRoleDefault:
		ctx.WriteKeyWord(" DEFAULT")
	case SetRoleNone:
		ctx.WriteKeyWord(" NONE")
	case SetRoleAll:
		ctx.WriteKeyWord(" ALL")
	case SetRoleAllExcept:
		ctx.WriteKeyWord(" ALL EXCEPT")
	}
	for i, role := range n.RoleList {
		ctx.WritePlain(" ")
		err := role.Pretty(ctx, level, indent, char)
		if err != nil {
			return errors.Annotate(err, "An error occurred while restore SetRoleStmt.RoleList")
		}
		if i != len(n.RoleList)-1 {
			ctx.WritePlain(",")
		}
	}
	return nil
}

func (n *SetDefaultRoleStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("SET DEFAULT ROLE")
	switch n.SetRoleOpt {
	case SetRoleNone:
		ctx.WriteKeyWord(" NONE")
	case SetRoleAll:
		ctx.WriteKeyWord(" ALL")
	default:
	}
	for i, role := range n.RoleList {
		ctx.WritePlain(" ")
		err := role.Pretty(ctx, level, indent, char)
		if err != nil {
			return errors.Annotate(err, "An error occurred while restore SetDefaultRoleStmt.RoleList")
		}
		if i != len(n.RoleList)-1 {
			ctx.WritePlain(",")
		}
	}
	ctx.WritePlain(" TO")
	for i, user := range n.UserList {
		ctx.WritePlain(" ")
		err := user.Pretty(ctx, indent, indent, char)
		if err != nil {
			return errors.Annotate(err, "An error occurred while restore SetDefaultRoleStmt.UserList")
		}
		if i != len(n.UserList)-1 {
			ctx.WritePlain(",")
		}
	}
	return nil
}

func (n *UserSpec) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if err := n.User.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore UserSpec.User")
	}
	if n.AuthOpt != nil {
		ctx.WritePlain(" ")
		if err := n.AuthOpt.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore UserSpec.AuthOpt")
		}
	}
	return nil
}

func (t *TLSOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch t.Type {
	case TslNone:
		ctx.WriteKeyWord("NONE")
	case Ssl:
		ctx.WriteKeyWord("SSL")
	case X509:
		ctx.WriteKeyWord("X509")
	case Cipher:
		ctx.WriteKeyWord("CIPHER ")
		ctx.WriteString(t.Value)
	case Issuer:
		ctx.WriteKeyWord("ISSUER ")
		ctx.WriteString(t.Value)
	case SAN:
		ctx.WriteKeyWord("SAN ")
		ctx.WriteString(t.Value)
	case Subject:
		ctx.WriteKeyWord("SUBJECT ")
		ctx.WriteString(t.Value)
	default:
		return errors.Errorf("Unsupported TLSOption.Type %d", t.Type)
	}
	return nil
}

func (r *ResourceOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch r.Type {
	case MaxQueriesPerHour:
		ctx.WriteKeyWord("MAX_QUERIES_PER_HOUR ")
	case MaxUpdatesPerHour:
		ctx.WriteKeyWord("MAX_UPDATES_PER_HOUR ")
	case MaxConnectionsPerHour:
		ctx.WriteKeyWord("MAX_CONNECTIONS_PER_HOUR ")
	case MaxUserConnections:
		ctx.WriteKeyWord("MAX_USER_CONNECTIONS ")
	default:
		return errors.Errorf("Unsupported ResourceOption.Type %d", r.Type)
	}
	ctx.WritePlainf("%d", r.Count)
	return nil
}

func (p *PasswordOrLockOption) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch p.Type {
	case PasswordExpire:
		ctx.WriteKeyWord("PASSWORD EXPIRE")
	case PasswordExpireDefault:
		ctx.WriteKeyWord("PASSWORD EXPIRE DEFAULT")
	case PasswordExpireNever:
		ctx.WriteKeyWord("PASSWORD EXPIRE NEVER")
	case PasswordExpireInterval:
		ctx.WriteKeyWord("PASSWORD EXPIRE INTERVAL")
		ctx.WritePlainf(" %d", p.Count)
		ctx.WriteKeyWord(" DAY")
	case Lock:
		ctx.WriteKeyWord("ACCOUNT LOCK")
	case Unlock:
		ctx.WriteKeyWord("ACCOUNT UNLOCK")
	default:
		return errors.Errorf("Unsupported PasswordOrLockOption.Type %d", p.Type)
	}
	return nil
}

func (n *CreateUserStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.IsCreateRole {
		ctx.WriteKeyWord("CREATE ROLE ")
	} else {
		ctx.WriteKeyWord("CREATE USER ")
	}
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	for i, v := range n.Specs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.Specs[%d]", i)
		}
	}

	if len(n.TLSOptions) != 0 {
		ctx.WriteKeyWord(" REQUIRE ")
	}

	for i, option := range n.TLSOptions {
		if i != 0 {
			ctx.WriteKeyWord(" AND ")
		}
		if err := option.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.TLSOptions[%d]", i)
		}
	}

	if len(n.ResourceOptions) != 0 {
		ctx.WriteKeyWord(" WITH")
	}

	for i, v := range n.ResourceOptions {
		ctx.WritePlain(" ")
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.ResourceOptions[%d]", i)
		}
	}

	for i, v := range n.PasswordOrLockOptions {
		ctx.WritePlain(" ")
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.PasswordOrLockOptions[%d]", i)
		}
	}
	return nil
}

func (n *AlterUserStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ALTER USER ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	if n.CurrentAuth != nil {
		ctx.WriteKeyWord("USER")
		ctx.WritePlain("() ")
		if err := n.CurrentAuth.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterUserStmt.CurrentAuth")
		}
	}
	for i, v := range n.Specs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.Specs[%d]", i)
		}
	}

	if len(n.TLSOptions) != 0 {
		ctx.WriteKeyWord(" REQUIRE ")
	}

	for i, option := range n.TLSOptions {
		if i != 0 {
			ctx.WriteKeyWord(" AND ")
		}
		if err := option.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.TLSOptions[%d]", i)
		}
	}

	if len(n.ResourceOptions) != 0 {
		ctx.WriteKeyWord(" WITH")
	}

	for i, v := range n.ResourceOptions {
		ctx.WritePlain(" ")
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.ResourceOptions[%d]", i)
		}
	}

	for i, v := range n.PasswordOrLockOptions {
		ctx.WritePlain(" ")
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.PasswordOrLockOptions[%d]", i)
		}
	}
	return nil
}

func (n *AlterInstanceStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("ALTER INSTANCE")
	if n.ReloadTLS {
		ctx.WriteKeyWord(" RELOAD TLS")
	}
	if n.NoRollbackOnError {
		ctx.WriteKeyWord(" NO ROLLBACK ON ERROR")
	}
	return nil
}

func (n *DropUserStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.IsDropRole {
		ctx.WriteKeyWord("DROP ROLE ")
	} else {
		ctx.WriteKeyWord("DROP USER ")
	}
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	for i, v := range n.UserList {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DropUserStmt.UserList[%d]", i)
		}
	}
	return nil
}

func (n *CreateBindingStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("CREATE ")
	if n.GlobalScope {
		ctx.WriteKeyWord("GLOBAL ")
	} else {
		ctx.WriteKeyWord("SESSION ")
	}
	ctx.WriteKeyWord("BINDING FOR ")
	if err := n.OriginNode.Pretty(ctx, level, indent, char); err != nil {
		return errors.Trace(err)
	}
	ctx.WriteKeyWord(" USING ")
	if err := n.HintedNode.Pretty(ctx, level, indent, char); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (n *DropBindingStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("DROP ")
	if n.GlobalScope {
		ctx.WriteKeyWord("GLOBAL ")
	} else {
		ctx.WriteKeyWord("SESSION ")
	}
	ctx.WriteKeyWord("BINDING FOR ")
	if err := n.OriginNode.Pretty(ctx, level, indent, char); err != nil {
		return errors.Trace(err)
	}
	if n.HintedNode != nil {
		ctx.WriteKeyWord(" USING ")
		if err := n.HintedNode.Pretty(ctx, level, indent, char); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Restore implements Node interface.
func (n *CreateStatisticsStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("CREATE STATISTICS ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.StatsName)
	switch n.StatsType {
	case StatsTypeCardinality:
		ctx.WriteKeyWord(" (cardinality) ")
	case StatsTypeDependency:
		ctx.WriteKeyWord(" (dependency) ")
	case StatsTypeCorrelation:
		ctx.WriteKeyWord(" (correlation) ")
	}
	ctx.WriteKeyWord("ON ")
	if err := n.Table.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateStatisticsStmt.Table")
	}

	ctx.WritePlain("(")
	for i, col := range n.Columns {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := col.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateStatisticsStmt.Columns: [%v]", i)
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Restore implements Node interface.
func (n *DropStatisticsStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("DROP STATISTICS ")
	ctx.WriteName(n.StatsName)
	return nil
}

func (n *DoStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("DO ")
	for i, v := range n.Exprs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DoStmt.Exprs[%d]", i)
		}
	}
	return nil
}

func (n *ShowSlow) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Tp {
	case ShowSlowRecent:
		ctx.WriteKeyWord("RECENT ")
	case ShowSlowTop:
		ctx.WriteKeyWord("TOP ")
		switch n.Kind {
		case ShowSlowKindDefault:
			// do nothing
		case ShowSlowKindInternal:
			ctx.WriteKeyWord("INTERNAL ")
		case ShowSlowKindAll:
			ctx.WriteKeyWord("ALL ")
		default:
			return errors.New("Unsupported kind of ShowSlowTop")
		}
	default:
		return errors.New("Unsupported type of ShowSlow")
	}
	ctx.WritePlainf("%d", n.Count)
	return nil
}

func (n *AdminStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	restoreTables := func() error {
		for i, v := range n.Tables {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AdminStmt.Tables[%d]", i)
			}
		}
		return nil
	}
	restoreJobIDs := func() {
		for i, v := range n.JobIDs {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WritePlainf("%d", v)
		}
	}

	ctx.WriteKeyWord("ADMIN ")
	switch n.Tp {
	case AdminShowDDL:
		ctx.WriteKeyWord("SHOW DDL")
	case AdminShowDDLJobs:
		ctx.WriteKeyWord("SHOW DDL JOBS")
		if n.JobNumber != 0 {
			ctx.WritePlainf(" %d", n.JobNumber)
		}
		if n.Where != nil {
			ctx.WriteKeyWord(" WHERE ")
			if err := n.Where.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotate(err, "An error occurred while restore ShowStmt.Where")
			}
		}
	case AdminShowNextRowID:
		ctx.WriteKeyWord("SHOW ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WriteKeyWord(" NEXT_ROW_ID")
	case AdminCheckTable:
		ctx.WriteKeyWord("CHECK TABLE ")
		if err := restoreTables(); err != nil {
			return err
		}
	case AdminCheckIndex:
		ctx.WriteKeyWord("CHECK INDEX ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WritePlainf(" %s", n.Index)
	case AdminRecoverIndex:
		ctx.WriteKeyWord("RECOVER INDEX ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WritePlainf(" %s", n.Index)
	case AdminCleanupIndex:
		ctx.WriteKeyWord("CLEANUP INDEX ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WritePlainf(" %s", n.Index)
	case AdminCheckIndexRange:
		ctx.WriteKeyWord("CHECK INDEX ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WritePlainf(" %s", n.Index)
		if n.HandleRanges != nil {
			ctx.WritePlain(" ")
			for i, v := range n.HandleRanges {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				ctx.WritePlainf("(%d,%d)", v.Begin, v.End)
			}
		}
	case AdminChecksumTable:
		ctx.WriteKeyWord("CHECKSUM TABLE ")
		if err := restoreTables(); err != nil {
			return err
		}
	case AdminCancelDDLJobs:
		ctx.WriteKeyWord("CANCEL DDL JOBS ")
		restoreJobIDs()
	case AdminShowDDLJobQueries:
		ctx.WriteKeyWord("SHOW DDL JOB QUERIES ")
		restoreJobIDs()
	case AdminShowSlow:
		ctx.WriteKeyWord("SHOW SLOW ")
		if err := n.ShowSlow.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore AdminStmt.ShowSlow")
		}
	case AdminReloadExprPushdownBlacklist:
		ctx.WriteKeyWord("RELOAD EXPR_PUSHDOWN_BLACKLIST")
	case AdminReloadOptRuleBlacklist:
		ctx.WriteKeyWord("RELOAD OPT_RULE_BLACKLIST")
	case AdminPluginEnable:
		ctx.WriteKeyWord("PLUGINS ENABLE")
		for i, v := range n.Plugins {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			ctx.WritePlain(v)
		}
	case AdminPluginDisable:
		ctx.WriteKeyWord("PLUGINS DISABLE")
		for i, v := range n.Plugins {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			ctx.WritePlain(v)
		}
	case AdminFlushBindings:
		ctx.WriteKeyWord("FLUSH BINDINGS")
	case AdminCaptureBindings:
		ctx.WriteKeyWord("CAPTURE BINDINGS")
	case AdminEvolveBindings:
		ctx.WriteKeyWord("EVOLVE BINDINGS")
	case AdminReloadBindings:
		ctx.WriteKeyWord("RELOAD BINDINGS")
	default:
		return errors.New("Unsupported AdminStmt type")
	}
	return nil
}

func (n *PrivElem) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if n.Priv == 0 {
		ctx.WritePlain("/* UNSUPPORTED TYPE */")
	} else if n.Priv == mysql.AllPriv {
		ctx.WriteKeyWord("ALL")
	} else {
		str, ok := mysql.Priv2Str[n.Priv]
		if ok {
			ctx.WriteKeyWord(str)
		} else {
			return errors.New("Undefined privilege type")
		}
	}
	if n.Cols != nil {
		ctx.WritePlain(" (")
		for i, v := range n.Cols {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PrivElem.Cols[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

func (n ObjectTypeType) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n {
	case ObjectTypeNone:
		// do nothing
	case ObjectTypeTable:
		ctx.WriteKeyWord("TABLE")
	default:
		return errors.New("Unsupported object type")
	}
	return nil
}

func (n *GrantLevel) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	switch n.Level {
	case GrantLevelDB:
		if n.DBName == "" {
			ctx.WritePlain("*")
		} else {
			ctx.WriteName(n.DBName)
			ctx.WritePlain(".*")
		}
	case GrantLevelGlobal:
		ctx.WritePlain("*.*")
	case GrantLevelTable:
		if n.DBName != "" {
			ctx.WriteName(n.DBName)
			ctx.WritePlain(".")
		}
		ctx.WriteName(n.TableName)
	}
	return nil
}

func (n *RevokeStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("REVOKE ")
	for i, v := range n.Privs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore RevokeStmt.Privs[%d]", i)
		}
	}
	ctx.WriteKeyWord(" ON ")
	if n.ObjectType != ObjectTypeNone {
		if err := n.ObjectType.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore RevokeStmt.ObjectType")
		}
		ctx.WritePlain(" ")
	}
	if err := n.Level.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore RevokeStmt.Level")
	}
	ctx.WriteKeyWord(" FROM ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore RevokeStmt.Users[%d]", i)
		}
	}
	return nil
}

func (n *RevokeRoleStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("REVOKE ")
	for i, role := range n.Roles {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := role.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore RevokeRoleStmt.Roles[%d]", i)
		}
	}
	ctx.WriteKeyWord(" FROM ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore RevokeRoleStmt.Users[%d]", i)
		}
	}
	return nil
}

func (n *GrantStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("GRANT ")
	for i, v := range n.Privs {
		if i != 0 && v.Priv != 0 {
			ctx.WritePlain(", ")
		} else if v.Priv == 0 {
			ctx.WritePlain(" ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore GrantStmt.Privs[%d]", i)
		}
	}
	ctx.WriteKeyWord(" ON ")
	if n.ObjectType != ObjectTypeNone {
		if err := n.ObjectType.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotate(err, "An error occurred while restore GrantStmt.ObjectType")
		}
		ctx.WritePlain(" ")
	}
	if err := n.Level.Pretty(ctx, level, indent, char); err != nil {
		return errors.Annotate(err, "An error occurred while restore GrantStmt.Level")
	}
	ctx.WriteKeyWord(" TO ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore GrantStmt.Users[%d]", i)
		}
	}
	if n.TLSOptions != nil {
		if len(n.TLSOptions) != 0 {
			ctx.WriteKeyWord(" REQUIRE ")
		}
		for i, option := range n.TLSOptions {
			if i != 0 {
				ctx.WriteKeyWord(" AND ")
			}
			if err := option.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore GrantStmt.TLSOptions[%d]", i)
			}
		}
	}
	if n.WithGrant {
		ctx.WriteKeyWord(" WITH GRANT OPTION")
	}
	return nil
}

func (n *GrantRoleStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("GRANT ")
	if len(n.Roles) > 0 {
		for i, role := range n.Roles {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := role.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore GrantRoleStmt.Roles[%d]", i)
			}
		}
	}
	ctx.WriteKeyWord(" TO ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Pretty(ctx, level, indent, char); err != nil {
			return errors.Annotatef(err, "An error occurred while restore GrantStmt.Users[%d]", i)
		}
	}
	return nil
}

func (n *ShutdownStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord("SHUTDOWN")
	return nil
}

func (n *BRIEStmt) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord(n.Kind.String())

	switch {
	case len(n.Tables) != 0:
		ctx.WriteKeyWord(" TABLE ")
		for index, table := range n.Tables {
			if index != 0 {
				ctx.WritePlain(", ")
			}
			if err := table.Pretty(ctx, level, indent, char); err != nil {
				return errors.Annotatef(err, "An error occurred while restore BRIEStmt.Tables[%d]", index)
			}
		}
	case len(n.Schemas) != 0:
		ctx.WriteKeyWord(" DATABASE ")
		for index, schema := range n.Schemas {
			if index != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(schema)
		}
	default:
		ctx.WriteKeyWord(" DATABASE")
		ctx.WritePlain(" *")
	}

	switch n.Kind {
	case BRIEKindBackup:
		ctx.WriteKeyWord(" TO ")
	case BRIEKindRestore, BRIEKindImport:
		ctx.WriteKeyWord(" FROM ")
	}
	ctx.WriteString(n.Storage)

	for _, opt := range n.Options {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord(opt.Tp.String())
		ctx.WritePlain(" = ")
		switch opt.Tp {
		case BRIEOptionBackupTS, BRIEOptionLastBackupTS, BRIEOptionBackend, BRIEOptionOnDuplicate, BRIEOptionTiKVImporter, BRIEOptionCSVDelimiter, BRIEOptionCSVNull, BRIEOptionCSVSeparator:
			ctx.WriteString(opt.StrValue)
		case BRIEOptionBackupTimeAgo:
			ctx.WritePlainf("%d ", opt.UintValue/1000)
			ctx.WriteKeyWord("MICROSECOND AGO")
		case BRIEOptionRateLimit:
			ctx.WritePlainf("%d ", opt.UintValue/1048576)
			ctx.WriteKeyWord("MB")
			ctx.WritePlain("/")
			ctx.WriteKeyWord("SECOND")
		case BRIEOptionCSVHeader:
			if opt.UintValue == BRIECSVHeaderIsColumns {
				ctx.WriteKeyWord("COLUMNS")
			} else {
				ctx.WritePlainf("%d", opt.UintValue)
			}
		default:
			ctx.WritePlainf("%d", opt.UintValue)
		}
	}

	return nil
}

func (ht *HintTable) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) {
	if ht.DBName.L != "" {
		ctx.WriteName(ht.DBName.String())
		ctx.WriteKeyWord(".")
	}
	ctx.WriteName(ht.TableName.String())
	if ht.QBName.L != "" {
		ctx.WriteKeyWord("@")
		ctx.WriteName(ht.QBName.String())
	}
	if len(ht.PartitionList) > 0 {
		ctx.WriteKeyWord(" PARTITION")
		ctx.WritePlain("(")
		for i, p := range ht.PartitionList {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(p.String())
		}
		ctx.WritePlain(")")
	}
}

func (n *TableOptimizerHint) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteKeyWord(n.HintName.String())
	ctx.WritePlain("(")
	if n.QBName.L != "" {
		if n.HintName.L != "qb_name" {
			ctx.WriteKeyWord("@")
		}
		ctx.WriteName(n.QBName.String())
	}
	// Hints without args except query block.
	switch n.HintName.L {
	case "hash_agg", "stream_agg", "agg_to_cop", "read_consistent_replica", "no_index_merge", "qb_name", "ignore_plan_cache":
		ctx.WritePlain(")")
		return nil
	}
	if n.QBName.L != "" {
		ctx.WritePlain(" ")
	}
	// Hints with args except query block.
	switch n.HintName.L {
	case "max_execution_time":
		ctx.WritePlainf("%d", n.HintData.(uint64))
	case "nth_plan":
		ctx.WritePlainf("%d", n.HintData.(int64))
	case "tidb_hj", "tidb_smj", "tidb_inlj", "hash_join", "merge_join", "inl_join":
		for i, table := range n.Tables {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			table.Pretty(ctx, level, indent, char)
		}
	case "use_index", "ignore_index", "use_index_merge":
		n.Tables[0].Pretty(ctx, level, indent, char)
		ctx.WritePlain(" ")
		for i, index := range n.Indexes {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(index.String())
		}
	case "use_toja", "use_cascades":
		if n.HintData.(bool) {
			ctx.WritePlain("TRUE")
		} else {
			ctx.WritePlain("FALSE")
		}
	case "query_type":
		ctx.WriteKeyWord(n.HintData.(model.CIStr).String())
	case "memory_quota":
		ctx.WritePlainf("%d MB", n.HintData.(int64)/1024/1024)
	case "read_from_storage":
		ctx.WriteKeyWord(n.HintData.(model.CIStr).String())
		for i, table := range n.Tables {
			if i == 0 {
				ctx.WritePlain("[")
			}
			table.Pretty(ctx, level, indent, char)
			if i == len(n.Tables)-1 {
				ctx.WritePlain("]")
			} else {
				ctx.WritePlain(", ")
			}
		}
	case "time_range":
		hintData := n.HintData.(HintTimeRange)
		ctx.WriteString(hintData.From)
		ctx.WritePlain(", ")
		ctx.WriteString(hintData.To)
	}
	ctx.WritePlain(")")
	return nil
}
