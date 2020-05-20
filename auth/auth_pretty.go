package auth

import (
	"github.com/daiguadaidai/parser/format"
)

func (user *UserIdentity) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	if user.CurrentUser {
		ctx.WriteKeyWord("CURRENT_USER")
	} else {
		ctx.WriteName(user.Username)
		if user.Hostname != "" {
			ctx.WritePlain("@")
			ctx.WriteName(user.Hostname)
		}
	}
	return nil
}

func (role *RoleIdentity) Pretty(ctx *format.RestoreCtx, level, indent int64, char string) error {
	ctx.WriteName(role.Username)
	if role.Hostname != "" {
		ctx.WritePlain("@")
		ctx.WriteName(role.Hostname)
	}
	return nil
}
