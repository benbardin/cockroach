// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "fmt"

// AlterBackupSchedule represents an ALTER BACKUP SCHEDULE statement.
type AlterBackupSchedule struct {
	ScheduleID int64
	Cmds       AlterBackupScheduleCmds
}

var _ Statement = &AlterBackupSchedule{}

// Format implements the NodeFormatter interface.
func (node *AlterBackupSchedule) Format(ctx *FmtCtx) {
	ctx.WriteString(`ALTER BACKUP SCHEDULE `)
	ctx.WriteString(fmt.Sprintf(" %d ", node.ScheduleID))
	ctx.FormatNode(&node.Cmds)
}

// AlterBackupScheduleCmds represents a list of changefeed alterations
type AlterBackupScheduleCmds []AlterBackupScheduleCmd

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleCmds) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(" ")
		}
		ctx.FormatNode(n)
	}
}

// AlterBackupScheduleCmd represents a changefeed modification operation.
type AlterBackupScheduleCmd interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types
	// (AlterBackupSchedule*) conform to the AlterBackupScheduleCmd interface.
	alterBackupScheduleCmd()
}

func (*AlterBackupScheduleSetLabel) alterBackupScheduleCmd()            {}
func (*AlterBackupScheduleSetInto) alterBackupScheduleCmd()             {}
func (*AlterBackupScheduleSetWith) alterBackupScheduleCmd()             {}
func (*AlterBackupScheduleUnsetWith) alterBackupScheduleCmd()           {}
func (*AlterBackupScheduleSetRecurring) alterBackupScheduleCmd()        {}
func (*AlterBackupScheduleSetFullBackup) alterBackupScheduleCmd()       {}
func (*AlterBackupScheduleSetScheduleOption) alterBackupScheduleCmd()   {}
func (*AlterBackupScheduleUnsetScheduleOption) alterBackupScheduleCmd() {}

var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetLabel{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetInto{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetWith{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleUnsetWith{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetRecurring{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetFullBackup{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetScheduleOption{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleUnsetScheduleOption{}

// AlterBackupScheduleSetLabel represents an ADD <label> command
type AlterBackupScheduleSetLabel struct {
	Label Expr
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetLabel) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET LABEL ")
	ctx.FormatNode(node.Label)
}

// AlterBackupScheduleSetInto represents a SET <destinations> command
type AlterBackupScheduleSetInto struct {
	Into StringOrPlaceholderOptList
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetInto) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET INTO ")
	ctx.FormatNode(&node.Into)
}

// AlterBackupScheduleSetWith represents an SET <options> command
type AlterBackupScheduleSetWith struct {
	With *BackupOptions
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetWith) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET WITH ")
	ctx.FormatNode(node.With)
}

// AlterBackupScheduleUnsetWith represents an UNSET <options> command
type AlterBackupScheduleUnsetWith struct {
	With *BackupOptions
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleUnsetWith) Format(ctx *FmtCtx) {
	ctx.WriteString(" UNSET ")
	// TODO(bardin): Can we print only keys here without being too verbose?
	ctx.FormatNode(node.With)
}

// AlterBackupScheduleSetRecurring represents an SET RECURRING <recurrence> command
type AlterBackupScheduleSetRecurring struct {
	Recurrence Expr
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetRecurring) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET RECURRING ")
	if node.Recurrence == nil {
		ctx.WriteString("NEVER")
	} else {
		ctx.FormatNode(node.Recurrence)
	}
}

// AlterBackupScheduleSetFullBackup represents an SET FULL BACKUP <recurrence> command
type AlterBackupScheduleSetFullBackup struct {
	FullBackup FullBackupClause
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetFullBackup) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET FULL BACKUP ")
	if node.FullBackup.AlwaysFull {
		ctx.WriteString("ALWAYS")
	} else {
		ctx.FormatNode(node.FullBackup.Recurrence)
	}
}

// AlterBackupScheduleSetScheduleOption represents an SET SCHEDULE OPTION <kv_options> command
type AlterBackupScheduleSetScheduleOption struct {
	Option KVOption
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetScheduleOption) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET SCHEDULE OPTION ")
	ctx.WriteString(fmt.Sprintf("%s=%s", node.Option.Key, node.Option.Value))
}

// AlterBackupScheduleUnsetScheduleOption represents an UNSET SCHEDULE OPTION <option> command
type AlterBackupScheduleUnsetScheduleOption struct {
	Key Name
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleUnsetScheduleOption) Format(ctx *FmtCtx) {
	ctx.WriteString(" UNSET SCHEDULE OPTION ")
	ctx.FormatNode(&node.Key)
}
