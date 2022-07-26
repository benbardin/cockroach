// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/kr/pretty"
)

const alterBackupScheduleOp = "ALTER BACKUP SCHEDULE"

func singleInitializedField(obj interface{}) (string, error) {
	names := make([]string, 0)
	objValue := reflect.ValueOf(obj)
	if objValue.Kind() != reflect.Struct {
		// Programming defensively because reflection.
		// This shouldn't happen; its presence indicates developer error.
		return "", errors.Newf("Expected struct, got %s", objValue.Kind())
	}
	for i := 0; i < objValue.NumField(); i++ {
		field := objValue.Field(i)
		if field.IsZero() {
			continue
		}
		names = append(names, objValue.Type().Field(i).Name)
	}
	if len(names) != 1 {
		// Programming defensively because reflection.
		// This shouldn't happen; its presence indicates developer error.
		return "", errors.Newf("Expected 1 backup option, got %d: %s", len(names), names)
	}
	return names[0], nil
}

func validateAlterSchedule(alterSchedule *tree.AlterBackupSchedule) error {
	if alterSchedule.ScheduleID == 0 {
		return errors.Newf("Schedule ID expected, none found")
	}
	if len(alterSchedule.Cmds) == 0 {
		return errors.Newf("Found no attributes to alter")
	}
	cmdTypeHistogram := make(map[string]int)

	observe := func(tokens ...string) {
		key := strings.Join(tokens, ":")
		if _, ok := cmdTypeHistogram[key]; !ok {
			cmdTypeHistogram[key] = 0
		}
		cmdTypeHistogram[key] += 1
	}

	for _, cmd := range alterSchedule.Cmds {
		cmdType := reflect.ValueOf(cmd).Type().String()
		switch typedCmd := cmd.(type) {
		case *tree.AlterBackupScheduleSetWith:
			fieldName, err := singleInitializedField(*typedCmd.With)
			if err != nil {
				return err
			}
			observe(cmdType, fieldName)
		case *tree.AlterBackupScheduleUnsetWith:
			fieldName, err := singleInitializedField(*typedCmd.With)
			if err != nil {
				return err
			}
			observe(cmdType, fieldName)
		case *tree.AlterBackupScheduleSetScheduleOption:
			observe(cmdType, string(typedCmd.Option.Key))
		case *tree.AlterBackupScheduleUnsetScheduleOption:
			observe(cmdType, string(typedCmd.Key))
		default:
			observe(cmdType)
		}
	}

	for key := range cmdTypeHistogram {
		val := cmdTypeHistogram[key]
		if val > 1 {
			return errors.Newf("Observed %d instances of %s, expected at most one.", val, key)
		}
	}
	return nil
}

func findFullBackupClause(alterSchedule *tree.AlterBackupSchedule) *tree.FullBackupClause {
	for _, cmd := range alterSchedule.Cmds {
		fullBackupCmd, ok := cmd.(*tree.AlterBackupScheduleSetFullBackup)
		if !ok {
			continue
		}
		return &fullBackupCmd.FullBackup
	}
	return nil
}

func sortedClauses(cmds *tree.AlterBackupScheduleCmds) tree.AlterBackupScheduleCmds {
	retval := make(tree.AlterBackupScheduleCmds, len(*cmds))
	copy(retval, *cmds)
	for i, cmd := range retval {
		_, ok := cmd.(*tree.AlterBackupScheduleSetFullBackup)
		if !ok {
			continue
		}
		retval[0], retval[i] = retval[i], retval[0]
	}
	return retval
}

// doAlterBackupSchedule creates requested schedule (or schedules).
// It is a plan hook implementation responsible for the creating of scheduled backup.
func doAlterBackupSchedules(
	ctx context.Context,
	p sql.PlanHookState,
	alterSchedule *tree.AlterBackupSchedule,
	resultsCh chan<- tree.Datums,
) error {
	if err := p.RequireAdminRole(ctx, alterBackupScheduleOp); err != nil {
		return err
	}

	if err := validateAlterSchedule(alterSchedule); err != nil {
		return errors.Wrapf(err, "Invalid ALTER BACKUP command")
	}

	scheduleID := alterSchedule.ScheduleID
	_, _ = pretty.Println(alterSchedule)

	execCfg := p.ExecCfg()
	env := sql.JobSchedulerEnv(execCfg)

	schedule, err := jobs.LoadScheduledJob(ctx, env, scheduleID, execCfg.InternalExecutor, p.Txn())
	if err != nil {
		return err
	}

	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, args); err != nil {
		return errors.Wrap(err, "un-marshaling args")
	}
	node, err := parser.ParseOne(args.BackupStatement)
	if err != nil {
		return err
	}
	backupStmt, ok := node.AST.(*tree.Backup)
	if !ok {
		return errors.Newf("unexpected node type %T", node)
	}

	var dependentSchedule *jobs.ScheduledJob
	dependentArgs := &backuppb.ScheduledBackupExecutionArgs{}
	var dependentBackupStmt *tree.Backup
	if args.DependentScheduleID != 0 {
		dependentSchedule, err = jobs.LoadScheduledJob(ctx, env, args.DependentScheduleID, execCfg.InternalExecutor, p.Txn())
		if err != nil {
			return err
		}
		if err := pbtypes.UnmarshalAny(dependentSchedule.ExecutionArgs().Args, dependentArgs); err != nil {
			return errors.Wrap(err, "un-marshaling args")
		}
		node, err := parser.ParseOne(dependentArgs.BackupStatement)
		if err != nil {
			return err
		}
		dependentBackupStmt, ok = node.AST.(*tree.Backup)
		if !ok {
			return errors.Newf("unexpected node type %T", node)
		}
	}

	var fullJob, incJob *jobs.ScheduledJob
	var fullArgs, incArgs *backuppb.ScheduledBackupExecutionArgs
	var fullStmt, incStmt *tree.Backup
	if args.BackupType == backuppb.ScheduledBackupExecutionArgs_FULL {
		fullJob, fullArgs, fullStmt = schedule, args, backupStmt
		incJob, incArgs, incStmt = dependentSchedule, dependentArgs, dependentBackupStmt
	} else {
		fullJob, fullArgs, fullStmt = dependentSchedule, dependentArgs, dependentBackupStmt
		incJob, incArgs, incStmt = schedule, args, backupStmt
	}

	fmt.Println("^^^^^^^^^^^^^^^^^^^^^^^")
	_, _ = pretty.Println(fullJob)
	_, _ = pretty.Println(fullArgs)
	_, _ = pretty.Println(fullStmt)
	fmt.Println("-----------------------")
	_, _ = pretty.Println(incJob)
	_, _ = pretty.Println(incArgs)
	_, _ = pretty.Println(incStmt)
	fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$")

	fullBackupClause := findFullBackupClause(alterSchedule)
	ex := p.ExecCfg().InternalExecutor
	if fullBackupClause != nil && fullBackupClause.AlwaysFull && incJob != nil {
		if err := fullJob.SetSchedule(incJob.ScheduleExpr()); err != nil {
			return err
		}
		fullArgs.DependentScheduleID = 0
		if err := incJob.Delete(ctx, ex, p.Txn()); err != nil {
			return err
		}
	} else if fullBackupClause != nil && !fullBackupClause.AlwaysFull && incJob == nil {
		incStmt = &tree.Backup{}
		*incStmt = *fullStmt
		incStmt.AppendToLatest = true
		incRecurrence := &scheduleRecurrence{
			// No need to set frequency here. That's only used to guess a full
			// backup cadence, but we can only get here with an explicit full
			// cadence.
			cron: fullJob.ScheduleExpr(),
		}
		incJob, incArgs, err = makeBackupSchedule(
			env,
			p.User(),
			fullJob.ScheduleLabel(),
			incRecurrence,
			*fullJob.ScheduleDetails(),
			jobs.InvalidScheduleID,
			fullArgs.UpdatesLastBackupMetric,
			incStmt,
			fullArgs.ChainProtectedTimestampRecords,
		)

		if err != nil {
			return err
		}

		// Incremental is paused until FULL completes.
		incJob.Pause()
		incJob.SetScheduleStatus("Waiting for initial backup to complete")

		_, _ = pretty.Println(incArgs)

		if err := incJob.Create(ctx, ex, p.Txn()); err != nil {
			return err
		}

		// Update full recurrence, unpause ID
		fullRecurrenceFn, err := p.TypeAsString(ctx, fullBackupClause.Recurrence, alterBackupScheduleOp)
		if err != nil {
			return err
		}
		fullRecurrenceStr, err := fullRecurrenceFn()
		if err != nil {
			return err
		}
		if err := fullJob.SetSchedule(fullRecurrenceStr); err != nil {
			return err
		}
		fullArgs.UnpauseOnSuccess = incJob.ScheduleID()
	}

	fullAny, err := pbtypes.MarshalAny(fullArgs)
	if err != nil {
		return err
	}
	fullJob.SetExecutionDetails(schedule.ExecutorType(), jobspb.ExecutionArguments{Args: fullAny})

	incAny, err := pbtypes.MarshalAny(incArgs)
	if err != nil {
		return err
	}
	incJob.SetExecutionDetails(schedule.ExecutorType(), jobspb.ExecutionArguments{Args: incAny})

	// [DONE] Validate: No duplicate command-types
	// [DONE] Is new schedule incremental?
	// - Yes: If old schedule is full, add incremental
	// [DONE] - No:  If old schedule is inc, delete incremental.
	// Make changes
	// Verify backup
	// save + return

	return nil
	/*
		// Prepare backup statement (full).
		backupNode := &tree.Backup{
			Options: tree.BackupOptions{
				CaptureRevisionHistory: eval.BackupOptions.CaptureRevisionHistory,
				Detached:               true,
			},
			Nested:         true,
			AppendToLatest: false,
		}

		// Evaluate encryption passphrase if set.
		if eval.encryptionPassphrase != nil {
			pw, err := eval.encryptionPassphrase()
			if err != nil {
				return errors.Wrapf(err, "failed to evaluate backup encryption_passphrase")
			}
			backupNode.Options.EncryptionPassphrase = tree.NewStrVal(pw)
		}

		// Evaluate encryption KMS URIs if set.
		// Only one of encryption passphrase and KMS URI should be set, but this check
		// is done during backup planning so we do not need to worry about it here.
		var kmsURIs []string
		if eval.kmsURIs != nil {
			kmsURIs, err = eval.kmsURIs()
			if err != nil {
				return errors.Wrapf(err, "failed to evaluate backup kms_uri")
			}
			for _, kmsURI := range kmsURIs {
				backupNode.Options.EncryptionKMSURI = append(backupNode.Options.EncryptionKMSURI,
					tree.NewStrVal(kmsURI))
			}
		}

		// Evaluate required backup destinations.
		destinations, err := eval.destination()
		if err != nil {
			return errors.Wrapf(err, "failed to evaluate backup destination paths")
		}

		for _, dest := range destinations {
			backupNode.To = append(backupNode.To, tree.NewStrVal(dest))
		}

		backupNode.Targets = eval.Targets

		// Run full backup in dry-run mode.  This will do all of the sanity checks
		// and validation we need to make in order to ensure the schedule is sane.
		if err := dryRunBackup(ctx, p, backupNode); err != nil {
			return errors.Wrapf(err, "failed to dry run backup")
		}

	*/
}

func alterBackupScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	schedule, ok := stmt.(*tree.AlterBackupSchedule)
	if !ok {
		return nil, nil, nil, false, nil
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		err := doAlterBackupSchedules(ctx, p, schedule, resultsCh)
		if err != nil {
			telemetry.Count("scheduled-backup.alter.failed")
			return err
		}

		return nil
	}
	return fn, scheduledBackupHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook("schedule backup", alterBackupScheduleHook)
}
