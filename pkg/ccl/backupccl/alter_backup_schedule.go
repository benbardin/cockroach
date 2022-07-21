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

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/kr/pretty"
)

const alterBackupScheduleOp = "ALTER BACKUP SCHEDULE"

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

	var dependentSchedule *jobs.ScheduledJob
	dependentArgs := &backuppb.ScheduledBackupExecutionArgs{}
	if args.DependentScheduleID != 0 {
		dependentSchedule, err = jobs.LoadScheduledJob(ctx, env, args.DependentScheduleID, execCfg.InternalExecutor, p.Txn())
		if err != nil {
			return err
		}
		if err := pbtypes.UnmarshalAny(dependentSchedule.ExecutionArgs().Args, args); err != nil {
			return errors.Wrap(err, "un-marshaling args")
		}
	}

	fmt.Println("-----------------------")
	_, _ = pretty.Println(schedule)
	_, _ = pretty.Println(args)
	_, _ = pretty.Println(dependentSchedule)
	_, _ = pretty.Println(dependentArgs)
	fmt.Println("^^^^^^^^^^^^^^^^^^^^^^^")
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
