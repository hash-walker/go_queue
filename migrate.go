package goqueue

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
)

//go:embed migrations/001_create_jobs.sql
var createJobsSql string

func runMigrations(ctx context.Context, db PoolInterface, schemaName string, tableName string) error {
	if schemaName == "" {
		schemaName = "public"
	}

	if tableName == "" {
		tableName = "goqueue_jobs"
	}

	sql := createJobsSql
	sql = strings.ReplaceAll(sql, "{schema}", schemaName)
	sql = strings.ReplaceAll(sql, "{table}", tableName)

	_, err := db.Exec(ctx, sql)

	if err != nil {
		return fmt.Errorf("failed to run migrations for table %s.%s: %w", schemaName, tableName, err)
	}
	return nil

}
