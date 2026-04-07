package db

import (
	"context"
	"fmt"
	"github.com/hamouksha/fast-etl/internal/config"
	"github.com/jackc/pgx/v5"
	"strings"
)

type Writer struct {
	input     <-chan map[string]any
	fields    []config.Field
	conn      *pgx.Conn
	query     string
	batchSize int
}

func NewWriter(inputChan <-chan map[string]any,
	fields []config.Field,
	conn *pgx.Conn,
	table string,
	batchSize int) *Writer {

	query := queryBuilder(table, fields)

	return &Writer{input: inputChan, fields: fields, conn: conn, query: query, batchSize: batchSize}

}

func (w *Writer) Write() <-chan error {
	errchan := make(chan error, 1000)

	var batch [][]any
	ctx := context.Background()

	go func() {
		defer close(errchan)
		for row := range w.input {

			if len(batch) > w.batchSize {
				err := w.flush(ctx, batch)
				if err != nil {
					errchan <- err
				}
				batch = batch[:0]
			}

			values := make([]any, len(w.fields))
			for i, field := range w.fields {
				values[i] = row[field.Name]
			}
			batch = append(batch, values)
		}

		if len(batch) > 0 {
			err := w.flush(ctx, batch)
			if err != nil {
				errchan <- err
			}
		}

	}()
	return errchan

}

func (w *Writer) flush(ctx context.Context, batch [][]any) error {

	pgxBatch := &pgx.Batch{}
	for _, row := range batch {
		pgxBatch.Queue(w.query, row...)

	}
	results := w.conn.SendBatch(ctx, pgxBatch)
	defer results.Close()

	for range batch {
		_, err := results.Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

func queryBuilder(table string, fields []config.Field) string {

	cols := make([]string, len(fields))
	vals := make([]string, len(fields))

	for i, field := range fields {
		cols[i] = field.Name
		vals[i] = fmt.Sprintf("$%d", i+1)
	}

	return fmt.Sprintf("INSERT INTO %s (%s) values (%s)",
		table,
		strings.Join(cols, ", "),
		strings.Join(vals, ", "),
	)

}
