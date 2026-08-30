package db

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hamouksha/fast-etl/internal/config"
	"github.com/jackc/pgx/v5"
)

type Writer struct {
	input     <-chan map[string]any
	fields    []config.Field
	conn      *pgx.Conn
	query     string
	batchSize int
	ctx       context.Context
}

func NewWriter(inputChan <-chan map[string]any,
	fields []config.Field,
	conn *pgx.Conn,
	table string,
	batchSize int) *Writer {

	createTable(conn, table, fields)

	query := queryBuilder(table, fields)

	return &Writer{input: inputChan, fields: fields, conn: conn, query: query, batchSize: batchSize, ctx: context.Background()}

}

func (w *Writer) Write() <-chan error {
	errchan := make(chan error, 1000)

	var batch [][]any

	go func() {
		defer close(errchan)
		for row := range w.input {

			if len(batch) >= w.batchSize {
				err := w.flush(w.ctx, batch)
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
			err := w.flush(w.ctx, batch)
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

func createTable(conn *pgx.Conn, tablename string, fields []config.Field) {

	var b strings.Builder
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	b.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v ( PK BIGSERIAL PRIMARY KEY,", tablename))

	for _, f := range fields {
		switch strings.ToLower(f.Type) {
		case "string", "str", "":
			if f.Required {
				b.WriteString(fmt.Sprintf(" %v TEXT NOT NULL, ", f.Name))
				continue
			}
			b.WriteString(fmt.Sprintf(" %v TEXT,", f.Name))

		case "integer", "int":
			if f.Required {
				b.WriteString(fmt.Sprintf(" %v BIGINT NOT NULL, ", f.Name))
				continue
			}
			b.WriteString(fmt.Sprintf(" %v INT,", f.Name))

		case "float", "float64":
			if f.Required {
				b.WriteString(fmt.Sprintf(" %v REAL NOT NULL, ", f.Name))
				continue
			}
			b.WriteString(fmt.Sprintf(" %v REAL,", f.Name))

		case "timestamp":
			if f.Required {
				b.WriteString(fmt.Sprintf(" %v TIMESTAMP NOT NULL, ", f.Name))
				continue
			}
			b.WriteString(fmt.Sprintf(" %v TIMESTAMP,", f.Name))
		}
	}

	b.WriteString(");")

	tag, err := conn.Exec(ctx, b.String())

	if err != nil {
		fmt.Errorf("couldn't create db table or couldn't connect to db with err : %v", err)
		return
	}

	log.Printf("pgx tag : %v", tag)

}
