package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/hamouksha/fast-etl/internal/config"
	"github.com/hamouksha/fast-etl/internal/db"
	"github.com/hamouksha/fast-etl/internal/engine"
	"github.com/jackc/pgx/v5"
)

type Pipeline struct {
	cfg  *config.PipelineConfig
	conn *pgx.Conn
}

func NewPipe(cfgPath string, connString string) (*Pipeline, error) {

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("can't connect to the database : %w \n", err)
	}

	return &Pipeline{cfg: cfg, conn: conn}, nil

}

func (p *Pipeline) Run(numWorkers, batchSize int) error {

	csvReader, err := engine.NewCSVReader(&p.cfg.Sources[0], p.cfg.Fields)
	if err != nil {
		return err
	}
	readerChan, readerErrChan, err := csvReader.Extract()
	if err != nil {
		return err
	}

	transformer := engine.NewTransformer(readerChan, p.cfg.Fields)
	transformedChan, transformedErrChan := transformer.Transform(numWorkers)

	writer := db.NewWriter(transformedChan, p.cfg.Fields, p.conn, p.cfg.TargetTable, batchSize)
	writerErrChan := writer.Write()
	defer p.conn.Close(context.Background())

	var wg sync.WaitGroup
	for _, errChan := range []<-chan error{readerErrChan, transformedErrChan, writerErrChan} {
		wg.Add(1)
		go func(errChan <-chan error) {
			defer wg.Done()
			for err := range errChan {
				log.Println(err)
			}

		}(errChan)

	}

	wg.Wait()

	return nil
}
