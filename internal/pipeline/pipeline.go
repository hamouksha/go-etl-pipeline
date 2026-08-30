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

type ETLPipeline struct {
	cfg  *config.PipelineConfig
	conn *pgx.Conn
}

type ValidationPipeline struct {
	cfg *config.PipelineConfig
}

func NewETLPipe(cfgPath string, connString string) (*ETLPipeline, error) {

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	var conn *pgx.Conn

	conn, err = pgx.Connect(ctx, connString)
	if err != nil {
		conn, err = pgx.Connect(ctx, cfg.Target.Connection)
		if err != nil {
			return nil, fmt.Errorf("couldn't connect to the database provide them explicitly or in the config file : %v", err)
		}
	}

	return &ETLPipeline{cfg: cfg, conn: conn}, nil

}

func (p *ETLPipeline) Run(numWorkers, batchSize int) error {

	csvReader := engine.NewCSVReader(&p.cfg.Source, p.cfg.Fields)

	readerChan, readerErrChan, err := csvReader.Extract()
	if err != nil {
		return err
	}

	transformer := engine.NewTransformer(readerChan, p.cfg.Fields)
	transformedChan, transformedErrChan := transformer.Transform(numWorkers)

	writer := db.NewWriter(transformedChan, p.cfg.Fields, p.conn, p.cfg.Target.Table, batchSize)
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

func NewValidationPipeline(cfgPath string) (*ValidationPipeline, error) {

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}

	return &ValidationPipeline{cfg: cfg}, nil

}
func (p *ValidationPipeline) Validate(numWorkers int) error {

	csvReader := engine.NewCSVReader(&p.cfg.Source, p.cfg.Fields)

	readerChan, readerErrChan, err := csvReader.Extract()
	if err != nil {
		return err
	}

	transformer := engine.NewTransformer(readerChan, p.cfg.Fields)
	transformedChan, transformedErrChan := transformer.Transform(numWorkers)

	go func() {

		for range transformedChan {
		}
	}()

	var wg sync.WaitGroup
	for _, errChan := range []<-chan error{readerErrChan, transformedErrChan} {
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
