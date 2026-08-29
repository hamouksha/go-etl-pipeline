package cmd

import (
	"fmt"

	"github.com/hamouksha/fast-etl/internal/pipeline"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "run the full pipeline that ingest data from source to sink",
	RunE:  runIngest,
}

func init() {
	ingestCmd.Flags().IntP("workers", "w", 4, "number of transformers go routines")
	ingestCmd.Flags().IntP("batch-size", "b", 500, "batch size to insert to database table")

	viper.BindPFlag("pipeline.workers", ingestCmd.Flags().Lookup("workers"))
	viper.BindPFlag("pipeline.batch-size", ingestCmd.Flags().Lookup("batch-size"))

	rootCmd.AddCommand(ingestCmd)

}

func runIngest(cmd *cobra.Command, args []string) error {
	configFilePath := viper.ConfigFileUsed()
	if configFilePath == "" {
		return fmt.Errorf("no config file found - define the file path using --config explicitly")
	}

	// get the database variables to to make a conn
	dbDsn := viper.GetString("db-dsn")
	if dbDsn == "" {
		return fmt.Errorf("the DB_NAME , DB_PASS vars are not set either you set them or provide them explicitly to the command")
	}

	numWorkers := viper.GetInt("pipeline.workers")
	batchSize := viper.GetInt("pipeline.batch-size")

	pipe, err := pipeline.NewETLPipe(configFilePath, dbDsn)
	if err != nil {
		return fmt.Errorf("pipeline init failed: %w", err)
	}

	if err := pipe.Run(numWorkers, batchSize); err != nil {
		return fmt.Errorf("error happend while ingesting the data : %w", err)
	}

	fmt.Println("ingesting finished")
	return nil

}
