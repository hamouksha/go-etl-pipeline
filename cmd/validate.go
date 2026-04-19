package cmd

import (
	"fmt"

	"github.com/hamouksha/fast-etl/internal/pipeline"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "validating the csv file and transforming it without writing to target",
	RunE:  runValidate,
}

func init() {
	validateCmd.Flags().IntP("workers", "w", 4, "num of transformers go routines")

	viper.BindPFlag("pipeline.workers", validateCmd.Flags().Lookup("workers"))

	rootCmd.AddCommand(validateCmd)

}

func runValidate(cmd *cobra.Command, args []string) error {

	config := viper.ConfigFileUsed()

	numworkers := viper.GetInt("pipeline.workers")

	pipe, err := pipeline.NewValidationPipeline(config)
	if err != nil {
		return fmt.Errorf("pipeline init failed : %w", err)

	}

	err = pipe.Validate(numworkers)
	if err != nil {
		return fmt.Errorf("error happend while validating : %w", err)
	}

	return nil
}
