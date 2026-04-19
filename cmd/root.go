package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgfile string

var rootCmd = &cobra.Command{
	Use:   "ingot",
	Short: "an agnostic tool for ingesting data",
	Long:  `an agnostic tool for ingesting data from any source to different sink`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(os.Stderr)
		os.Exit(1)
	}

}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().String("db-dsn", "", "the database string (OVERRIDES CONFIG)")
	rootCmd.PersistentFlags().StringVar(&cfgfile, "config", "", "the path to the config file")
	rootCmd.PersistentFlags().String("log-level", "", "log level = debug | info | warn | error")

	viper.BindPFlag("db-dsn", rootCmd.PersistentFlags().Lookup("db-dsn"))

}

func initConfig() {

	if cfgfile != "" {
		viper.SetConfigFile(cfgfile)
	} else {
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.SetConfigName("schema")
		viper.SetConfigType("yaml")

	}

	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "using config", viper.ConfigFileUsed())
	}

}
