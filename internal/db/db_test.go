package db

import (
	"fmt"
	"testing"
	"time"

	"github.com/hamouksha/fast-etl/internal/config"
	"github.com/jackc/pgx/v5"
)

func TestCreatingTables(t *testing.T) {

	sampleconf, err := config.LoadConfig("./testdata/schema.yaml")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(sampleconf.PipelineName)

	createTable(&pgx.Conn{}, sampleconf.Target.Table, sampleconf.Fields)
}

func TestWriterWithFlushing(t *testing.T) {

	sampleconf, err := config.LoadConfig("./testdata/schema.yaml")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(sampleconf.PipelineName)
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, sampleconf.Target.Connection)
	if err != nil {
		t.Fatalf("couldn't connect to the database : %v", err)
	}

	samplechan := make(chan map[string]any, 100)
	data := []map[string]any{
		{
			"Date":              mustParseDate("2022-01-01"),
			"Domain":            "RESTRAUNT",
			"Location":          "Bhuj",
			"Value":             365554.0,
			"Transaction_count": 1932,
		},
		{
			"Date":              mustParseDate("2022-01-01"),
			"Domain":            "INVESTMENTS",
			"Location":          "Ludhiana",
			"Value":             847444.0,
			"Transaction_count": 1721,
		},
		{
			"Date":              mustParseDate("2022-01-01"),
			"Domain":            "RETAIL",
			"Location":          "Goa",
			"Value":             786941.0,
			"Transaction_count": 1573,
		},
		{
			"Date":              mustParseDate("2022-01-01"),
			"Domain":            "INTERNATIONAL",
			"Location":          "Mathura",
			"Value":             368610.0,
			"Transaction_count": 2049,
		},
		{
			"Date":              mustParseDate("2022-01-01"),
			"Domain":            "RESTRAUNT",
			"Location":          "Madurai",
			"Value":             615681.0,
			"Transaction_count": 1519,
		},
		{
			"Date":              mustParseDate("2022-01-01"),
			"Domain":            "INTERNATIONAL",
			"Location":          "Daman",
			"Value":             1191092.0,
			"Transaction_count": 1813,
		},
		{
			"Date":              mustParseDate("2022-01-01"),
			"Domain":            "INTERNATIONAL",
			"Location":          "Buxar",
			"Value":             968883.0,
			"Transaction_count": 2098,
		},
		{
			"Date":              mustParseDate("2022-01-01"),
			"Domain":            "PUBLIC",
			"Location":          "Trichy",
			"Value":             1030297.0,
			"Transaction_count": 606,
		},
		{
			"Date":              mustParseDate("2022-01-01"),
			"Domain":            "RESTRAUNT",
			"Location":          "Kullu",
			"Value":             688655.0,
			"Transaction_count": 1463,
		},
	}

	for _, v := range data {
		samplechan <- v
	}

	writer := NewWriter(samplechan, sampleconf.Fields, conn, sampleconf.Target.Table, 8)
	fmt.Println("start writing to db")
	errchan := writer.Write()
	close(samplechan)
	for err := range errchan {

		t.Logf("error while writing to db : %v", err)
	}

}

func mustParseDate(date string) time.Time {
	t, err := time.Parse("2006-01-02", date)
	if err != nil {
		fmt.Errorf("err : %v", err)
	}
	return t
}
