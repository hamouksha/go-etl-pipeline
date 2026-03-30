package engine

import (
	"fmt"
	"testing"

	"github.com/hamouksha/fast-etl/internal/config"
)

func TestCSVReader(t *testing.T) {
	sampleCSV, err := config.LoadConfig("testdata/schema.yaml")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("source: %v\n", sampleCSV.Source.SourceName)

	csvreader := NewCSVReader(&sampleCSV.Source, sampleCSV.Fields)

	rowschan, errchan, err := csvreader.Extract()
	if err != nil {
		t.Fatal(err)
	}

	for row := range rowschan {
		fmt.Printf("row : %v\n", row)
	}

	for err := range errchan {
		t.Log(err)
	}

}

func TestTransform(t *testing.T) {

	sampleCSV, err := config.LoadConfig("testdata/schema.yaml")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("source: %v\n", sampleCSV.Source.SourceName)

	csvreader := NewCSVReader(&sampleCSV.Source, sampleCSV.Fields)

	rowschan, errchan, err := csvreader.Extract()
	if err != nil {
		t.Fatal(err)
	}

	for err := range errchan {
		t.Log(err)
	}

	tf := NewTransformer(rowschan, sampleCSV.Fields)

	rowsmap, errchan := tf.Transform(2)

	for row := range rowsmap {
		fmt.Printf("row map : %v \n", row)
	}

	for err := range errchan {
		t.Log(err)
	}

}
