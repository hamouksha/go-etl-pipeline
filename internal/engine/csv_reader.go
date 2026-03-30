package engine

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hamouksha/fast-etl/internal/config"
)

type Row = []string

type CSVReader struct {
	Location  string
	Delimeter rune
	Fields    []config.Field
}

func NewCSVReader(source *config.Source, cfgFields []config.Field) *CSVReader {
	return &CSVReader{source.Location, rune(source.Delimeter[0]), cfgFields}
}

func (c *CSVReader) Extract() (<-chan Row, <-chan error, error) {

	out := make(chan Row, 1000)
	errchan := make(chan error, 1000)

	file, err := os.Open(c.Location)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open csv file : %v \n", err)
	}

	reader := csv.NewReader(file)
	reader.Comma = c.Delimeter

	headers, err := reader.Read()
	if err != nil {
		//errchan <- fmt.Errorf("unable to read the file : %v \n", err)
		return nil, nil, fmt.Errorf("unable to read the file : %v \n", err)
	}

	headersMap, err := validateHeaders(headers, c.Fields)
	if err != nil {
		file.Close()
		return nil, nil, err
	}

	go func() {

		defer close(out)
		defer close(errchan)

		for {

			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errchan <- fmt.Errorf("unable to read this line : %v ", err)
				continue
			}

			err = validateRow(row, headersMap, c.Fields)
			if err != nil {
				errchan <- err
				continue

			}

			out <- row
		}
	}()

	return out, errchan, nil
}

func validateHeaders(headers []string, fields []config.Field) (map[string]int, error) {

	headersMap := make(map[string]int, len(headers))

	for i, v := range headers {
		headersMap[v] = i
	}

	for _, field := range fields {
		_, exists := headersMap[field.Name]
		if !exists && field.Required {
			return nil, fmt.Errorf("required column %q not found", field.Name)
		}

	}

	return headersMap, nil

}

func validateRow(row []string, headersMap map[string]int, fields []config.Field) error {

	for _, field := range fields {
		idx := headersMap[field.Name]
		if idx >= len(row) {
			return fmt.Errorf("row has fewer columns than expected")
		}

		if (len(row[idx]) == 0 || strings.ToLower(row[idx]) == "null") && field.Required {
			return fmt.Errorf("null value at required field %q", field.Name)
		}

	}

	return nil

}
