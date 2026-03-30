package engine

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hamouksha/fast-etl/internal/config"
)

type Transformer struct {
	fields []config.Field
	input  <-chan Row
}

func NewTransformer(input <-chan Row, fields []config.Field) *Transformer {
	return &Transformer{input: input, fields: fields}
}

func (t *Transformer) Transform(numWorkers int) (chan map[string]any, chan error) {

	out := make(chan map[string]any, 1000)
	errchan := make(chan error, 1000)

	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range t.input {
				result, err := t.transformRow(row)
				if err != nil {
					errchan <- err
					continue
				}
				out <- result
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
		close(errchan)
	}()
	return out, errchan
}

func (t *Transformer) transformRow(row Row) (map[string]any, error) {

	result := make(map[string]any, len(t.fields))

	for i, field := range t.fields {
		raw := strings.TrimSpace(row[i])
		if raw == "" {
			if field.Required {
				return nil, fmt.Errorf("field %q is required but empty", field.Name)
			}
			result[field.Name] = nil
			continue
		}

		switch strings.ToLower(field.Type) {
		case "string", "str", "":
			result[field.Name] = raw
		case "integer", "int":
			v, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				if field.Required {
					return nil, fmt.Errorf("can't parse required value %q", field.Name)
				}
				result[field.Name] = 0
				continue
			}
			result[field.Name] = v
		case "float", "float64":
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				if field.Required {
					return nil, fmt.Errorf("can't parse required value %q", field.Name)
				}
				result[field.Name] = 0.0
				continue
			}
			result[field.Name] = v
		case "boolean", "bool":
			v, err := strconv.ParseBool(raw)
			if err != nil {
				if field.Required {
					return nil, fmt.Errorf("can't parse required value %q", field.Name)
				}
				result[field.Name] = nil
				continue
			}
			result[field.Name] = v

		case "timestamp":
			v, err := time.Parse(field.Layout, raw)

			if err != nil {
				if field.Required {
					return nil, fmt.Errorf("can't parse required value %q", field.Name)
				}
				result[field.Name] = nil
				continue
			}
			result[field.Name] = v
		default:
			if field.Required {
				return nil, fmt.Errorf("can't parse required value %q", field.Name)
			}
			result[field.Name] = nil
		}
	}
	return result, nil
}
