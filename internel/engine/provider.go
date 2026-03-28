package engine

type Reader interface {
	Extract() (<-chan Row, <-chan error, error)
}

func NewExtractor()
