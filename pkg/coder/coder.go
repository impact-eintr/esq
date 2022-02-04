package coder

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

type Coder interface {
	Encode(data interface{}) ([]byte, error)
	Decode(data []byte, v interface{}) error
}

func New(kind string) Coder {
	switch kind {
	case "json":
		return &JsonCoder{}
	case "gob":
		return &GobCoder{}
	}
	return nil
}

type JsonCoder struct {
}

func (j *JsonCoder) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (j *JsonCoder) Decode(data []byte, to interface{}) error {
	return json.Unmarshal(data, to)
}

type GobCoder struct {
}

func (g *GobCoder) Encode(data interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g *GobCoder) Decode(data []byte, to interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}
