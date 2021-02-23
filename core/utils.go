package core

import (
	"bytes"
	"encoding/json"
	"io"
)

func jsonify(value interface{}) io.Reader {
	jsonValue, _ := json.MarshalIndent(value, "", "  ")
	return bytes.NewBuffer(jsonValue)
}