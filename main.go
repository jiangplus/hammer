package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)


func jsonify(value interface{}) io.Reader {
	jsonValue, _ := json.Marshal(value)
	fmt.Println(jsonValue)
	return bytes.NewBuffer(jsonValue)
}

func http_request(url string, data io.Reader) {
	resp, err := http.Post(url, "application/json", data)
	if err != nil {
		fmt.Println("http request err")
		return
	}
	//defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func main() {
  TaskRunner()
}
