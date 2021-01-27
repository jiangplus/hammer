package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
)

func jsonify(value interface{}) io.Reader {
	jsonValue, _ := json.MarshalIndent(value, "", "  ")
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
	runCmd := flag.NewFlagSet("run", flag.ExitOnError)
	job_spec_path := runCmd.String("job", "", "job spec path")

	switch os.Args[1] {
	case "run":
		runCmd.Parse(os.Args[2:])
		TaskRunner(*job_spec_path)
	case "list":
		fmt.Println("not implemented")
	case "submit":
		fmt.Println("not implemented")
	case "server":
		fmt.Println("not implemented")
	default:
		fmt.Println("expected to run subcommands")
		os.Exit(1)
	}

}
