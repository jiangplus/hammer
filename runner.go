package main

import (
	"fmt"
	"bytes"
	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"github.com/osteele/liquid"
	"os"
	"log"
	"os/exec"
)

type JobSpec struct {
	Name   string
	Author string
	Desc   string
	Labels []string
	Tasks  []TaskSpec
	Params map[string]interface{}
}

type TaskSpec struct {
	Name    string
	Command string
	Envs    []string
	Deps    []string
	Inputs  []InputSpec
	Outputs []OutputSpec
}

type InputSpec struct {
	S3   string
	Path string
}

type OutputSpec struct {
	S3   string
	Path string
}

type JobContext struct {
	S3Session *session.Session
	S3Client  *s3.S3
	Params    map[string]interface{}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func execCmd(command string, envs []string) string {
	if command == "" {
		panic("command is empty")
	}

	cmd := exec.Command("bash", "-c", command)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Env = os.Environ()
	if envs == nil {
		envs = []string{}
	}
	for _, env := range envs {
		cmd.Env = append(cmd.Env, env)
	}

	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
		panic(err)
	}
	fmt.Println(out.String())
	return out.String()
}

func renderCommand(ctx JobContext, command string) string {
	engine := liquid.NewEngine()
	template := command
	bindings := ctx.Params
	out, err := engine.ParseAndRenderString(template, bindings)
	if err != nil {
		log.Fatalln(err)
	}
	return out
}

func execTask(ctx JobContext, task TaskSpec) {
	fmt.Println("downloading")
	for _, input := range task.Inputs {
		fmt.Println(input)
		DownloadS3Dir(ctx.S3Session, ctx.S3Client, input.S3, input.Path)
	}

	command := renderCommand(ctx, task.Command)
	execCmd(command, task.Envs)

	fmt.Println("uploading")
	for _, output := range task.Outputs {
		fmt.Println(output)
		UploadS3Dir(ctx.S3Session, ctx.S3Client, output.Path, output.S3)
	}
}

func TaskRunner() {
	svc, sess := CreateS3Client()
	filename := os.Args[1]
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	var jobspec JobSpec
	if _, err := toml.Decode(string(data), &jobspec); err != nil {
		panic(err)
	}

	tasks := jobspec.Tasks

	// toposort
	graph := NewGraph(len(tasks))
	for _, task := range tasks {
		graph.AddNode(task.Name)
	}
	for _, task := range tasks {
		if task.Deps != nil {
			for _, dep_name := range task.Deps {
				graph.AddEdge(task.Name, dep_name)
			}
		}
	}
	result, ok := graph.Toposort()
	if !ok {
		panic("cycle detected")
	}
	sorted_tasks := []TaskSpec{}
	for _, task_name := range result {
		for _, task := range tasks {
			if task.Name == task_name {
				sorted_tasks = append(sorted_tasks, task)
			}
		}
	}

	ctx := JobContext{S3Session: sess, S3Client: svc, Params: jobspec.Params}
	for _, task := range sorted_tasks {
		execTask(ctx, task)
	}
}