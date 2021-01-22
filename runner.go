package main

import (
	"fmt"
	"os"
	"log"
	"bytes"
	"os/exec"
	"strings"
	"io/ioutil"
	yamlutil "gopkg.in/yaml.v2"
	"github.com/osteele/liquid"
	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"time"
)

type JobSpec struct {
	Name   string
	Author string
	Desc   string
	Labels []string
	Envs    []string
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

type TaskState struct {
	Name string
	Status string
	StartTime time.Time
	EndTime time.Time
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
	Envs      []string
	Params    map[string]interface{}
	TaskStates map[string]TaskState
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

func renderString(ctx JobContext, command string) string {
	engine := liquid.NewEngine()
	template := command
	bindings := ctx.Params
	out, err := engine.ParseAndRenderString(template, bindings)
	if err != nil {
		log.Fatalln(err)
	}
	return out
}

func renderCommand(ctx JobContext, command string) string {
	return renderString(ctx, command)
}

func renderEnvs(ctx JobContext, envs []string) []string {
	new_envs := []string{}
	for _, env := range envs {
		new_envs = append(new_envs, renderString(ctx, env))
	}
	return new_envs
}

func execTask(ctx JobContext, task TaskSpec) {
	for _, input := range task.Inputs {
		fmt.Println(input)
		DownloadS3Dir(ctx.S3Session, ctx.S3Client, input.S3, input.Path)
	}

    envs := []string{}
    envs = append(envs, task.Envs...)
	envs = append(envs, ctx.Envs...)

	envs = renderEnvs(ctx, envs)
	command := renderCommand(ctx, task.Command)
	execCmd(command, envs)

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
	if strings.HasSuffix(filename, ".toml") {
		if _, err := toml.Decode(string(data), &jobspec); err != nil {
			log.Fatalf("error: %v", err)
			panic(err)
		}
	} else if strings.HasSuffix(filename, ".yaml") {
		err := yamlutil.Unmarshal([]byte(data), &jobspec)
		if err != nil {
			log.Fatalf("error: %v", err)
			panic(err)
		}
	} else {
		panic("cannot recognize data format")
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

	task_states := map[string]TaskState{}
	for _, task := range sorted_tasks {
		task_states[task.Name] = TaskState{Name: task.Name, Status: "new", StartTime: time.Now()}
	}
	// check deps exists
	for _, task := range sorted_tasks {
		for _, dep := range task.Deps {
			if _, ok = task_states[dep]; !ok {
				panic(fmt.Sprintf("dep [%s] for task [%s] is not satified", dep, task.Name))
			}
		}
	}
	ctx := JobContext{
		S3Session: sess,
		S3Client: svc,
		Params: jobspec.Params,
		Envs: jobspec.Envs,
		TaskStates: task_states}
	for _, task := range sorted_tasks {
		execTask(ctx, task)
	}
}