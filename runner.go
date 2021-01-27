package main

import (
	"context"
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
	Timeout int64
	Labels []string
	Envs    []string
	Tasks  []TaskSpec
	Params map[string]interface{}
}

type RangeSpec struct {
	From int
	To int
	Step int
}

type TaskSpec struct {
	Name    string
	Command string
	Envs    []string
	Deps    []string
	Inputs  []InputSpec
	Outputs []OutputSpec
	Params map[string]interface{}
	WithItems []interface{} `yaml:"with_items"`
	WithRange RangeSpec `yaml:"with_range"`
	Namegen string
	ParentTask *TaskSpec
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
	Timeout   int64
	Envs      []string
	Params    map[string]interface{}
	TaskStates map[string]TaskState
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func execCmd(command string, envs []string, timeout int64) string {
	if command == "" {
		panic("command is empty")
	}

	duration := time.Duration(timeout)
	ctx, cancel := context.WithTimeout(context.Background(), duration * time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx,"bash", "-c", command)

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
		fmt.Println(err)
		log.Fatal(err)
		panic(err)
	}
	fmt.Println(out.String())
	return out.String()
}

func renderString(params map[string]interface{}, command string) string {
	engine := liquid.NewEngine()
	out, err := engine.ParseAndRenderString(command, params)
	if err != nil {
		log.Fatalln(err)
	}
	return out
}

func renderCommand(params map[string]interface{}, command string) string {
	return renderString(params, command)
}

func renderEnvs(params map[string]interface{}, envs []string) []string {
	new_envs := []string{}
	for _, env := range envs {
		new_envs = append(new_envs, renderString(params, env))
	}
	return new_envs
}

func execTask(ctx JobContext, task TaskSpec) {
	for _, input := range task.Inputs {
		fmt.Println(input)
		DownloadS3Dir(ctx.S3Session, ctx.S3Client, input.S3, input.Path)
	}

	params := make(map[string]interface{})
	for k, v := range ctx.Params {
		params[k] = v
	}
	for k, v := range task.Params {
		params[k] = v
	}

    envs := []string{}
    envs = append(envs, task.Envs...)
	envs = append(envs, ctx.Envs...)

	envs = renderEnvs(params, envs)
	command := renderCommand(params, task.Command)
	execCmd(command, envs, ctx.Timeout)

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
	if len(data) == 0 {
		panic("input file is empty")
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
	ok, sorted_tasks := sort_tasks(tasks)

	task_states := map[string]TaskState{}
	for _, task := range sorted_tasks {
		task_states[task.Name] = TaskState{Name: task.Name, Status: "new", StartTime: time.Now()}
	}

	check_deps_exists(sorted_tasks, ok, task_states)

	check_params_not_empty(jobspec)

	ctx := JobContext{
		S3Session:  sess,
		S3Client:   svc,
		Params:     jobspec.Params,
		Envs:       jobspec.Envs,
		TaskStates: task_states}

	if jobspec.Timeout == 0 {
		ctx.Timeout = 365 * 86400 * 1000
	} else {
		ctx.Timeout = jobspec.Timeout
	}

	for _, task := range sorted_tasks {
		if len(task.WithItems) > 0 {
			if task.Params == nil {
				task.Params = make(map[string]interface{})
			}
			for _, item := range task.WithItems {
				subtask := TaskSpec{
					Params: task.Params,
					Command: task.Command,
					Envs: task.Envs,
					Deps: task.Deps,
					Inputs: task.Inputs,
					Outputs: task.Outputs,
					ParentTask: &task}

				subtask.Params["item"] = item
				subtask.Name = renderString(subtask.Params, task.Namegen)
				fmt.Println(subtask.Name)

				task_states[subtask.Name] = TaskState{Name: subtask.Name, Status: "new", StartTime: time.Now()}
				execTask(ctx, subtask)
			}
		} else if task.WithRange != (RangeSpec{}) {
			if task.WithRange.Step == 0 {
				task.WithRange.Step = 1
			}
			for i := task.WithRange.From; i <= task.WithRange.To; i += task.WithRange.Step {
				if task.Params == nil {
					task.Params = make(map[string]interface{})
				}
				task.Params["item"] = i
				execTask(ctx, task)
			}

		} else {
			execTask(ctx, task)
		}
	}
}

func sort_tasks(tasks []TaskSpec) (bool, []TaskSpec) {
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
	return ok, sorted_tasks
}

func check_deps_exists(sorted_tasks []TaskSpec, ok bool, task_states map[string]TaskState) {
	for _, task := range sorted_tasks {
		for _, dep := range task.Deps {
			if _, ok = task_states[dep]; !ok {
				panic(fmt.Sprintf("dep [%s] for task [%s] is not satified", dep, task.Name))
			}
		}
	}
}

func check_params_not_empty(jobspec JobSpec) {
	for _, val := range jobspec.Params {
		if val == nil {
			panic(fmt.Sprintf("param %s is not set", val))
		}
	}
}