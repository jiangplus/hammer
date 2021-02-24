package core

import (
	"bytes"
	"context"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/osteele/liquid"
	yamlutil "gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
	"sync"
)

type PipelineSpec struct {
	Name   string
	Author string
	Desc   string
	Timeout int64
	Labels []string
	Envs    []string
	Tasks  []TaskSpec
	Params map[string]interface{}
	TaskType string `yaml:"task_type"`
	DockerImage string `yaml:"docker_image"`
}

type RangeSpec struct {
	From int
	To int
	Step int
}

type WhenSpec struct {
	Input string
	Operator string
	Values interface{}
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
	TaskType string `yaml:"task_type"`
	DockerImage string `yaml:"docker_image"`
	Binds []string
	When []WhenSpec
}

type TaskState struct {
	Name string
	Status string
	StartTime time.Time
	EndTime time.Time
	Task *TaskSpec
}

type InputSpec struct {
	S3   string
	Path string
}

type OutputSpec struct {
	S3   string
	Path string
}

type RunContext struct {
	S3Session *session.Session
	S3Client  *s3.S3
	Timeout   int64
	Envs      []string
	Params    map[string]interface{}
	TaskStates map[string]*TaskState
	Runtime string
	TaskMap map[string]map[string]bool
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func execDocker(task_name string, command string, docker_image string, envs []string, binds []string) string {
	if command == "" {
		panic("command is empty")
	}

	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}

	var host_config *container.HostConfig = nil
	if len(binds) > 0 {
		for _, i := range binds {
			splited := strings.Split(i, ":")
			source := splited[0]
			target := splited[1]
			host_config = &container.HostConfig{
				Mounts: []mount.Mount{
					{
						Type:   mount.TypeBind,
						Source: source,
						Target: target,
					},
				},
			}
		}
	}

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: docker_image,
		Cmd:   []string{"sh", "-c", command},
		Env:   envs,
		Tty:   false,
	}, host_config, nil, nil, "")
	if err != nil {
		panic(err)
	}

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		panic(err)
	}

	statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			panic(err)
		}
	case <-statusCh:
	}

	out, err := cli.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStdout: true})
	if err != nil {
		panic(err)
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return resp.ID
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

func contains(s []interface{}, e interface{}) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func ExecTask(ctx RunContext, task TaskSpec) {
	for _, input := range task.Inputs {
		fmt.Println(input)
		DownloadS3Dir(ctx.S3Session, ctx.S3Client, input.S3, input.Path)
	}

	// check when condiction
	shouldRun := true
	if len(task.When) > 0 {
		for _, cond := range task.When {
			val := ctx.Params[cond.Input]
			if cond.Values == nil {
				cond.Values = true
			}
			if cond.Operator == "eq" || cond.Operator == "" {
				if val != cond.Values {
					shouldRun = false
				}
			} else if cond.Operator == "in" {
				condVals := cond.Values.([]interface{})
				if !contains(condVals, val) {
					shouldRun = false
				}
			}
		}
	}
	if !shouldRun {
		return
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


	if task.TaskType == "docker" {
		execDocker(task.Name, command, task.DockerImage , envs, task.Binds)
	} else if task.TaskType == "kubernetes" {
		execKuber(task.Name, command, task.DockerImage, []string{}, []string{})
	} else {
		execCmd(command, envs, ctx.Timeout)
	}

	for _, output := range task.Outputs {
		fmt.Println(output)
		UploadS3Dir(ctx.S3Session, ctx.S3Client, output.Path, output.S3)
	}
}

func RunPipeline(job_spec_path string) {
	svc, sess := CreateS3Client()
	jobspec := parseSpec(job_spec_path)
	tasks := jobspec.Tasks

	ok, sorted_tasks := sort_tasks(tasks)

	task_states := map[string]*TaskState{}
	task_map := map[string]map[string]bool{}
	for _, task := range sorted_tasks {
		task_states[task.Name] = &TaskState{Name: task.Name, Status: "new", StartTime: time.Now(), Task: &task}

		if task_map[task.Name] == nil {
			task_map[task.Name] = make(map[string]bool)
		}
		for _, dep := range task.Deps {
			task_map[task.Name][dep] = true
		}
	}

	check_deps_exists(sorted_tasks, ok, task_states)
	check_params_not_empty(jobspec)

	ctx := RunContext{
		S3Session:  sess,
		S3Client:   svc,
		Params:     jobspec.Params,
		Envs:       jobspec.Envs,
		TaskStates: task_states,
		TaskMap: task_map}

	if jobspec.Timeout == 0 {
		ctx.Timeout = 365 * 86400 * 1000
	} else {
		ctx.Timeout = jobspec.Timeout
	}

	if jobspec.TaskType == "" {
		ctx.Runtime = "local"
	} else {
		ctx.Runtime = jobspec.TaskType
	}

	task_chan := make(chan TaskSpec)
	result_chan := make(chan string)

	var wg sync.WaitGroup

	for worker_id := 1; worker_id <= 3; worker_id++ {
		go worker(worker_id, &wg, ctx, task_chan, result_chan, sorted_tasks)
	}

	for _, task := range sorted_tasks {
		if satisfied(task, ctx) {
			// modify task_state status
			ctx.TaskStates[task.Name].Status = "running"

			wg.Add(1)
			task_chan <- task
			fmt.Println("started task", task.Name)

		}
	}

	go reschedule(result_chan, ctx, sorted_tasks, wg, task_chan)

	wg.Wait()
}

func reschedule(result_chan chan string, ctx RunContext, sorted_tasks []TaskSpec, wg sync.WaitGroup, task_chan chan TaskSpec) {
	for result := range result_chan {
		fmt.Println(result)
	}
}


func satisfied(task TaskSpec, ctx RunContext) bool {
	if len(ctx.TaskMap[task.Name]) == 0 && ctx.TaskStates[task.Name].Status == "new" {
		return true
	} else {
		return false
	}
}

func worker(id int, wg *sync.WaitGroup, ctx RunContext, task_chan chan TaskSpec, result_chan chan<- string, sorted_tasks []TaskSpec) {
	for task := range task_chan {
		RunTask(task, ctx)
		result_chan <- task.Name
		//time.Sleep(100 * time.Millisecond) // todo remove this sleep

		// send dep tasks if satisfied
		for k, _ := range ctx.TaskMap {
			delete(ctx.TaskMap[k], task.Name) // todo handle race condiction
		}
		for _, task := range sorted_tasks {
			if satisfied(task, ctx) {
				// modify task_state status
				ctx.TaskStates[task.Name].Status = "running"

				wg.Add(1)
				task_chan <- task
				fmt.Println("started dep task", task.Name)
			}
		}

		wg.Done()
	}
}

func RunTask(task TaskSpec, ctx RunContext) {
	if len(task.WithItems) > 0 {
		if task.Params == nil {
			task.Params = make(map[string]interface{})
		}
		for _, item := range task.WithItems {
			subtask := TaskSpec{
				Params:     task.Params,
				Command:    task.Command,
				Envs:       task.Envs,
				Deps:       task.Deps,
				Inputs:     task.Inputs,
				Outputs:    task.Outputs,
				ParentTask: &task}

			if task.Namegen == "" {
				log.Fatalln("subtask namegen is empty")
			}

			subtask.Params["item"] = item
			subtask.Name = renderString(subtask.Params, task.Namegen)

			ctx.TaskStates[subtask.Name] = &TaskState{Name: subtask.Name, Status: "new", StartTime: time.Now()}
			ExecTask(ctx, subtask)
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
			ExecTask(ctx, task)
		}

	} else {
		ExecTask(ctx, task)
	}
}

func parseSpec(filename string) PipelineSpec {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	if len(data) == 0 {
		log.Fatal("input file is empty")
	}

	var jobspec PipelineSpec
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
	return jobspec
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
		log.Fatal("cycle detected")
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

func check_deps_exists(sorted_tasks []TaskSpec, ok bool, task_states map[string]*TaskState) {
	for _, task := range sorted_tasks {
		for _, dep := range task.Deps {
			if _, ok = task_states[dep]; !ok {
				panic(fmt.Sprintf("dep [%s] for task [%s] is not satified", dep, task.Name))
			}
		}
	}
}

func check_params_not_empty(jobspec PipelineSpec) {
	for _, val := range jobspec.Params {
		if val == nil {
			panic(fmt.Sprintf("param %s is not set", val))
		}
	}
}