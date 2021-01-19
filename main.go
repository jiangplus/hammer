package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/osteele/liquid"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
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

func CreateS3Client() (*s3.S3, *session.Session) {
	//$ export AWS_ACCESS_KEY_ID=YOUR_AKID
	//$ export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
	//$ export AWS_ENDPOINT=http://s3.cn-northwest-1.amazonaws.com.cn

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("cn-northwest-1")},
	)
	if err != nil {
		panic(err)
	}
	aws_endpoint := os.Getenv("AWS_ENDPOINT")
	svc := s3.New(sess, &aws.Config{Endpoint: aws.String(aws_endpoint)})
	return svc, sess
}

type fileWalk chan string

func (f fileWalk) Walk(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if !info.IsDir() {
		f <- path
	}
	return nil
}

func UploadS3Dir(sess *session.Session, svc *s3.S3, src string, dst string) {
	dstUrl, err := url.Parse(dst)
	if err != nil {
		panic(err)
	}
	bucket := dstUrl.Host
	targetPath := strings.TrimPrefix(dstUrl.Path, "/")
	fmt.Println("o", src, dst, bucket, targetPath)

	walker := make(fileWalk)
	go func() {
		// Gather the files to upload by walking the path recursively
		if err := filepath.Walk(src, walker.Walk); err != nil {
			log.Fatalln("Walk failed:", err)
		}
		close(walker)
	}()

	// For each file found walking, upload it to S3
	uploader := s3manager.NewUploader(sess)
	for path := range walker {
		rel, err := filepath.Rel(src, path)
		if err != nil {
			log.Fatalln("Unable to get relative path:", path, err)
		}
		file, err := os.Open(path)
		if err != nil {
			log.Println("Failed opening file", path, err)
			continue
		}
		defer file.Close()
		result, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: &bucket,
			Key:    aws.String(filepath.Join(targetPath, rel)),
			Body:   file,
		})
		if err != nil {
			log.Fatalln("Failed to upload", path, err)
		}
		log.Println("Uploaded", path, result.Location)
	}
}

func DownloadS3Dir(sess *session.Session, svc *s3.S3, src string, dst string) {
	srcUrl, err := url.Parse(src)
	if err != nil {
		log.Fatal(err)
		panic(err)
	}
	bucket := srcUrl.Host
	path := strings.TrimPrefix(srcUrl.Path, "/")
	obj_result, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(path), Delimiter: aws.String("/")})
	if err != nil {
		exitErrorf("Unable to list objects, %v, %s, %s", err, src, dst)
	}
	log.Println("Objects:")
	downloader := s3manager.NewDownloader(sess)

	for _, o := range obj_result.Contents {
		fmt.Printf("* %s %s %s\n",
			aws.StringValue(o.Key), aws.StringValue(o.ETag), aws.TimeValue(o.LastModified))

		item := aws.StringValue(o.Key)
		item = strings.TrimPrefix(item, path)
		item = filepath.Join(dst, item)
		file, err := os.Create(item)
		if err != nil {
			os.MkdirAll(filepath.Dir(item), os.ModePerm)
			file, err = os.Create(item)
			if err != nil {
				exitErrorf("Unable to open file %q, %v", item, err)
			}
		}
		defer file.Close()

		numBytes, err := downloader.Download(file,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    o.Key,
			})
		if err != nil {
			exitErrorf("Unable to download item %q, %v", item, err)
		}

		fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
	}
}

func krun() {

	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}

	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

	// Examples for error handling:
	// - Use helper functions like e.g. errors.IsNotFound()
	// - And/or cast to StatusError and use its properties like e.g. ErrStatus.Message
	namespace := "default"
	pod := "example"
	_, err = clientset.CoreV1().Pods(namespace).Get(context.TODO(), pod, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		fmt.Printf("Pod %s in namespace %s not found\n", pod, namespace)
	} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
		fmt.Printf("Error getting pod %s in namespace %s: %v\n",
			pod, namespace, statusError.ErrStatus.Message)
	} else if err != nil {
		panic(err.Error())
	} else {
		fmt.Printf("Found pod %s in namespace %s\n", pod, namespace)
	}

	time.Sleep(10 * time.Second)
}

func TaskRunner() {
	svc, sess := CreateS3Client()
	// bk_result, err := svc.ListBuckets(nil)
	// if err != nil {
	// 	exitErrorf("Unable to list buckets, %v", err)
	// }

	// fmt.Println("Buckets:")
	// for _, b := range bk_result.Buckets {
	// 	fmt.Printf("* %s created on %s\n",
	// 		aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	// }

	filename := os.Args[1]
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	var jobspec JobSpec
	if _, err := toml.Decode(string(data), &jobspec); err != nil {
		panic(err)
	}
	//fmt.Println(jobspec)

	jsonspec, err := json.MarshalIndent(jobspec, "", "  ")
	if err != nil {
		panic(err)
	}
	log.Println(string(jsonspec))
	log.Println("")

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

func main() {
  TaskRunner()
}
