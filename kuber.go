package main

import (
	"context"
	"flag"
	"fmt"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"path/filepath"
	"strings"

	core "k8s.io/api/core/v1"
)

var clientset *kubernetes.Clientset

func makeClient() *kubernetes.Clientset {
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

	clientsetx, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	return clientsetx
}

func createPodObject(name string, namespace string, labels map[string]string,
	conatiner_name string, docker_image string, command_args []string, envs []string, binds []string) *core.Pod {
	env_vars := []core.EnvVar{}
	for _, env := range envs {
		env_splited := strings.Split(env, "=")
		env_vars = append(env_vars, core.EnvVar{Name: env_splited[0], Value: env_splited[1]})
	}
	return &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: name+"-",
			Namespace: namespace,
			Labels: labels,
		},
		Spec: core.PodSpec{
			Containers: []core.Container{
				{
					Name: conatiner_name,
					Image:           docker_image,
					ImagePullPolicy: core.PullIfNotPresent,
					Command: command_args,
					Env: env_vars,
				},
			},
		},
	}
}

func execKuber(task_name string, command string, docker_image string, envs []string, binds []string) string {
	if clientset == nil {
		clientset = makeClient()
	}
    pod := createPodObject(task_name, "default", map[string]string{}, "main", docker_image, []string{"sh", "-c", command}, envs, binds)
	pod, err := clientset.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Println("Pod", pod.Name)
	return pod.Name
}

func krun() {
    if clientset == nil {
		clientset = makeClient()
	}
	pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

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
}
