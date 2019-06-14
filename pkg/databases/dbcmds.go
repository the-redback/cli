package databases

import (
	"fmt"
	"log"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"kmodules.xyz/client-go/tools/portforward"
	"path/filepath"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/homedir"
)

func AddDatabaseCMDs(cmds *cobra.Command) {
	addPostgresCMD(cmds)
	addMysqlCMD(cmds)
}

func tunnelToDBPod(dbPort int, namespace string, dbObjectName string, customSecretName string) (*v1.Secret, *portforward.Tunnel, error) {
	//TODO: Always close the tunnel after using thing function
	masterURL := ""
	var podName string
	var secretName string
	kubeconfigPath := filepath.Join(homedir.HomeDir(), ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfigPath)
	if err != nil {
		println("Could not get Kubernetes config: %s", err)
		return nil, nil, err
	}

	// kubedb mysql connect -n demo  quick-mysql

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, err
	}
	if namespace == "" {
		println("Using default namespace. Enter your namespace using -n=<your-namespace>")
	}
	podName = dbObjectName + "-0"

	if customSecretName == "" {
		secretName = dbObjectName + "-auth"
	} else {
		secretName = customSecretName
	}

	//dbPort := 3306

	_, err = client.CoreV1().Pods(namespace).Get(podName, metav1.GetOptions{})
	if err != nil {
		if kerr.IsNotFound(err) {
			fmt.Println("Pod doesn't exist")
		}
		return nil, nil, err
	}
	auth, err := client.CoreV1().Secrets(namespace).Get(secretName, metav1.GetOptions{})
	if err != nil {
		log.Fatalln(err)
	}

	tunnel := portforward.NewTunnel(client.CoreV1().RESTClient(), config, namespace, podName, dbPort)
	err = tunnel.ForwardPort()
	if err != nil {
		log.Fatalln(err)
	}

	return auth, tunnel, err
}
