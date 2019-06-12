package cmds

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	v "github.com/appscode/go/version"
	shell "github.com/codeskyblue/go-sh"
	"github.com/kubedb/cli/pkg/cmds/create"
	"github.com/kubedb/cli/pkg/cmds/get"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cliflag "k8s.io/component-base/cli/flag"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/util/templates"
	"kmodules.xyz/client-go/logs"
	"kmodules.xyz/client-go/tools/cli"
	"kmodules.xyz/client-go/tools/portforward"
	kerr "k8s.io/apimachinery/pkg/api/errors"
)

// NewKubeDBCommand creates the `kubedb` command and its nested children.
func NewKubeDBCommand(in io.Reader, out, err io.Writer) *cobra.Command {
	cmds := &cobra.Command{
		Use:   "kubedb",
		Short: "Command line interface for KubeDB",
		Long: templates.LongDesc(`
      KubeDB by AppsCode - Kubernetes ready production-grade Databases

      Find more information at https://github.com/kubedb/cli.`),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			cli.SendAnalytics(cmd, v.Version.Version)
		},
		Run: runHelp,
	}
	var dbName string
	var namespace string
	var connectCmd = &cobra.Command{
		Use:   "connect",
		Short: "Connect to a DB pod",
		Long:  `All DBs have pods. Use this cmd to exec into a primary DB pod.`,
		Run: func(cmd *cobra.Command, args []string) {
			println("Connect to a DB pod")
			println("arg[0] = ", args[0])
			if args[0]==""{
				log.Fatal("Enter DB name as an argument")
			}
			dbName = args[0]
			println("DB name = ", dbName)
			connectToPg(namespace,dbName)
		},
	}
	cmds.AddCommand(connectCmd)
	//connectCmd.SetArgs([]string{""})
	//connectCmd.Flags().StringVarP(&dbName, "db-name", "d", "", "Name of the DB to connect to.")
	connectCmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the DB to connect to.")

	flags := cmds.PersistentFlags()
	// Normalize all flags that are coming from other packages or pre-configurations
	// a.k.a. change all "_" to "-". e.g. glog package
	flags.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)

	kubeConfigFlags := genericclioptions.NewConfigFlags(true)
	kubeConfigFlags.AddFlags(flags)
	matchVersionKubeConfigFlags := cmdutil.NewMatchVersionFlags(kubeConfigFlags)
	matchVersionKubeConfigFlags.AddFlags(flags)

	flags.AddGoFlagSet(flag.CommandLine)
	logs.ParseFlags()
	flags.BoolVar(&cli.EnableAnalytics, "enable-analytics", cli.EnableAnalytics, "Send analytical events to Google Analytics")

	f := cmdutil.NewFactory(matchVersionKubeConfigFlags)

	ioStreams := genericclioptions.IOStreams{In: in, Out: out, ErrOut: err}

	groups := templates.CommandGroups{
		{
			Message: "Basic Commands (Beginner):",
			Commands: []*cobra.Command{
				create.NewCmdCreate(f, ioStreams),
			},
		},
		{
			Message: "Basic Commands (Intermediate):",
			Commands: []*cobra.Command{
				get.NewCmdGet("kubedb", f, ioStreams),
				NewCmdEdit(f, ioStreams),
				NewCmdDelete(f, ioStreams),
			},
		},
		{
			Message: "Troubleshooting and Debugging Commands:",
			Commands: []*cobra.Command{
				NewCmdDescribe("kubedb", f, ioStreams),
				NewCmdApiResources(f, ioStreams),
				v.NewCmdVersion(),
			},
		},
	}
	groups.Add(cmds)
	templates.ActsAsRootCommand(cmds, nil, groups...)

	return cmds
}

func runHelp(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func connectToPg(ns string, dbName string) {
	masterURL := ""
	var podName string
	var secretName string
	kubeconfigPath := filepath.Join(homedir.HomeDir(), ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfigPath)
	if err != nil {
		log.Fatalf("Could not get Kubernetes config: %s", err)
	}

	// kubedb connect -it -n demo postgres quick-postgres

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalln(err)
	}
	if ns == ""{
		println("Enter namespace: -n=<your-namespace>")
		return
	}
	//if podName == ""{
	//	podName = "quick-postgres-0"
	//	secretName = "quick-postgres-auth"
	//}else {
	//	podName = dbName+"-0"
	//	secretName = dbName+"-auth"
	//}
	podName = dbName+"-0"
	secretName = dbName+"-auth"

	port := 5432
	fmt.Println("ns = ", ns, " podname = ",podName)

	_, err = client.CoreV1().Pods(ns).Get(podName, metav1.GetOptions{})
	if err != nil {
		if kerr.IsNotFound(err){
			fmt.Println("Pod doesn't exist")
		}
		return
	}

	tunnel := portforward.NewTunnel(client.CoreV1().RESTClient(), config, ns, podName, port)
	err = tunnel.ForwardPort()
	if err != nil {
		log.Fatalln(err)
	}
	defer tunnel.Close()

	auth, err := client.CoreV1().Secrets(ns).Get(secretName, metav1.GetOptions{})
	if err != nil {
		log.Fatalln(err)
	}



	sh := shell.NewSession()
	sh.SetEnv("PGPASSWORD", string(auth.Data["POSTGRES_PASSWORD"]))
	sh.ShowCMD = true
	err = sh.Command("docker", "run", "--network=host", "-it",
		"postgres:11.1-alpine",
		"psql",
		"--host=127.0.0.1",
		fmt.Sprintf("--port=%d", tunnel.Local),
		"--username=postgres").SetStdin(os.Stdin).Run()
	if err != nil {
		log.Fatalln(err)
	}
}
