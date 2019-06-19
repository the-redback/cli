package databases

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	shell "github.com/codeskyblue/go-sh"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
)

func addPostgresCMD(cmds *cobra.Command) {
	var pgPort = 5432
	var pgName string
	var dbname string
	var namespace string
	var secretName string
	var fileName string
	var command string
	var pgCmd = &cobra.Command{
		Use:   "postgres",
		Short: "Use to operate postgres pods",
		Long:  `Use this cmd to operate postgres pods.`,
		Run: func(cmd *cobra.Command, args []string) {
			println("Use subcommands such as connect or apply to operate PSQL pods")
		},
	}
	var pgConnectCmd = &cobra.Command{
		Use:   "connect",
		Short: "Connect to a postgres object's pod",
		Long:  `Use this cmd to exec into a postgres object's primary pod.`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				log.Fatal("Enter postgres object's name as an argument")
			}
			pgName = args[0]
			//pgConnect(namespace, pgName)
			//TODO: proper podname secretname extraction from postgres
			podName := pgName+"-0"
			secretName := pgName+"-auth"
			auth, tunnel, err := tunnelToDBPod(pgPort, namespace, podName, secretName)
			if err != nil {
				log.Fatal("Couldn't tunnel through. Error = ", err)
			}
			pgConnect(auth, tunnel.Local)
			tunnel.Close()
		},
	}

	var pgApplyCmd = &cobra.Command{
		Use:   "apply",
		Short: "Apply SQL commands to a postgres object's pod",
		Long: `Use this cmd to apply SQL commands from a file to a postgres object's primary pod.
				Syntax: $ kubedb postgres apply <pg-object-name> -n <namespace> -f <fileName>
				`,
		Run: func(cmd *cobra.Command, args []string) {
			println("Applying SQl")
			if len(args) == 0 {
				log.Fatal("Enter postgres object's name as an argument. Your commands will be applied on a database inside it's primary pod")
			}
			pgName = args[0]

			if fileName == "" && command == "" {
				log.Fatal(" Use --file or --command to apply supported commands to a postgres object's pods")
			}

			//TODO: proper podname secretname extraction from postgres
			podName := pgName+"-0"
			secretName := pgName+"-auth"
			auth, tunnel, err := tunnelToDBPod(pgPort, namespace, podName, secretName)
			if err != nil {
				log.Fatal("Couldn't tunnel through. Error = ", err)
			}

			if command != "" {
				pgApplyCommand(auth, tunnel.Local,dbname, command)
			}

			if fileName != "" {
				pgApplySql(auth, tunnel.Local, fileName)
			}

			tunnel.Close()
		},
	}

	//var pgCreateDbCmd = &cobra.Command{
	//	Use:   "createdb",
	//	Short: "Create logical database inside a postgres database pod",
	//	Long: `Use this cmd to applyreate logical database inside a postgres DB primary pod.
	//			Syntax: $ kubedb postgres createdb <logical-db-name> -n <namespace> <pg-name>
	//			`,
	//	Run: func(cmd *cobra.Command, args []string) {
	//		println("Creating Logical DataBase")
	//		if len(args) < 2 {
	//			log.Fatal("Enter names of logical database and its parent postgres database. Your logical database will be created" +
	//				" in a database inside postgres' primary pod")
	//		}
	//		dbName = args[0]
	//		pgName = args[1]
	//
	//		auth, tunnel, err := tunnelToDBPod(namespace, pgName)
	//		if err != nil {
	//			log.Fatal("Couldn't tunnel through. Error = ", err)
	//		}
	//
	//		if command != "" {
	//			pgApplyCommand(auth, tunnel.Local, command, dbname)
	//		}
	//
	//		if fileName != "" {
	//			pgApplySql(auth, tunnel.Local, fileName)
	//		}
	//
	//		tunnel.Close()
	//	},
	//}

	cmds.AddCommand(pgCmd)
	pgCmd.AddCommand(pgConnectCmd)
	pgCmd.AddCommand(pgApplyCmd)
	//pgCmd.AddCommand(pgCreateDbCmd)
	pgCmd.PersistentFlags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the postgres object to connect to.")
	pgCmd.PersistentFlags().StringVarP(&secretName, "customsecret", "", "", "Name of custom secret of the postgres object to connect to.")

	//pgApplyCmd.Flags().StringVarP(&secretName, "secret", "s", "", "Name of user created secret for the postgres object.")
	pgApplyCmd.Flags().StringVarP(&fileName, "file", "f", "", "path to sql file")
	pgApplyCmd.Flags().StringVarP(&command, "command", "c", "", "command to execute")
	pgApplyCmd.Flags().StringVarP(&dbname, "dbname", "d", "postgres", "name of database inside postgres object's pod to execute command")
}

//func tunnelToPod(namespace string, dbCrdName string, customSecretName string) (*v1.Secret, *portforward.Tunnel, error) {
//	//TODO: Always close the tunnel after using thing function
//	masterURL := ""
//	var podName string
//	var secretName string
//	kubeconfigPath := filepath.Join(homedir.HomeDir(), ".kube", "config")
//
//	config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfigPath)
//	if err != nil {
//		println("Could not get Kubernetes config: %s", err)
//		return nil, nil, err
//	}
//
//	// kubedb postgres connect -n demo  quick-postgres
//
//	client, err := kubernetes.NewForConfig(config)
//	if err != nil {
//		return nil, nil, err
//	}
//	if namespace == "" {
//		println("Using default namespace. Enter your namespace using -n=<your-namespace>")
//	}
//	podName = dbCrdName + "-0"
//
//	if customSecretName == "" {
//		secretName = dbCrdName + "-auth"
//	} else {
//		secretName = customSecretName
//	}
//
//	port := 5432
//
//	_, err = client.CoreV1().Pods(namespace).Get(podName, metav1.GetOptions{})
//	if err != nil {
//		if kerr.IsNotFound(err) {
//			fmt.Println("Pod doesn't exist")
//		}
//		return nil, nil, err
//	}
//	auth, err := client.CoreV1().Secrets(namespace).Get(secretName, metav1.GetOptions{})
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	tunnel := portforward.NewTunnel(client.CoreV1().RESTClient(), config, namespace, podName, port)
//	err = tunnel.ForwardPort()
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	return auth, tunnel, err
//}

func pgConnect(auth *v1.Secret, localPort int) {
	sh := shell.NewSession()
	sh.SetEnv("PGPASSWORD", string(auth.Data["POSTGRES_PASSWORD"]))
	sh.ShowCMD = true

	err := sh.Command("docker", "run", "--network=host", "-it",
		"postgres:11.1-alpine",
		"psql",
		"--host=127.0.0.1", fmt.Sprintf("--port=%d", localPort),
		fmt.Sprintf("--username=%s", auth.Data["POSTGRES_USER"])).SetStdin(os.Stdin).Run()
	if err != nil {
		log.Fatalln(err)
	}
}

func pgApplySql(auth *v1.Secret, localPort int, fileName string) {
	sh := shell.NewSession()
	sh.SetEnv("PGPASSWORD", string(auth.Data["POSTGRES_PASSWORD"]))
	sh.ShowCMD = true

	fileName, err := filepath.Abs(fileName)
	if err != nil {
		log.Fatalln(err)
	}

	err = sh.Command("docker", "run", "--network=host", "-it", "-v",
		fmt.Sprintf("%s:/tmp/pgsql.sql", fileName),
		"postgres:11.1-alpine",
		"psql",
		"--host=127.0.0.1", fmt.Sprintf("--port=%d", localPort),
		fmt.Sprintf("--username=%s", auth.Data["POSTGRES_USER"]),
		"--file=/tmp/pgsql.sql").SetStdin(os.Stdin).Run()
	if err != nil {
		log.Fatalln(err)
	}
}

func pgApplyCommand(auth *v1.Secret, localPort int, dbname string,  command string) {
	sh := shell.NewSession()
	sh.SetEnv("PGPASSWORD", string(auth.Data["POSTGRES_PASSWORD"]))

	sh.ShowCMD = true

	err := sh.Command("docker", "run", "--network=host", "-it",
		"postgres:11.1-alpine",
		"psql",
		"--host=127.0.0.1", fmt.Sprintf("--port=%d", localPort),
		fmt.Sprintf("--dbname=%s", dbname),
		fmt.Sprintf("--username=%s", auth.Data["POSTGRES_USER"]),
		fmt.Sprintf("--command=%s", command)).SetStdin(os.Stdin).Run()
	if err != nil {
		log.Fatalln(err)
	}
}
