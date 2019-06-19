package databases

import (
	"fmt"
	shell "github.com/codeskyblue/go-sh"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"log"
	"os"
	"path/filepath"
)

func addMysqlCMD(cmds *cobra.Command) {
	var mysqlPort = 3306
	var mysqlName string
	var dbname string
	var namespace string
	var userName string
	var secretName string
	var fileName string
	var command string
	var mysqlCmd = &cobra.Command{
		Use:   "mysql",
		Short: "Use to operate mysql pods",
		Long:  `Use this cmd to operate mysql pods.`,
		Run: func(cmd *cobra.Command, args []string) {
			println("Use subcommands such as connect or apply to operate mysql pods")
		},
	}
	var mysqlConnectCmd = &cobra.Command{
		Use:   "connect",
		Short: "Connect to a mysql object's pod",
		Long:  `Use this cmd to exec into a mysql object's primary pod.`,
		Run: func(cmd *cobra.Command, args []string) {
			println("Connect to a mysql pod")
			if len(args) == 0 {
				log.Fatal("Enter mysql object's name as an argument")
			}
			mysqlName = args[0]
			//mysqlConnect(namespace, mysqlName)
			//TODO: proper podname secretname extraction from mysql
			podName := mysqlName+"-0"
			secretName := mysqlName+"-auth"
			auth, tunnel, err := tunnelToDBPod(mysqlPort, namespace, podName, secretName)
			if err != nil {
				log.Fatal("Couldn't tunnel through. Error = ", err)
			}
			mysqlConnect(auth, tunnel.Local, userName)
			tunnel.Close()
		},
	}

	var mysqlApplyCmd = &cobra.Command{
		Use:   "apply",
		Short: "Apply SQL commands to a mysql pod",
		Long: `Use this cmd to apply SQL commands from a file to a mysql object's' primary pod.
				Syntax: $ kubedb mysql apply <mysql-name> -n <namespace> -f <fileName>
				`,
		Run: func(cmd *cobra.Command, args []string) {
			println("Applying SQl")
			if len(args) == 0 {
				log.Fatal("Enter mysql object's name as an argument. Your commands will be applied on a database inside it's primary pod")
			}
			mysqlName = args[0]
			//TODO: proper podname secretname extraction from mysql
			podName := mysqlName+"-0"
			secretName := mysqlName+"-auth"
			auth, tunnel, err := tunnelToDBPod(mysqlPort, namespace, podName, secretName)
			if err != nil {
				log.Fatal("Couldn't tunnel through. Error = ", err)
			}

			if command != "" {
				mysqlApplyCommand(auth, tunnel.Local, dbname, command)
			}

			if fileName != "" {
				mysqlApplyFile(auth, tunnel.Local, dbname, fileName)
			}

			if fileName == "" && command == "" {
				log.Fatal(" Use --file or --command to apply SQL to mysql database pods")
			}

			tunnel.Close()
		},
	}

	cmds.AddCommand(mysqlCmd)
	mysqlCmd.AddCommand(mysqlConnectCmd)
	mysqlCmd.AddCommand(mysqlApplyCmd)
	mysqlCmd.PersistentFlags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the mysql object to connect to.")
	mysqlCmd.PersistentFlags().StringVarP(&userName, "username", "u", "root", "Username of the mysql object to connect to.")
	mysqlCmd.PersistentFlags().StringVarP(&secretName, "customsecret", "", "", "Name of custom secret of the mysql object to connect to.")

	mysqlApplyCmd.Flags().StringVarP(&fileName, "file", "f", "", "path to sql file")
	mysqlApplyCmd.Flags().StringVarP(&command, "command", "c", "", "command to execute")
	mysqlApplyCmd.Flags().StringVarP(&dbname, "dbname", "d", "mysql", "name of database inside mysql-db pod to execute command")

}

func mysqlConnect(auth *v1.Secret, localPort int, username string) {
	sh := shell.NewSession()
	sh.ShowCMD = false
	err := sh.Command("docker", "run",
		"-e", fmt.Sprintf("MYSQL_PWD=%s", auth.Data["password"]),
		"--network=host", "-it", "mysql",
		"mysql",
		"--host=127.0.0.1", fmt.Sprintf("--port=%d", localPort),
		fmt.Sprintf("--user=%s", username)).SetStdin(os.Stdin).Run()
	if err != nil {
		log.Fatalln(err)
	}
}

func mysqlApplyFile(auth *v1.Secret, localPort int, dbname string, fileName string) {
	fileName, err := filepath.Abs(fileName)
	if err != nil {
		log.Fatalln(err)
	}
	tempFileName := "/tmp/my.sql"

	println("Applying commands from file: ", fileName)
	sh := shell.NewSession()
	err = sh.Command("docker", "run",
		"--network=host",
		"-e", fmt.Sprintf("MYSQL_PWD=%s", auth.Data["password"]),
		"-v", fmt.Sprintf("%s:%s", fileName, tempFileName), "mysql",
		"mysql",
		"--host=127.0.0.1", fmt.Sprintf("--port=%d", localPort),
		fmt.Sprintf("--user=%s", auth.Data["username"]),
		"-e", fmt.Sprintf("source %s", tempFileName),
	).SetStdin(os.Stdin).Run()
	if err != nil {
		log.Fatalln(err)
	}
	println("Commands applied")
}

func mysqlApplyCommand(auth *v1.Secret, localPort int, dbname string, command string) {
	println("Applying command(s): ", command)
	sh := shell.NewSession()
	err := sh.Command("docker", "run",
		"-e", fmt.Sprintf("MYSQL_PWD=%s", auth.Data["password"]),
		"--network=host", "mysql",
		"mysql",
		"--host=127.0.0.1", fmt.Sprintf("--port=%d", localPort),
		fmt.Sprintf("--user=%s", auth.Data["username"]),
		dbname, "-e",command,
	).SetStdin(os.Stdin).Run()
	if err != nil {
		log.Fatalln(err)
	}

	println("Commands applied")
}
