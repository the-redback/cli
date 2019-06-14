package databases

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	v1 "k8s.io/api/core/v1"

	shell "github.com/codeskyblue/go-sh"
	"github.com/spf13/cobra"
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
			auth, tunnel, err := tunnelToDBPod(mysqlPort, namespace, mysqlName, secretName)
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

			auth, tunnel, err := tunnelToDBPod(mysqlPort, namespace, mysqlName, secretName)
			if err != nil {
				log.Fatal("Couldn't tunnel through. Error = ", err)
			}

			if command != "" {
				mysqlApplyCommand(auth, tunnel.Local, command, dbname)
			}

			if fileName != "" {
				mysqlApplySql(auth, tunnel.Local, fileName, dbname)
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
	sh.ShowCMD = true
	pass := string(auth.Data["password"])
	err := sh.Command("docker", "run",
		"-e", fmt.Sprintf("MYSQL_PWD=%s", pass),
		"--network=host", "-it", "mysql",
		"mysql",
		"--host=127.0.0.1",
		fmt.Sprintf("--port=%d", localPort),
		fmt.Sprintf("--user=%s", username)).SetStdin(os.Stdin).Run()
	if err != nil {
		log.Fatalln(err)
	}
}

func mysqlApplySql(auth *v1.Secret, localPort int, fileName string, dbname string) {
	sh := shell.NewSession()
	sh.ShowCMD = false

	fileName, err := filepath.Abs(fileName)
	if err != nil {
		log.Fatalln(err)
	}
	var reader io.Reader
	reader, err = os.Open(fileName)
	if err != nil {
		log.Fatalln(err)
	}

	err = sh.Command("mysql",
		"--host=127.0.0.1", fmt.Sprintf("--port=%d", localPort),
		fmt.Sprintf("--user=%s", auth.Data["username"]),
		fmt.Sprintf("--password=%s", auth.Data["password"]), dbname).SetStdin(reader).Run()
	if err != nil {
		log.Fatalln(err)
	}
	println("Commands applied from file")
}

func mysqlApplyCommand(auth *v1.Secret, localPort int, command string, dbname string) {
	sh := shell.NewSession()
	sh.ShowCMD = false

	var reader io.Reader
	reader = strings.NewReader(command)

	err := sh.Command("mysql",
		"--host=127.0.0.1", fmt.Sprintf("--port=%d", localPort),
		fmt.Sprintf("--user=%s", auth.Data["username"]),
		fmt.Sprintf("--password=%s", auth.Data["password"]), dbname).SetStdin(reader).Run()
	if err != nil {
		log.Fatalln(err)
	}
	println("Command applied")
}
