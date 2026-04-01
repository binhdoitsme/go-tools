package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	rootCmd = &cobra.Command{
		Use: "init",
		Run: initProject,
	}
)

func createGoModFile(version string, moduleName string, path string) error {
	content := fmt.Sprintf("module %s\n\ngo %s", moduleName, version)
	return os.WriteFile(fmt.Sprintf("%s/go.mod", path), []byte(content), 0666)
}

func initProject(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		fmt.Println("module name must be provided")
		os.Exit(1)
	}
	viper.BindEnv("SCM")

	re := regexp.MustCompile(`go([0-9]+(?:\.[0-9]+(?:\.[0-9]+)?)?)( .*)?`)
	versionOutput, err := exec.Command("go", "version").Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	versionMatch := re.FindStringSubmatch(string(versionOutput))
	if versionMatch == nil {
		fmt.Printf("error getting version: %s\n", versionOutput)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	version := versionMatch[1]

	projectName := args[0]
	scmBaseUrl, err := cmd.Flags().GetString("scm")
	if len(scmBaseUrl) == 0 {
		scmBaseUrl = viper.GetString("SCM")
	}
	if len(scmBaseUrl) == 0 {
		fmt.Println("SCM URL must be provided either via envvar or --scm flag")
		os.Exit(1)
	}
	moduleName := fmt.Sprintf("%s/%s", scmBaseUrl, projectName)
	fmt.Printf("Creating module %s\n", moduleName)
	// create subfolder
	os.Mkdir(projectName, os.ModePerm)
	wd, _ := os.Getwd()
	gomodPath := path.Join(wd, projectName)
	err = createGoModFile(version, moduleName, gomodPath)
	if err != nil {
		fmt.Println("error creating go mod file")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Env: export SCM=github.com/binhdoitsme
// Cmd: go-init --scm=$SCM
func main() {
	rootCmd.PersistentFlags().String("scm", "", "SCM URL (e.g. github.com/your_profile)")
	// rootCmd.PersistentFlags().StringP("output", "o", "", "Output path")
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
