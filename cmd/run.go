package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"hammer/core"
)


func init() {
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run hammer job locally",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(args)
		filename := ""
		if len(args) < 1 {
			log.Fatalln("filename is needed")
		}
		filename = args[0]
		fmt.Println(filename)
		core.RunPipeline(filename)
	},
}

