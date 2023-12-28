package main

import (
	"fmt"
	"os"
	"path/filepath"
)

var defaultHomeDir string

func main() {
	setup()
	if err := root(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func setup() {
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	defaultHomeDir = filepath.Join(homedir, ".mdb")
}
