package main

import (
	"github.com/codegangsta/cli"
)

var flags = []cli.Flag{
	cliTempFile,
}

var cliTempFile = cli.StringFlag{
	Name:   "tempfile, t",
	Value:  "/tmp/mackerel-plugin-linux-cpus",
	Usage:  "Set temporary file path.",
	EnvVar: "ENVVAR_TEMPFILE",
}
