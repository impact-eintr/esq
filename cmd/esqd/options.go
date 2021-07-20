package main

import (
	"flag"

	"github.com/impact-eintr/esq/esqd"
)

func esqFlagSet(opts *esqd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("esqd", flag.ExitOnError)

	flagSet.Bool("version", false, "print version")
	flagSet.String("config", "", "path to config file")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log level : debug info warn error fatal")
	flagSet.String("log-prefix", "[esqd]", "log message prefix")
}
