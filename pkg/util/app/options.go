package app

import (
	"github.com/spf13/pflag"
)

// CliOptions abstracts configuration options for reading parameters from the
// command line.
type CliOptions interface {
	// AddFlags adds flags to the specified FlagSet object.
	AddFlags(fs *pflag.FlagSet)
}

// ConfigurableOptions abstracts configuration options for reading parameters
// from a configuration file.
type ConfigurableOptions interface {
	// ApplyFlags parsing parameters from the command line or configuration file
	// to the options instance.
	ApplyFlags() []error
}

// OptionValidater abstracts option validater.
type OptionValidater interface {
	Validate() []error
}
