package app

import (
	"github.com/spf13/pflag"
)

// CliOptions abstracts configuration options for reading parameters from the
// command line.
type CliOptions interface {
	// AddFlags adds flags to the specified FlagSet object.
	AddFlags(fs *pflag.FlagSet)

	// Validate would be called after init flags and configration file
	Validate() []error
}
