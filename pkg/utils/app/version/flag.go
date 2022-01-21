package version

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/pflag"
)

const flagName = "version"
const flagShortHand = "V"

type value int

const (
	boolFalse value = 0
	boolTrue  value = 1
	allInfo   value = 3

	strAllVersionInfo string = "all"
)

var (
	v = boolFalse
)

func (v *value) Set(s string) error {
	if s == strAllVersionInfo {
		*v = allInfo
		return nil
	}
	boolVal, err := strconv.ParseBool(s)
	if boolVal {
		*v = boolTrue
	} else {
		*v = boolFalse
	}
	return err
}
func (v *value) String() string {
	return fmt.Sprintf("%v", bool(*v == boolTrue))
}

// The type of the flag as required by the pflag.value interface
func (v *value) Type() string {
	return "version"
}

// AddFlags registers this package's flags on arbitrary FlagSets, such that they
// point to the same value as the global flags.
func AddFlags(fs *pflag.FlagSet) {
	fs.VarP(&v, flagName, flagShortHand, "Print version information and quit.")
	// "--version" will be treated as "--version=true"
	fs.Lookup(flagName).NoOptDefVal = "true"
}

// PrintAndExitIfRequested will check if the -version flag was passed and, if so,
// print the version and exit.
func PrintAndExitIfRequested(appName string) {
	if v == allInfo {
		fmt.Printf("%s\n", Get())
		os.Exit(0)
	} else if v == boolTrue {
		fmt.Printf("%s %s\n", appName, Get().GitVersion)
		os.Exit(0)
	}
}
