package app

import (
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/gosuri/uitable"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var cfgFile string

// addConfigFlag adds flags for a specific server to the specified FlagSet
// object.
func addConfigFlag(basename string, fs *pflag.FlagSet) {
	viper.SetEnvPrefix(strings.Replace(strings.ToUpper(basename), "-", "_", -1))
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	cobra.OnInitialize(initForCobra)
	fs.StringVarP(&cfgFile, "config", "C", cfgFile,
		"Read configuration from specified `FILE`, support JSON, TOML, YAML, HCL, or Java properties formats.")
}

func initForCobra() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)

		if err := viper.ReadInConfig(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error: failed to read configuration file(%s): %v\n", cfgFile, err)
			os.Exit(1)
		}
	}
}

func printConfig() {
	keys := viper.AllKeys()
	if len(keys) > 0 {
		fmt.Printf("%v Configuration items:\n", color.GreenString("==>"))
		table := uitable.New()
		table.Separator = " "
		table.MaxColWidth = 80
		table.RightAlign(0)
		for _, k := range keys {
			table.AddRow(fmt.Sprintf("%s:", k), viper.Get(k))
		}
		fmt.Println(table)
	}
}

func UnmarshalConfig(rawVal interface{}) error {
	return viper.Unmarshal(rawVal)
}
