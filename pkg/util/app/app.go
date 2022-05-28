package app

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"Laputa/pkg/util/app/version"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	progressMessage = color.GreenString("==>")
	usageTemplate   = fmt.Sprintf(`%s{{if .Runnable}}
  %s{{end}}{{if .HasAvailableSubCommands}}
  %s{{end}}{{if gt (len .Aliases) 0}}

%s
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

%s
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

%s{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  %s {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

%s
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

%s
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

%s{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "%s --help" for more information about a command.{{end}}
`,
		color.CyanString("Usage:"),
		color.GreenString("{{.UseLine}}"),
		color.GreenString("{{.CommandPath}} [command]"),
		color.CyanString("Aliases:"),
		color.CyanString("Examples:"),
		color.CyanString("Available Commands:"),
		color.GreenString("{{rpad .Name .NamePadding }}"),
		color.CyanString("Flags:"),
		color.CyanString("Global Flags:"),
		color.CyanString("Additional help topics:"),
		color.GreenString("{{.CommandPath}} [command]"),
	)
)

// App is the main structure of a cli application.
// It is recommended that an app be created with the app.NewApp() function.
type App struct {
	name         string
	description  string
	options      CliOptions
	runFunc      RunFunc
	silence      bool
	noVersion    bool
	commands     []*Command
	configurable interface{}
}

// Option defines optional parameters for initializing the application
// structure.
type Option func(*App)

// WithOptions to open the application's function to read from the command line
// or read parameters from the configuration file.
func WithOptions(opt CliOptions) Option {
	return func(a *App) {
		a.options = opt
	}
}

// RunFunc defines the application's startup callback function.
type RunFunc func(basename string) error

// WithRunFunc is used to set the application startup callback function option.
func WithRunFunc(run RunFunc) Option {
	return func(a *App) {
		a.runFunc = run
	}
}

// WithDescription is used to set the description of the application.
func WithDescription(desc string) Option {
	return func(a *App) {
		a.description = desc
	}
}

// WithSilence sets the application to silent mode, in which the program startup
// information, configuration information, and version information are not
// printed in the console.
func WithSilence() Option {
	return func(a *App) {
		a.silence = true
	}
}

// WithNoVersion set the application does not provide version flag.
func WithNoVersion() Option {
	return func(a *App) {
		a.noVersion = true
	}
}

// WithConfiguration would Unmarshal configuration file into conf
func WithConfiguration(conf interface{}) Option {
	return func(a *App) {
		a.configurable = conf
	}
	// return viper.Unmarshal(conf)
}

// NewApp creates a new application instance based on the given application name,
// binary name, and other options.
func NewApp(name string, opts ...Option) *App {
	a := &App{
		name: name,
	}

	for _, o := range opts {
		o(a)
	}

	return a
}

// Run is used to launch the application.
func (a *App) Run() {
	rand.Seed(time.Now().UTC().UnixNano())

	initFlag()

	cmd := cobra.Command{
		Use:           FormatBaseName(a.name),
		Long:          a.description,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	cmd.SetUsageTemplate(usageTemplate)
	cmd.Flags().SortFlags = false
	if len(a.commands) > 0 {
		for _, command := range a.commands {
			cmd.AddCommand(command.cobraCommand())
		}
		cmd.SetHelpCommand(helpCommand(a.name))
	}
	if a.runFunc != nil {
		cmd.Run = a.runCommand
	}

	if a.configurable != nil {
		addConfigFlag(a.name, cmd.Flags())
	}

	cmd.Flags().AddGoFlagSet(flag.CommandLine)
	if a.options != nil {
		a.options.AddFlags(cmd.Flags())
	}

	if !a.noVersion {
		version.AddFlags(cmd.Flags())
	}
	addHelpFlag(a.name, cmd.Flags())

	if err := cmd.Execute(); err != nil {
		fmt.Printf("%v %v\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
}

func (a *App) runCommand(cmd *cobra.Command, args []string) {
	if !a.noVersion {
		version.PrintAndExitIfRequested(a.name)
	}
	if !a.silence {
		fmt.Printf("%v Starting %s...\n", progressMessage, a.name)
		wd, _ := os.Getwd()
		fmt.Printf("%v WorkingDir: %s\n", progressMessage, wd)
		fmt.Printf("%v Args: %v\n", progressMessage, os.Args)
	}

	if a.configurable != nil {
		if err := viper.Unmarshal(a.configurable); err != nil {
			fmt.Printf("%v %v\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		if !a.silence {
			printConfig()
		}
	}

	if a.options != nil {
		if errs := a.options.Validate(); len(errs) > 0 {
			for _, err := range errs {
				fmt.Printf("%v %v\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}

	}

	if !a.silence && !a.noVersion {
		fmt.Printf("%v Version:\n", progressMessage)
		fmt.Printf("%s\n", version.Get())
	}

	if a.runFunc != nil {
		if err := a.runFunc(a.name); err != nil {
			fmt.Printf("%v %v\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
	}
}

// AddCommand adds sub command to the application.
func (a *App) AddCommand(cmd *Command) {
	a.commands = append(a.commands, cmd)
}

// AddCommands adds multiple sub commands to the application.
func (a *App) AddCommands(cmds ...*Command) {
	a.commands = append(a.commands, cmds...)
}
