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
)

var (
	progressMessage = color.GreenString("==>")
)

// App is the main structure of a cli application.
type App struct {
	basename    string
	name        string
	description string
	options     CliOptions
	runFunc     RunFunc
	silence     bool
	noVersion   bool
	commands    []*Command
}

// RunFunc defines the application's startup callback function.
type RunFunc func(basename string) error

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

// AddCommand adds sub command to the application.
func (a *App) AddCommand(cmd *Command) {
	a.commands = append(a.commands, cmd)
}

// AddCommands adds multiple sub commands to the application.
func (a *App) AddCommands(cmds ...*Command) {
	a.commands = append(a.commands, cmds...)
}

// NewApp creates a new application instance based on the given application name,
// binary name, and other options.
func NewApp(name string, basename string, opts ...Option) *App {
	a := &App{
		name:     name,
		basename: basename,
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
		Use:           a.basename,
		Long:          a.description,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	//cmd.SetUsageTemplate(usageTemplate) //
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

	cmd.Flags().AddGoFlagSet(flag.CommandLine)
	if a.options != nil {
		if _, ok := a.options.(ConfigurableOptions); ok {
			addConfigFlag(a.basename, cmd.Flags())
		}
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

	// merge configuration and print it
	if a.options != nil {
		configurableOptions := a.options.(ConfigurableOptions)
		validater := a.options.(OptionValidater)
		if !a.silence && (configurableOptions != nil || validater != nil) {
			printConfig()
		}

		if configurableOptions != nil {
			if errs := configurableOptions.ApplyFlags(); len(errs) > 0 {
				for _, err := range errs {
					fmt.Printf("%v %v\n", color.RedString("Error:"), err)
				}
				os.Exit(1)
			}
		}

		if validater != nil {
			if errs := validater.Validate(); len(errs) > 0 {
				for _, err := range errs {
					fmt.Printf("%v %v\n", color.RedString("Error:"), err)
				}
				os.Exit(1)
			}
		}
	}

	if !a.silence && !a.noVersion {
		fmt.Printf("%v Version:\n", progressMessage)
		fmt.Printf("%s\n", version.Get())
	}

	if a.runFunc != nil {
		if err := a.runFunc(a.basename); err != nil {
			fmt.Printf("%v %v\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
	}
}
