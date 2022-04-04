package app

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

// Command is a sub command structure of a cli application.
type Command struct {
	usage    string
	desc     string
	options  CliOptions
	commands []*Command
	runFunc  RunCommandFunc
}

type RunCommandFunc func(args []string) error
type CommandOption func(*Command)

func NewCommand(usage string, desc string, opts ...CommandOption) *Command {
	c := &Command{
		usage: usage,
		desc:  desc,
	}

	for _, o := range opts {
		o(c)
	}

	return c
}

func (c *Command) AddCommand(cmd *Command) {
	c.commands = append(c.commands, cmd)
}

func (c *Command) AddCommands(cmds ...*Command) {
	c.commands = append(c.commands, cmds...)
}

func (c *Command) cobraCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   c.usage,
		Short: c.desc,
	}
	cmd.SetOutput(os.Stdout)
	cmd.Flags().SortFlags = false
	if len(c.commands) > 0 {
		for _, command := range c.commands {
			cmd.AddCommand(command.cobraCommand())
		}
	}
	if c.runFunc != nil {
		cmd.Run = c.runCommand
	}
	if c.options != nil {
		c.options.AddFlags(cmd.Flags())
	}
	addHelpCommandFlag(c.usage, cmd.Flags())
	return cmd
}

func (c *Command) runCommand(cmd *cobra.Command, args []string) {
	if c.runFunc != nil {
		if err := c.runFunc(args); err != nil {
			fmt.Printf("%v %v\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
	}
}

// WithCommandOptions to open the application's function to read from the
// command line.
func WithCommandOptions(opt CliOptions) CommandOption {
	return func(c *Command) {
		c.options = opt
	}
}

// WithCommandRunFunc is used to set the application's command startup callback
// function option.
func WithCommandRunFunc(run RunCommandFunc) CommandOption {
	return func(c *Command) {
		c.runFunc = run
	}
}
