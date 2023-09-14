package repl

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	uuid "github.com/google/uuid"
)

// REPL struct.
type REPL struct {
	commands map[string]func(string, *REPLConfig) error
	help     map[string]string
}

// REPL Config struct.
type REPLConfig struct {
	writer   io.Writer
	clientId uuid.UUID
}

// Get writer.
func (replConfig *REPLConfig) GetWriter() io.Writer {
	return replConfig.writer
}

// Get address.
func (replConfig *REPLConfig) GetAddr() uuid.UUID {
	return replConfig.clientId
}

// Construct an empty REPL.
func NewRepl() *REPL {
	return &REPL{commands: make(map[string]func(string, *REPLConfig) error), help: make(map[string]string)}
}

// Combines a slice of REPLs.
func CombineRepls(repls []*REPL) (*REPL, error) {
	var frequency_map = make(map[string]int)
	var union_map = make(map[string]func(string, *REPLConfig) error)
	var help_map = make(map[string]string)
	if len(repls) == 0 {
		return NewRepl(), errors.New("No REPL found")
	} else {
		for i := 0; i < len(repls); i++ {
				for key := range repls[i].commands {
					_, exists := frequency_map[key]
					if exists {
						return nil, errors.New("Found overlapping triggers")
					} else {
						union_map[key] = repls[i].commands[key]
						help_map[key] = repls[i].help[key]
				}
			}
		}
	}
	return &REPL{commands: union_map, help: help_map}, nil
}

// Get commands.
func (r *REPL) GetCommands() map[string]func(string, *REPLConfig) error {
	return r.commands
}

// Get help.
func (r *REPL) GetHelp() map[string]string {
	return r.help
}

// Add a command, along with its help string, to the set of commands.
func (r *REPL) AddCommand(trigger string, action func(string, *REPLConfig) error, help string) {
	r.commands[trigger] = action
	r.help[trigger] = help
}

// Return all REPL usage information as a string.
func (r *REPL) HelpString() string {
	var sb strings.Builder
	for k, v := range r.help {
		sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
	}
	return sb.String()
}

// Run the REPL.
func (r *REPL) Run(c net.Conn, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	var reader io.Reader
	var writer io.Writer
	if c == nil {
		reader = os.Stdin
		writer = os.Stdout
	} else {
		reader = c
		writer = c
	}
	scanner := bufio.NewScanner((reader))
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	// Begin the repl loop!
	for {
		// Prompt string
		fmt.Println("Enter a command to the terminal: ")
		scanner.Scan()
		prompt = scanner.Text()
		fmt.Println(prompt)
		prompt = cleanInput(prompt)
		if prompt == "EOF" {
			fmt.Println("Exiting REPL...")
			break
		}
		// Tokenize user inputted prompt to find trigger
		split_input := strings.Split(prompt, " ")
		trigger := split_input[0]
		f, exists := r.commands[trigger]
		if exists {
			f(prompt, replConfig)
		} else if trigger == ".help" {
			for key, _ := range r.commands {
				fmt.Println(key)
			}
		}
	}
}

// Run the REPL.
func (r *REPL) RunChan(c chan string, clientId uuid.UUID, prompt string) {
	panic("function not yet implemented");
}

// cleanInput preprocesses input to the db repl.
func cleanInput(text string) string {
	output := strings.TrimSpace(text)
	output = strings.ToLower(output)
	return output
}
