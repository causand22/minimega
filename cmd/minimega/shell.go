// Copyright (2012) Sandia Corporation.
// Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
// the U.S. Government retains certain rights in this software.

package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/sandia-minimega/minimega/v2/pkg/minicli"
	log "github.com/sandia-minimega/minimega/v2/pkg/minilog"
)

var shellCLIHandlers = []minicli.Handler{
	{ // shell
		HelpShort: "execute a command",
		HelpLong: `
Execute a command under the credentials of the running user.

Commands run until they complete or error, so take care not to execute a command
that does not return.`,
		Patterns: []string{
			"shell <command>...",
		},
		Call: wrapSimpleCLI(func(ns *Namespace, c *minicli.Command, resp *minicli.Response) error {
			return cliShell(c, resp)
		}),
	},
	{ // background
		HelpShort: "execute a command in the background",
		HelpLong: `
Execute a command under the credentials of the running user.

Commands run in the background and control returns immediately. Any output is
logged at the "info" level.`,
		Patterns: []string{
			"background <command>...",
		},
		Call: cliBackground,
	},
	{ // background status
		HelpShort: "Get the status of background commands",
		HelpLong: `
Get the status of a background command / commands.

To get the status of all background commands run

	background-status

To get the status of a specific command, run

	background-status [id]`,
		Patterns: []string{
			"background-status [id]",
		},
		Call: cliBackgroundStatus,
	},
	{ //clear background-status
		HelpShort: "Clear background-status information",
		Patterns: []string{
			"clear background-status",
		},
		Call: cliClearBackgroundStatus,
	},
}

func cliShell(c *minicli.Command, resp *minicli.Response) error {
	var sOut bytes.Buffer
	var sErr bytes.Buffer

	p, err := exec.LookPath(c.ListArgs["command"][0])
	if err != nil {
		return err
	}

	args := []string{p}
	if len(c.ListArgs["command"]) > 1 {
		args = append(args, c.ListArgs["command"][1:]...)
	}

	cmd := &exec.Cmd{
		Path:   p,
		Args:   args,
		Env:    nil,
		Dir:    "",
		Stdout: &sOut,
		Stderr: &sErr,
	}
	log.Info("starting: %v", args)
	if err := cmd.Start(); err != nil {
		return err
	}

	if err = cmd.Wait(); err != nil {
		return err
	}

	resp.Response = sOut.String()
	resp.Error = sErr.String()

	return nil
}

type BackgroundProcess struct {
	ID        int
	Command   *exec.Cmd
	Running   bool
	Error     error
	TimeStart time.Time
	TimeEnd   time.Time
}

func (bp BackgroundProcess) ToTabular() []string {
	errorsString := ""
	if bp.Error != nil {
		errorsString = bp.Error.Error()
	}
	return []string{
		strconv.FormatInt(int64(bp.ID), 10),
		strconv.FormatBool(bp.Running),
		errorsString,
		bp.TimeStart.Format("Jan 02 15:04:05 MST"),
		bp.TimeEnd.Format("Jan 02 15:04:05 MST"),
		bp.Command.String(),
	}
}

var (
	backgroundProcessesRWLock sync.RWMutex
	backgroundProcesses           = make(map[int]*BackgroundProcess)
	backgroundProcessNextID   int = 1

	backgroundProcessTableHeader = []string{"PID", "RUNNING", "ERROR", "TIME_START", "TIME_END", "COMMAND"}
)

func cliBackground(c *minicli.Command, respChan chan<- minicli.Responses) {

	var sOut bytes.Buffer
	var sErr bytes.Buffer

	p, err := exec.LookPath(c.ListArgs["command"][0])
	if err != nil {
		respChan <- minicli.Responses{&minicli.Response{Error: fmt.Sprintf("%v", err)}}
		return
	}

	args := []string{p}
	if len(c.ListArgs["command"]) > 1 {
		args = append(args, c.ListArgs["command"][1:]...)
	}

	cmd := &exec.Cmd{
		Path:   p,
		Args:   args,
		Env:    nil,
		Dir:    "",
		Stdout: &sOut,
		Stderr: &sErr,
	}

	backgroundProcessesRWLock.Lock()

	bp := &BackgroundProcess{
		ID:        backgroundProcessNextID,
		Command:   cmd,
		Running:   true,
		TimeStart: time.Now(),
	}
	backgroundProcessNextID += 1
	backgroundProcesses[bp.ID] = bp

	backgroundProcessesRWLock.Unlock()

	id := bp.ID

	log.Info("starting process id %v: %v", id, args)
	respChan <- minicli.Responses{&minicli.Response{
		Host:     hostname,
		Response: fmt.Sprintf("Started background process with id %d", id),
	}}

	go func() {
		err := cmd.Run()

		backgroundProcessesRWLock.Lock()
		backgroundProcesses[id].Running = false
		backgroundProcesses[id].Error = err
		backgroundProcesses[id].TimeEnd = time.Now()
		backgroundProcessesRWLock.Unlock()

		log.Info("command %v exited", args)
		if out := sOut.String(); out != "" {
			log.Info(out)
		}
		if err := sErr.String(); err != "" {
			log.Info(err)
		}

	}()
}

func cliBackgroundStatus(c *minicli.Command, respChan chan<- minicli.Responses) {
	idStr := c.StringArgs["id"]

	if idStr == "" {
		cliBackgroundStatusAll(respChan)
		return
	}

	idInt, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		respChan <- errResp(err)
		return
	}

	backgroundProcessesRWLock.RLock()
	defer backgroundProcessesRWLock.RUnlock()

	entry, ok := backgroundProcesses[int(idInt)]
	if !ok {
		respChan <- errResp(fmt.Errorf("provided id does not exist in background process table"))
		return
	}

	resp := &minicli.Response{
		Host:    hostname,
		Header:  backgroundProcessTableHeader,
		Tabular: [][]string{entry.ToTabular()},
	}
	respChan <- minicli.Responses{resp}

}

func cliBackgroundStatusAll(respChan chan<- minicli.Responses) {
	backgroundProcessesRWLock.RLock()
	defer backgroundProcessesRWLock.RUnlock()

	var table [][]string
	for _, entry := range backgroundProcesses {
		table = append(table, entry.ToTabular())
	}

	resp := &minicli.Response{
		Host:    hostname,
		Header:  backgroundProcessTableHeader,
		Tabular: table,
	}
	respChan <- minicli.Responses{resp}

}

func cliClearBackgroundStatus(c *minicli.Command, respChan chan<- minicli.Responses) {
	backgroundProcessesRWLock.Lock()
	backgroundProcessesRWLock.Unlock()

	var keys []int
	for key, entry := range backgroundProcesses {
		if !entry.Running {
			keys = append(keys, key)
		}
	}

	if len(keys) == len(backgroundProcesses) {
		backgroundProcesses = make(map[int]*BackgroundProcess)
	} else {
		for _, key := range keys {
			delete(backgroundProcesses, key)
		}

	}

	resp := &minicli.Response{
		Host:     hostname,
		Response: fmt.Sprintf("Cleared %d elements from background-status", len(keys)),
	}

	respChan <- minicli.Responses{resp}
}
