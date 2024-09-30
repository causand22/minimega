package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"sync"

	"github.com/sandia-minimega/minimega/v2/pkg/minicli"
	log "github.com/sandia-minimega/minimega/v2/pkg/minilog"
)

const MAX_MESH_BACKGROUND_PROCESS = 16

type BackgroundProcess struct {
	PID     int32
	Command exec.Cmd
	Running bool
	Error   error
}

func (bp BackgroundProcess) ToTabular() []string {
	errorsString := "no"
	if bp.Error != nil {
		errorsString = "yes"
	}
	return []string{
		strconv.FormatInt(int64(bp.PID), 10),
		strconv.FormatBool(bp.Running),
		errorsString,
		bp.Command.String(),
	}
}

var (
	backgroundProcesses       = make(map[int]*BackgroundProcess)
	backgroundProcessesRWLock sync.RWMutex
)

func meshageBackgroundHandler() {
	for {
		m := <-meshageBackgroundChan
		go func() {
			mCmd := m.Body.(meshageBackground)
			if mCmd.Status { //background status
				pid, err := getStatusPIDFromString(mCmd.Command)
				if err != nil {

				}
				header := []string{"PID", "RUNNING", "ERROR", "COMMAND"}
				status, err := getMeshBackgroundStatus(pid)

				response := minicli.Response{
					Host:    hostname,
					Header:  header,
					Tabular: status,
				}
				resp := meshageResponse{Response: response, TID: mCmd.TID}
				_, err = meshageNode.Set([]string{m.Source}, resp)
				if err != nil {
					log.Errorln(err)
				}

			} else { //background start
				mesh_pid := startNewProcess(mCmd.Command)
				msg := fmt.Sprintf("Process started with ID: %d", mesh_pid)
				response := minicli.Response{
					Host:     hostname,
					Response: fmt.Sprintf(msg),
				}

				resp := meshageResponse{Response: response, TID: mCmd.TID}
				recipientList := []string{m.Source}

				_, err := meshageNode.Set(recipientList, resp)
				if err != nil {
					log.Errorln(err)
				}

			}
		}()
	}
}

func getStatusPIDFromString(command []string) (int32, error) {
	if len(command) != 1 {
		return -1, fmt.Errorf("Error: len command is not 1")
	}

	if command[0] == "" {
		return 0, nil
	}

	num, err := strconv.ParseInt(command[0], 10, 32)
	if err != nil {
		return -1, fmt.Errorf("error parsing string: %v", err)
	}

	if num > MAX_MESH_BACKGROUND_PROCESS || num < 0 {
		return -1, fmt.Errorf("error: supplied int out of range")
	}

	return int32(num), nil
}

func getMeshBackgroundStatus(pid int32) ([][]string, error) {
	var response [][]string
	if pid > MAX_MESH_BACKGROUND_PROCESS || pid < 0 {
		return response, fmt.Errorf("Supplied PID is out of range: %v", pid)
	}

	backgroundProcessesRWLock.RLock()
	defer backgroundProcessesRWLock.RUnlock()

	pidInt := int(pid)

	if pid != 0 {
		return [][]string{backgroundProcesses[pidInt].ToTabular()}, nil
	}
	for _, entry := range backgroundProcesses {
		response = append(response, entry.ToTabular())
	}

	return response, nil
}

func startNewProcess(command []string) int32 {
	pid := getNextPID()
	if pid == -1 {
		//no space
		return pid
	}
	go func() {
		backgroundProcessesRWLock.Lock()
		backgroundProcesses[int(pid)] = &BackgroundProcess{
			PID:     pid,
			Command: *exec.Command(command[0], command[1:]...),
			Running: true,
		}
		backgroundProcessesRWLock.Unlock()

		err := backgroundProcesses[int(pid)].Command.Run()

		backgroundProcessesRWLock.Lock()
		backgroundProcesses[int(pid)].Running = false
		backgroundProcesses[int(pid)].Error = err
		backgroundProcessesRWLock.Unlock()
	}()
	return pid
}

func getNextPID() int32 {
	for i := 1; i <= MAX_MESH_BACKGROUND_PROCESS; i++ {
		if _, ok := backgroundProcesses[i]; !ok {
			return int32(i)
		}
	}
	return -1
}
