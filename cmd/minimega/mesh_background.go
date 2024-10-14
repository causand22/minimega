package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/sandia-minimega/minimega/v2/internal/meshage"
	"github.com/sandia-minimega/minimega/v2/pkg/minicli"
	log "github.com/sandia-minimega/minimega/v2/pkg/minilog"
)

const MAX_MESH_BACKGROUND_PROCESS = 16
const DELETE_DONE_PROCESS_MINS = 60.0

const (
	MESH_BG_START int32 = iota
	MESH_BG_STATUS
)

type BackgroundProcess struct {
	PID       int32
	Command   exec.Cmd
	Running   bool
	Error     error
	TimeStart time.Time
	TimeEnd   time.Time
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
		bp.TimeStart.Format("Jan 02 15:04:05 MST"),
		bp.TimeEnd.Format("Jan 02 15:04:05 MST"),
		bp.Command.String(),
	}
}

var (
	backgroundProcesses       = make(map[int]*BackgroundProcess)
	backgroundProcessesRWLock sync.RWMutex
	// backgroundProcessHistory  []*BackgroundProcess
)

func meshageBackgroundHandler() {
	for {
		m := <-meshageBackgroundChan
		go handleMeshageBackgroundRequest(m)
	}
}

func meshBackgroundRespondError(errorMsg string, tid int32, dest []string) {

	response := minicli.Response{
		Host:  hostname,
		Error: errorMsg,
	}
	resp := meshageResponse{Response: response, TID: tid}
	_, err := meshageNode.Set(dest, resp)
	if err != nil {
		log.Errorln(err)
	}
	return
}

func handleMeshageBackgroundRequest(m *meshage.Message) {
	mCmd := m.Body.(meshageBackground)
	switch mCmd.Type {
	case MESH_BG_STATUS:
		pid, err := getStatusPIDFromString(mCmd.Command)
		if err != nil {
			meshBackgroundRespondError("No valid status PID provided", mCmd.TID, []string{m.Source})
			return
		}
		header := []string{"PID", "RUNNING", "ERROR", "TIME_START", "TIME_END", "COMMAND"}
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
		return

	case MESH_BG_START:
		mesh_pid := startNewProcess(mCmd.Command)
		if mesh_pid == -1 {
			meshBackgroundRespondError("Cannot start a new process. Too many processes running or waiting for clearing. Run `mesh background status`", mCmd.TID, []string{m.Source})
			return
		}

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
	default:
		meshBackgroundRespondError("Error: invalid mesh request provided", mCmd.TID, []string{m.Source})
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
		tabular := backgroundProcesses[pidInt].ToTabular()
		if backgroundProcesses[pidInt].Running == false {
			delete(backgroundProcesses, pidInt)
		}
		return [][]string{tabular}, nil
	}

	var keysToDelete []int

	for key, entry := range backgroundProcesses {
		response = append(response, entry.ToTabular())
		if !entry.Running {
			keysToDelete = append(keysToDelete, key)
		}
	}

	//flush out of table after status read
	for _, key := range keysToDelete {
		delete(backgroundProcesses, key)
	}

	return response, nil
}

func startNewProcess(command []string) int32 {
	pid := getNextPID()
	if pid == -1 {
		return pid
	}
	go func() {
		backgroundProcessesRWLock.Lock()
		backgroundProcesses[int(pid)] = &BackgroundProcess{
			PID:       pid,
			Command:   *exec.Command(command[0], command[1:]...),
			Running:   true,
			TimeStart: time.Now(),
		}
		backgroundProcessesRWLock.Unlock()

		//Blocking process. will end when command ends
		err := backgroundProcesses[int(pid)].Command.Run()

		backgroundProcessesRWLock.Lock()
		backgroundProcesses[int(pid)].Running = false
		backgroundProcesses[int(pid)].Error = err
		backgroundProcesses[int(pid)].TimeEnd = time.Now()
		backgroundProcessesRWLock.Unlock()
	}()
	return pid
}

func getNextPID() int32 {
	//if a spot open, take it
	for i := 1; i <= MAX_MESH_BACKGROUND_PROCESS; i++ {
		if _, ok := backgroundProcesses[i]; !ok {
			return int32(i)
		}
	}

	//if no spot open, delete the oldest (if over an hour)
	var oldest = time.Now()
	oldestPID := -1

	for key, val := range backgroundProcesses {
		if val.Running {
			continue
		}

		if oldest.After(val.TimeEnd) {
			oldest = val.TimeEnd
			oldestPID = key
		}
	}

	//can't remove any. all running
	if oldestPID == -1 {
		return -1
	}

	duration := time.Since(oldest)
	if duration.Minutes() > DELETE_DONE_PROCESS_MINS {
		delete(backgroundProcesses, oldestPID)
		return int32(oldestPID)
	}

	return -1
}
