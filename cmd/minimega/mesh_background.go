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

const MAX_MESH_BACKGROUND_HISTORY = 10000

const (
	MESH_BG_START int32 = iota
	MESH_BG_STATUS
	MESH_BG_HISTORY
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
	backgroundProcesses            = make(map[int]*BackgroundProcess)
	backgroundProcessesRWLock      sync.RWMutex
	backgroundProcessHistory       []BackgroundProcess
	backgroundProcessHistoryRWLock sync.RWMutex
	backgroundProcessNextID        int32 = 1

	backgroundProcessTableHeader = []string{"PID", "RUNNING", "ERROR", "TIME_START", "TIME_END", "COMMAND"}
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
		status, err := getMeshBackgroundStatus(pid)

		response := minicli.Response{
			Host:    hostname,
			Header:  backgroundProcessTableHeader,
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

		msg := fmt.Sprintf("Process started with ID: %d", mesh_pid)
		response := minicli.Response{
			Host:     hostname,
			Response: fmt.Sprintf(msg),
			Data:     mesh_pid,
		}

		resp := meshageResponse{Response: response, TID: mCmd.TID}
		recipientList := []string{m.Source}

		_, err := meshageNode.Set(recipientList, resp)
		if err != nil {
			log.Errorln(err)
		}
	case MESH_BG_HISTORY:
		if len(mCmd.Command) == 1 {
			if mCmd.Command[0] == "clear" {

				clearMeshBackgroundHistory()
				response := minicli.Response{
					Host:     hostname,
					Response: "History cleared",
				}
				resp := meshageResponse{Response: response, TID: mCmd.TID}
				_, err := meshageNode.Set([]string{m.Source}, resp)
				if err != nil {
					log.Errorln(err)
				}
				return
			}
		}

		history := getMeshBackgroundHistory()

		response := minicli.Response{
			Host:    hostname,
			Header:  backgroundProcessTableHeader,
			Tabular: history,
		}
		resp := meshageResponse{Response: response, TID: mCmd.TID}
		_, err := meshageNode.Set([]string{m.Source}, resp)
		if err != nil {
			log.Errorln(err)
		}
		return

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

	return int32(num), nil
}

func getMeshBackgroundStatus(pid int32) ([][]string, error) {
	var response [][]string

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

		finalProcess := backgroundProcesses[int(pid)]
		backgroundProcessesRWLock.Unlock()

		backgroundProcessHistoryRWLock.Lock()
		backgroundProcessHistory = append(backgroundProcessHistory, *finalProcess)
		if len(backgroundProcessHistory) > MAX_MESH_BACKGROUND_HISTORY {
			backgroundProcessHistory = backgroundProcessHistory[1:]
		}
		backgroundProcessHistoryRWLock.Unlock()
	}()
	return pid
}

func getMeshBackgroundHistory() [][]string {

	backgroundProcessHistoryRWLock.RLock()
	defer backgroundProcessHistoryRWLock.RUnlock()

	var table [][]string
	for _, entry := range backgroundProcessHistory {
		table = append(table, entry.ToTabular())

	}
	return table
}

func clearMeshBackgroundHistory() {
	backgroundProcessHistoryRWLock.Lock()
	defer backgroundProcessHistoryRWLock.Unlock()

	backgroundProcessHistory = nil
}

func getNextPID() int32 {
	//overflow at 2 billion. reset on any mm reset. probably safe?
	id := backgroundProcessNextID
	backgroundProcessNextID += 1
	return id
}
