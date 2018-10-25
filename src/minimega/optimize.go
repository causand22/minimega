// Copyright (2012) Sandia Corporation.
// Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
// the U.S. Government retains certain rights in this software.

package main

import (
	"errors"
	"fmt"
	"minicli"
	log "minilog"
	"os"
	"ranges"
	"runtime"
	"sort"
	"strconv"
	"sync"
)

// cpuRange to parse filter ranges
var cpuRange, _ = ranges.NewRange("", 0, runtime.NumCPU()-1)

var optimizeCLIHandlers = []minicli.Handler{
	{ // optimize
		HelpShort: "enable or disable several virtualization optimizations",
		HelpLong: `
Enable or disable several virtualization optimizations, including Kernel
Samepage Merging, CPU affinity for VMs, and the use of hugepages.

To enable/disable Kernel Samepage Merging (KSM):

	optimize ksm [true,false]

To enable hugepage support for future VM launches:

	optimize hugepages </path/to/hugepages_mount>

To disable hugepage support:

	clear optimize hugepages

To enable/disable CPU affinity support for running VMs:

	optimize affinity [true,false]

To set a CPU set filter for the affinity scheduler, for example (to use only
CPUs 1, 2-20):

	optimize affinity filter [1,2-20]

If affinity is already enabled, will cause reassignment of affinity for all
running VMs to match the new filter.

To clear a CPU set filter:

	clear optimize affinity filter

See note above about reassigning affinity.

To view current CPU affinity mappings (by PID):

	optimize affinity

To disable all optimizations, use "clear optimize".

Note: affinity and hugepages can be selectively enabled in particular
namespaces. KSM affects VMs across all namespaces.`,
		Patterns: []string{
			"optimize",
			"optimize <affinity,> <filter,> <filter>",
			"optimize <affinity,> [true,false]",
			"optimize <hugepages,> [path]",
			"optimize <ksm,> [true,false]",
		},
		Call: wrapBroadcastCLI(cliOptimize),
	},
	{ // clear optimize
		HelpShort: "reset virtualization optimization state",
		HelpLong: `
Resets state for virtualization optimizations. See "help optimize" for more
information.`,
		Patterns: []string{
			"clear optimize",
			"clear optimize <affinity,> [filter,]",
			"clear optimize <hugepages,>",
			"clear optimize <ksm,>",
		},
		Call: wrapBroadcastCLI(cliOptimizeClear),
	},
}

func cliOptimize(ns *Namespace, c *minicli.Command, resp *minicli.Response) error {
	switch {
	case c.BoolArgs["ksm"]:
		return cliOptimizeKSM(ns, c, resp)
	case c.BoolArgs["hugepages"]:
		return cliOptimizeHugePages(ns, c, resp)
	case c.BoolArgs["affinity"]:
		return cliOptimizeAffinity(ns, c, resp)
	}

	// display optimizations
	resp.Header = []string{"ksm", "hugepages", "affinity"}
	row := []string{}

	if ksmEnabled {
		row = append(row, "enabled")
	} else {
		row = append(row, "disabled")
	}

	if ns.hugepagesMountPath != "" {
		row = append(row, ns.hugepagesMountPath)
	} else {
		row = append(row, "disabled")
	}

	if ns.affinityEnabled {
		row = append(row, "enabled")
	} else {
		row = append(row, "disabled")
	}

	resp.Tabular = append(resp.Tabular, row)
	return nil
}

func cliOptimizeKSM(ns *Namespace, c *minicli.Command, resp *minicli.Response) error {
	switch {
	case c.BoolArgs["true"]:
		return ksmEnable()
	case c.BoolArgs["false"]:
		return ksmDisable()
	}

	// show current status
	resp.Response = strconv.FormatBool(ksmEnabled)
	return nil
}

func cliOptimizeHugePages(ns *Namespace, c *minicli.Command, resp *minicli.Response) error {
	if v := c.StringArgs["path"]; v != "" {
		if _, err := os.Stat(v); os.IsNotExist(err) {
			log.Warn("file does not exist: %v", v)
		}

		ns.hugepagesMountPath = v

		return nil
	}

	// show current status
	resp.Response = ns.hugepagesMountPath
	return nil
}

func cliOptimizeAffinity(ns *Namespace, c *minicli.Command, resp *minicli.Response) error {
	switch {
	case c.BoolArgs["filter"]:
		cpus, err := cpuRange.SplitRange(c.StringArgs["filter"])
		if err != nil {
			return fmt.Errorf("cannot expand CPU range: %v", err)
		}

		ns.affinityCPUSets = make(map[string][]int)
		for _, v := range cpus {
			ns.affinityCPUSets[v] = []int{}
		}

		if ns.affinityEnabled {
			return affinityEnable(ns)
		}

		return nil
	case c.BoolArgs["true"]:
		return affinityEnable(ns)
	case c.BoolArgs["false"]:
		return affinityDisable(ns)
	}

	// Must want to print affinity status
	resp.Header = []string{"cpu", "pids"}

	var cpus []string
	for k, _ := range ns.affinityCPUSets {
		cpus = append(cpus, k)
	}

	sort.Strings(cpus)

	for _, cpu := range cpus {
		row := []string{
			cpu,
			fmt.Sprintf("%v", ns.affinityCPUSets[cpu]),
		}
		resp.Tabular = append(resp.Tabular, row)
	}

	return nil
}

func cliOptimizeClear(ns *Namespace, c *minicli.Command, resp *minicli.Response) error {
	switch {
	case c.BoolArgs["filter"]:
		affinityClearFilter(ns)
	case c.BoolArgs["affinity"]:
		affinityDisable(ns)
	case c.BoolArgs["hugepages"]:
		ns.hugepagesMountPath = ""
	case c.BoolArgs["ksm"]:
		return ksmDisable()
	default:
		if ksmEnabled {
			if err := ksmDisable(); err != nil {
				return err
			}
		}

		ns.hugepagesMountPath = ""
		affinityDisable(ns)
		affinityClearFilter(ns)
	}

	return nil
}

func affinityEnable(ns *Namespace) error {
	if ns.affinityEnabled {
		return errors.New("affinity is already enabled for this namespace")
	}

	// first enable, no filter
	if ns.affinityCPUSets == nil {
		for i := 0; i < runtime.NumCPU(); i++ {
			ns.affinityCPUSets[strconv.Itoa(i)] = []int{}
		}
	}

	var mu sync.Mutex
	err := ns.Apply(Wildcard, func(vm VM, _ bool) (bool, error) {
		// make synchronous
		mu.Lock()
		defer mu.Unlock()

		// find cpu with the fewest number of entries
		var cpu string
		for k, v := range ns.affinityCPUSets {
			if cpu == "" {
				cpu = k
				continue
			}
			if len(v) < len(ns.affinityCPUSets[cpu]) {
				cpu = k
			}
		}

		if cpu == "" {
			return true, errors.New("could not find a valid CPU set!")
		}

		if err := setAffinity(cpu, vm.GetPID()); err != nil {
			return true, err
		}

		ns.affinityCPUSets[cpu] = append(ns.affinityCPUSets[cpu], vm.GetPID())
		return true, nil
	})

	if err == nil {
		ns.affinityEnabled = true
	}

	return err
}

func affinityDisable(ns *Namespace) error {
	if !ns.affinityEnabled {
		return errors.New("affinity is not enabled for this namespace")
	}

	for cpu, pids := range ns.affinityCPUSets {
		for _, pid := range pids {
			if err := clearAffinity(pid); err != nil {
				return err
			}
		}

		ns.affinityCPUSets[cpu] = nil
	}

	ns.affinityEnabled = false
	return nil
}

func affinityClearFilter(ns *Namespace) {
	ns.affinityCPUSets = nil
	if ns.affinityEnabled {
		affinityEnable(ns)
	}
}

// setAffinity sets the affinity for the PID to the given CPU.
func setAffinity(cpu string, pid int) error {
	log.Debug("set affinity to %v for %v", cpu, pid)

	out, err := processWrapper("taskset", "-a", "-p", cpu, strconv.Itoa(pid))
	if err != nil {
		return fmt.Errorf("%v: %v", err, out)
	}
	return nil
}

// clearAffinity removes the affinity for a PID.
func clearAffinity(pid int) error {
	log.Debug("clear affinity for %v", pid)

	out, err := processWrapper("taskset", "-p", "0xffffffffffffffff", strconv.Itoa(pid))
	if err != nil {
		return fmt.Errorf("%v: %v", err, out)
	}
	return nil
}
