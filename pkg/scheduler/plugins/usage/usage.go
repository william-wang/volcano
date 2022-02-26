/*
Copyright 2022 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package usage

import (
	"fmt"
	"k8s.io/klog"
	"strings"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "usage"
	cpuUsageAvgPrefix = "CpuUsageAvg."
	memUsageAvgPrefix = "MemUsageAvg."
	thresholdSection  = "thresholds"
	cpuUsageAvg5m = "5m"
)

/*
   actions: "enqueue, allocate, backfill"
   tiers:
   - plugins:
     - name: usage
       arguments:
          thresholds:
            CpuUsageAvg.5m: 80%
            MemUsageAvg.5m: 90%
*/

type thresholdConfig struct {
	cpuUsageAvg map[string]float64
	memUsageAvg map[string]float64
}

type usagePlugin struct {
	//pluginArguments framework.Arguments
	weight int
	threshold thresholdConfig
}

// New function returns usagePlugin object
func New(args framework.Arguments) framework.Plugin {
	thresholdStore := thresholdConfig{}
	usageWeight := 1
	args.GetInt(&usageWeight, "usage.weight")
	for k, v := range args[thresholdSection].(map[string]float64){
		if strings.Contains(k, cpuUsageAvgPrefix) {
			thresholdStore.cpuUsageAvg[strings.Replace(k, cpuUsageAvgPrefix, "", 1)] = v
		}
		if strings.Contains(k, memUsageAvgPrefix) {
			thresholdStore.memUsageAvg[strings.Replace(k, memUsageAvgPrefix, "", 1)] = v
		}
	}
	return &usagePlugin{
		weight: usageWeight,
		threshold: thresholdStore,
	}
}

func (up *usagePlugin) Name() string {
	return PluginName
}

func (up *usagePlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(4).Infof("Enter usage plugin ...")
	if klog.V(4) {
		defer func() {
			klog.V(4).Infof("Leaving usage plugin.")
		}()
	}

	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) error {

		for period, value := range up.threshold.cpuUsageAvg {
			klog.V(4).Infof("cpuUsageAvg:%v", up.threshold.cpuUsageAvg)
			if node.ResourceUsage.CpuUsageAvg[period] > value {
				// failed log
				msg := fmt.Sprintf("task %s/%s is not allow to dispatch to node %s", task.Namespace, task.Name, node.Name)
				return fmt.Errorf("plugin %s cpu usage predicates %s", up.Name(), msg)
			}
		}

		for period, value := range up.threshold.memUsageAvg {
			klog.V(4).Infof("memUsageAvg:%v", up.threshold.memUsageAvg)
			if node.ResourceUsage.MemUsageAvg[period] > value {
				// failed log
				msg := fmt.Sprintf("task %s/%s is not allow to dispatch to node %s", task.Namespace, task.Name, node.Name)
				return fmt.Errorf("plugin %s mem usage predicates %s", up.Name(), msg)
			}
		}
		klog.V(4).Infof("Usage plugin filter for Task %s/%s on node %s pass.", task.Namespace, task.Name, node.Name)
		return nil
	}
	nodeOrderFn := func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		score := 0.0
		cpuUsage, exist := node.ResourceUsage.CpuUsageAvg[cpuUsageAvg5m]
		if !exist {
			return 0, nil
		}
		score = float64(up.weight) * (1 - cpuUsage)
		return score, nil
	}

	ssn.AddPredicateFn(up.Name(), predicateFn)
	ssn.AddNodeOrderFn(up.Name(), nodeOrderFn)
}

func (up *usagePlugin) OnSessionClose(ssn *framework.Session) {}

