package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/codegangsta/cli"
	mp "github.com/mackerelio/go-mackerel-plugin-helper"
)

const (
	pathStat = "/proc/stat"
)

// metric value structure
// note: all metrics are add dynamic at collect*().
var graphdef = map[string](mp.Graphs){}

// LinuxPlugin mackerel plugin for linux
type LinuxPlugin struct {
	Tempfile string
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// GraphDefinition interface for mackerelplugin
func (c LinuxPlugin) GraphDefinition() map[string](mp.Graphs) {
	var err error

	p := make(map[string]interface{})

	err = collectCpus(pathStat, &p)
	if err != nil {
		return nil
	}

	return graphdef
}

// main function
func doMain(c *cli.Context) {
	var linux LinuxPlugin

	helper := mp.NewMackerelPlugin(linux)
	helper.Tempfile = c.String("tempfile")

	if os.Getenv("MACKEREL_AGENT_PLUGIN_META") != "" {
		helper.OutputDefinitions()
	} else {
		helper.OutputValues()
	}
}

// FetchMetrics interface for mackerelplugin
func (c LinuxPlugin) FetchMetrics() (map[string]interface{}, error) {
	var err error

	p := make(map[string]interface{})

	err = collectCpus(pathStat, &p)
	if err != nil {
		return nil, err
	}

	return p, nil
}

var cpuUsageMetricNames = []string{
	"user", "nice", "system", "idle", "iowait",
	"irq", "softirq", "steal", "guest",
}

// collect /proc/stat
func collectCpus(path string, p *map[string]interface{}) error {

	cpus, err := getCpus(path)
	if err != nil {
		return err
	}
	for _, cpu := range cpus {
		metrics := make([](mp.Metrics), len(cpus))
		for j, name := range cpuUsageMetricNames {
			if j >= len(cpu.values) {
				break
			}
			metrics[j] = mp.Metrics{Name: cpu.name + "." + name, Label: name, Diff: true}
		}
		graphdef["linux."+cpu.name] = mp.Graphs{
			Label:   "Linux " + cpu.name,
			Unit:    "float",
			Metrics: metrics,
		}
	}
	err = setMetrics(cpus, p)
	if err != nil {
		return err
	}

	return nil
}

// parsing metrics from /proc/stat
func setMetrics(cpus []cpuCols, p *map[string]interface{}) error {
	for _, cpu := range cpus {
		for i, value := range cpu.values {
			if i >= len(cpuUsageMetricNames) {
				break
			}
			(*p)[cpu.name+"."+cpuUsageMetricNames[i]] = value
		}
	}
	return nil
}

type cpuCols struct {
	name   string
	values []float64
}

// Getting /proc/*
func getCpus(path string) ([]cpuCols, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Printf("Failed (skip these metrics): %s", err)
		return nil, err
	}

	lineScanner := bufio.NewScanner(bufio.NewReader(file))

	firstLine := true

	cpuFields := make([][]string, 0, 1024)
	for lineScanner.Scan() {
		line := lineScanner.Text()
		if firstLine {
			firstLine = false
			continue
		}
		if strings.HasPrefix(line, "cpu") {
			cpuFields = append(cpuFields, strings.Fields(line))
		}
	}

	cpuValues := make([]cpuCols, len(cpuFields))
	for i, cols := range cpuFields {
		cc := cpuCols{
			name:   cols[0],
			values: make([]float64, len(cols[1:])),
		}
		for j, strValue := range cols[1:] {
			cc.values[j], err = strconv.ParseFloat(strValue, 64)
			if err != nil {
				log.Printf("Failed to parse cpuUsage metrics (skip these metrics): %s", err)
				return nil, err
			}
		}
		cpuValues[i] = cc
	}
	return cpuValues, nil
}

// main
func main() {
	app := cli.NewApp()
	app.Name = "mackerel-plugin-linux-cpus"
	app.Version = version
	app.Usage = "Get metrics from Linux cpus."
	app.Author = "YAMASAKI Masahide"
	app.Email = "masahide.y@gmail.com"
	app.Flags = flags
	app.Action = doMain

	app.Run(os.Args)
}
