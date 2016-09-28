package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"

	mp "github.com/mackerelio/go-mackerel-plugin-helper"
)

// MemcachedPlugin mackerel plugin for memchached
type MemcachedPlugin struct {
	Target     string
	Socket     string
	Tempfile   string
	Prefix     string
	StatsSlabs bool
}

// MetricKeyPrefix interface for PluginWithPrefix
func (m MemcachedPlugin) MetricKeyPrefix() string {
	if m.Prefix == "" {
		m.Prefix = "memcached"
	}
	return m.Prefix
}

// FetchMetrics interface for mackerelplugin
func (m MemcachedPlugin) FetchMetrics() (map[string]interface{}, error) {
	network := "tcp"
	target := m.Target
	if m.Socket != "" {
		network = "unix"
		target = m.Socket
	}
	conn, err := net.Dial(network, target)
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(conn, "stats")
	stat, err := m.parseStats(conn)
	if err != nil {
		return stat, err
	}
	if m.StatsSlabs {
		fmt.Fprintln(conn, "stats slabs")
		err = m.parseStatsSlabs(conn, stat)
	}
	return stat, err
}

func (m MemcachedPlugin) parseStats(conn io.Reader) (map[string]interface{}, error) {
	scanner := bufio.NewScanner(conn)
	stat := make(map[string]interface{})

	for scanner.Scan() {
		line := scanner.Text()
		s := string(line)
		if s == "END" {
			return stat, nil
		}

		res := strings.Split(s, " ")
		if res[0] == "STAT" {
			stat[res[1]] = res[2]
		}
	}
	if err := scanner.Err(); err != nil {
		return stat, err
	}
	return nil, nil
}

func (m MemcachedPlugin) parseStatsSlabs(conn io.Reader, stat map[string]interface{}) error {
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		s := string(line)
		if s == "END" {
			return nil
		}

		res := strings.Split(s, " ")
		if res[0] == "STAT" {
			key := strings.Split(res[1], ":")
			if len(key) < 2 {
				continue
			}
			statKey := statsSlabsKey(key)
			if statKey == "" {
				continue
			}
			stat[statKey] = res[2]
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func statsSlabsKey(key []string) string {
	class := key[0]
	statKey := key[1]
	switch statKey {
	case "used_chunks", "free_chunks":
		return fmt.Sprintf("chunks.slab.class%s.%s", class, statKey)
	case "get_hits", "cmd_set", "delete_hits", "incr_hits", "decr_hits", "cas_hits", "cas_badval", "touch_hits":
		return fmt.Sprintf("hits.slab.class%s.%s", class, statKey)
	case "chunk_size", "chunks_per_page", "total_pages", "mem_requested":
		return fmt.Sprintf("%s.slab.class%s.%s", statKey, class, statKey)
	}
	return ""
}

// GraphDefinition interface for mackerelplugin
func (m MemcachedPlugin) GraphDefinition() map[string](mp.Graphs) {
	labelPrefix := strings.Title(m.Prefix)

	// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
	var graphdef = map[string](mp.Graphs){
		"connections": mp.Graphs{
			Label: (labelPrefix + " Connections"),
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "curr_connections", Label: "Connections", Diff: false},
			},
		},
		"cmd": mp.Graphs{
			Label: (labelPrefix + " Command"),
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "cmd_get", Label: "Get", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "cmd_set", Label: "Set", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "cmd_flush", Label: "Flush", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "cmd_touch", Label: "Touch", Diff: true, Type: "uint64"},
			},
		},
		"hitmiss": mp.Graphs{
			Label: (labelPrefix + " Hits/Misses"),
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "get_hits", Label: "Get Hits", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "get_misses", Label: "Get Misses", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "delete_hits", Label: "Delete Hits", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "delete_misses", Label: "Delete Misses", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "incr_hits", Label: "Incr Hits", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "incr_misses", Label: "Incr Misses", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "cas_hits", Label: "Cas Hits", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "cas_misses", Label: "Cas Misses", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "touch_hits", Label: "Touch Hits", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "touch_misses", Label: "Touch Misses", Diff: true, Type: "uint64"},
			},
		},
		"evictions": mp.Graphs{
			Label: (labelPrefix + " Evictions"),
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "evictions", Label: "Evictions", Diff: true, Type: "uint64"},
			},
		},
		"unfetched": mp.Graphs{
			Label: (labelPrefix + " Unfetched"),
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "expired_unfetched", Label: "Expired unfetched", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "evicted_unfetched", Label: "Evicted unfetched", Diff: true, Type: "uint64"},
			},
		},
		"rusage": mp.Graphs{
			Label: (labelPrefix + " Resouce Usage"),
			Unit:  "float",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "rusage_user", Label: "User", Diff: true},
				mp.Metrics{Name: "rusage_system", Label: "System", Diff: true},
			},
		},
		"bytes": mp.Graphs{
			Label: (labelPrefix + " Traffics"),
			Unit:  "bytes",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "bytes_read", Label: "Read", Diff: true, Type: "uint64"},
				mp.Metrics{Name: "bytes_written", Label: "Write", Diff: true, Type: "uint64"},
			},
		},
		"cachesize": mp.Graphs{
			Label: (labelPrefix + " Cache Size"),
			Unit:  "bytes",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "limit_maxbytes", Label: "Total", Diff: false},
				mp.Metrics{Name: "bytes", Label: "Used", Diff: false, Type: "uint64"},
			},
		},
		"chunks.slab.#": mp.Graphs{
			Label: "chunks",
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "used_chunks", Label: "used_chunks", Diff: false, Stacked: true},
				mp.Metrics{Name: "free_chunks", Label: "free_chunks", Diff: false, Stacked: true},
			},
		},
		"hits.slab.#": mp.Graphs{
			Label: "Slabs hits",
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "get_hits", Label: "get_hits", Diff: true, Stacked: false},
				mp.Metrics{Name: "cmd_set", Label: "cmd_set", Diff: false, Stacked: false},
				mp.Metrics{Name: "delete_hits", Label: "delete_hits", Diff: true, Stacked: false},
				mp.Metrics{Name: "incr_hits", Label: "incr_hits", Diff: true, Stacked: false},
				mp.Metrics{Name: "decr_hits", Label: "decr_hits", Diff: true, Stacked: false},
				mp.Metrics{Name: "cas_hits", Label: "cas_hits", Diff: true, Stacked: false},
				mp.Metrics{Name: "cas_badval", Label: "cas_badval", Diff: true, Stacked: false},
				mp.Metrics{Name: "touch_hits", Label: "touch_hits", Diff: true, Stacked: false},
			},
		},
		"chunk_size.slab.#": mp.Graphs{
			Label: "Slabs stats",
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "chunk_size", Label: "chunk_size", Diff: false, Stacked: false},
			},
		},
		"chunks_per_page.slab.#": mp.Graphs{
			Label: "Chunks per page",
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "chunks_per_page", Label: "chunks_per_page", Diff: false, Stacked: false},
			},
		},
		"total_pages.slab.#": mp.Graphs{
			Label: "total_pages",
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "total_pages", Label: "total_pages", Diff: false, Stacked: false},
			},
		},
		"mem_requested.slab.#": mp.Graphs{
			Label: "mem_requested",
			Unit:  "bytes",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "mem_requested", Label: "mem_requested", Diff: false, Stacked: true},
			},
		},
	}
	return graphdef
}

func main() {
	optHost := flag.String("host", "localhost", "Hostname")
	optPort := flag.String("port", "11211", "Port")
	optSocket := flag.String("socket", "", "Server socket (overrides hosts and port)")
	optPrefix := flag.String("metric-key-prefix", "memcached", "Metric key prefix")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	optStatsSlabs := flag.Bool("stats-slabs", false, "get stats slabs")
	flag.Parse()

	var memcached MemcachedPlugin

	memcached.Prefix = *optPrefix

	if *optSocket != "" {
		memcached.Socket = *optSocket
	} else {
		memcached.Target = fmt.Sprintf("%s:%s", *optHost, *optPort)
	}
	memcached.StatsSlabs = *optStatsSlabs
	helper := mp.NewMackerelPlugin(memcached)
	helper.Tempfile = *optTempfile
	helper.Run()
}
