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
	StatsItems bool
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
	stat := make(map[string]interface{})
	err = m.parseStats(stat, conn)
	if err == nil && m.StatsSlabs {
		fmt.Fprintln(conn, "stats slabs")
		err = m.parseStatsSlabs(stat, conn)
	}
	if err == nil && m.StatsItems {
		fmt.Fprintln(conn, "stats items")
		err = m.parseStatsItems(stat, conn)
	}
	return stat, err
}

func (m MemcachedPlugin) scanStatsLine(conn io.Reader, cb func(res []string)) error {
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		s := string(line)
		if s == "END" {
			return nil
		}

		res := strings.Split(s, " ")
		if res[0] == "STAT" {
			cb(res)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (m MemcachedPlugin) parseStats(stat map[string]interface{}, conn io.Reader) error {
	return m.scanStatsLine(conn, func(res []string) {
		stat[res[1]] = res[2]
	})
}
func (m MemcachedPlugin) parseStatsSlabs(stat map[string]interface{}, conn io.Reader) error {
	return m.scanStatsLine(conn, func(res []string) {
		key := strings.Split(res[1], ":")
		if len(key) < 2 {
			return
		}
		statKey := m.statsSlabsKey(key)
		if statKey == "" {
			return
		}
		stat[statKey] = res[2]
	})
}
func (m MemcachedPlugin) parseStatsItems(stat map[string]interface{}, conn io.Reader) error {
	return m.scanStatsLine(conn, func(res []string) {
		key := strings.Split(res[1], ":")
		if len(key) < 3 {
			return
		}
		statKey := m.statsSlabsKey(key[1:])
		if statKey == "" {
			return
		}
		stat[statKey] = res[2]
	})
}

func (m MemcachedPlugin) statsSlabsKey(key []string) string {
	class := key[0]
	if len(class) == 1 {
		class = "0" + class
	}
	statKey := key[1]
	switch statKey {
	case "get_hits", "cmd_set", "delete_hits", "incr_hits", "decr_hits", "cas_hits", "cas_badval", "touch_hits":
		return fmt.Sprintf("hits.slab.class%s.%s", class, statKey)
	case /*"chunk_size", "chunks_per_page", "total_pages",*/ "mem_requested":
		return fmt.Sprintf("%s.slab.class%s.%s", statKey, class, statKey)
	case /*"number", "age",*/ "evicted_time":
		return fmt.Sprintf("%s.slab.class%s.%s", statKey, class, statKey)
	case "expired_unfetched", "evicted_unfetched", "crawler_reclaimed", "crawler_items_checked", "lrutail_reflocked":
		return fmt.Sprintf("number_of_items.slab.class%s.%s", class, statKey)
		/*
			case "used_chunks", "free_chunks":
				return fmt.Sprintf("chunks.slab.class%s.%s", class, statKey)
			case "evicted", "evicted_nonzero", "outofmemory", "tailrepairs", "reclaimed":
				return fmt.Sprintf("number_of_times.slab.class%s.%s", class, statKey)
		*/
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
		"items": mp.Graphs{
			Label: (labelPrefix + " Items"),
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "curr_items", Label: "Current Items", Diff: false},
			},
		},
		"hits.slab.#": mp.Graphs{
			Label: (labelPrefix + " Slabs Hits"),
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "get_hits", Label: "get_hits", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "cmd_set", Label: "cmd_set", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "delete_hits", Label: "delete_hits", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "incr_hits", Label: "incr_hits", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "decr_hits", Label: "decr_hits", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "cas_hits", Label: "cas_hits", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "cas_badval", Label: "cas_badval", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "touch_hits", Label: "touch_hits", Diff: true, Stacked: false, Type: "uint64"},
			},
		},
		/*
			"chunks.slab.#": mp.Graphs{
				Label: (labelPrefix + " Chunks"),
				Unit:  "integer",
				Metrics: [](mp.Metrics){
					mp.Metrics{Name: "used_chunks", Label: "used_chunks", Diff: false, Stacked: true, Type: "uint64"},
					mp.Metrics{Name: "free_chunks", Label: "free_chunks", Diff: false, Stacked: true, Type: "uint64"},
				},
			},
			"chunk_size.slab.#": mp.Graphs{
				Label: (labelPrefix + " Slabs Stats"),
				Unit:  "integer",
				Metrics: [](mp.Metrics){
					mp.Metrics{Name: "chunk_size", Label: "chunk_size", Diff: false, Stacked: false, Type: "uint64"},
				},
			},
			"chunks_per_page.slab.#": mp.Graphs{
				Label: (labelPrefix + " Chunks Per Page"),
				Unit:  "integer",
				Metrics: [](mp.Metrics){
					mp.Metrics{Name: "chunks_per_page", Label: "chunks_per_page", Diff: false, Stacked: false, Type: "uint64"},
				},
			},
			"total_pages.slab.#": mp.Graphs{
				Label: (labelPrefix + " Total Pages"),
				Unit:  "integer",
				Metrics: [](mp.Metrics){
					mp.Metrics{Name: "total_pages", Label: "total_pages", Diff: false, Stacked: false, Type: "uint64"},
				},
			},
		*/
		"mem_requested.slab.#": mp.Graphs{
			Label: (labelPrefix + " Mem Requested"),
			Unit:  "bytes",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "mem_requested", Label: "mem_requested", Diff: false, Stacked: true, Type: "uint64"},
			},
		},
		/*
			"number.slab.#": mp.Graphs{
				Label: (labelPrefix + " Number of items"),
				Unit:  "integer",
				Metrics: [](mp.Metrics){
					mp.Metrics{Name: "number", Label: "number", Diff: false, Stacked: false, Type: "uint64"},
				},
			},
			"age.slab.#": mp.Graphs{
				Label: (labelPrefix + " Age of the oldest item"),
				Unit:  "integer",
				Metrics: [](mp.Metrics){
					mp.Metrics{Name: "age", Label: "age", Diff: false, Stacked: false, Type: "uint64"},
				},
			},
		*/
		"evicted_time.slab.#": mp.Graphs{
			Label: (labelPrefix + " Time of the last evicted entry"),
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "evicted_time", Label: "evicted_time", Diff: false, Stacked: false, Type: "uint64"},
			},
		},
		/*
			"number_of_times.slab.#": mp.Graphs{
				Label: (labelPrefix + " Number of times"),
				Unit:  "integer",
				Metrics: [](mp.Metrics){
					mp.Metrics{Name: "evicted", Label: "evicted", Diff: true, Stacked: false, Type: "uint64"},
					mp.Metrics{Name: "evicted_nonzero", Label: "evicted_nonzero", Diff: true, Stacked: false, Type: "uint64"},
					mp.Metrics{Name: "outofmemory", Label: "outofmemory", Diff: true, Stacked: false, Type: "uint64"},
					mp.Metrics{Name: "tailrepairs", Label: "tailrepairs", Diff: true, Stacked: false, Type: "uint64"},
					mp.Metrics{Name: "reclaimed", Label: "reclaimed", Diff: true, Stacked: false, Type: "uint64"},
				},
			},
		*/
		"number_of_items.slab.#": mp.Graphs{
			Label: (labelPrefix + " Number of items"),
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "expired_unfetched", Label: "expired_unfetched", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "evicted_unfetched", Label: "evicted_unfetched", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "crawler_reclaimed", Label: "crawler_reclaimed", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "crawler_items_checked", Label: "crawler_items_checked", Diff: true, Stacked: false, Type: "uint64"},
				mp.Metrics{Name: "lrutail_reflocked", Label: "lrutail_reflocked", Diff: true, Stacked: false, Type: "uint64"},
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
	optStatsSlabs := flag.Bool("stats-slabs", false, "Enable stats slabs metrics")
	optStatsItems := flag.Bool("stats-items", false, "Enable stats items metrics")
	flag.Parse()

	var memcached MemcachedPlugin

	memcached.Prefix = *optPrefix

	if *optSocket != "" {
		memcached.Socket = *optSocket
	} else {
		memcached.Target = fmt.Sprintf("%s:%s", *optHost, *optPort)
	}
	memcached.StatsSlabs = *optStatsSlabs
	memcached.StatsItems = *optStatsItems
	helper := mp.NewMackerelPlugin(memcached)
	helper.Tempfile = *optTempfile
	helper.Run()
}
