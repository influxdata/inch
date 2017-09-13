package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
)

// ErrConnectionRefused indicates that the connection to the remote server was refused.
var ErrConnectionRefused = errors.New("connection refused")

func main() {
	m := NewMain()
	if err := m.ParseFlags(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := m.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct {
	mu             sync.Mutex
	writtenN       int
	startTime      time.Time
	now            time.Time
	timePerSeries  int64 // How much the client is backing off due to unacceptible response times.
	currentDelay   time.Duration
	wmaLatency     float64
	latencyHistory []time.Duration
	totalLatency   time.Duration
	currentErrors  int // The current number of errors since last reporting.

	// Client to be used to report statistics to an Influx instance.
	clt client.Client

	// Decay factor used when weighting average latency returned by server.
	alpha float64

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	Verbose          bool
	ReportHost       string
	ReportTags       map[string]string
	DryRun           bool
	Strict           bool
	Host             string
	Consistency      string
	Concurrency      int
	Measurements     int   // Number of measurements
	Tags             []int // tag cardinalities
	PointsPerSeries  int
	FieldsPerPoint   int
	BatchSize        int
	TargetMaxLatency time.Duration

	Database      string
	ShardDuration string        // Set a custom shard duration.
	TimeSpan      time.Duration // The length of time to span writes over.
	Delay         time.Duration // A delay inserted in between writes.
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdin:          os.Stdin,
		Stdout:         os.Stdout,
		Stderr:         os.Stderr,
		alpha:          0.5, // Weight the mean latency by 50% history / 50% latest value.
		latencyHistory: make([]time.Duration, 0, 200),
	}
}

// ParseFlags parses the command line flags.
func (m *Main) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("inch", flag.ContinueOnError)
	fs.BoolVar(&m.Verbose, "v", false, "Verbose")
	fs.StringVar(&m.ReportHost, "report-host", "", "Host to send metrics")
	reportTags := fs.String("report-tags", "", "Comma separated k=v tags to report alongside metrics")
	fs.BoolVar(&m.DryRun, "dry", false, "Dry run (maximum writer perf of inch on box)")
	fs.BoolVar(&m.Strict, "strict", false, "Terminate process if error encountered")
	fs.StringVar(&m.Host, "host", "http://localhost:8086", "Host")
	fs.StringVar(&m.Consistency, "consistency", "any", "Write consistency (default any)")
	fs.IntVar(&m.Concurrency, "c", 1, "Concurrency")
	fs.IntVar(&m.Measurements, "m", 1, "Measurements")
	tags := fs.String("t", "10,10,10", "Tag cardinality")
	fs.IntVar(&m.PointsPerSeries, "p", 100, "Points per series")
	fs.IntVar(&m.FieldsPerPoint, "f", 1, "Fields per point")
	fs.IntVar(&m.BatchSize, "b", 5000, "Batch size")
	fs.StringVar(&m.Database, "db", "stress", "Database to write to")
	fs.StringVar(&m.ShardDuration, "shard-duration", "7d", "Set shard duration (default 7d)")
	fs.DurationVar(&m.TimeSpan, "time", 0, "Time span to spread writes over")
	fs.DurationVar(&m.Delay, "delay", 0, "Delay between writes")
	fs.DurationVar(&m.TargetMaxLatency, "target-latency", 0, "If set inch will attempt to adapt write delay to meet target")

	if err := fs.Parse(args); err != nil {
		return err
	}

	switch m.Consistency {
	case "any", "quorum", "one", "all":
	default:
		fmt.Fprintf(os.Stderr, `Consistency must be one of: {"any", "quorum", "one", "all"}`)
		os.Exit(1)
	}

	if m.FieldsPerPoint < 1 {
		fmt.Fprintf(os.Stderr, "number of fields must be > 0")
		os.Exit(1)
	}

	// Parse tag cardinalities.
	for _, s := range strings.Split(*tags, ",") {
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("cannot parse tag cardinality: %s", s)
		}
		m.Tags = append(m.Tags, v)
	}

	// Basic report tags.
	m.ReportTags = map[string]string{
		"stress_tool": "inch",
		"t":           *tags,
		"batch_size":  fmt.Sprint(m.BatchSize),
		"p":           fmt.Sprint(m.PointsPerSeries),
		"c":           fmt.Sprint(m.Concurrency),
		"m":           fmt.Sprint(m.Measurements),
		"f":           fmt.Sprint(m.FieldsPerPoint),
		"sd":          m.ShardDuration,
	}

	// Parse report tags.
	if *reportTags != "" {
		for _, tagPair := range strings.Split(*reportTags, ",") {
			tag := strings.Split(tagPair, "=")
			if len(tag) != 2 {
				fmt.Fprintf(os.Stderr, "invalid tag pair %q", tagPair)
				os.Exit(1)
			}
			m.ReportTags[tag[0]] = tag[1]
		}
	}

	// Setup reporting client?
	if m.ReportHost != "" {
		var err error
		m.clt, err = client.NewHTTPClient(client.HTTPConfig{
			Addr: m.ReportHost,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		if _, err := m.clt.Query(client.NewQuery(fmt.Sprintf(`CREATE DATABASE "ingest_benchmarks"`), "", "")); err != nil {
			fmt.Fprintf(os.Stderr, "unable to connect to %q", m.ReportHost)
			os.Exit(1)
		}
	}

	// Get product type and version and commit, if available.
	resp, err := http.Get(strings.TrimSuffix(m.Host, "/") + "/ping")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return nil
	}
	defer resp.Body.Close()

	build := resp.Header.Get("X-Influxdb-Build")
	if len(build) > 0 {
		m.ReportTags["build"] = build
	}

	version := resp.Header.Get("X-Influxdb-Version")
	if len(version) > 0 {
		m.ReportTags["version"] = version
	}

	return nil
}

// Run executes the program.
func (m *Main) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Print settings.
	fmt.Fprintf(m.Stdout, "Host: %s\n", m.Host)
	fmt.Fprintf(m.Stdout, "Concurrency: %d\n", m.Concurrency)
	fmt.Fprintf(m.Stdout, "Measurements: %d\n", m.Measurements)
	fmt.Fprintf(m.Stdout, "Tag cardinalities: %+v\n", m.Tags)
	fmt.Fprintf(m.Stdout, "Points per series: %d\n", m.PointsPerSeries)
	fmt.Fprintf(m.Stdout, "Total series: %d\n", m.SeriesN())
	fmt.Fprintf(m.Stdout, "Total points: %d\n", m.PointN())
	fmt.Fprintf(m.Stdout, "Total fields per point: %d\n", m.FieldsPerPoint)
	fmt.Fprintf(m.Stdout, "Batch Size: %d\n", m.BatchSize)
	fmt.Fprintf(m.Stdout, "Database: %s (Shard duration: %s)\n", m.Database, m.ShardDuration)
	fmt.Fprintf(m.Stdout, "Write Consistency: %s\n", m.Consistency)

	if m.TargetMaxLatency > 0 {
		fmt.Fprintf(m.Stdout, "Adaptive latency on. Max target: %s\n", m.TargetMaxLatency)
	} else if m.Delay > 0 {
		fmt.Fprintf(m.Stdout, "Fixed write delay: %s\n", m.Delay)
	}

	dur := fmt.Sprint(m.TimeSpan)
	if m.TimeSpan == 0 {
		dur = "off"
	}

	// Initialize database.
	if err := m.setup(); err != nil {
		return err
	}

	// Record start time.
	m.now = time.Now().UTC()
	m.startTime = m.now
	if m.TimeSpan != 0 {
		absTimeSpan := int64(math.Abs(float64(m.TimeSpan)))
		m.timePerSeries = absTimeSpan / int64(m.PointN())

		// If we're back-filling then we need to move the start time back.
		if m.TimeSpan < 0 {
			m.startTime = m.startTime.Add(m.TimeSpan)
		}
	}
	fmt.Fprintf(m.Stdout, "Start time: %s\n", m.startTime)
	if m.TimeSpan < 0 {
		fmt.Fprintf(m.Stdout, "Approx End time: %s\n", time.Now().UTC())
	} else if m.TimeSpan > 0 {
		fmt.Fprintf(m.Stdout, "Approx End time: %s\n", m.startTime.Add(m.TimeSpan).UTC())
	} else {
		fmt.Fprintf(m.Stdout, "Time span: %s\n", dur)
	}

	// Stream batches from a separate goroutine.
	ch := m.generateBatches()

	// Start clients.
	var wg sync.WaitGroup
	for i := 0; i < m.Concurrency; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); m.runClient(ctx, ch) }()
	}

	// Start monitor.
	var monitorWaitGroup sync.WaitGroup
	if m.Verbose {
		monitorWaitGroup.Add(1)
		go func() { defer monitorWaitGroup.Done(); m.runMonitor(ctx) }()
	}

	// Wait for all clients to complete.
	wg.Wait()

	// Wait for monitor.
	cancel()
	monitorWaitGroup.Wait()

	// Report stats.
	elapsed := time.Since(m.now)
	fmt.Fprintln(m.Stdout, "")
	fmt.Fprintf(m.Stdout, "Total time: %0.1f seconds\n", elapsed.Seconds())

	return nil
}

// WrittenN returns the total number of points written.
func (m *Main) WrittenN() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writtenN
}

// TagsN returns the total number of tags.
func (m *Main) TagsN() int {
	i := m.Tags[0]
	for _, v := range m.Tags[1:] {
		i *= v
	}
	return i
}

// SeriesN returns the total number of series to write.
func (m *Main) SeriesN() int {
	return m.TagsN() * m.Measurements
}

// PointN returns the total number of points to write.
func (m *Main) PointN() int {
	return int(m.PointsPerSeries) * m.SeriesN()
}

// BatchN returns the total number of batches.
func (m *Main) BatchN() int {
	n := m.PointN() / m.BatchSize
	if m.PointN()%m.BatchSize != 0 {
		n++
	}
	return n
}

// generateBatches returns a channel for streaming batches.
func (m *Main) generateBatches() <-chan []byte {
	ch := make(chan []byte, 10)

	go func() {
		var buf bytes.Buffer
		values := make([]int, len(m.Tags))
		lastWrittenTotal := m.WrittenN()

		// Generate field string.
		var fields []byte
		for i := 0; i < m.FieldsPerPoint; i++ {
			var delim string
			if i < m.FieldsPerPoint-1 {
				delim = ","
			}
			fields = append(fields, []byte(fmt.Sprintf("v%d=1%s", i, delim))...)
		}

		for i := 0; i < m.PointN(); i++ {
			// Write point.
			buf.Write([]byte(fmt.Sprintf("m%d", i%m.Measurements)))
			for j, value := range values {
				fmt.Fprintf(&buf, ",tag%d=value%d", j, value)
			}

			// Write fields
			buf.Write(append([]byte(" "), fields...))

			if m.timePerSeries != 0 {
				delta := time.Duration(int64(lastWrittenTotal+i) * m.timePerSeries)
				buf.Write([]byte(fmt.Sprintf(" %d\n", m.startTime.Add(delta).UnixNano())))
			} else {
				fmt.Fprint(&buf, "\n")
			}

			// Increment next tag value.
			for i := range values {
				values[i]++
				if values[i] < m.Tags[i] {
					break
				} else {
					values[i] = 0 // reset to zero, increment next value
					continue
				}
			}

			// Start new batch, if necessary.
			if i > 0 && i%m.BatchSize == 0 {
				ch <- copyBytes(buf.Bytes())
				buf.Reset()
			}
		}

		// Add final batch.
		if buf.Len() > 0 {
			ch <- copyBytes(buf.Bytes())
		}

		// Close channel.
		close(ch)
	}()

	return ch
}

// Vars is a subset of the data fields found at the /debug/vars endpoint.
type Vars struct {
	Memstats struct {
		HeapAlloc   int
		HeapInUse   int
		HeapObjects int
	} `json:"memstats"`
}

type Stats struct {
	Time   time.Time
	Tags   map[string]string
	Fields models.Fields
}

func (m *Main) Stats() *Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	elapsed := time.Since(m.now).Seconds()
	pThrough := float64(m.writtenN) / elapsed
	s := &Stats{
		Time: time.Unix(0, int64(time.Since(m.now))),
		Tags: m.ReportTags,
		Fields: models.Fields(map[string]interface{}{
			"T":              int(elapsed),
			"points_written": m.writtenN,
			"values_written": m.writtenN * m.FieldsPerPoint,
			"points_ps":      pThrough,
			"values_ps":      pThrough * float64(m.FieldsPerPoint),
			"write_error":    m.currentErrors,
			"resp_wma":       int(m.wmaLatency),
			"resp_mean":      int(m.totalLatency) / len(m.latencyHistory) / int(time.Millisecond),
			"resp_90":        int(m.quartileResponse(0.9) / time.Millisecond),
			"resp_95":        int(m.quartileResponse(0.95) / time.Millisecond),
			"resp_99":        int(m.quartileResponse(0.99) / time.Millisecond),
		}),
	}

	var isCreating bool
	if m.writtenN < m.SeriesN() {
		isCreating = true
	}
	s.Tags["creating_series"] = fmt.Sprint(isCreating)

	// Reset error count for next reporting.
	m.currentErrors = 0

	// Add runtime stats for the remote instance.
	var vars Vars
	resp, err := http.Get(strings.TrimSuffix(m.Host, "/") + "/debug/vars")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return s
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&vars); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return s
	}

	s.Fields["heap_alloc"] = vars.Memstats.HeapAlloc
	s.Fields["heap_in_use"] = vars.Memstats.HeapInUse
	s.Fields["heap_objects"] = vars.Memstats.HeapObjects
	return s
}

// runMonitor periodically prints the current status.
func (m *Main) runMonitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.printMonitorStats()
			if m.ReportHost != "" {
				m.sendMonitorStats(true)
			}
			return
		case <-ticker.C:
			m.printMonitorStats()
			if m.ReportHost != "" {
				m.sendMonitorStats(false)
			}
		}
	}
}

func (m *Main) sendMonitorStats(final bool) {
	stats := m.Stats()
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database: "ingest_benchmarks",
	})
	if err != nil {
		panic(err)
	}

	measurement := "runtime"
	t := stats.Time
	if final {
		measurement = "summary"
		t = time.Now().UTC()
	}

	p, err := client.NewPoint(measurement, stats.Tags, stats.Fields, t)
	if err != nil {
		panic(err)
	}
	bp.AddPoint(p)

	if err := m.clt.Write(bp); err != nil {
		fmt.Fprintf(os.Stderr, "unable to report stats to Influx: %v", err)
	}
}

func (m *Main) printMonitorStats() {
	writtenN := m.WrittenN()
	elapsed := time.Since(m.now).Seconds()
	var delay string
	var responses string

	m.mu.Lock()
	if m.TargetMaxLatency > 0 {
		delay = fmt.Sprintf(" | Writer delay currently: %s. WMA write latency: %s", m.currentDelay, time.Duration(m.wmaLatency))
	}

	if len(m.latencyHistory) >= 100 {
		responses = fmt.Sprintf(" | μ: %s, 90%%: %s, 95%%: %s, 99%%: %s", m.totalLatency/time.Duration(len(m.latencyHistory)), m.quartileResponse(0.9), m.quartileResponse(0.95), m.quartileResponse(0.99))
	}
	m.mu.Unlock()

	fmt.Printf("T=%08d %d points written (%0.1f pt/sec | %0.1f val/sec)%s%s\n",
		int(elapsed), writtenN, float64(writtenN)/elapsed, float64(m.FieldsPerPoint)*(float64(writtenN)/elapsed),
		delay, responses)
}

// This is really not the best way to do this, but it will give a reasonable
// approximation.
func (m *Main) quartileResponse(q float64) time.Duration {
	i := int(float64(len(m.latencyHistory))*q) - 1
	if i < 0 || i >= len(m.latencyHistory) {
		return time.Duration(-1) // Problem..
	}
	return m.latencyHistory[i]
}

// runClient executes a client to send points in a separate goroutine.
func (m *Main) runClient(ctx context.Context, ch <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return

		case buf, ok := <-ch:
			if !ok {
				return
			}

			// Keep trying batch until successful.
			// Stop client if it cannot connect.
			for {
				if err := m.sendBatch(buf); err == ErrConnectionRefused {
					return
				} else if err != nil {
					fmt.Fprintln(m.Stderr, err)
					if m.Strict {
						os.Exit(1)
					}
					continue
				}
				break
			}

			// Increment batch size.
			m.mu.Lock()
			m.writtenN += m.BatchSize
			m.mu.Unlock()
		}
	}
}

// setup initializes the database.
func (m *Main) setup() error {
	var client http.Client
	resp, err := client.Post(fmt.Sprintf("%s/query", m.Host), "application/x-www-form-urlencoded", strings.NewReader("q=CREATE+DATABASE+"+m.Database+"+WITH+DURATION+"+m.ShardDuration))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// sendBatch writes a batch to the server. Continually retries until successful.
func (m *Main) sendBatch(buf []byte) error {
	// Don't send the batch anywhere..
	if m.DryRun {
		return nil
	}

	// Send batch.
	var client http.Client
	now := time.Now().UTC()
	resp, err := client.Post(fmt.Sprintf("%s/write?db=%s&precision=ns&consistency=%s", m.Host, m.Database, m.Consistency), "text/ascii", bytes.NewReader(buf))
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			return ErrConnectionRefused
		}
		return err
	}
	defer resp.Body.Close()

	// Return body as error if unsuccessful.
	if resp.StatusCode != 204 {
		m.mu.Lock()
		m.currentErrors++
		m.mu.Unlock()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			body = []byte(err.Error())
		}
		return fmt.Errorf("[%d] %s", resp.StatusCode, body)
	}

	latency := time.Since(now)
	m.mu.Lock()
	m.totalLatency += latency

	// Maintain sorted list of latencies for quantile reporting
	i := sort.Search(len(m.latencyHistory), func(i int) bool { return m.latencyHistory[i] >= latency })
	if i >= len(m.latencyHistory) {
		m.latencyHistory = append(m.latencyHistory, latency)
	} else {
		m.latencyHistory = append(m.latencyHistory, 0)
		copy(m.latencyHistory[i+1:], m.latencyHistory[i:])
		m.latencyHistory[i] = latency
	}
	m.mu.Unlock()

	// Fixed delay.
	if m.Delay > 0 {
		time.Sleep(m.Delay)
		return nil
	} else if m.TargetMaxLatency <= 0 {
		return nil
	}

	// We're using an adaptive delay. The general idea is that inch will backoff
	// writers using a delay, if the average response from the server is getting
	// slower than the desired maximum latency. We use a weighted moving average
	// to determine that, favouring recent latencies over historic ones.
	//
	// The implementation is pretty ghetto at the moment, it has the following
	// rules:
	//
	//  - wma reponse time faster than desired latency and currentDelay > 0?
	//		* reduce currentDelay by 1/n * 0.25 * (desired latency - wma latency).
	//	- response time slower than desired latency?
	//		* increase currentDelay by 1/n * 0.25 * (desired latency - wma response).
	//	- currentDelay < 100ms?
	//		* set currentDelay to 0
	//
	// n is the number of concurent writers. The general rule then, is that
	// we look at how far away from the desired latency and move a quarter of the
	// way there in total (over all writers). If we're coming un under the max
	// latency and our writers are using a delay (currentDelay > 0) then we will
	// try to reduce this to increase throughput.
	m.mu.Lock()

	// Calculate the weighted moving average latency. We weight this response
	// latency by 1-alpha, and the historic average by alpha.
	m.wmaLatency = (m.alpha * m.wmaLatency) + ((1.0 - m.alpha) * (float64(latency) - m.wmaLatency))

	// Update how we adjust our latency by
	delta := 1.0 / float64(m.Concurrency) * 0.5 * (m.wmaLatency - float64(m.TargetMaxLatency))
	m.currentDelay += time.Duration(delta)
	if m.currentDelay < time.Millisecond*100 {
		m.currentDelay = 0
	}

	thisDelay := m.currentDelay

	m.mu.Unlock()

	time.Sleep(thisDelay)
	return nil
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	tmp := make([]byte, len(b))
	copy(tmp, b)
	return tmp
}
