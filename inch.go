package inch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
)

// ErrConnectionRefused indicates that the connection to the remote server was refused.
var ErrConnectionRefused = errors.New("connection refused")

// ErrorList is a simple error aggrigator to return multiple errors as one.
type ErrorList []error

func (el ErrorList) Error() string {
	msg := ""

	for _, err := range el {
		msg = fmt.Sprintf("%s%s\n", msg, err)
	}

	return msg
}

// Main represents the main program execution.
type Inch struct {
	mu             sync.Mutex
	writtenN       int
	startTime      time.Time
	now            time.Time
	timePerSeries  int64 // How much the client is backing off due to unacceptible response times.
	currentDelay   time.Duration
	wmaLatency     float64
	latencyHistory []time.Duration
	totalLatency   time.Duration
	currentErrors  int   // The current number of errors since last reporting.
	totalErrors    int64 // The total number of errors encountered.

	Stdout io.Writer
	Stderr io.Writer

	// Client to be used to report statistics to an Influx instance.
	clt client.Client

	// Client for writing and manipulating influxdb host
	writeClient *http.Client

	// Decay factor used when weighting average latency returned by server.
	alpha float64

	Verbose        bool
	ReportHost     string
	ReportUser     string
	ReportPassword string
	ReportTags     map[string]string
	DryRun         bool
	MaxErrors      int

	Host             string
	User             string
	Password         string
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
func NewInch() *Inch {
	writeClient := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}}

	// create an inch object with reasonable defaults
	return &Inch{

		alpha:          0.5, // Weight the mean latency by 50% history / 50% latest value.
		latencyHistory: make([]time.Duration, 0, 200),
		writeClient:    writeClient,

		Consistency:     "any",
		Concurrency:     1,
		Measurements:    1,
		Tags:            []int{10, 10, 10},
		PointsPerSeries: 100,
		FieldsPerPoint:  1,
		BatchSize:       5000,
		Database:        "db",
		ShardDuration:   "7d",
	}
}

// ParseFlags parses the command line flags.
func (inch *Inch) Valid() error {
	el := ErrorList{}

	switch inch.Consistency {
	case "any", "quorum", "one", "all":
	default:
		el = append(el, errors.New(`Consistency must be one of: {"any", "quorum", "one", "all"}`))
	}

	if inch.FieldsPerPoint < 1 {
		el = append(el, errors.New("number of fields must be > 0"))
	}

	// validate reporting client is accessable
	if inch.ReportHost != "" {
		var err error
		inch.clt, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:               inch.ReportHost,
			Username:           inch.ReportUser,
			Password:           inch.ReportPassword,
			InsecureSkipVerify: true})
		if err != nil {
			el = append(el, fmt.Errorf("failed to communicate with %q: %s", inch.ReportHost, err))
			return el
		}

		if _, err := inch.clt.Query(client.NewQuery(fmt.Sprintf(`CREATE DATABASE "ingest_benchmarks"`), "", "")); err != nil {
			el = append(el, fmt.Errorf("unable to connect to %q: %s", inch.ReportHost, err))
			return el
		}
	}

	if len(el) > 0 {
		return el
	}

	return nil
}

// Run executes the Simulator.
func (inch *Inch) Run() error {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Print settings.
	fmt.Fprintf(inch.Stdout, "Host: %s\n", inch.Host)
	fmt.Fprintf(inch.Stdout, "Concurrency: %d\n", inch.Concurrency)
	fmt.Fprintf(inch.Stdout, "Measurements: %d\n", inch.Measurements)
	fmt.Fprintf(inch.Stdout, "Tag cardinalities: %+v\n", inch.Tags)
	fmt.Fprintf(inch.Stdout, "Points per series: %d\n", inch.PointsPerSeries)
	fmt.Fprintf(inch.Stdout, "Total series: %d\n", inch.SeriesN())
	fmt.Fprintf(inch.Stdout, "Total points: %d\n", inch.PointN())
	fmt.Fprintf(inch.Stdout, "Total fields per point: %d\n", inch.FieldsPerPoint)
	fmt.Fprintf(inch.Stdout, "Batch Size: %d\n", inch.BatchSize)
	fmt.Fprintf(inch.Stdout, "Database: %s (Shard duration: %s)\n", inch.Database, inch.ShardDuration)
	fmt.Fprintf(inch.Stdout, "Write Consistency: %s\n", inch.Consistency)

	if inch.TargetMaxLatency > 0 {
		fmt.Fprintf(inch.Stdout, "Adaptive latency on. Max target: %s\n", inch.TargetMaxLatency)
	} else if inch.Delay > 0 {
		fmt.Fprintf(inch.Stdout, "Fixed write delay: %s\n", inch.Delay)
	}

	dur := fmt.Sprint(inch.TimeSpan)
	if inch.TimeSpan == 0 {
		dur = "off"
	}

	// Initialize database.
	if err := inch.setup(); err != nil {
		return err
	}

	// Record start time.
	inch.now = time.Now().UTC()
	inch.startTime = inch.now
	if inch.TimeSpan != 0 {
		absTimeSpan := int64(math.Abs(float64(inch.TimeSpan)))
		inch.timePerSeries = absTimeSpan / int64(inch.PointN())

		// If we're back-filling then we need to move the start time back.
		if inch.TimeSpan < 0 {
			inch.startTime = inch.startTime.Add(inch.TimeSpan)
		}
	}
	fmt.Fprintf(inch.Stdout, "Start time: %s\n", inch.startTime)
	if inch.TimeSpan < 0 {
		fmt.Fprintf(inch.Stdout, "Approx End time: %s\n", time.Now().UTC())
	} else if inch.TimeSpan > 0 {
		fmt.Fprintf(inch.Stdout, "Approx End time: %s\n", inch.startTime.Add(inch.TimeSpan).UTC())
	} else {
		fmt.Fprintf(inch.Stdout, "Time span: %s\n", dur)
	}

	// Stream batches from a separate goroutine.
	ch := inch.generateBatches()

	// Start clients.
	var wg sync.WaitGroup
	for j := 0; j < inch.Concurrency; j++ {
		wg.Add(1)
		go func() { defer wg.Done(); inch.runClient(ctx, ch) }()
	}

	// Start monitor.
	var monitorWaitGroup sync.WaitGroup
	if inch.Verbose {
		monitorWaitGroup.Add(1)
		go func() { defer monitorWaitGroup.Done(); inch.runMonitor(ctx) }()
	}

	// Wait for all clients to complete.
	wg.Wait()

	// Wait for monitor.
	cancel()
	monitorWaitGroup.Wait()

	// Report stats.
	elapsed := time.Since(inch.now)
	fmt.Fprintln(inch.Stdout, "")
	fmt.Fprintf(inch.Stdout, "Total time: %0.1f seconds\n", elapsed.Seconds())

	return nil
}

// WrittenN returns the total number of points written.
func (inch *Inch) WrittenN() int {
	inch.mu.Lock()
	defer inch.mu.Unlock()
	return inch.writtenN
}

// TagsN returns the total number of tags.
func (inch *Inch) TagsN() int {
	tagTotal := inch.Tags[0]
	for _, v := range inch.Tags[1:] {
		tagTotal *= v
	}
	return tagTotal
}

// SeriesN returns the total number of series to write.
func (inch *Inch) SeriesN() int {
	return inch.TagsN() * inch.Measurements
}

// PointN returns the total number of points to write.
func (inch *Inch) PointN() int {
	return int(inch.PointsPerSeries) * inch.SeriesN()
}

// BatchN returns the total number of batches.
func (inch *Inch) BatchN() int {
	n := inch.PointN() / inch.BatchSize
	if inch.PointN()%inch.BatchSize != 0 {
		n++
	}
	return n
}

// generateBatches returns a channel for streaming batches.
func (inch *Inch) generateBatches() <-chan []byte {
	ch := make(chan []byte, 10)

	go func() {
		var buf bytes.Buffer
		values := make([]int, len(inch.Tags))
		lastWrittenTotal := inch.WrittenN()

		// Generate field string.
		var fields []byte
		for i := 0; i < inch.FieldsPerPoint; i++ {
			var delim string
			if i < inch.FieldsPerPoint-1 {
				delim = ","
			}
			fields = append(fields, []byte(fmt.Sprintf("v%d=1%s", i, delim))...)
		}

		for i := 0; i < inch.PointN(); i++ {
			// Write point.
			buf.Write([]byte(fmt.Sprintf("m%d", i%inch.Measurements)))
			for j, value := range values {
				fmt.Fprintf(&buf, ",tag%d=value%d", j, value)
			}

			// Write fields
			buf.Write(append([]byte(" "), fields...))

			if inch.timePerSeries != 0 {
				delta := time.Duration(int64(lastWrittenTotal+i) * inch.timePerSeries)
				buf.Write([]byte(fmt.Sprintf(" %d\n", inch.startTime.Add(delta).UnixNano())))
			} else {
				fmt.Fprint(&buf, "\n")
			}

			// Increment next tag value.
			for i := range values {
				values[i]++
				if values[i] < inch.Tags[i] {
					break
				} else {
					values[i] = 0 // reset to zero, increment next value
					continue
				}
			}

			// Start new batch, if necessary.
			if i > 0 && i%inch.BatchSize == 0 {
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

func (inch *Inch) Stats() *Stats {
	inch.mu.Lock()
	defer inch.mu.Unlock()
	elapsed := time.Since(inch.now).Seconds()
	pThrough := float64(inch.writtenN) / elapsed
	s := &Stats{
		Time: time.Unix(0, int64(time.Since(inch.now))),
		Tags: inch.ReportTags,
		Fields: models.Fields(map[string]interface{}{
			"T":              int(elapsed),
			"points_written": inch.writtenN,
			"values_written": inch.writtenN * inch.FieldsPerPoint,
			"points_ps":      pThrough,
			"values_ps":      pThrough * float64(inch.FieldsPerPoint),
			"write_error":    inch.currentErrors,
			"resp_wma":       int(inch.wmaLatency),
			"resp_mean":      int(inch.totalLatency) / len(inch.latencyHistory) / int(time.Millisecond),
			"resp_90":        int(inch.quartileResponse(0.9) / time.Millisecond),
			"resp_95":        int(inch.quartileResponse(0.95) / time.Millisecond),
			"resp_99":        int(inch.quartileResponse(0.99) / time.Millisecond),
		}),
	}

	var isCreating bool
	if inch.writtenN < inch.SeriesN() {
		isCreating = true
	}
	s.Tags["creating_series"] = fmt.Sprint(isCreating)

	// Reset error count for next reporting.
	inch.currentErrors = 0

	// Add runtime stats for the remote instance.
	var vars Vars
	resp, err := http.Get(strings.TrimSuffix(inch.Host, "/") + "/debug/vars")
	if err != nil {
		// Don't log error as it can get spammy.
		return s
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&vars); err != nil {
		fmt.Fprintln(inch.Stderr, err)
		return s
	}

	s.Fields["heap_alloc"] = vars.Memstats.HeapAlloc
	s.Fields["heap_in_use"] = vars.Memstats.HeapInUse
	s.Fields["heap_objects"] = vars.Memstats.HeapObjects
	return s
}

// runMonitor periodically prints the current status.
func (inch *Inch) runMonitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			inch.printMonitorStats()
			if inch.ReportHost != "" {
				inch.sendMonitorStats(true)
			}
			return
		case <-ticker.C:
			inch.printMonitorStats()
			if inch.ReportHost != "" {
				inch.sendMonitorStats(false)
			}
		}
	}
}

func (inch *Inch) sendMonitorStats(final bool) {
	stats := inch.Stats()
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

	if err := inch.clt.Write(bp); err != nil {
		fmt.Fprintf(inch.Stderr, "unable to report stats to Influx: %v", err)
	}
}

func (inch *Inch) printMonitorStats() {
	writtenN := inch.WrittenN()
	elapsed := time.Since(inch.now).Seconds()
	var delay string
	var responses string

	inch.mu.Lock()
	if inch.TargetMaxLatency > 0 {
		delay = fmt.Sprintf(" | Writer delay currently: %s. WMA write latency: %s", inch.currentDelay, time.Duration(inch.wmaLatency))
	}

	if len(inch.latencyHistory) >= 100 {
		responses = fmt.Sprintf(" | μ: %s, 90%%: %s, 95%%: %s, 99%%: %s", inch.totalLatency/time.Duration(len(inch.latencyHistory)), inch.quartileResponse(0.9), inch.quartileResponse(0.95), inch.quartileResponse(0.99))
	}
	inch.mu.Unlock()

	fmt.Printf("T=%08d %d points written (%0.1f pt/sec | %0.1f val/sec)%s%s\n",
		int(elapsed), writtenN, float64(writtenN)/elapsed, float64(inch.FieldsPerPoint)*(float64(writtenN)/elapsed),
		delay, responses)
}

// This is really not the best way to do this, but it will give a reasonable
// approximation.
func (inch *Inch) quartileResponse(q float64) time.Duration {
	i := int(float64(len(inch.latencyHistory))*q) - 1
	if i < 0 || i >= len(inch.latencyHistory) {
		return time.Duration(-1) // Probleinch..
	}
	return inch.latencyHistory[i]
}

// runClient executes a client to send points in a separate goroutine.
func (inch *Inch) runClient(ctx context.Context, ch <-chan []byte) {
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
				if err := inch.sendBatch(buf); err == ErrConnectionRefused {
					return
				} else if err != nil {
					fmt.Fprintln(inch.Stderr, err)
					inch.mu.Lock()
					totalErrors := inch.totalErrors
					inch.mu.Unlock()

					if inch.MaxErrors > 0 && totalErrors >= int64(inch.MaxErrors) {
						fmt.Fprintf(inch.Stderr, "Exiting due to reaching %d errors.\n", totalErrors)
						os.Exit(1)
					}
					continue
				}
				break
			}

			// Increment batch size.
			inch.mu.Lock()
			inch.writtenN += inch.BatchSize
			inch.mu.Unlock()
		}
	}
}

// setup pulls the build and version from the server and initializes the database.
func (inch *Inch) setup() error {

	// Validate that we can connect to the test host
	resp, err := http.Get(strings.TrimSuffix(inch.Host, "/") + "/ping")
	if err != nil {
		return fmt.Errorf("unable to connect to %q: %s", inch.Host, err)
	}
	defer resp.Body.Close()

	build := resp.Header.Get("X-Influxdb-Build")
	if len(build) > 0 {
		inch.ReportTags["build"] = build
	}

	version := resp.Header.Get("X-Influxdb-Version")
	if len(version) > 0 {
		inch.ReportTags["version"] = version
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/query", inch.Host), strings.NewReader("q=CREATE+DATABASE+"+inch.Database+"+WITH+DURATION+"+inch.ShardDuration))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if inch.User != "" && inch.Password != "" {
		req.SetBasicAuth(inch.User, inch.Password)
	}

	resp, err = inch.writeClient.Do(req)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// sendBatch writes a batch to the server. Continually retries until successful.
func (inch *Inch) sendBatch(buf []byte) error {
	// Don't send the batch anywhere..
	if inch.DryRun {
		return nil
	}

	// Send batch.
	now := time.Now().UTC()
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/write?db=%s&precision=ns&consistency=%s", inch.Host, inch.Database, inch.Consistency), bytes.NewReader(buf))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/ascii")

	if inch.User != "" && inch.Password != "" {
		req.SetBasicAuth(inch.User, inch.Password)
	}

	resp, err := inch.writeClient.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			return ErrConnectionRefused
		}
		return err
	}
	defer resp.Body.Close()

	// Return body as error if unsuccessful.
	if resp.StatusCode != 204 {
		inch.mu.Lock()
		inch.currentErrors++
		inch.totalErrors++
		inch.mu.Unlock()

		// If it looks like the server is down and we're hitting the gateway
		// or a load balancer, then add a delay.
		if resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusServiceUnavailable {
			time.Sleep(time.Second)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			body = []byte(err.Error())
		}
		return fmt.Errorf("[%d] %s", resp.StatusCode, body)
	}

	latency := time.Since(now)
	inch.mu.Lock()
	inch.totalLatency += latency

	// Maintain sorted list of latencies for quantile reporting
	i := sort.Search(len(inch.latencyHistory), func(i int) bool { return inch.latencyHistory[i] >= latency })
	if i >= len(inch.latencyHistory) {
		inch.latencyHistory = append(inch.latencyHistory, latency)
	} else {
		inch.latencyHistory = append(inch.latencyHistory, 0)
		copy(inch.latencyHistory[i+1:], inch.latencyHistory[i:])
		inch.latencyHistory[i] = latency
	}
	inch.mu.Unlock()

	// Fixed delay.
	if inch.Delay > 0 {
		time.Sleep(inch.Delay)
		return nil
	} else if inch.TargetMaxLatency <= 0 {
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
	inch.mu.Lock()

	// Calculate the weighted moving average latency. We weight this response
	// latency by 1-alpha, and the historic average by alpha.
	inch.wmaLatency = (inch.alpha * inch.wmaLatency) + ((1.0 - inch.alpha) * (float64(latency) - inch.wmaLatency))

	// Update how we adjust our latency by
	delta := 1.0 / float64(inch.Concurrency) * 0.5 * (inch.wmaLatency - float64(inch.TargetMaxLatency))
	inch.currentDelay += time.Duration(delta)
	if inch.currentDelay < time.Millisecond*100 {
		inch.currentDelay = 0
	}

	thisDelay := inch.currentDelay

	inch.mu.Unlock()

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
