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
type Simulator struct {
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
<<<<<<< HEAD
func NewInch() *Inch {
	writeClient := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}}
=======
func NewSimulator() *Simulator {
>>>>>>> 70a3d77... resolve the issues reported by benbjohnson

	// create an inch object with reasonable defaults
	return &Simulator{

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
func (sim *Simulator) Validate() error {
	var el ErrorList

	switch sim.Consistency {
	case "any", "quorum", "one", "all":
	default:
		el = append(el, errors.New(`Consistency must be one of: {"any", "quorum", "one", "all"}`))
	}

	if sim.FieldsPerPoint < 1 {
		el = append(el, errors.New("number of fields must be > 0"))
	}

	// validate reporting client is accessable
	if sim.ReportHost != "" {
		var err error
<<<<<<< HEAD
		inch.clt, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:               inch.ReportHost,
			Username:           inch.ReportUser,
			Password:           inch.ReportPassword,
			InsecureSkipVerify: true})
=======
		sim.clt, err = client.NewHTTPClient(client.HTTPConfig{
			Addr: sim.ReportHost,
		})
>>>>>>> 70a3d77... resolve the issues reported by benbjohnson
		if err != nil {
			el = append(el, fmt.Errorf("failed to communicate with %q: %s", sim.ReportHost, err))
			return el
		}

		if _, err := sim.clt.Query(client.NewQuery(fmt.Sprintf(`CREATE DATABASE "ingest_benchmarks"`), "", "")); err != nil {
			el = append(el, fmt.Errorf("unable to connect to %q: %s", sim.ReportHost, err))
			return el
		}
	}

	if len(el) > 0 {
		return el
	}

	return nil
}

<<<<<<< HEAD
// Run executes the Simulator.
func (inch *Inch) Run() error {
=======
// Run executes the program.
func (sim *Simulator) Run() error {
	// check valid settings before starting
	err := sim.Validate()
	if err != nil {
		return err
	}
>>>>>>> 70a3d77... resolve the issues reported by benbjohnson

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Print settings.
	fmt.Fprintf(sim.Stdout, "Host: %s\n", sim.Host)
	fmt.Fprintf(sim.Stdout, "Concurrency: %d\n", sim.Concurrency)
	fmt.Fprintf(sim.Stdout, "Measurements: %d\n", sim.Measurements)
	fmt.Fprintf(sim.Stdout, "Tag cardinalities: %+v\n", sim.Tags)
	fmt.Fprintf(sim.Stdout, "Points per series: %d\n", sim.PointsPerSeries)
	fmt.Fprintf(sim.Stdout, "Total series: %d\n", sim.SeriesN())
	fmt.Fprintf(sim.Stdout, "Total points: %d\n", sim.PointN())
	fmt.Fprintf(sim.Stdout, "Total fields per point: %d\n", sim.FieldsPerPoint)
	fmt.Fprintf(sim.Stdout, "Batch Size: %d\n", sim.BatchSize)
	fmt.Fprintf(sim.Stdout, "Database: %s (Shard duration: %s)\n", sim.Database, sim.ShardDuration)
	fmt.Fprintf(sim.Stdout, "Write Consistency: %s\n", sim.Consistency)

	if sim.TargetMaxLatency > 0 {
		fmt.Fprintf(sim.Stdout, "Adaptive latency on. Max target: %s\n", sim.TargetMaxLatency)
	} else if sim.Delay > 0 {
		fmt.Fprintf(sim.Stdout, "Fixed write delay: %s\n", sim.Delay)
	}

	dur := fmt.Sprint(sim.TimeSpan)
	if sim.TimeSpan == 0 {
		dur = "off"
	}

	// Initialize database.
	if err := sim.setup(); err != nil {
		return err
	}

	// Record start time.
	sim.now = time.Now().UTC()
	sim.startTime = sim.now
	if sim.TimeSpan != 0 {
		absTimeSpan := int64(math.Abs(float64(sim.TimeSpan)))
		sim.timePerSeries = absTimeSpan / int64(sim.PointN())

		// If we're back-filling then we need to move the start time back.
		if sim.TimeSpan < 0 {
			sim.startTime = sim.startTime.Add(sim.TimeSpan)
		}
	}
	fmt.Fprintf(sim.Stdout, "Start time: %s\n", sim.startTime)
	if sim.TimeSpan < 0 {
		fmt.Fprintf(sim.Stdout, "Approx End time: %s\n", time.Now().UTC())
	} else if sim.TimeSpan > 0 {
		fmt.Fprintf(sim.Stdout, "Approx End time: %s\n", sim.startTime.Add(sim.TimeSpan).UTC())
	} else {
		fmt.Fprintf(sim.Stdout, "Time span: %s\n", dur)
	}

	// Stream batches from a separate goroutine.
	ch := sim.generateBatches()

	// Start clients.
	var wg sync.WaitGroup
	for i := 0; i < sim.Concurrency; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sim.runClient(ctx, ch) }()
	}

	// Start monitor.
	var monitorWaitGroup sync.WaitGroup
	if sim.Verbose {
		monitorWaitGroup.Add(1)
		go func() { defer monitorWaitGroup.Done(); sim.runMonitor(ctx) }()
	}

	// Wait for all clients to complete.
	wg.Wait()

	// Wait for monitor.
	cancel()
	monitorWaitGroup.Wait()

	// Report stats.
	elapsed := time.Since(sim.now)
	fmt.Fprintln(sim.Stdout, "")
	fmt.Fprintf(sim.Stdout, "Total time: %0.1f seconds\n", elapsed.Seconds())

	return nil
}

// WrittenN returns the total number of points written.
func (sim *Simulator) WrittenN() int {
	sim.mu.Lock()
	defer sim.mu.Unlock()
	return sim.writtenN
}

// TagsN returns the total number of tags.
func (sim *Simulator) TagsN() int {
	tagTotal := sim.Tags[0]
	for _, v := range sim.Tags[1:] {
		tagTotal *= v
	}
	return tagTotal
}

// SeriesN returns the total number of series to write.
func (sim *Simulator) SeriesN() int {
	return sim.TagsN() * sim.Measurements
}

// PointN returns the total number of points to write.
func (sim *Simulator) PointN() int {
	return int(sim.PointsPerSeries) * sim.SeriesN()
}

// BatchN returns the total number of batches.
func (sim *Simulator) BatchN() int {
	n := sim.PointN() / sim.BatchSize
	if sim.PointN()%sim.BatchSize != 0 {
		n++
	}
	return n
}

// generateBatches returns a channel for streaming batches.
func (sim *Simulator) generateBatches() <-chan []byte {
	ch := make(chan []byte, 10)

	go func() {
		var buf bytes.Buffer
		values := make([]int, len(sim.Tags))
		lastWrittenTotal := sim.WrittenN()

		// Generate field string.
		var fields []byte
		for i := 0; i < sim.FieldsPerPoint; i++ {
			var delim string
			if i < sim.FieldsPerPoint-1 {
				delim = ","
			}
			fields = append(fields, []byte(fmt.Sprintf("v%d=1%s", i, delim))...)
		}

		for i := 0; i < sim.PointN(); i++ {
			// Write point.
			buf.Write([]byte(fmt.Sprintf("m%d", i%sim.Measurements)))
			for j, value := range values {
				fmt.Fprintf(&buf, ",tag%d=value%d", j, value)
			}

			// Write fields
			buf.Write(append([]byte(" "), fields...))

			if sim.timePerSeries != 0 {
				delta := time.Duration(int64(lastWrittenTotal+i) * sim.timePerSeries)
				buf.Write([]byte(fmt.Sprintf(" %d\n", sim.startTime.Add(delta).UnixNano())))
			} else {
				fmt.Fprint(&buf, "\n")
			}

			// Increment next tag value.
			for i := range values {
				values[i]++
				if values[i] < sim.Tags[i] {
					break
				} else {
					values[i] = 0 // reset to zero, increment next value
					continue
				}
			}

			// Start new batch, if necessary.
			if i > 0 && i%sim.BatchSize == 0 {
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

func (sim *Simulator) Stats() *Stats {
	sim.mu.Lock()
	defer sim.mu.Unlock()
	elapsed := time.Since(sim.now).Seconds()
	pThrough := float64(sim.writtenN) / elapsed
	s := &Stats{
		Time: time.Unix(0, int64(time.Since(sim.now))),
		Tags: sim.ReportTags,
		Fields: models.Fields(map[string]interface{}{
			"T":              int(elapsed),
			"points_written": sim.writtenN,
			"values_written": sim.writtenN * sim.FieldsPerPoint,
			"points_ps":      pThrough,
			"values_ps":      pThrough * float64(sim.FieldsPerPoint),
			"write_error":    sim.currentErrors,
			"resp_wma":       int(sim.wmaLatency),
			"resp_mean":      int(sim.totalLatency) / len(sim.latencyHistory) / int(time.Millisecond),
			"resp_90":        int(sim.quartileResponse(0.9) / time.Millisecond),
			"resp_95":        int(sim.quartileResponse(0.95) / time.Millisecond),
			"resp_99":        int(sim.quartileResponse(0.99) / time.Millisecond),
		}),
	}

	var isCreating bool
	if sim.writtenN < sim.SeriesN() {
		isCreating = true
	}
	s.Tags["creating_series"] = fmt.Sprint(isCreating)

	// Reset error count for next reporting.
	sim.currentErrors = 0

	// Add runtime stats for the remote instance.
	var vars Vars
	resp, err := http.Get(strings.TrimSuffix(sim.Host, "/") + "/debug/vars")
	if err != nil {
		// Don't log error as it can get spammy.
		return s
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&vars); err != nil {
		fmt.Fprintln(sim.Stderr, err)
		return s
	}

	s.Fields["heap_alloc"] = vars.Memstats.HeapAlloc
	s.Fields["heap_in_use"] = vars.Memstats.HeapInUse
	s.Fields["heap_objects"] = vars.Memstats.HeapObjects
	return s
}

// runMonitor periodically prints the current status.
func (sim *Simulator) runMonitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			sim.printMonitorStats()
			if sim.ReportHost != "" {
				sim.sendMonitorStats(true)
			}
			return
		case <-ticker.C:
			sim.printMonitorStats()
			if sim.ReportHost != "" {
				sim.sendMonitorStats(false)
			}
		}
	}
}

func (sim *Simulator) sendMonitorStats(final bool) {
	stats := sim.Stats()
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

	if err := sim.clt.Write(bp); err != nil {
		fmt.Fprintf(sim.Stderr, "unable to report stats to Influx: %v", err)
	}
}

func (sim *Simulator) printMonitorStats() {
	writtenN := sim.WrittenN()
	elapsed := time.Since(sim.now).Seconds()
	var delay string
	var responses string

	sim.mu.Lock()
	if sim.TargetMaxLatency > 0 {
		delay = fmt.Sprintf(" | Writer delay currently: %s. WMA write latency: %s", sim.currentDelay, time.Duration(sim.wmaLatency))
	}

	if len(sim.latencyHistory) >= 100 {
		responses = fmt.Sprintf(" | μ: %s, 90%%: %s, 95%%: %s, 99%%: %s", sim.totalLatency/time.Duration(len(sim.latencyHistory)), sim.quartileResponse(0.9), sim.quartileResponse(0.95), sim.quartileResponse(0.99))
	}
	sim.mu.Unlock()

	fmt.Printf("T=%08d %d points written (%0.1f pt/sec | %0.1f val/sec)%s%s\n",
		int(elapsed), writtenN, float64(writtenN)/elapsed, float64(sim.FieldsPerPoint)*(float64(writtenN)/elapsed),
		delay, responses)
}

// This is really not the best way to do this, but it will give a reasonable
// approximation.
func (sim *Simulator) quartileResponse(q float64) time.Duration {
	i := int(float64(len(sim.latencyHistory))*q) - 1
	if i < 0 || i >= len(sim.latencyHistory) {
		return time.Duration(-1) // Problesim..
	}
	return sim.latencyHistory[i]
}

// runClient executes a client to send points in a separate goroutine.
func (sim *Simulator) runClient(ctx context.Context, ch <-chan []byte) {
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
				if err := sim.sendBatch(buf); err == ErrConnectionRefused {
					return
				} else if err != nil {
					fmt.Fprintln(sim.Stderr, err)
					sim.mu.Lock()
					totalErrors := sim.totalErrors
					sim.mu.Unlock()

					if sim.MaxErrors > 0 && totalErrors >= int64(sim.MaxErrors) {
						fmt.Fprintf(sim.Stderr, "Exiting due to reaching %d errors.\n", totalErrors)
						os.Exit(1)
					}
					continue
				}
				break
			}

			// Increment batch size.
			sim.mu.Lock()
			sim.writtenN += sim.BatchSize
			sim.mu.Unlock()
		}
	}
}

// setup pulls the build and version from the server and initializes the database.
func (sim *Simulator) setup() error {

	// Validate that we can connect to the test host
	resp, err := http.Get(strings.TrimSuffix(sim.Host, "/") + "/ping")
	if err != nil {
		return fmt.Errorf("unable to connect to %q: %s", sim.Host, err)
	}
	defer resp.Body.Close()

	build := resp.Header.Get("X-Influxdb-Build")
	if len(build) > 0 {
		sim.ReportTags["build"] = build
	}

	version := resp.Header.Get("X-Influxdb-Version")
	if len(version) > 0 {
		sim.ReportTags["version"] = version
	}

<<<<<<< HEAD
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/query", inch.Host), strings.NewReader("q=CREATE+DATABASE+"+inch.Database+"+WITH+DURATION+"+inch.ShardDuration))
=======
	var client http.Client
	resp, err = client.Post(fmt.Sprintf("%s/query", sim.Host), "application/x-www-form-urlencoded", strings.NewReader("q=CREATE+DATABASE+"+sim.Database+"+WITH+DURATION+"+sim.ShardDuration))
>>>>>>> 70a3d77... resolve the issues reported by benbjohnson
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
func (sim *Simulator) sendBatch(buf []byte) error {
	// Don't send the batch anywhere..
	if sim.DryRun {
		return nil
	}

	// Send batch.
	now := time.Now().UTC()
<<<<<<< HEAD
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/write?db=%s&precision=ns&consistency=%s", inch.Host, inch.Database, inch.Consistency), bytes.NewReader(buf))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/ascii")

	if inch.User != "" && inch.Password != "" {
		req.SetBasicAuth(inch.User, inch.Password)
	}

	resp, err := inch.writeClient.Do(req)
=======
	resp, err := client.Post(fmt.Sprintf("%s/write?db=%s&precision=ns&consistency=%s", sim.Host, sim.Database, sim.Consistency), "text/ascii", bytes.NewReader(buf))
>>>>>>> 70a3d77... resolve the issues reported by benbjohnson
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			return ErrConnectionRefused
		}
		return err
	}
	defer resp.Body.Close()

	// Return body as error if unsuccessful.
	if resp.StatusCode != 204 {
		sim.mu.Lock()
		sim.currentErrors++
		sim.totalErrors++
		sim.mu.Unlock()

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
	sim.mu.Lock()
	sim.totalLatency += latency

	// Maintain sorted list of latencies for quantile reporting
	i := sort.Search(len(sim.latencyHistory), func(i int) bool { return sim.latencyHistory[i] >= latency })
	if i >= len(sim.latencyHistory) {
		sim.latencyHistory = append(sim.latencyHistory, latency)
	} else {
		sim.latencyHistory = append(sim.latencyHistory, 0)
		copy(sim.latencyHistory[i+1:], sim.latencyHistory[i:])
		sim.latencyHistory[i] = latency
	}
	sim.mu.Unlock()

	// Fixed delay.
	if sim.Delay > 0 {
		time.Sleep(sim.Delay)
		return nil
	} else if sim.TargetMaxLatency <= 0 {
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
	sim.mu.Lock()

	// Calculate the weighted moving average latency. We weight this response
	// latency by 1-alpha, and the historic average by alpha.
	sim.wmaLatency = (sim.alpha * sim.wmaLatency) + ((1.0 - sim.alpha) * (float64(latency) - sim.wmaLatency))

	// Update how we adjust our latency by
	delta := 1.0 / float64(sim.Concurrency) * 0.5 * (sim.wmaLatency - float64(sim.TargetMaxLatency))
	sim.currentDelay += time.Duration(delta)
	if sim.currentDelay < time.Millisecond*100 {
		sim.currentDelay = 0
	}

	thisDelay := sim.currentDelay

	sim.mu.Unlock()

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
