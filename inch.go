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

// ErrorList is a simple error aggregator to return multiple errors as one.
type ErrorList []error

func (el ErrorList) Error() string {
	var msg string
	for _, err := range el {
		msg = fmt.Sprintf("%s%s\n", msg, err)
	}
	return msg
}

// Simulator represents the main program execution.
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

// NewSimulator returns a new instance of Simulator.
func NewSimulator() *Simulator {
	writeClient := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}}

	// Create an Simulator object with reasonable defaults.
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

// Validate parses the command line flags.
func (s *Simulator) Validate() error {
	var el ErrorList

	switch s.Consistency {
	case "any", "quorum", "one", "all":
	default:
		el = append(el, errors.New(`Consistency must be one of: {"any", "quorum", "one", "all"}`))
	}

	if s.FieldsPerPoint < 1 {
		el = append(el, errors.New("number of fields must be > 0"))
	}

	// validate reporting client is accessable
	if s.ReportHost != "" {
		var err error
		s.clt, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:               s.ReportHost,
			Username:           s.ReportUser,
			Password:           s.ReportPassword,
			InsecureSkipVerify: true,
		})
		if err != nil {
			el = append(el, fmt.Errorf("failed to communicate with %q: %s", s.ReportHost, err))
			return el
		}

		if _, err := s.clt.Query(client.NewQuery(fmt.Sprintf(`CREATE DATABASE "ingest_benchmarks"`), "", "")); err != nil {
			el = append(el, fmt.Errorf("unable to connect to %q: %s", s.ReportHost, err))
			return el
		}
	}

	if len(el) > 0 {
		return el
	}

	return nil
}

// Run executes the program.
func (s *Simulator) Run(ctx context.Context) error {
	// check valid settings before starting
	err := s.Validate()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Print settings.
	fmt.Fprintf(s.Stdout, "Host: %s\n", s.Host)
	fmt.Fprintf(s.Stdout, "Concurrency: %d\n", s.Concurrency)
	fmt.Fprintf(s.Stdout, "Measurements: %d\n", s.Measurements)
	fmt.Fprintf(s.Stdout, "Tag cardinalities: %+v\n", s.Tags)
	fmt.Fprintf(s.Stdout, "Points per series: %d\n", s.PointsPerSeries)
	fmt.Fprintf(s.Stdout, "Total series: %d\n", s.SeriesN())
	fmt.Fprintf(s.Stdout, "Total points: %d\n", s.PointN())
	fmt.Fprintf(s.Stdout, "Total fields per point: %d\n", s.FieldsPerPoint)
	fmt.Fprintf(s.Stdout, "Batch Size: %d\n", s.BatchSize)
	fmt.Fprintf(s.Stdout, "Database: %s (Shard duration: %s)\n", s.Database, s.ShardDuration)
	fmt.Fprintf(s.Stdout, "Write Consistency: %s\n", s.Consistency)

	if s.TargetMaxLatency > 0 {
		fmt.Fprintf(s.Stdout, "Adaptive latency on. Max target: %s\n", s.TargetMaxLatency)
	} else if s.Delay > 0 {
		fmt.Fprintf(s.Stdout, "Fixed write delay: %s\n", s.Delay)
	}

	dur := fmt.Sprint(s.TimeSpan)
	if s.TimeSpan == 0 {
		dur = "off"
	}

	// Initialize database.
	if err := s.setup(); err != nil {
		return err
	}

	// Record start time.
	s.now = time.Now().UTC()
	s.startTime = s.now
	if s.TimeSpan != 0 {
		absTimeSpan := int64(math.Abs(float64(s.TimeSpan)))
		s.timePerSeries = absTimeSpan / int64(s.PointN())

		// If we're back-filling then we need to move the start time back.
		if s.TimeSpan < 0 {
			s.startTime = s.startTime.Add(s.TimeSpan)
		}
	}
	fmt.Fprintf(s.Stdout, "Start time: %s\n", s.startTime)
	if s.TimeSpan < 0 {
		fmt.Fprintf(s.Stdout, "Approx End time: %s\n", time.Now().UTC())
	} else if s.TimeSpan > 0 {
		fmt.Fprintf(s.Stdout, "Approx End time: %s\n", s.startTime.Add(s.TimeSpan).UTC())
	} else {
		fmt.Fprintf(s.Stdout, "Time span: %s\n", dur)
	}

	// Stream batches from a separate goroutine.
	ch := s.generateBatches()

	// Start clients.
	var wg sync.WaitGroup
	for i := 0; i < s.Concurrency; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.runClient(ctx, ch) }()
	}

	// Start monitor.
	var monitorWaitGroup sync.WaitGroup
	if s.Verbose {
		monitorWaitGroup.Add(1)
		go func() { defer monitorWaitGroup.Done(); s.runMonitor(ctx) }()
	}

	// Wait for all clients to complete.
	wg.Wait()

	// Wait for monitor.
	cancel()
	monitorWaitGroup.Wait()

	// Report stats.
	elapsed := time.Since(s.now)
	fmt.Fprintln(s.Stdout, "")
	fmt.Fprintf(s.Stdout, "Total time: %0.1f seconds\n", elapsed.Seconds())

	return nil
}

// WrittenN returns the total number of points written.
func (s *Simulator) WrittenN() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writtenN
}

// TagsN returns the total number of tags.
func (s *Simulator) TagsN() int {
	tagTotal := s.Tags[0]
	for _, v := range s.Tags[1:] {
		tagTotal *= v
	}
	return tagTotal
}

// SeriesN returns the total number of series to write.
func (s *Simulator) SeriesN() int {
	return s.TagsN() * s.Measurements
}

// PointN returns the total number of points to write.
func (s *Simulator) PointN() int {
	return int(s.PointsPerSeries) * s.SeriesN()
}

// BatchN returns the total number of batches.
func (s *Simulator) BatchN() int {
	n := s.PointN() / s.BatchSize
	if s.PointN()%s.BatchSize != 0 {
		n++
	}
	return n
}

// generateBatches returns a channel for streaming batches.
func (s *Simulator) generateBatches() <-chan []byte {
	ch := make(chan []byte, 10)

	go func() {
		var buf bytes.Buffer
		values := make([]int, len(s.Tags))
		lastWrittenTotal := s.WrittenN()

		// Generate field string.
		var fields []byte
		for i := 0; i < s.FieldsPerPoint; i++ {
			var delim string
			if i < s.FieldsPerPoint-1 {
				delim = ","
			}
			fields = append(fields, []byte(fmt.Sprintf("v%d=1%s", i, delim))...)
		}

		for i := 0; i < s.PointN(); i++ {
			// Write point.
			buf.Write([]byte(fmt.Sprintf("m%d", i%s.Measurements)))
			for j, value := range values {
				fmt.Fprintf(&buf, ",tag%d=value%d", j, value)
			}

			// Write fields
			buf.Write(append([]byte(" "), fields...))

			if s.timePerSeries != 0 {
				delta := time.Duration(int64(lastWrittenTotal+i) * s.timePerSeries)
				buf.Write([]byte(fmt.Sprintf(" %d\n", s.startTime.Add(delta).UnixNano())))
			} else {
				fmt.Fprint(&buf, "\n")
			}

			// Increment next tag value.
			for i := range values {
				values[i]++
				if values[i] < s.Tags[i] {
					break
				} else {
					values[i] = 0 // reset to zero, increment next value
					continue
				}
			}

			// Start new batch, if necessary.
			if i > 0 && i%s.BatchSize == 0 {
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

// Stats stores statistics in a format that can be sent to an InfluxDB server
// using tags and fields.
type Stats struct {
	Time   time.Time
	Tags   map[string]string
	Fields models.Fields
}

// Stats returns up-to-date statistics about the current Simulator.
func (s *Simulator) Stats() *Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	elapsed := time.Since(s.now).Seconds()
	pThrough := float64(s.writtenN) / elapsed
	respMean := 0
	if len(s.latencyHistory) > 0 {
		respMean = int(s.totalLatency) / len(s.latencyHistory) / int(time.Millisecond)
	}
	stats := &Stats{
		Time: time.Unix(0, int64(time.Since(s.now))),
		Tags: s.ReportTags,
		Fields: models.Fields(map[string]interface{}{
			"T":              int(elapsed),
			"points_written": s.writtenN,
			"values_written": s.writtenN * s.FieldsPerPoint,
			"points_ps":      pThrough,
			"values_ps":      pThrough * float64(s.FieldsPerPoint),
			"write_error":    s.currentErrors,
			"resp_wma":       int(s.wmaLatency),
			"resp_mean":      respMean,
			"resp_90":        int(s.quartileResponse(0.9) / time.Millisecond),
			"resp_95":        int(s.quartileResponse(0.95) / time.Millisecond),
			"resp_99":        int(s.quartileResponse(0.99) / time.Millisecond),
		}),
	}

	var isCreating bool
	if s.writtenN < s.SeriesN() {
		isCreating = true
	}
	stats.Tags["creating_series"] = fmt.Sprint(isCreating)

	// Reset error count for next reporting.
	s.currentErrors = 0

	// Add runtime stats for the remote instance.
	var vars Vars
	resp, err := http.Get(strings.TrimSuffix(s.Host, "/") + "/debug/vars")
	if err != nil {
		// Don't log error as it can get spammy.
		return stats
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&vars); err != nil {
		fmt.Fprintln(s.Stderr, err)
		return stats
	}

	stats.Fields["heap_alloc"] = vars.Memstats.HeapAlloc
	stats.Fields["heap_in_use"] = vars.Memstats.HeapInUse
	stats.Fields["heap_objects"] = vars.Memstats.HeapObjects
	return stats
}

// runMonitor periodically prints the current status.
func (s *Simulator) runMonitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.printMonitorStats()
			if s.ReportHost != "" {
				s.sendMonitorStats(true)
			}
			return
		case <-ticker.C:
			s.printMonitorStats()
			if s.ReportHost != "" {
				s.sendMonitorStats(false)
			}
		}
	}
}

func (s *Simulator) sendMonitorStats(final bool) {
	stats := s.Stats()
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

	if err := s.clt.Write(bp); err != nil {
		fmt.Fprintf(s.Stderr, "unable to report stats to Influx: %v", err)
	}
}

func (s *Simulator) printMonitorStats() {
	writtenN := s.WrittenN()
	elapsed := time.Since(s.now).Seconds()
	var delay string
	var responses string

	s.mu.Lock()
	if s.TargetMaxLatency > 0 {
		delay = fmt.Sprintf(" | Writer delay currently: %s. WMA write latency: %s", s.currentDelay, time.Duration(s.wmaLatency))
	}

	if len(s.latencyHistory) >= 100 {
		responses = fmt.Sprintf(" | μ: %s, 90%%: %s, 95%%: %s, 99%%: %s", s.totalLatency/time.Duration(len(s.latencyHistory)), s.quartileResponse(0.9), s.quartileResponse(0.95), s.quartileResponse(0.99))
	}
	currentErrors := s.currentErrors
	s.mu.Unlock()

	fmt.Printf("T=%08d %d points written (%0.1f pt/sec | %0.1f val/sec) errors: %d%s%s\n",
		int(elapsed), writtenN, float64(writtenN)/elapsed, float64(s.FieldsPerPoint)*(float64(writtenN)/elapsed),
		currentErrors,
		delay, responses)
}

// This is really not the best way to do this, but it will give a reasonable
// approximation.
func (s *Simulator) quartileResponse(q float64) time.Duration {
	i := int(float64(len(s.latencyHistory))*q) - 1
	if i < 0 || i >= len(s.latencyHistory) {
		return time.Duration(-1) // Probles..
	}
	return s.latencyHistory[i]
}

// runClient executes a client to send points in a separate goroutine.
func (s *Simulator) runClient(ctx context.Context, ch <-chan []byte) {
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
				if err := s.sendBatch(buf); err == ErrConnectionRefused {
					return
				} else if err != nil {
					fmt.Fprintln(s.Stderr, err)
					s.mu.Lock()
					totalErrors := s.totalErrors
					s.mu.Unlock()

					if s.MaxErrors > 0 && totalErrors >= int64(s.MaxErrors) {
						fmt.Fprintf(s.Stderr, "Exiting due to reaching %d errors.\n", totalErrors)
						os.Exit(1)
					}
					continue
				}
				break
			}

			// Increment batch size.
			s.mu.Lock()
			s.writtenN += s.BatchSize
			s.mu.Unlock()
		}
	}
}

// setup pulls the build and version from the server and initializes the database.
func (s *Simulator) setup() error {

	// Validate that we can connect to the test host
	resp, err := http.Get(strings.TrimSuffix(s.Host, "/") + "/ping")
	if err != nil {
		return fmt.Errorf("unable to connect to %q: %s", s.Host, err)
	}
	defer resp.Body.Close()

	build := resp.Header.Get("X-Influxdb-Build")
	if len(build) > 0 {
		s.ReportTags["build"] = build
	}

	version := resp.Header.Get("X-Influxdb-Version")
	if len(version) > 0 {
		s.ReportTags["version"] = version
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/query", s.Host), strings.NewReader("q=CREATE+DATABASE+"+s.Database+"+WITH+DURATION+"+s.ShardDuration))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if s.User != "" && s.Password != "" {
		req.SetBasicAuth(s.User, s.Password)
	}

	resp, err = s.writeClient.Do(req)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// sendBatch writes a batch to the server. Continually retries until successful.
func (s *Simulator) sendBatch(buf []byte) error {
	// Don't send the batch anywhere..
	if s.DryRun {
		return nil
	}

	// Send batch.
	now := time.Now().UTC()
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/write?db=%s&precision=ns&consistency=%s", s.Host, s.Database, s.Consistency), bytes.NewReader(buf))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/ascii")

	if s.User != "" && s.Password != "" {
		req.SetBasicAuth(s.User, s.Password)
	}

	resp, err := s.writeClient.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			return ErrConnectionRefused
		}
		return err
	}
	defer resp.Body.Close()

	// Return body as error if unsuccessful.
	if resp.StatusCode != 204 {
		s.mu.Lock()
		s.currentErrors++
		s.totalErrors++
		s.mu.Unlock()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			body = []byte(err.Error())
		}

		// If it looks like the server is down and we're hitting the gateway
		// or a load balancer, then add a delay.
		switch resp.StatusCode {
		case http.StatusBadGateway, http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			time.Sleep(time.Second)
		}

		// Flatten any error message.
		return fmt.Errorf("[%d] %s", resp.StatusCode, strings.Replace(string(body), "\n", " ", -1))
	}

	latency := time.Since(now)
	s.mu.Lock()
	s.totalLatency += latency

	// Maintain sorted list of latencies for quantile reporting
	i := sort.Search(len(s.latencyHistory), func(i int) bool { return s.latencyHistory[i] >= latency })
	if i >= len(s.latencyHistory) {
		s.latencyHistory = append(s.latencyHistory, latency)
	} else {
		s.latencyHistory = append(s.latencyHistory, 0)
		copy(s.latencyHistory[i+1:], s.latencyHistory[i:])
		s.latencyHistory[i] = latency
	}
	s.mu.Unlock()

	// Fixed delay.
	if s.Delay > 0 {
		time.Sleep(s.Delay)
		return nil
	} else if s.TargetMaxLatency <= 0 {
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
	s.mu.Lock()

	// Calculate the weighted moving average latency. We weight this response
	// latency by 1-alpha, and the historic average by alpha.
	s.wmaLatency = (s.alpha * s.wmaLatency) + ((1.0 - s.alpha) * (float64(latency) - s.wmaLatency))

	// Update how we adjust our latency by
	delta := 1.0 / float64(s.Concurrency) * 0.5 * (s.wmaLatency - float64(s.TargetMaxLatency))
	s.currentDelay += time.Duration(delta)
	if s.currentDelay < time.Millisecond*100 {
		s.currentDelay = 0
	}

	thisDelay := s.currentDelay

	s.mu.Unlock()

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
