package inch

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb1-client/models"
	client "github.com/influxdata/influxdb1-client/v2"
)

var precisionMultiplier = map[string]int64{"ns": 1, "u": 1000, "ms": 1000000, "s": 1000000000, "m": 60000000000, "h": 3600000000000}

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
	writtenN       int    // number of values written.
	batchesWritten uint64 // number of batches written.
	startTime      time.Time
	baseTime       time.Time
	now            time.Time
	timePerSeries  int64 // How much the client is backing off due to unacceptable response times.
	currentDelay   time.Duration
	wmaLatency     float64
	latencyHistory []time.Duration
	totalLatency   time.Duration
	latestValues   int64 // Number of values written during latest period (usually 1 second).
	currentErrors  int   // The current number of errors since last reporting.
	totalErrors    int64 // The total number of errors encountered.

	Stdout io.Writer
	Stderr io.Writer

	// Client to be used to report statistics to an Influx instance.
	clt client.Client

	SetupFn func(s *Simulator) error

	// Client for writing and manipulating influxdb host
	writeClient *http.Client

	// Function for writing batches of points.
	WriteBatch func(s *Simulator, buf []byte) (statusCode int, body io.ReadCloser, err error)

	// Decay factor used when weighting average latency returned by server.
	alpha          float64
	V2             bool
	Token          string
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
	Measurements     int    // Number of measurements
	Tags             []int  // tag cardinalities
	VHosts           uint64 // Simulate multiple virtual hosts
	PointsPerSeries  int
	FieldsPerPoint   int
	RandomizeFields  bool
	OneFieldPerLine  bool
	WritesPerPoint   int
	FieldPrefix      string
	BatchSize        int
	TargetMaxLatency time.Duration
	Gzip             bool
	Precision        string

	Database        string
	RetentionPolicy string        // Write to a specific retention policy
	ShardDuration   string        // Set a custom shard duration.
	StartTime       string        // Set a custom start time.
	TimeSpan        time.Duration // The length of time to span writes over.
	Delay           time.Duration // A delay inserted in between writes.
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
		alpha:           0.5, // Weight the mean latency by 50% history / 50% latest value.
		latencyHistory:  make([]time.Duration, 0, 200),
		SetupFn:         defaultSetupFn,
		writeClient:     writeClient,
		WriteBatch:      defaultWriteBatch,
		Consistency:     "any",
		Concurrency:     1,
		Measurements:    1,
		Tags:            []int{10, 10, 10},
		VHosts:          0,
		PointsPerSeries: 100,
		FieldsPerPoint:  1,
		RandomizeFields: false,
		OneFieldPerLine: false,
		WritesPerPoint:  1,
		FieldPrefix:     "v0",
		BatchSize:       5000,
		Database:        "db",
		RetentionPolicy: "autogen",
		ShardDuration:   "7d",
		Precision:       "ns",
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

	if _, ok := precisionMultiplier[s.Precision]; !ok {
		el = append(el, fmt.Errorf("invalid precision: %s", s.Precision))
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
	fmt.Fprintf(s.Stdout, "Virtual Hosts: %d\n", s.VHosts)
	fmt.Fprintf(s.Stdout, "Measurements: %d\n", s.Measurements)
	fmt.Fprintf(s.Stdout, "Tag cardinalities: %+v\n", s.Tags)
	fmt.Fprintf(s.Stdout, "Points per series: %d\n", s.PointsPerSeries)
	fmt.Fprintf(s.Stdout, "Total series: %d\n", s.SeriesN())
	fmt.Fprintf(s.Stdout, "Total points: %d\n", s.PointN())
	fmt.Fprintf(s.Stdout, "Total fields per point: %d\n", s.FieldsPerPoint)
	fmt.Fprintf(s.Stdout, "Randomized field values: %t\n", s.RandomizeFields)
	fmt.Fprintf(s.Stdout, "Multiple writes per point: %t\n", s.OneFieldPerLine)
	fmt.Fprintf(s.Stdout, "Batch Size: %d\n", s.BatchSize)
	fmt.Fprintf(s.Stdout, "Database: %s (Shard duration: %s)\n", s.Database, s.ShardDuration)
	fmt.Fprintf(s.Stdout, "Retention Policy: %s\n", s.RetentionPolicy)
	fmt.Fprintf(s.Stdout, "Write Consistency: %s\n", s.Consistency)
	fmt.Fprintf(s.Stdout, "Writing into InfluxDB 2.0: %t\n", s.V2)
	fmt.Fprintf(s.Stdout, "InfluxDB 2.0 Authorization Token: %s\n", s.Token)
	fmt.Fprintf(s.Stdout, "Precision: %s\n", s.Precision)

	if s.V2 == true && s.Token == "" {
		fmt.Println("ERROR: Need to provide a token in ordere to write into InfluxDB 2.0")
		return err
	}

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
	if err := s.SetupFn(s); err != nil {
		return err
	}

	// Record start time.
	s.now = time.Now().UTC()

	s.baseTime = s.now
	if s.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, s.StartTime); err != nil {
			return err
		} else {
			s.baseTime = t.UTC()
		}
	}
	s.startTime = s.baseTime

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
		fmt.Fprintf(s.Stdout, "Approx End time: %s\n", s.baseTime)
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

func (s *Simulator) makeField(val int) []string {
	fields := make([]string, 0, s.FieldsPerPoint)

	for i := 0; i < s.FieldsPerPoint; i++ {
		// First field doesn't have a number incremented.
		pair := fmt.Sprintf("%s=%d", s.FieldPrefix, val)
		if i > 0 {
			pair = fmt.Sprintf("%s%d=%d", s.FieldPrefix, i, val)
		}
		fields = append(fields, pair)
	}
	if !s.OneFieldPerLine {
		fields = []string{strings.Join(fields, ",")}
	}
	return fields
}

// generateBatches returns a channel for streaming batches.
func (s *Simulator) generateBatches() <-chan []byte {
	ch := make(chan []byte, 10)

	go func() {
		values := make([]int, len(s.Tags))
		lastWrittenTotal := s.WrittenN()

		// Generate field strings
		var fields [][]string
		maxFieldVal := 1
		if s.RandomizeFields {
			maxFieldVal = 10000
		}
		for i := 0; i < maxFieldVal; i++ {
			fields = append(fields, s.makeField(i))
		}

		// Size internal buffer to consider mx+tags+ +fields.
		buf := bytes.NewBuffer(make([]byte, 0, 2+len(values)+1+len(fields)))

		// Write points.
		var lastMN int
		lastM := []byte("m0")
		fieldRandomize := rand.New(rand.NewSource(1234))
		var tags []byte

		if s.OneFieldPerLine {
			s.WritesPerPoint = s.FieldsPerPoint
			s.BatchSize /= s.WritesPerPoint
		}

		timeDivisor := precisionMultiplier[s.Precision]

		for i := 0; i < s.PointN(); i++ {
			lastMN = i % s.Measurements
			lastM = append(lastM[:1], []byte(strconv.Itoa(lastMN))...)
			tags = tags[:0] // Reset slice but use backing array.
			for j, value := range values {
				tags = append(tags, fmt.Sprintf(",tag%d=value%d", j, value)...)
			}

			fieldValueIndex := 0
			if s.RandomizeFields {
				fieldValueIndex = fieldRandomize.Intn(maxFieldVal)
			}

			var delta time.Duration
			if s.timePerSeries != 0 {
				delta = time.Duration(int64(lastWrittenTotal+i) * s.timePerSeries)
			} else {
				delta = time.Duration(int64(lastWrittenTotal + i))
				if soFar := time.Since(s.startTime); delta < soFar {
					delta = soFar
				}
			}
			timestamp := s.startTime.Add(delta).UnixNano()

			for f := 0; f < s.WritesPerPoint; f++ {
				s.formatWrites(buf, lastM, tags, fields[fieldValueIndex][f], timestamp, timeDivisor)
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

var space []byte = []byte(" ")

func (s *Simulator) formatWrites(buf *bytes.Buffer, measurement []byte, tags []byte, fieldValues string, timestamp int64, timeDivisor int64) {
	buf.Write(measurement) // Write measurement
	buf.Write(tags)
	buf.Write(space) // Write a space.
	buf.WriteString(fieldValues)
	buf.WriteString(fmt.Sprintf(" %d\n", timestamp/timeDivisor))
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

	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			d := int64(time.Since(last) / time.Second)
			if d == 0 {
				d = 1
			}
			throughput := atomic.SwapInt64(&s.latestValues, 0) / d
			s.printMonitorStats(throughput)
			if s.ReportHost != "" {
				s.sendMonitorStats(true, throughput)
			}
			return
		case t := <-ticker.C:
			d := int64(time.Since(last) / time.Second)
			if d == 0 {
				d = 1
			}
			throughput := atomic.SwapInt64(&s.latestValues, 0) / d
			s.printMonitorStats(throughput)
			if s.ReportHost != "" {
				s.sendMonitorStats(false, throughput)
			}
			last = t // Update time seen most recently.
		}
	}
}

func (s *Simulator) sendMonitorStats(final bool, latestThroughput int64) {
	stats := s.Stats()
	stats.Fields["current_values_ps"] = latestThroughput
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

func (s *Simulator) printMonitorStats(latestThroughput int64) {
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

	percentComplete := int(float32(writtenN) / float32(s.PointN()) * 100)

	fmt.Printf("T=%08d %d points written (%d%%). Total throughput: %0.1f pt/sec | %0.1f val/sec. Current throughput: %d val/sec. Errors: %d%s%s\n",
		int(elapsed), writtenN, percentComplete, float64(writtenN)/elapsed, float64(s.FieldsPerPoint)*(float64(writtenN)/elapsed), latestThroughput,
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
	b := bytes.NewBuffer(make([]byte, 0, 1024))
	g := gzip.NewWriter(b)

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
				b.Reset()

				if s.Gzip {
					g.Reset(b)

					if _, err := g.Write(buf); err != nil {
						fmt.Fprintln(s.Stderr, err)
						fmt.Fprintf(s.Stderr, "Exiting due to fatal errors: %v.\n", err)
						os.Exit(1)
					}

					if err := g.Close(); err != nil {
						fmt.Fprintln(s.Stderr, err)
						fmt.Fprintf(s.Stderr, "Exiting due to fatal errors: %v.\n", err)
						os.Exit(1)
					}
				} else {
					_, err := io.Copy(b, bytes.NewReader(buf))
					if err != nil {
						fmt.Fprintln(s.Stderr, err)
						fmt.Fprintf(s.Stderr, "Exiting due to fatal errors: %v.\n", err)
						os.Exit(1)
					}
				}

				if err := s.sendBatch(b.Bytes()); err == ErrConnectionRefused {
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

			atomic.AddUint64(&s.batchesWritten, 1)

			// Increment batch size.
			s.mu.Lock()
			s.writtenN += s.BatchSize
			s.mu.Unlock()

			// Update current throughput
			atomic.AddInt64(&s.latestValues, int64(s.BatchSize))
		}
	}
}

// setup pulls the build and version from the server and initializes the database.
var defaultSetupFn = func(s *Simulator) error {
	// Validate that we can connect to the test host
	resp, err := s.writeClient.Get(strings.TrimSuffix(s.Host, "/") + "/ping")
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
	if s.V2 == true {
		req.Header.Set("Authorization", "Token "+s.Token)
	}

	if s.User != "" && s.Password != "" {
		req.SetBasicAuth(s.User, s.Password)
	}
	if s.Verbose == true {
		for name, headers := range req.Header {
			fmt.Printf("%s:%s\n", name, headers)
		}
	}
	resp, err = s.writeClient.Do(req)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// defaultWriteBatch is the default implementation of the WriteBatch function.
// It's the caller's responsibility to close the response body.
var defaultWriteBatch = func(s *Simulator, buf []byte) (statusCode int, body io.ReadCloser, err error) {
	var url string
	if s.RetentionPolicy == "" {
		url = fmt.Sprintf("%s/write?db=%s&precision=%s&consistency=%s", s.Host, s.Database, s.Precision, s.Consistency)
	} else {
		url = fmt.Sprintf("%s/write?db=%s&rp=%s&precision=%s&consistency=%s", s.Host, s.Database, s.RetentionPolicy, s.Precision, s.Consistency)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
	if err != nil {
		return 0, nil, err
	}

	if s.V2 == true {
		req.Header.Set("Authorization", "Token "+s.Token)
	}

	var hostID uint64
	if s.VHosts > 0 {
		hostID = atomic.LoadUint64(&s.batchesWritten) % s.VHosts
		req.Header.Set("X-Influxdb-Host", fmt.Sprintf("tenant%d.example.com", hostID))
	}

	req.Header.Set("Content-Type", "text/ascii")
	if s.Gzip {
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	}

	if s.User != "" && s.Password != "" {
		req.SetBasicAuth(s.User, s.Password)
	}

	resp, err := s.writeClient.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			return 0, nil, ErrConnectionRefused
		}
		return 0, nil, err
	}
	return resp.StatusCode, resp.Body, nil
}

// sendBatch writes a batch to the server. Continually retries until successful.
func (s *Simulator) sendBatch(buf []byte) error {
	// Don't send the batch anywhere..
	if s.DryRun {
		return nil
	}

	// Send batch.
	now := time.Now().UTC()
	code, bodyreader, err := s.WriteBatch(s, buf)
	if err != nil {
		return err
	}

	// Return body as error if unsuccessful.
	if code != 204 {
		s.mu.Lock()
		s.currentErrors++
		s.totalErrors++
		s.mu.Unlock()

		body, err := ioutil.ReadAll(bodyreader)
		if err != nil {
			body = []byte(err.Error())
		}

		// Close the body.
		bodyreader.Close()

		// If it looks like the server is down and we're hitting the gateway
		// or a load balancer, then add a delay.
		switch code {
		case http.StatusBadGateway, http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			time.Sleep(time.Second)
		}

		// Flatten any error message.
		return fmt.Errorf("[%d] %s", code, strings.Replace(string(body), "\n", " ", -1))
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
