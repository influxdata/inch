package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
	mu            sync.Mutex
	writtenN      int
	startTime     time.Time
	now           time.Time
	timePerSeries int64

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	Verbose         bool
	DryRun          bool
	Strict          bool
	Host            string
	Consistency     string
	Concurrency     int
	Measurements    int   // Number of measurements
	Tags            []int // tag cardinalities
	PointsPerSeries int
	FieldsPerPoint  int
	BatchSize       int

	Database string
	TimeSpan time.Duration // The length of time to span writes over.
	Delay    time.Duration // A delay inserted in between writes.
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// ParseFlags parses the command line flags.
func (m *Main) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("inch", flag.ContinueOnError)
	fs.BoolVar(&m.Verbose, "v", false, "Verbose")
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
	fs.DurationVar(&m.TimeSpan, "time", 0, "Time span to spread writes over")
	fs.DurationVar(&m.Delay, "delay", 0, "Delay between writes")

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
	fmt.Fprintf(m.Stdout, "Database: %s\n", m.Database)
	fmt.Fprintf(m.Stdout, "Write Consistency: %s\n", m.Consistency)
	fmt.Fprintf(m.Stdout, "Write Delay: %s\n", m.Delay)

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

// runMonitor periodically prints the current status.
func (m *Main) runMonitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.printMonitorStats()
			return
		case <-ticker.C:
			m.printMonitorStats()
		}
	}
}

func (m *Main) printMonitorStats() {
	writtenN := m.WrittenN()
	elapsed := time.Since(m.now).Seconds()
	fmt.Printf("T=%08d %d points written (%0.1f pt/sec)\n", int(elapsed), writtenN, float64(writtenN)/elapsed)
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
	resp, err := client.Post(fmt.Sprintf("%s/query", m.Host), "application/x-www-form-urlencoded", strings.NewReader("q=CREATE+DATABASE+"+m.Database))
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
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			body = []byte(err.Error())
		}
		return fmt.Errorf("[%d] %s", resp.StatusCode, body)
	}

	if m.Delay > 0 {
		time.Sleep(m.Delay)
	}
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
