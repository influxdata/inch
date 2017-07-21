package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	timePerSeries int64

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	Verbose         bool
	Host            string
	Concurrency     int
	Measurements    int   // Number of measurements
	Tags            []int // tag cardinalities
	PointsPerSeries int
	BatchSize       int

	Database string
	TimeSpan time.Duration // The length of time to span writes over.
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
	fs.StringVar(&m.Host, "host", "http://localhost:8086", "Host")
	fs.IntVar(&m.Concurrency, "c", 1, "Concurrency")
	fs.IntVar(&m.Measurements, "m", 1, "Measurements")
	tags := fs.String("t", "10,10,10", "Tag cardinality")
	fs.IntVar(&m.PointsPerSeries, "p", 100, "Points per series")
	fs.IntVar(&m.BatchSize, "b", 5000, "Batch size")
	fs.StringVar(&m.Database, "db", "stress", "Database to write to")
	fs.DurationVar(&m.TimeSpan, "time", 0, "Time span to spread writes over")

	if err := fs.Parse(args); err != nil {
		return err
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
	fmt.Fprintf(m.Stdout, "Batch Size: %d\n", m.BatchSize)
	fmt.Fprintf(m.Stdout, "Database: %s\n", m.Database)
	dur := fmt.Sprint(m.TimeSpan)
	if m.TimeSpan == 0 {
		dur = "off"
	}
	fmt.Fprintf(m.Stdout, "Time span: %s\n", dur)

	// Initialize database.
	if err := m.setup(); err != nil {
		return err
	}

	// Record start time.
	m.startTime = time.Now()
	if m.TimeSpan > 0 {
		m.timePerSeries = int64(m.TimeSpan) / int64(m.PointN())
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
	elapsed := time.Since(m.startTime)
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
		for i := 0; i < m.PointN(); i++ {
			// Write point.
			buf.Write([]byte(fmt.Sprintf("m%d", i%m.Measurements)))
			for j, value := range values {
				fmt.Fprintf(&buf, ",tag%d=value%d", j, value)
			}

			var tme string
			if m.timePerSeries > 0 {
				tme = fmt.Sprintf(" %d", m.startTime.Add(time.Duration(int64(lastWrittenTotal+i)*m.timePerSeries)).UnixNano())
			}

			buf.Write([]byte(fmt.Sprintf(" value=1%s\n", tme)))

			// Increment next tag value.
			for j := range values {
				values[j]++
				if values[j] < m.Tags[j] {
					break
				} else {
					values[j] = 0 // reset to zero, increment next value
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
	elapsed := time.Since(m.startTime).Seconds()
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
	// Send batch.
	var client http.Client
	resp, err := client.Post(fmt.Sprintf("%s/write?db=%s&precision=ns", m.Host, m.Database), "text/ascii", bytes.NewReader(buf))
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
