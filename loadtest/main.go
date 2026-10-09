
package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

type Result struct {
	ID            int
	Duration      time.Duration
	BytesSent     int64
	BytesReceived int64
	Backend       string
	Err           error
}

func main() {
	address := flag.String(
		"addr",
		"localhost:8080",
		"load balancer address",
	)

	connections := flag.Int(
		"connections",
		100,
		"total number of TCP connections to attempt",
	)

	concurrency := flag.Int(
		"concurrency",
		20,
		"maximum number of workers running simultaneously",
	)

	timeout := flag.Duration(
		"timeout",
		5*time.Second,
		"connection and I/O timeout",
	)

	payload := flag.String(
		"payload",
		"load-test",
		"message prefix sent to each backend",
	)

	verbose := flag.Bool(
		"verbose",
		false,
		"print the result of every connection",
	)

	flag.Parse()

	if *connections < 1 {
		fmt.Println("Error: connections must be at least 1")
		return
	}

	if *concurrency < 1 {
		fmt.Println("Error: concurrency must be at least 1")
		return
	}

	if *timeout <= 0 {
		fmt.Println("Error: timeout must be greater than zero")
		return
	}

	workerCount := *concurrency
	if workerCount > *connections {
		workerCount = *connections
	}

	fmt.Println("Concurrent TCP load test")
	fmt.Println("------------------------")
	fmt.Println("Target:      ", *address)
	fmt.Println("Connections: ", *connections)
	fmt.Println("Concurrency: ", workerCount)
	fmt.Println("Timeout:     ", *timeout)
	fmt.Println()

	jobs := make(chan int)
	results := make(chan Result, *connections)

	var workers sync.WaitGroup

	// Each worker performs one connection at a time.
	for i := 0; i < workerCount; i++ {
		workers.Add(1)

		go func() {
			defer workers.Done()

			for id := range jobs {
				result := runConnection(
					*address,
					id,
					*timeout,
					*payload,
				)

				results <- result
			}
		}()
	}

	start := time.Now()

	// Submit all connection jobs.
	for id := 1; id <= *connections; id++ {
		jobs <- id
	}

	close(jobs)

	// Wait until every worker has finished.
	workers.Wait()

	elapsed := time.Since(start)
	close(results)

	var (
		successes     int
		failures      int
		totalSent     int64
		totalReceived int64
		totalLatency  time.Duration
		latencies     []time.Duration
	)

	backendCounts := make(map[string]int)

	for result := range results {
		totalSent += result.BytesSent
		totalReceived += result.BytesReceived

		if result.Err != nil {
			failures++

			fmt.Printf(
				"FAIL connection=%d duration=%s error=%v\n",
				result.ID,
				result.Duration,
				result.Err,
			)

			continue
		}

		successes++
		totalLatency += result.Duration
		latencies = append(latencies, result.Duration)
		backendCounts[result.Backend]++

		if *verbose {
			fmt.Printf(
				"OK connection=%d backend=%s duration=%s sent=%d received=%d\n",
				result.ID,
				result.Backend,
				result.Duration,
				result.BytesSent,
				result.BytesReceived,
			)
		}
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	fmt.Println()
	fmt.Println("Results")
	fmt.Println("-------")
	fmt.Printf("Total attempts:       %d\n", *connections)
	fmt.Printf("Successful:           %d\n", successes)
	fmt.Printf("Failed:               %d\n", failures)
	fmt.Printf("Elapsed time:         %s\n", elapsed)
	fmt.Printf(
		"Successful conn/sec:  %.2f\n",
		float64(successes)/elapsed.Seconds(),
	)
	fmt.Printf("Application bytes sent:     %d\n", totalSent)
	fmt.Printf("Application bytes received: %d\n", totalReceived)

	if successes > 0 {
		fmt.Printf(
			"Average latency:      %s\n",
			totalLatency/time.Duration(successes),
		)
		fmt.Printf(
			"P50 latency:           %s\n",
			percentile(latencies, 0.50),
		)
		fmt.Printf(
			"P95 latency:           %s\n",
			percentile(latencies, 0.95),
		)
	}

	fmt.Println()
	fmt.Println("Backend distribution")
	fmt.Println("--------------------")

	addresses := make([]string, 0, len(backendCounts))
	for backend := range backendCounts {
		addresses = append(addresses, backend)
	}

	sort.Strings(addresses)

	for _, backend := range addresses {
		fmt.Printf(
			"%s: %d connections\n",
			backend,
			backendCounts[backend],
		)
	}
}

func runConnection(
	address string,
	id int,
	timeout time.Duration,
	payload string,
) (result Result) {
	result.ID = id

	start := time.Now()
	defer func() {
		result.Duration = time.Since(start)
	}()

	dialer := net.Dialer{
		Timeout: timeout,
	}

	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		result.Err = fmt.Errorf("connect: %w", err)
		return result
	}
	defer conn.Close()

	// Apply an I/O deadline after the connection is established.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		result.Err = fmt.Errorf("set deadline: %w", err)
		return result
	}

	// The test message ends with a newline because our demo
	// backend uses newline-delimited messages.
	message := fmt.Sprintf("%s-%d\n", payload, id)
	data := []byte(message)

	n, err := conn.Write(data)
	result.BytesSent = int64(n)

	if err != nil {
		result.Err = fmt.Errorf("write: %w", err)
		return result
	}

	if n != len(data) {
		result.Err = io.ErrShortWrite
		return result
	}

	// Read until the peer closes the connection.
	response, err := io.ReadAll(conn)
	result.BytesReceived = int64(len(response))

	if err != nil {
		result.Err = fmt.Errorf("read response: %w", err)
		return result
	}

	responseText := strings.TrimSpace(string(response))
	const prefix = "Hello from BACKEND "

	if !strings.HasPrefix(responseText, prefix) {
		result.Err = fmt.Errorf(
			"unexpected backend response %q",
			responseText,
		)
		return result
	}

	result.Backend = strings.TrimSpace(
		strings.TrimPrefix(responseText, prefix),
	)

	return result
}

func percentile(
	sortedValues []time.Duration,
	p float64,
) time.Duration {
	if len(sortedValues) == 0 {
		return 0
	}

	// Nearest-rank percentile, using a one-based rank.
	rank := int(math.Ceil(p * float64(len(sortedValues))))

	if rank < 1 {
		rank = 1
	}
	if rank > len(sortedValues) {
		rank = len(sortedValues)
	}

	return sortedValues[rank-1]
}