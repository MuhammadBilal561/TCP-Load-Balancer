package main

import (
	"testing"
)

func resetTestState() {
	backends = []Backend{
		{Address: "localhost:9001", Healthy: true},
		{Address: "localhost:9002", Healthy: true},
		{Address: "localhost:9003", Healthy: true},
	}

	currentBackend = 0
}

// ------------------------------------------------------------
// Test parseBackends
// ------------------------------------------------------------

func TestParseBackends(t *testing.T) {
	input := "localhost:9001,localhost:9002,localhost:9003"

	result := parseBackends(input)

	if len(result) != 3 {
		t.Fatalf(
			"expected 3 backends, got %d",
			len(result),
		)
	}

	expected := []string{
		"localhost:9001",
		"localhost:9002",
		"localhost:9003",
	}

	for i, backend := range result {
		if backend.Address != expected[i] {
			t.Errorf(
				"backend %d: expected %s, got %s",
				i,
				expected[i],
				backend.Address,
			)
		}

		if !backend.Healthy {
			t.Errorf(
				"backend %s should initially be healthy",
				backend.Address,
			)
		}
	}
}

// ------------------------------------------------------------
// Test parseBackends with spaces
// ------------------------------------------------------------

func TestParseBackendsWithSpaces(t *testing.T) {
	input := "localhost:9001, localhost:9002,  localhost:9003"

	result := parseBackends(input)

	if len(result) != 3 {
		t.Fatalf(
			"expected 3 backends, got %d",
			len(result),
		)
	}

	expected := []string{
		"localhost:9001",
		"localhost:9002",
		"localhost:9003",
	}

	for i, backend := range result {
		if backend.Address != expected[i] {
			t.Errorf(
				"backend %d: expected %s, got %s",
				i,
				expected[i],
				backend.Address,
			)
		}
	}
}

// ------------------------------------------------------------
// Test empty backend entries
// ------------------------------------------------------------

func TestParseBackendsIgnoresEmptyEntries(t *testing.T) {
	input := "localhost:9001,,localhost:9002,"

	result := parseBackends(input)

	if len(result) != 2 {
		t.Fatalf(
			"expected 2 backends, got %d",
			len(result),
		)
	}
}

// ------------------------------------------------------------
// Test round robin
// ------------------------------------------------------------

func TestRoundRobin(t *testing.T) {
	resetTestState()

	expected := []string{
		"localhost:9001",
		"localhost:9002",
		"localhost:9003",
		"localhost:9001",
		"localhost:9002",
		"localhost:9003",
	}

	for i, expectedBackend := range expected {
		actualBackend, ok := getNextHealthyBackend()

		if !ok {
			t.Fatalf(
				"connection %d: expected healthy backend",
				i+1,
			)
		}

		if actualBackend != expectedBackend {
			t.Errorf(
				"connection %d: expected %s, got %s",
				i+1,
				expectedBackend,
				actualBackend,
			)
		}
	}
}

// ------------------------------------------------------------
// Test unhealthy backend skipping
// ------------------------------------------------------------

func TestRoundRobinSkipsUnhealthyBackend(t *testing.T) {
	resetTestState()

	// Disable backend 9002.
	markBackend(
		"localhost:9002",
		false,
	)

	expected := []string{
		"localhost:9001",
		"localhost:9003",
		"localhost:9001",
		"localhost:9003",
	}

	for i, expectedBackend := range expected {
		actualBackend, ok := getNextHealthyBackend()

		if !ok {
			t.Fatalf(
				"connection %d: expected healthy backend",
				i+1,
			)
		}

		if actualBackend != expectedBackend {
			t.Errorf(
				"connection %d: expected %s, got %s",
				i+1,
				expectedBackend,
				actualBackend,
			)
		}
	}
}

// ------------------------------------------------------------
// Test all backends unhealthy
// ------------------------------------------------------------

func TestNoHealthyBackends(t *testing.T) {
	resetTestState()

	markBackend("localhost:9001", false)
	markBackend("localhost:9002", false)
	markBackend("localhost:9003", false)

	backend, ok := getNextHealthyBackend()

	if ok {
		t.Fatalf(
			"expected no healthy backend, got %s",
			backend,
		)
	}

	if backend != "" {
		t.Fatalf(
			"expected empty backend address, got %s",
			backend,
		)
	}
}

// ------------------------------------------------------------
// Test backend recovery
// ------------------------------------------------------------

func TestBackendRecovery(t *testing.T) {
	resetTestState()

	markBackend(
		"localhost:9002",
		false,
	)

	markBackend(
		"localhost:9002",
		true,
	)

	// Round robin should now include 9002 again.
	expected := []string{
		"localhost:9001",
		"localhost:9002",
		"localhost:9003",
	}

	for i, expectedBackend := range expected {
		actualBackend, ok := getNextHealthyBackend()

		if !ok {
			t.Fatalf(
				"connection %d: expected healthy backend",
				i+1,
			)
		}

		if actualBackend != expectedBackend {
			t.Errorf(
				"connection %d: expected %s, got %s",
				i+1,
				expectedBackend,
				actualBackend,
			)
		}
	}
}
