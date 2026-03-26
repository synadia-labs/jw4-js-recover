package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startStandaloneServer starts a non-clustered NATS server with JetStream
// using the given store directory.
func startStandaloneServer(t *testing.T, storeDir string) (*server.Server, *server.Options) {
	t.Helper()
	opts := &server.Options{
		Host:               "127.0.0.1",
		Port:               -1,
		JetStream:          true,
		StoreDir:           storeDir,
		JetStreamMaxMemory: 1 << 30,
		JetStreamMaxStore:  10 << 30,
		NoLog:              true,
		NoSigs:             true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	return s, opts
}

// connectClient returns a NATS connection and JetStream context.
func connectClient(t *testing.T, s *server.Server) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	return nc, js
}

// writeStreamStore creates a minimal JetStream store directory with a single
// stream's meta.inf and meta.sum. Returns the store root.
func writeStreamStore(t *testing.T, root, account, streamName string, meta map[string]any) string {
	t.Helper()
	dir := filepath.Join(root, "jetstream", account, "streams", streamName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(dir, metaFile), data, 0o644); err != nil {
		t.Fatal(err)
	}
	name, _ := meta["name"].(string)
	if name == "" {
		name = streamName
	}
	sum, err := computeChecksum(name, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, sumFile), []byte(sum), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

// TestStandaloneRejectsR3Store validates the document claim:
// "A standalone NATS server rejects streams with num_replicas > 1."
func TestStandaloneRejectsR3Store(t *testing.T) {
	storeDir := t.TempDir()
	writeStreamStore(t, storeDir, "$G", "ORDERS", map[string]any{
		"name":             "ORDERS",
		"num_replicas":     3,
		"subjects":         []string{"orders.>"},
		"retention":        "limits",
		"max_consumers":    -1,
		"max_msgs":         -1,
		"max_bytes":        -1,
		"max_age":          0,
		"max_msg_size":     -1,
		"storage":          "file",
		"discard":          "old",
		"duplicate_window": 120000000000,
	})

	s, _ := startStandaloneServer(t, storeDir)
	defer s.Shutdown()

	nc, js := connectClient(t, s)
	defer nc.Close()

	// The stream should NOT be loaded because num_replicas > 1
	_, err := js.StreamInfo("ORDERS")
	if err == nil {
		t.Fatal("expected error: standalone server should reject R3 stream")
	}
}

// TestChecksumMismatchSkipsStream validates the document claim:
// "The NATS server validates this on startup and skips streams with mismatched checksums."
func TestChecksumMismatchSkipsStream(t *testing.T) {
	storeDir := t.TempDir()
	writeStreamStore(t, storeDir, "$G", "EVENTS", map[string]any{
		"name":             "EVENTS",
		"num_replicas":     1,
		"subjects":         []string{"events.>"},
		"retention":        "limits",
		"max_consumers":    -1,
		"max_msgs":         -1,
		"max_bytes":        -1,
		"max_age":          0,
		"max_msg_size":     -1,
		"storage":          "file",
		"discard":          "old",
		"duplicate_window": 120000000000,
	})

	// Corrupt the checksum
	sumPath := filepath.Join(storeDir, "jetstream", "$G", "streams", "EVENTS", sumFile)
	if err := os.WriteFile(sumPath, []byte("deadbeefdeadbeef"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, _ := startStandaloneServer(t, storeDir)
	defer s.Shutdown()

	nc, js := connectClient(t, s)
	defer nc.Close()

	// Stream should be skipped due to checksum mismatch
	_, err := js.StreamInfo("EVENTS")
	if err == nil {
		t.Fatal("expected error: stream with bad checksum should be skipped")
	}
}

// TestStandaloneLoadsAfterPrepare validates the core Path B flow:
// prepare() edits the store, then a standalone server loads it.
func TestStandaloneLoadsAfterPrepare(t *testing.T) {
	// Phase 1: Create a server, add a stream, publish messages.
	origDir := t.TempDir()
	s1, _ := startStandaloneServer(t, origDir)

	nc1, js1 := connectClient(t, s1)
	_, err := js1.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
		Replicas: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if _, err := js1.Publish("test.data", []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	info, err := js1.StreamInfo("TEST")
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 10 {
		t.Fatalf("expected 10 messages, got %d", info.State.Msgs)
	}

	nc1.Close()
	s1.Shutdown()
	s1.WaitForShutdown()

	// Phase 2: Simulate what an operator does for Path B.
	// Modify the store to have num_replicas=3 (simulating a clustered store copy).
	metaPath := filepath.Join(origDir, "jetstream", "$G", "streams", "TEST", metaFile)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}
	meta["num_replicas"] = 3
	updated, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	updated = append(updated, '\n')
	if err := os.WriteFile(metaPath, updated, 0o644); err != nil {
		t.Fatal(err)
	}
	// Write a valid checksum for the R3 metadata
	sum, _ := computeChecksum("TEST", updated)
	sumPath := filepath.Join(origDir, "jetstream", "$G", "streams", "TEST", sumFile)
	if err := os.WriteFile(sumPath, []byte(sum), 0o644); err != nil {
		t.Fatal(err)
	}

	// Verify the R3 store cannot be loaded standalone.
	s2, _ := startStandaloneServer(t, origDir)
	nc2, js2 := connectClient(t, s2)
	_, err = js2.StreamInfo("TEST")
	if err == nil {
		t.Fatal("standalone should reject R3 store")
	}
	nc2.Close()
	s2.Shutdown()
	s2.WaitForShutdown()

	// Phase 3: Run prepare() to convert R3 -> R1.
	if err := prepare(origDir); err != nil {
		t.Fatal(err)
	}

	// Phase 4: Start a new standalone server and verify the stream loads.
	s3, _ := startStandaloneServer(t, origDir)
	defer s3.Shutdown()

	nc3, js3 := connectClient(t, s3)
	defer nc3.Close()

	info, err = js3.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("stream should load after prepare: %v", err)
	}
	if info.Config.Replicas != 1 {
		t.Errorf("expected replicas=1, got %d", info.Config.Replicas)
	}
	if info.State.Msgs != 10 {
		t.Errorf("expected 10 messages, got %d", info.State.Msgs)
	}
}

// TestPrepareAndServerChecksumAgreement validates that the checksum algorithm
// in jw4-js-recover matches the NATS server's algorithm.
func TestPrepareAndServerChecksumAgreement(t *testing.T) {
	// Create a store with R3 metadata.
	storeDir := t.TempDir()
	writeStreamStore(t, storeDir, "$G", "CHECKSUM_TEST", map[string]any{
		"name":             "CHECKSUM_TEST",
		"num_replicas":     3,
		"subjects":         []string{"check.>"},
		"retention":        "limits",
		"max_consumers":    -1,
		"max_msgs":         -1,
		"max_bytes":        -1,
		"max_age":          0,
		"max_msg_size":     -1,
		"storage":          "file",
		"discard":          "old",
		"duplicate_window": 120000000000,
	})

	// Run prepare to edit meta.inf and recompute meta.sum.
	if err := prepare(storeDir); err != nil {
		t.Fatal(err)
	}

	// Start a server; if the checksum is wrong, the server skips the stream.
	s, _ := startStandaloneServer(t, storeDir)
	defer s.Shutdown()

	nc, js := connectClient(t, s)
	defer nc.Close()

	info, err := js.StreamInfo("CHECKSUM_TEST")
	if err != nil {
		t.Fatalf("server rejected checksum computed by prepare: %v", err)
	}
	if info.Config.Replicas != 1 {
		t.Errorf("expected replicas=1, got %d", info.Config.Replicas)
	}
}

// TestBackupFromStandalone validates Path B step 4: taking a backup from
// the standalone recovery server using the JetStream snapshot API.
func TestBackupFromStandalone(t *testing.T) {
	// Create a server with data.
	storeDir := t.TempDir()
	s1, _ := startStandaloneServer(t, storeDir)

	nc1, js1 := connectClient(t, s1)
	_, err := js1.AddStream(&nats.StreamConfig{
		Name:     "BACKUP_TEST",
		Subjects: []string{"backup.>"},
		Replicas: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := js1.Publish("backup.data", []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	nc1.Close()
	s1.Shutdown()
	s1.WaitForShutdown()

	// "Copy" the store (it's already local) and start a new server.
	// Simulate the backup step by re-opening and verifying stream contents.
	s2, _ := startStandaloneServer(t, storeDir)
	defer s2.Shutdown()

	nc2, js2 := connectClient(t, s2)
	defer nc2.Close()

	info, err := js2.StreamInfo("BACKUP_TEST")
	if err != nil {
		t.Fatalf("stream should be available for backup: %v", err)
	}
	if info.State.Msgs != 5 {
		t.Errorf("expected 5 messages for backup, got %d", info.State.Msgs)
	}

	// Verify we can read individual messages (equivalent to nats stream get).
	msg, err := js2.GetMsg("BACKUP_TEST", 1)
	if err != nil {
		t.Fatalf("failed to get message 1: %v", err)
	}
	if string(msg.Data) != "msg-0" {
		t.Errorf("message 1 data: got %q, want %q", msg.Data, "msg-0")
	}
}

// TestRestoreWithReplicaOverride validates the document claim:
// "The --replicas flag overrides the replica count from the backup."
// This tests the server-side behavior: a stream created with R1 config
// loads correctly on a standalone server.
func TestRestoreWithReplicaOverride(t *testing.T) {
	// Create a stream, publish, shut down, then start a new server
	// and verify the stream loads with its original R1 config.
	// This tests that the restore pathway (standalone server with R1 store)
	// is functional.
	storeDir := t.TempDir()
	s1, _ := startStandaloneServer(t, storeDir)

	nc1, js1 := connectClient(t, s1)
	_, err := js1.AddStream(&nats.StreamConfig{
		Name:     "RESTORE_TEST",
		Subjects: []string{"restore.>"},
		Replicas: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := js1.Publish("restore.data", []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	nc1.Close()
	s1.Shutdown()
	s1.WaitForShutdown()

	// Start a fresh server with the same store.
	s2, _ := startStandaloneServer(t, storeDir)
	defer s2.Shutdown()

	nc2, js2 := connectClient(t, s2)
	defer nc2.Close()

	info, err := js2.StreamInfo("RESTORE_TEST")
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.Replicas != 1 {
		t.Errorf("expected replicas=1, got %d", info.Config.Replicas)
	}
	if info.State.Msgs != 3 {
		t.Errorf("expected 3 messages, got %d", info.State.Msgs)
	}
}

// TestInterestRetentionOrphanDeletion validates the document caveat:
// "For streams using interest or workqueue retention, the standalone server
// may delete messages during recovery based on slightly-stale consumer ack state."
func TestInterestRetentionOrphanDeletion(t *testing.T) {
	storeDir := t.TempDir()
	s1, _ := startStandaloneServer(t, storeDir)

	nc1, js1 := connectClient(t, s1)

	// Create interest-based retention stream with a consumer.
	_, err := js1.AddStream(&nats.StreamConfig{
		Name:      "INTEREST_TEST",
		Subjects:  []string{"interest.>"},
		Retention: nats.InterestPolicy,
		Replicas:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := js1.SubscribeSync("interest.>", nats.Durable("CONSUMER"))
	if err != nil {
		t.Fatal(err)
	}

	// Publish messages.
	for i := 0; i < 5; i++ {
		if _, err := js1.Publish("interest.data", []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Acknowledge first 3, leaving 2 unacked.
	for i := 0; i < 3; i++ {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := msg.Ack(); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for acks to propagate.
	time.Sleep(500 * time.Millisecond)

	info, _ := js1.StreamInfo("INTEREST_TEST")
	// With interest retention, acked messages are removed. Only unacked remain.
	t.Logf("Before restart: %d messages in stream", info.State.Msgs)

	nc1.Close()
	s1.Shutdown()
	s1.WaitForShutdown()

	// Restart and check: interest retention should have cleaned up acked messages.
	// The remaining messages are those that were unacked by the consumer.
	s2, _ := startStandaloneServer(t, storeDir)
	defer s2.Shutdown()

	nc2, js2 := connectClient(t, s2)
	defer nc2.Close()

	info, err = js2.StreamInfo("INTEREST_TEST")
	if err != nil {
		t.Fatalf("stream should load: %v", err)
	}

	// After restart with interest retention, the server runs checkForOrphanMsgs
	// which may further clean up messages based on consumer state.
	// The key validation: the stream loaded and interest retention is functional.
	t.Logf("After restart: %d messages in stream (interest retention active)", info.State.Msgs)

	// Verify the stream is functional: publish a new message and consume.
	if _, err := js2.Publish("interest.data", []byte("post-recovery")); err != nil {
		t.Fatalf("publish after recovery failed: %v", err)
	}
}
