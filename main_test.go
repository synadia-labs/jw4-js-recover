package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/highwayhash"
)

// createTestStore builds a minimal JetStream store directory structure.
// Layout: <root>/jetstream/<account>/streams/<stream>/meta.inf + meta.sum
func createTestStore(t *testing.T, root string, streams map[string]map[string]map[string]any) {
	t.Helper()
	for account, streamMap := range streams {
		for streamName, meta := range streamMap {
			dir := filepath.Join(root, "jetstream", account, "streams", streamName)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if meta == nil {
				continue
			}
			data, err := json.Marshal(meta)
			if err != nil {
				t.Fatal(err)
			}
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
		}
	}
}

func addRaftDirs(t *testing.T, root string) {
	t.Helper()
	dirs := []string{
		filepath.Join(root, "jetstream", "$SYS", "_js_", "_meta_"),
		filepath.Join(root, "jetstream", "$SYS", "_js_", "S-R3F-abc123"),
		filepath.Join(root, "jetstream", "$SYS", "_js_", "C-R3F-def456"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		// Write a dummy file so the directory isn't empty
		if err := os.WriteFile(filepath.Join(d, "raft.wal"), []byte("dummy"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func readMeta(t *testing.T, dir string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, metaFile))
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}
	return meta
}

func TestComputeChecksum(t *testing.T) {
	// Verify the checksum matches the NATS server algorithm:
	// key = sha256(streamName), hash = highwayhash64(key, data)
	data := []byte(`{"name":"TEST","num_replicas":1}`)
	got, err := computeChecksum("TEST", data)
	if err != nil {
		t.Fatal(err)
	}

	// Compute independently
	key := sha256.Sum256([]byte("TEST"))
	hh, _ := highwayhash.New64(key[:])
	hh.Write(data)
	var buf [8]byte
	want := hex.EncodeToString(hh.Sum(buf[:0]))

	if got != want {
		t.Errorf("checksum mismatch: got %s, want %s", got, want)
	}

	if len(got) != 16 {
		t.Errorf("checksum length: got %d, want 16", len(got))
	}
}

func TestComputeChecksumDifferentStreams(t *testing.T) {
	data := []byte(`{"name":"X","num_replicas":1}`)
	a, _ := computeChecksum("STREAM_A", data)
	b, _ := computeChecksum("STREAM_B", data)
	if a == b {
		t.Error("different stream names should produce different checksums")
	}
}

func TestFindStreams(t *testing.T) {
	root := t.TempDir()
	createTestStore(t, root, map[string]map[string]map[string]any{
		"APP": {
			"ORDERS":   {"name": "ORDERS", "num_replicas": float64(3), "subjects": "orders.>"},
			"PAYMENTS": {"name": "PAYMENTS", "num_replicas": float64(3)},
		},
		"ACCT2": {
			"EVENTS": {"name": "EVENTS", "num_replicas": float64(1)},
		},
	})
	addRaftDirs(t, root)

	jsRoot := filepath.Join(root, "jetstream")
	streams, err := findStreams(jsRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 3 {
		t.Fatalf("expected 3 streams, got %d", len(streams))
	}

	names := map[string]bool{}
	for _, s := range streams {
		names[s.name] = true
	}
	for _, want := range []string{"ORDERS", "PAYMENTS", "EVENTS"} {
		if !names[want] {
			t.Errorf("missing stream %s", want)
		}
	}
}

func TestFindStreamsSkipsSysAccount(t *testing.T) {
	root := t.TempDir()
	createTestStore(t, root, map[string]map[string]map[string]any{
		"APP": {
			"TEST": {"name": "TEST", "num_replicas": float64(3)},
		},
	})
	addRaftDirs(t, root)

	jsRoot := filepath.Join(root, "jetstream")
	streams, err := findStreams(jsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range streams {
		if s.account == "$SYS" {
			t.Error("$SYS account should be skipped")
		}
	}
}

func TestFindStreamsDetectsEncrypted(t *testing.T) {
	root := t.TempDir()
	// Create a stream with encrypted (non-JSON) meta.inf
	dir := filepath.Join(root, "jetstream", "APP", "streams", "ENCRYPTED")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Encrypted files start with a nonce (random bytes), not '{'
	if err := os.WriteFile(filepath.Join(dir, metaFile), []byte{0x89, 0x50, 0x4e, 0x47}, 0o644); err != nil {
		t.Fatal(err)
	}

	jsRoot := filepath.Join(root, "jetstream")
	streams, err := findStreams(jsRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if !streams[0].encrypted {
		t.Error("stream should be detected as encrypted")
	}
	if streams[0].name != "ENCRYPTED" {
		t.Errorf("encrypted stream name: got %s, want ENCRYPTED", streams[0].name)
	}
}

func TestEditStream(t *testing.T) {
	root := t.TempDir()
	meta := map[string]any{
		"name":         "ORDERS",
		"num_replicas": float64(3),
		"placement":    map[string]any{"tags": []any{"region"}},
		"subjects":     "orders.>",
		"retention":    "limits",
	}
	createTestStore(t, root, map[string]map[string]map[string]any{
		"APP": {"ORDERS": meta},
	})

	dir := filepath.Join(root, "jetstream", "APP", "streams", "ORDERS")
	jsRoot := filepath.Join(root, "jetstream")
	streams, _ := findStreams(jsRoot)
	if len(streams) != 1 {
		t.Fatal("expected 1 stream")
	}

	if err := editStream(streams[0]); err != nil {
		t.Fatal(err)
	}

	// Verify meta.inf was updated
	updated := readMeta(t, dir)

	// num_replicas should be 1
	if r, ok := updated["num_replicas"].(float64); !ok || int(r) != 1 {
		t.Errorf("num_replicas: got %v, want 1", updated["num_replicas"])
	}

	// placement should be removed
	if _, ok := updated["placement"]; ok {
		t.Error("placement should be removed")
	}

	// Other fields preserved
	if updated["name"] != "ORDERS" {
		t.Errorf("name: got %v, want ORDERS", updated["name"])
	}
	if updated["retention"] != "limits" {
		t.Errorf("retention: got %v, want limits", updated["retention"])
	}

	// Verify checksum is valid
	data, _ := os.ReadFile(filepath.Join(dir, metaFile))
	sum, _ := os.ReadFile(filepath.Join(dir, sumFile))
	wantSum, _ := computeChecksum("ORDERS", data)
	if string(sum) != wantSum {
		t.Errorf("checksum mismatch: got %s, want %s", sum, wantSum)
	}
}

func TestEditStreamEncryptedErrors(t *testing.T) {
	s := streamInfo{
		name:      "SECRET",
		encrypted: true,
	}
	err := editStream(s)
	if err == nil {
		t.Fatal("expected error for encrypted stream")
	}
	if got := err.Error(); got == "" {
		t.Error("error message should not be empty")
	}
}

func TestPrepare(t *testing.T) {
	root := t.TempDir()
	createTestStore(t, root, map[string]map[string]map[string]any{
		"APP": {
			"ORDERS":   {"name": "ORDERS", "num_replicas": float64(3)},
			"PAYMENTS": {"name": "PAYMENTS", "num_replicas": float64(5)},
		},
	})
	addRaftDirs(t, root)

	if err := prepare(root); err != nil {
		t.Fatal(err)
	}

	// All streams should have replicas=1
	for _, name := range []string{"ORDERS", "PAYMENTS"} {
		dir := filepath.Join(root, "jetstream", "APP", "streams", name)
		meta := readMeta(t, dir)
		if r, ok := meta["num_replicas"].(float64); !ok || int(r) != 1 {
			t.Errorf("%s: num_replicas = %v, want 1", name, meta["num_replicas"])
		}
	}

	// $SYS should be removed
	if _, err := os.Stat(filepath.Join(root, "jetstream", "$SYS")); !os.IsNotExist(err) {
		t.Error("$SYS directory should be removed")
	}

	// _js_ directories should be removed
	found := false
	filepath.WalkDir(filepath.Join(root, "jetstream"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && d.Name() == "_js_" {
			found = true
		}
		return nil
	})
	if found {
		t.Error("_js_ directories should be removed")
	}
}

func TestPrepareNoStreams(t *testing.T) {
	root := t.TempDir()
	// Create jetstream dir with just $SYS, no app accounts
	os.MkdirAll(filepath.Join(root, "jetstream", "$SYS"), 0o755)

	err := prepare(root)
	if err == nil {
		t.Fatal("expected error when no streams found")
	}
}

func TestPrepareBadDir(t *testing.T) {
	err := prepare("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Fatal("expected error for nonexistent directory")
	}
}

func TestPrepareEncryptedStreamErrors(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "jetstream", "APP", "streams", "SECRET")
	os.MkdirAll(dir, 0o755)
	os.WriteFile(filepath.Join(dir, metaFile), []byte{0xff, 0xfe, 0xfd}, 0o644)

	err := prepare(root)
	if err == nil {
		t.Fatal("expected error for encrypted stream")
	}
}

func TestPrepareIdempotent(t *testing.T) {
	root := t.TempDir()
	createTestStore(t, root, map[string]map[string]map[string]any{
		"APP": {
			"TEST": {"name": "TEST", "num_replicas": float64(3)},
		},
	})

	// Run prepare twice
	if err := prepare(root); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(root, "jetstream", "APP", "streams", "TEST")
	sum1, _ := os.ReadFile(filepath.Join(dir, sumFile))

	if err := prepare(root); err != nil {
		t.Fatal(err)
	}

	sum2, _ := os.ReadFile(filepath.Join(dir, sumFile))

	// Checksum should be the same both times
	if string(sum1) != string(sum2) {
		t.Errorf("prepare is not idempotent: checksums differ (%s vs %s)", sum1, sum2)
	}

	meta := readMeta(t, dir)
	if r, _ := meta["num_replicas"].(float64); int(r) != 1 {
		t.Error("num_replicas should still be 1 after second prepare")
	}
}

func TestPrepareMultipleAccounts(t *testing.T) {
	root := t.TempDir()
	createTestStore(t, root, map[string]map[string]map[string]any{
		"APP": {
			"STREAM_A": {"name": "STREAM_A", "num_replicas": float64(3)},
		},
		"ANALYTICS": {
			"STREAM_B": {"name": "STREAM_B", "num_replicas": float64(3)},
		},
	})

	if err := prepare(root); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{
		filepath.Join(root, "jetstream", "APP", "streams", "STREAM_A"),
		filepath.Join(root, "jetstream", "ANALYTICS", "streams", "STREAM_B"),
	} {
		meta := readMeta(t, path)
		if r, _ := meta["num_replicas"].(float64); int(r) != 1 {
			t.Errorf("%s: num_replicas = %v, want 1", path, meta["num_replicas"])
		}
	}
}

func TestChecksumMatchesNATSServer(t *testing.T) {
	// The NATS server computes: sha256(stream_name) as key, highwayhash64(key, file_bytes)
	// This test verifies our implementation produces the same output by computing
	// both sides independently and comparing.
	streamName := "PRODUCTION-ORDERS"
	fileContent := []byte(`{"name":"PRODUCTION-ORDERS","num_replicas":1,"subjects":["orders.>"],"retention":"limits","max_msgs":-1,"max_bytes":-1,"max_age":3600000000000,"max_msg_size":-1,"storage":"file","discard":"old","duplicate_window":120000000000,"max_consumers":-1}` + "\n")

	got, err := computeChecksum(streamName, fileContent)
	if err != nil {
		t.Fatal(err)
	}

	// Independent computation matching nats-server filestore.go:473,987-990
	key := sha256.Sum256([]byte(streamName))
	hh, _ := highwayhash.New64(key[:])
	hh.Write(fileContent)
	var buf [8]byte
	want := hex.EncodeToString(hh.Sum(buf[:0]))

	if got != want {
		t.Errorf("checksum does not match NATS server algorithm: got %s, want %s", got, want)
	}
}
