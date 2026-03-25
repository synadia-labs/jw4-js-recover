package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/minio/highwayhash"
)

const (
	metaFile   = "meta.inf"
	sumFile    = "meta.sum"
	streamsDir = "streams"
	sysAccount = "$SYS"
	jsDir      = "_js_"
	version    = "0.1.0"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "prepare":
		if len(os.Args) < 3 {
			fmt.Fprintf(os.Stderr, "usage: jw4-js-recover prepare <store-dir>\n")
			os.Exit(1)
		}
		if err := prepare(os.Args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "inspect":
		if len(os.Args) < 3 {
			fmt.Fprintf(os.Stderr, "usage: jw4-js-recover inspect <store-dir>\n")
			os.Exit(1)
		}
		if err := inspect(os.Args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "version":
		fmt.Printf("jw4-js-recover %s\n", version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `jw4-js-recover - Prepare a JetStream store for standalone recovery

EXPERIMENTAL -- FOR EDUCATIONAL PURPOSES ONLY
This tool is not supported by Synadia. It relies on internal NATS server
implementation details that may change without notice.

When a NATS cluster loses Raft quorum (e.g., 2-of-3 region loss), the
surviving replica's store data cannot be accessed through JetStream. This
tool prepares the store for loading by a standalone NATS server, enabling
a backup to be taken via "nats account backup".

Usage:
  jw4-js-recover <command> [arguments]

Commands:
  prepare <store-dir>   Edit stream metafiles for standalone recovery
  inspect <store-dir>   Show stream metadata without modifying anything
  version               Print version
  help                  Print this message

The prepare command:
  1. Finds all stream meta.inf files under <store-dir>
  2. Sets num_replicas to 1 and removes placement constraints
  3. Recomputes the meta.sum checksum (highway hash 64-bit)
  4. Removes Raft metadata (_js_/ directories and $SYS account)

Example:
  # Copy the surviving store
  cp -a /var/nats/jetstream /tmp/recovery/store

  # Prepare for standalone loading
  jw4-js-recover prepare /tmp/recovery/store

  # Start standalone server with store_dir pointing to /tmp/recovery/store
  # Then: nats account backup /tmp/recovery/backup -f
`)
}

func resolveJSRoot(storeDir string) (string, error) {
	jsRoot := filepath.Join(storeDir, "jetstream")
	if _, err := os.Stat(jsRoot); err != nil {
		if _, err2 := os.Stat(filepath.Join(storeDir, sysAccount)); err2 == nil {
			return storeDir, nil
		}
		return "", fmt.Errorf("cannot find jetstream store at %s: %w", storeDir, err)
	}
	return jsRoot, nil
}

// prepare edits all stream metafiles in a store directory for standalone recovery.
func prepare(storeDir string) error {
	jsRoot, err := resolveJSRoot(storeDir)
	if err != nil {
		return err
	}

	streams, err := findStreams(jsRoot)
	if err != nil {
		return err
	}
	if len(streams) == 0 {
		return fmt.Errorf("no streams found under %s", storeDir)
	}

	for _, s := range streams {
		if err := editStream(s); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
	}

	// Remove Raft metadata
	removed := 0
	err = filepath.WalkDir(jsRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && d.Name() == jsDir {
			if rerr := os.RemoveAll(path); rerr != nil {
				return rerr
			}
			removed++
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("removing raft metadata: %w", err)
	}

	// Remove $SYS account
	sysPath := filepath.Join(jsRoot, sysAccount)
	if _, err := os.Stat(sysPath); err == nil {
		if err := os.RemoveAll(sysPath); err != nil {
			return fmt.Errorf("removing $SYS: %w", err)
		}
		removed++
	}

	fmt.Printf("\nPrepared %d stream(s), removed %d raft/system director(ies).\n", len(streams), removed)
	fmt.Println("Start a standalone NATS server with store_dir pointing to this directory.")
	return nil
}

// inspect shows stream metadata without modifying anything.
func inspect(storeDir string) error {
	jsRoot, err := resolveJSRoot(storeDir)
	if err != nil {
		return err
	}

	streams, err := findStreams(jsRoot)
	if err != nil {
		return err
	}
	if len(streams) == 0 {
		return fmt.Errorf("no streams found under %s", storeDir)
	}

	for _, s := range streams {
		replicas := 0
		if r, ok := s.meta["num_replicas"].(float64); ok {
			replicas = int(r)
		}
		placement := ""
		if p, ok := s.meta["placement"]; ok && p != nil {
			b, _ := json.Marshal(p)
			placement = string(b)
		}
		fmt.Printf("%-30s  replicas=%d  account=%s", s.name, replicas, s.account)
		if placement != "" {
			fmt.Printf("  placement=%s", placement)
		}
		if s.encrypted {
			fmt.Printf("  [ENCRYPTED]")
		}
		fmt.Println()
	}

	fmt.Printf("\n%d stream(s) found.\n", len(streams))
	return nil
}

type streamInfo struct {
	name      string
	account   string
	dir       string
	meta      map[string]any
	encrypted bool
}

func findStreams(jsRoot string) ([]streamInfo, error) {
	var streams []streamInfo

	accounts, err := os.ReadDir(jsRoot)
	if err != nil {
		return nil, fmt.Errorf("reading store: %w", err)
	}

	for _, acct := range accounts {
		if !acct.IsDir() || acct.Name() == sysAccount {
			continue
		}
		sdir := filepath.Join(jsRoot, acct.Name(), streamsDir)
		entries, err := os.ReadDir(sdir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			streamDir := filepath.Join(sdir, entry.Name())
			metaPath := filepath.Join(streamDir, metaFile)
			data, err := os.ReadFile(metaPath)
			if err != nil {
				continue
			}

			si := streamInfo{
				dir:     streamDir,
				account: acct.Name(),
			}

			if len(data) > 0 && data[0] != '{' {
				si.encrypted = true
				si.name = entry.Name()
				streams = append(streams, si)
				continue
			}

			var meta map[string]any
			if err := json.Unmarshal(data, &meta); err != nil {
				continue
			}
			si.meta = meta
			if name, ok := meta["name"].(string); ok {
				si.name = name
			} else {
				si.name = entry.Name()
			}
			streams = append(streams, si)
		}
	}

	return streams, nil
}

func editStream(s streamInfo) error {
	if s.encrypted {
		return fmt.Errorf("meta.inf is encrypted; start the standalone server with the same jetstream key instead of using prepare")
	}

	oldReplicas := 0
	if r, ok := s.meta["num_replicas"].(float64); ok {
		oldReplicas = int(r)
	}

	s.meta["num_replicas"] = 1
	delete(s.meta, "placement")

	updated, err := json.MarshalIndent(s.meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	updated = append(updated, '\n')

	metaPath := filepath.Join(s.dir, metaFile)
	if err := os.WriteFile(metaPath, updated, 0o644); err != nil {
		return fmt.Errorf("write meta.inf: %w", err)
	}

	checksum, err := computeChecksum(s.name, updated)
	if err != nil {
		return fmt.Errorf("checksum: %w", err)
	}

	sumPath := filepath.Join(s.dir, sumFile)
	if err := os.WriteFile(sumPath, []byte(checksum), 0o644); err != nil {
		return fmt.Errorf("write meta.sum: %w", err)
	}

	fmt.Printf("  %s: replicas %d -> 1\n", s.name, oldReplicas)
	return nil
}

func computeChecksum(streamName string, data []byte) (string, error) {
	key := sha256.Sum256([]byte(streamName))
	hh, err := highwayhash.New64(key[:])
	if err != nil {
		return "", err
	}
	hh.Write(data)
	var buf [highwayhash.Size64]byte
	return hex.EncodeToString(hh.Sum(buf[:0])), nil
}
