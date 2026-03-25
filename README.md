# jw4-js-recover

**EXPERIMENTAL -- FOR EDUCATIONAL PURPOSES ONLY**

This tool is not supported by Synadia. It relies on internal NATS server
implementation details that may change without notice. Do not use in
production without understanding the risks and limitations described below.

## What it does

When a NATS cluster loses Raft quorum (e.g., 2-of-3 region failure), the
surviving replicas' data exists on disk but is inaccessible through
JetStream. This tool prepares the store so a standalone NATS server can
load it, enabling `nats account backup` to extract the data for restore
to a fresh cluster.

## Install

```bash
go install github.com/synadia-labs/jw4-js-recover@latest
```

## Usage

```
jw4-js-recover <command> [arguments]

Commands:
  prepare <store-dir>   Edit stream metafiles for standalone recovery
  inspect <store-dir>   Show stream metadata without modifying
  version               Print version
  help                  Print help
```

## Recovery procedure

```bash
# 1. Copy the surviving JetStream store
cp -a /var/nats/jetstream /tmp/recovery/store

# 2. Prepare for standalone loading
jw4-js-recover prepare /tmp/recovery/store

# 3. Start a standalone NATS server (no cluster block)
nats-server -c recovery.conf  # store_dir points to /tmp/recovery/store

# 4. Take a backup
nats account backup /tmp/recovery/backup -f

# 5. Restore to fresh cluster
nats stream restore /tmp/recovery/backup/STREAM_NAME --replicas N
```

## What `prepare` does

1. Finds all stream `meta.inf` files under the store directory
2. Sets `num_replicas` to 1 (standalone servers reject replicas > 1)
3. Removes placement constraints
4. Recomputes the `meta.sum` checksum (highway hash 64-bit, same algorithm as the NATS server)
5. Removes Raft metadata (`_js_/` directories) and the `$SYS` account

The `inspect` command shows stream metadata (name, replicas, account, placement, encryption status) without modifying anything.

## Limitations

- Does not handle encrypted JetStream stores. If `jetstream { key: "..." }` is configured, start the standalone server with the same key instead.
- Validated with limits-based retention only. Interest and work-queue retention streams may lose messages during standalone recovery.
- Consumer positions may lag 2-3 minutes behind the true state at the time of failure.
- Not tested with JWT/operator mode accounts, supercluster topologies, or source/mirror streams.

## Disclaimer

This tool is provided as-is for educational and experimental use. It
manipulates internal NATS JetStream storage files in ways that are not
part of any supported API or recovery procedure. The authors and Synadia
Communications assume no liability for data loss resulting from its use.

## License

Apache License 2.0. Copyright 2026 Synadia Communications, Inc.
