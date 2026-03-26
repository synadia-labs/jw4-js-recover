# js-recover

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
go install github.com/synadia-labs/jw4-js-recover/cmd/js-recover@latest
```

## Usage

```
js-recover <command> [arguments]

Commands:
  prepare <store-dir>   Edit stream metafiles for standalone recovery
  inspect <store-dir>   Show stream metadata without modifying
  version               Print version
  help                  Print help
```

The `<store-dir>` argument is the nats-server `store_dir` (the parent of the `jetstream/` subdirectory). You can also pass the `jetstream/` subdirectory directly.

## Recovery procedure

```bash
# 1. Copy the jetstream/ subdirectory from the surviving node's store_dir.
cp -a /var/nats/jetstream /tmp/recovery/jetstream

# 2. Inspect the store to review streams before modifying anything.
js-recover inspect /tmp/recovery

# 3. Prepare the store for standalone loading.
#    This sets replicas to 1, removes placement, recomputes checksums,
#    and removes Raft metadata so a standalone server can load the data.
js-recover prepare /tmp/recovery

# 4. Start a standalone nats-server pointing at the prepared store.
#    No cluster, gateway, or leafnode config. Just JetStream with the store_dir.
nats-server --jetstream --store_dir /tmp/recovery --addr 127.0.0.1 --port 4222

# 5. Back up all streams from the standalone server.
nats account backup /tmp/backup -f

# 6. Restore each stream to the new production cluster.
#    Use --replicas to set the desired replica count on the new cluster.
nats stream restore /tmp/backup/STREAM_NAME --replicas 3
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
