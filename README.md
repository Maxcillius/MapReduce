# MapReduce

A Go MapReduce word-count project. A coordinator schedules map and reduce tasks over Go RPC. Workers can run as local goroutines or as separate processes on other machines.

## Requirements

- Go 1.24.4 or newer.
- For workers on other machines, a shared filesystem such as NFS, mounted at the **same absolute path** on the coordinator and every worker. The shared directory must be writable by all processes.
- Network connectivity from workers to the coordinator's TCP RPC port.

The coordinator splits source inputs into files under the shared work root. Workers read those splits and write their intermediate files there. Original source input paths therefore only need to be available on the coordinator. Reducer output files are copied to the coordinator's output directory and atomically renamed into place.

## Run locally

From the repository root:

```sh
go run . -f Files/sample.txt -n 4 -o output/word-count -w 4
```

The job writes one tab-separated word/count file per reducer:

```text
output/word-count-r-00000
output/word-count-r-00001
...
```

Words are tokenized on whitespace and retain their original case and punctuation. Reducer keys are sorted for stable output. Temporary input splits and intermediate data are removed after successful completion.

Options:

- `-f`: comma-separated input paths.
- `-n`: number of reduce partitions.
- `-o`: output file prefix.
- `-w`: number of local worker goroutines. Set to `0` to use only separately launched workers.
- `-chunk`: target map split size in MiB (default 16).
- `-listen`: coordinator RPC listen address (default `127.0.0.1:9000`).
- `-work-root`: shared directory for split and intermediate files (default `./mr-work`).

## Run workers as separate processes or on other machines

Mount the same shared directory at the same absolute path everywhere. For example, mount an NFS export as `/mnt/mr-shared` on the coordinator host and each worker host. Give all processes compatible numeric UID/GID or shared-group permissions: workers need to read split files and create intermediate files in the job directory. Job and split directories are created with group read/write/execute and setgid permissions. Start the coordinator with local workers disabled and bind RPC to a network interface:

```sh
go run . \
  -f Files/sample.txt \
  -n 4 \
  -o output/word-count \
  -w 0 \
  -listen 0.0.0.0:9000 \
  -work-root /mnt/mr-shared
```

The coordinator prints the address it is listening on. On each worker host, run:

```sh
go run ./cmd/worker -coordinator 10.0.0.10:9000 -id 101
```

Replace `10.0.0.10` with the coordinator host's private network address. Start as many worker processes as needed, giving each a unique non-negative `-id`. If local workers and remote workers are mixed, remote worker IDs must not overlap the local IDs (`0` through `w-1`).

The worker obtains the shared job directory from the coordinator during registration. The shared mount must be available at the same path on every host, and the worker must be able to create files in it. Allow the coordinator TCP port through the private network firewall. The RPC server has no authentication or TLS; use a trusted private network or VPN and do not expose the port to the public internet.

The coordinator prints completion and exits after the job finishes. Workers exit after receiving the coordinator's `EXIT` response. If an RPC connection breaks, a standalone worker retries the connection and registers again.

## Resume after coordinator failure

The coordinator writes an atomic checkpoint before handing out work and after task state changes. By default it is stored at `mapreduce-checkpoint.json` under `-work-root`. Keep the checkpoint and job work directory on persistent storage, readable and writable by a replacement coordinator. An advisory lock next to the checkpoint prevents two coordinators from owning the same job at once; the shared filesystem must support file locking. If the coordinator process stops unexpectedly, restart it with the same shared mount and:

```sh
go run . -resume -w 0 -listen 0.0.0.0:9000 -work-root /mnt/mr-shared
```

If you used a non-default checkpoint path, pass it with `-checkpoint`. The checkpoint contains the job's input split paths and output prefix, so `-f`, `-n`, and `-o` are not needed when resuming. Restarted workers reconnect automatically. Completed map outputs are retained; tasks that were running when the coordinator stopped are requeued. Each coordinator restart advances a generation number, so a late completion from an earlier coordinator instance cannot overwrite the recovered task, even when its attempt number is reused. Replaying an interrupted attempt does not consume another worker retry.

Checkpoint writes are synchronous and happen while the coordinator lock is held. This favors recoverability over throughput and may limit scheduling speed for very large jobs.

## Fault handling

- Workers heartbeat every two seconds. The coordinator considers a worker silent after ten seconds and requeues its running task.
- A task is attempted up to three times. Expired or failed attempts are retried; old attempt results are ignored.
- Worker RPC connection attempts and calls have a five-second timeout. If a worker becomes unreachable, the coordinator detects its missed heartbeats and retries the task. Keep enough healthy workers running to make progress after a failure.
- Worker registrations and heartbeat timestamps exist only in coordinator memory; workers register again after recovery. The shared filesystem remains a single point of failure.

## Verify

```sh
go test ./...
go test -race ./...
```

The tests cover input splitting, multi-worker execution over RPC, checkpoint recovery, task assignment and heartbeat concurrency, worker timeout and retry behavior, stale completions, and output validation.
