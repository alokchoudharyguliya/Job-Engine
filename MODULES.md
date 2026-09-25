# Modules and functions

Forge is one program. `make` compiles five translation units and links them. The four engine files never call `epoll`, `kqueue`, `futex`, or `os_sync` by name. They call `forge::os`, and the Makefile picks one backend:

```
uname -s
  Linux  -->  os/linux/platform.cpp
  Darwin -->  os/mac/platform.cpp
```

```
main.cpp            CLI. No workers, no event loop.
controller.cpp      the long-lived process. Queue, fork, reap, persist.
worker.cpp          code that runs after fork, in the child.
store.cpp           files on disk: lock, ids, meta, logs, durable rename.
os/platform.hpp     the contract both backends implement.
os/linux/platform.cpp
os/mac/platform.cpp
```

A job crosses them in this order:

```
cmd_submit
  allocate_job_id, save_meta, send_fifo
        |
        v
on_fifo --> enqueue --> schedule --> launch_job
                                      fork
                                        |
                                        +--> worker_entry   (child)
                                        |
                                      parent waits
                                        |
on_event (wake fd) --> finalize --> publish_file
```

## forge.hpp

Shared types. Nothing here runs.

| Name | What it is |
|---|---|
| `Meta` | One job's record: id, op, input path, workers, io mode, limits, state, proc list. |
| `ByteRange` | `[offset, length)` of the input file. |
| `split_ranges` | Cuts the file into `workers` slices. The last slice absorbs the remainder. |
| `ChunkResult` | What one worker publishes: ok/error, a digest or a count, the bytes it touched. |
| `SharedRegion` | The small mapping parent and child share. Start gate, `chunks_done`, per-chunk results. The dataset is not copied here. |
| `UniqueFd` | An fd that closes itself. The controller's lock, log, fifo, and shared map live in these. |
| `FileInfo` | Size, blocks, and a direct-io flag, filled by `inspect_file`. |
| `ProcRef` | `pid@starttime` stored in meta so recovery does not kill a recycled pid. |

## main.cpp — the CLI

This process exits. It does not fork workers. `run` is the only command that stays up, and it does that by calling `run_controller`.

| Function | What it does |
|---|---|
| `usage` | Prints the command list. |
| `take_flag` | Pulls `--name value` out of the argument vector. |
| `has_flag` | True if `--name` is present, then removes it. |
| `flag_or` | `take_flag`, or the default if the flag was omitted. |
| `real_path` | `realpath` so a relative `--input` is stored as an absolute path. The worker's cwd is not the user's cwd. |
| `send_fifo` | Opens `control.fifo` and writes one line (`SUBMIT <id>`, `CANCEL <id>`, `SHUTDOWN`). |
| `cmd_run` | Parses `--workspace` and `--slots`, then `run_controller`. |
| `wait_job` | Polls the meta file until the state is terminal. Used by `--wait`. |
| `cmd_submit` | Validates flags, allocates an id, writes meta, sends `SUBMIT`. |
| `cmd_jobs` | Lists every job directory and its state. |
| `cmd_status` | Prints `status.txt` (slots, running ids). That file is telemetry and may tear. |
| `cmd_logs` | Prints `jobs/<id>/log`. |
| `print_filtered_proc` | Prints selected lines from a `/proc` file during `inspect`. |
| `cmd_inspect` | Meta, file info, and a short process snapshot for one job. |
| `cmd_cancel` | Sends `CANCEL <id>` on the fifo. |
| `cmd_device` | `os::print_devices`. |
| `cmd_storage` | `os::print_storage` for the workspace's filesystem. |
| `need_id` | Parses the positional job id or exits with a usage error. |
| `dispatch` | Switches on argv[1]. |
| `main` | Calls `dispatch`. |

## controller.cpp — the engine loop

One process holds `controller.lock`. Everything else in this file is a function on a `Controller` struct: the poller, the fifo, the slot count, and the map of live jobs.

### Talking and recording

| Function | What it does |
|---|---|
| `say` | One line on the controller log, with a timestamp. |
| `set_state` | Writes the new state into meta and the log. |
| `parse_procs` | Reads `pid@starttime` tokens out of meta. |
| `format_procs` | Writes them back. |
| `write_status_file` | Rewrites `status.txt`. Not crash-safe. `jobs` reads meta, not this. |

### Stopping a job

| Function | What it does |
|---|---|
| `release_runtime` | Drops the shared map, the wake fd, and the deadline timer. Slots return to the pool. |
| `signal_job` | `kill` of the worker or, if `setpgid` succeeded, of the whole process group. |
| `request_stop` | Marks the job cancelling or timing out and signals it. The reap path still finalizes. |

### Fork and reap

| Function | What it does |
|---|---|
| `fds_to_close` | The numbers the child must `close` before it works. `O_CLOEXEC` does not fire on `fork`. |
| `describe_wait` | Turns `siginfo_t` into `exited:N` or `signaled:N`. |
| `note_reap` | Records one child's exit on the matching `LiveJob`. |
| `reap` | `waitid(P_ALL, WNOHANG)` in a loop until no zombie is waiting. |
| `join_exits` | One string of every worker's exit, for the log. |
| `results_ok` | True when every chunk published success and the count of chunks matches. Builds the result body. |
| `finalize` | If the result is good, `publish_file` then state `completed`. Otherwise `failed`, `timed_out`, or `cancelled`. |
| `pick_next` | Highest Forge `--priority`, then oldest id. This is not Unix nice. |
| `schedule` | While a slot is free and a job is queued, `launch_job`. |
| `enqueue` | Moves a submitted job to `queued` and calls `schedule`. |
| `launch_job` | Creates the shared map and the wake fd, `fork`s one child per worker, records `pid@starttime`, arms the deadline timer. The child calls `worker_entry` and never returns. |

### The loop

| Function | What it does |
|---|---|
| `on_cancel` | Fifo `CANCEL`. Signals a live job or marks a queued one cancelled. |
| `on_event` | A worker wrote its wake. If every chunk is in, `finalize`. |
| `on_deadline` | The job's timer fired. `request_stop` with `SIGTERM`. |
| `on_tick` | Every 200 ms. `SIGKILL` for jobs that ignored `SIGTERM` for a second. Also the idle-exit check. |
| `handle_line` | Parses one fifo line. |
| `on_fifo` | Reads the fifo and splits lines. |
| `recover` | On startup, kills leftover workers whose start time still matches meta, then marks those jobs failed. |
| `setup` | Lock, pid file, poller, fifo, signals, 200 ms tick. |
| `idle` | True when nothing is queued or running. |
| `loop` | `poller.wait`, then dispatch on the token. |
| `run_controller` | `setup`, `recover`, `loop`, remove the pid file. |

## worker.cpp — after fork

The child is still the `forge` binary. It has not `exec`'d unless the operation is `exec`.

### SHA-256

`Sha256` is FIPS 180-4. `update` buffers bytes, `transform` does one 64-byte block, `final` pads and writes 32 bytes, `to_hex` is the 64-character digest. `self_test` checks the empty string and `"abc"` before any job runs.

### Reading the file

| Function | What it does |
|---|---|
| `log_fd_line` | Appends one line to the job log fd the child inherited. |
| `feed_range` | Reads `[offset, length)` and calls a sink. `mmap` uses `MAP_PRIVATE`. `read` uses `pread`. `direct` uses `open_readonly(..., true)`. |
| `hash_sink` | Feeds the bytes into one `Sha256`. |
| `count_sink` | Adds the byte count. Must see every byte; a short read is a failure. |
| `copy_sink` | Writes the bytes to the output temp fd. |

### Becoming a worker

| Function | What it does |
|---|---|
| `apply_one_limit` | One `setrlimit`. |
| `apply_resource_limits` | Address space, CPU seconds, file size, and open files from meta. |
| `apply_scheduler_hints` | `setpriority` for nice, `os::pin_cpu` for affinity. Pin returning 1 (macOS) is logged and ignored. Pin returning -1 fails the job. |
| `wait_for_start_gate` | Sleeps on the shared word until the parent stores 1 and wakes. 30 s timeout. |
| `publish_chunk` | Stores this worker's `ChunkResult` and writes 8 bytes to the wake fd. |
| `fail_chunk` | Publishes an error result and `_exit`s. |
| `redirect_stdio` | Points stdin at the input and stdout/stderr at the job log or `/dev/null`, for `exec`. |
| `run_exec` | `execve` of `meta.exec_path`. Does not return on success. |
| `open_start_gate` | Parent side: release-store 1, then `futex_wake`. Called from `launch_job`. |
| `worker_entry` | Child entry. Closes inherited fds, resets signals, `setpgid(0,0)`, waits on the gate, runs the operation, publishes, `_exit`s. |
| `self_test` | Hash vectors plus a tiny workspace round-trip. `./forge self-test`. |

`chunk-sha256` hashes each slice on its own. It is not the SHA-256 of the whole file. `sha256` uses one worker so the digest matches `shasum -a 256`.

## store.cpp — the workspace

Files:

```
forge-workspace/
  controller.lock     flock, held for the life of the controller
  controller.pid
  controller.log
  control.fifo        CLI writes, controller reads
  next_id             fcntl record lock around the increment
  status.txt          telemetry
  jobs/<id>/meta
  jobs/<id>/log
  jobs/<id>/output
```

| Function | What it does |
|---|---|
| `errno_string` | `strerror` into a `std::string`. |
| `write_all` | `write` in a loop until `n` bytes are out. Restarts on `EINTR`. |
| `write_line` | `write_all` plus a newline. |
| `parse_u64` / `parse_i64` | Decimal parse that rejects junk. |
| `mkdir_one` | `mkdir`. `EEXIST` is success. |
| `absolute_path` | `realpath` if the file exists, otherwise a cleaned absolute path. |
| `split_last` | Splits `dir/name` for `openat`. |
| `resolve_workspace` | The `--workspace` flag, or `./forge-workspace`. |
| `job_directory` | `root/jobs/<id>`. |
| `control_fifo_path` | `root/control.fifo`. |
| `ensure_workspace` | Creates the directory tree and the fifo (`mkfifo`). |
| `acquire_controller_lock` | Opens `controller.lock` and `flock(LOCK_EX\|LOCK_NB)`. A second controller gets "already running". |
| `write_pid_file` / `read_pid_file` / `remove_pid_file` | `controller.pid`. |
| `allocate_job_id` | `fcntl(F_SETLKW)` on `next_id`, read, increment, write, unlock. |
| `assign_meta_field` | One `key=value` line into a `Meta`. |
| `validate_meta` | Rejects a job the worker cannot run (unknown op, bad worker count, missing input). |
| `meta_to_text` / `text_to_meta` | The on-disk format of `jobs/<id>/meta`. |
| `read_text_file` | Whole file into a string. |
| `atomic_write_file` | Temp file, `fsync` the inode, `replace_at`, `fsync` the directory. |
| `publish_file` | Same durability for a temp the worker already wrote (the copy output). |
| `save_meta` / `load_meta` | `atomic_write_file` / `text_to_meta` for one job. |
| `append_job_log` | Opens the log `O_APPEND` and writes one line. |
| `open_job_log` | The fd the child inherits and writes for the whole job. |
| `list_job_ids` | Directory entries under `jobs/`, numeric names only. |

`inspect_file`, `proc_starttime`, and `pid_looks_like_forge` are declared next to the store API and defined in the OS file, because `/proc` and `statx` are not portable.

## os/platform.hpp — the contract

`Poller` hides the wait object.

| Method | Linux | macOS |
|---|---|---|
| `open` | `epoll_create1` + `signalfd` | `kqueue` |
| `watch_signals` | add the signalfd | `EVFILT_SIGNAL` for `SIGCHLD`, `SIGTERM`, `SIGINT` |
| `watch_fd` | `epoll_ctl ADD` | `EVFILT_READ` |
| `watch_timer` | `timerfd_create` + `timerfd_settime` | `EVFILT_TIMER` with `NOTE_USECONDS` |
| `wait` | `epoll_wait`, then drain timerfd and signalfd | `kevent` sleep; the kernel already reports the signal number |
| `inherited_fds` | fds the child must close | same |

The controller reads the fifo and the wake fd itself. `wait` only drains timers and signals, so a token is not consumed twice.

| Function | What it does |
|---|---|
| `create_shared_map` | A `SharedRegion` both processes see. |
| `destroy_shared_map` | `munmap` and close. |
| `create_notify` | The wake channel. Parent holds the read end. |
| `notify_write` / `notify_read` | 8-byte counter write, and a draining read. |
| `futex_wait` / `futex_wake` | Sleep and wake on one shared `uint32_t`. The name stays `futex_*` on both OSes. |
| `prepare_child_signals` | Child puts `SIGCHLD`, `SIGTERM`, and `SIGINT` back to `SIG_DFL` so a deadline signal actually kills it. |
| `replace_at` | Atomic rename inside a directory fd. |
| `advise_sequential` / `advise_mapping` | Tell the kernel the read is a scan, not a random walk. |
| `open_readonly` | Plain `open`, or a direct-io open. Failure is returned, not hidden. |
| `pin_cpu` | `0` pinned, `1` this OS cannot pin, `-1` the call failed. |
| `scheduler_name` | A label for the job log. |
| `log_vm_snapshot` | RSS and virtual size, written into the job log. |
| `print_devices` | Mounts or block devices. |
| `print_storage` | Space on the workspace filesystem, plus memory. |

`Notify` fields: `held` is what the parent reads. On macOS `write_end` is the other end of the pipe, kept open so the pipe does not see EOF. `child_write` is the fd the worker writes. `child_close` is the extra end the child must drop.

## os/linux/platform.cpp

| Call | Kernel object |
|---|---|
| `signalfd` | Queued signals become bytes on an fd. The process must block those signals first. |
| `epoll_create1` | The interest set. |
| `epoll_ctl` | Adds an fd. Level-triggered: it stays readable until you drain it. |
| `timerfd_create` | A `CLOCK_MONOTONIC` timer as an fd. |
| `epoll_wait` | Sleep until one registered fd is readable. |
| `memfd_create` | Anonymous tmpfs inode. `mmap` `MAP_SHARED` is the shared region. |
| `eventfd` | A 64-bit counter. The worker's 8-byte write adds 8. |
| `syscall(SYS_futex)` | `FUTEX_WAIT` / `FUTEX_WAKE` on the start-gate word. No `FUTEX_PRIVATE` flag, because the word is shared across processes. |
| `renameat2` | Directory entry swap. Falls back to `renameat` if the kernel returns `ENOSYS`. |
| `posix_fadvise` / `madvise` | `POSIX_FADV_SEQUENTIAL` and `MADV_SEQUENTIAL`. |
| `open(O_DIRECT)` | Bypass the page cache. The buffer and the offset must be multiples of 4096. |
| `sched_setaffinity` | Allowed CPU set. Not a reservation. |
| `statx` | File size and attributes for `inspect`. |

`proc_starttime` reads field 22 of `/proc/<pid>/stat`. `pid_looks_like_forge` checks `/proc/<pid>/cmdline`.

## os/mac/platform.cpp

Same functions. Different kernel objects.

| Call | Kernel object |
|---|---|
| `kqueue` | One queue for fds, signals, and timers. |
| `kevent` | Register a filter, or sleep until one fires. |
| `sigaction` (empty handler) | Required so `EVFILT_SIGNAL` receives the signal. `SIG_IGN` would reap `SIGCHLD` automatically and `waitid` would see nothing. |
| `EVFILT_TIMER` + `NOTE_USECONDS` | Timer in microseconds. This SDK has no `NOTE_MSECONDS`. |
| `mmap(MAP_ANON\|MAP_SHARED)` | Shared region with no file. `MAP_PRIVATE` would copy the page on fork and the start gate would not work. |
| `pipe` | Wake channel. The worker still writes 8 bytes, which is why the log says `eventfd +8` on a Mac. |
| `os_sync_wait_on_address` | Sleep while a memory word equals an expected value. `OS_SYNC_WAIT_ON_ADDRESS_SHARED` because the word is mapped shared. |
| `os_sync_wake_by_address_any` | Wake waiters on that word. |
| `fcntl(F_NOCACHE)` | Closest equivalent of `O_DIRECT`. It is not the same contract. |
| `pin_cpu` | Returns 1. macOS has no `sched_setaffinity`. The job continues. |
| `getrusage` + `task_info` | The vm snapshot. |
| `getmntinfo` | `forge device list`. |
| `statfs` + `sysctl hw.memsize` | `forge storage stats`. |
| `proc_pidinfo` | Start time (`pbi_start_tvsec`) and the path used to recognize a forge worker. |

## Kernel calls in the shared files

These are the calls that stay in the engine because both OSes have them.

| Where | Call | What it is doing |
|---|---|---|
| `controller.cpp` | `fork` | New process, copy-on-write address space, duplicated fd table. |
| `controller.cpp` | `setpgid` | New process group so `kill(-pid)` hits the worker and its children. |
| `controller.cpp` | `waitid` | Reap one zombie. `WNOHANG` so the loop never blocks here. |
| `controller.cpp` | `kill` | `SIGTERM`, then `SIGKILL`. Negative pid means the process group. |
| `controller.cpp` | `open` of the fifo | `O_RDWR` so the read end does not see EOF when the CLI closes. |
| `worker.cpp` | `open` / `mmap` / `munmap` | A readable VMA over the input. First touch faults a page in. |
| `worker.cpp` | `pread` | The `read` io mode. Offset is explicit, so the fd position does not matter. |
| `worker.cpp` | `setrlimit` | Ceilings inherited across `execve`. |
| `worker.cpp` | `setpriority` | Unix nice. Opposite sign from Forge `--priority`. |
| `worker.cpp` | `write` on the wake fd | Unblocks `poller.wait` in the parent. |
| `worker.cpp` | `execve` | Replaces the image for the `exec` operation. The pid stays. |
| `worker.cpp` | `_exit` | Skips C++ destructors. The child must not flush the parent's buffers. |
| `store.cpp` | `flock` | Exclusive, non-blocking lock. One controller per workspace. |
| `store.cpp` | `fcntl(F_SETLKW)` | Waiting record lock around `next_id`. |
| `store.cpp` | `openat` | Temp file relative to the job directory fd. |
| `store.cpp` | `fsync` | Data, then the directory entry, so a crash cannot resurrect the old name. |
