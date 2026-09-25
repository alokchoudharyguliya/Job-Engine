# Forge

A local job engine that runs data-processing jobs on one machine. Linux and macOS each have an `os/` folder for the kernel calls; the four engine files stay shared. You submit a file and an operation. Forge queues the job, forks workers, applies resource limits, reads the file through the page cache or `O_DIRECT`, and publishes the result with an `fsync` and an atomic rename.

It is a learning project. Read in this order:

1. This file, for the picture.
2. `MODULES.md`, for what every file and every function does.
3. The `.cpp` next to a kernel call. The comment names the syscall and the kernel object it touches.
4. `GUIDE.md`, for one job followed function by function.

On a Mac, from this directory:

```bash
make
./forge self-test
```

On Linux the same `make` compiles `os/linux` instead of `os/mac`. `scripts/demo.sh` is the Linux end-to-end check (`sha256sum`, `/bin/sleep`).

## What you type

Terminal A:

```bash
./forge run --workspace ./forge-workspace --slots 4
```

Terminal B:

```bash
./forge submit --input dataset.bin --operation sha256 --wait
./forge submit --input dataset.bin --operation count --workers 4 --io read --wait
./forge jobs
./forge status
./forge logs 1
./forge inspect 1
./forge cancel 2
./forge device list
./forge storage stats --workspace ./forge-workspace
```

`sha256` with one worker writes the same digest as `sha256sum`. `chunk-sha256` hashes each slice on its own and says so in the output. It is not the digest of the whole file.

`--workspace` is the filesystem experiment. `./forge-workspace` lands on whatever disk you started on. `/dev/shm/forge-ws` is tmpfs. Same binary, different page cache and durability behavior. `forge storage stats` prints which mount that path is on.

## The four files

```text
main.cpp                         controller.cpp
forge submit                     epoll_wait
   | write jobs/<id>/meta           | SUBMIT <id>  (fifo)
   | write the fifo                 | fork
   v                                v
store.cpp  <---- meta, output ----  worker.cpp
jobs/<id>/meta                      mmap / read / O_DIRECT
jobs/<id>/output                    eventfd_write, then _exit
```

| File | What it owns |
| --- | --- |
| `forge.hpp` | The nouns: job meta, the shared-memory layout, the state names |
| `store.cpp` | Directories, `flock`, the job-id lock, `fsync` + `renameat2`, `/proc`, `/sys` |
| `worker.cpp` | The child: limits, affinity, futex wait, the three I/O paths, `execve` |
| `controller.cpp` | The parent: epoll, the queue, `fork`, `waitid`, deadlines, cancel |
| `main.cpp` | The commands |

Read `forge.hpp` first, then `store.cpp`, then `worker.cpp`, then `controller.cpp`. `GUIDE.md` follows one `count` job through those functions by name.

## What the controller is waiting on

```text
                        epoll_wait
                            |
        +-------------------+-------------------+
        |           |           |               |
     signalfd    timerfd     eventfd          fifo
        |           |           |               |
     SIGCHLD     deadlines   chunk ready    SUBMIT / CANCEL
     SIGTERM     200ms tick                 SHUTDOWN
     SIGINT
```

The queue lives only in the controller thread. It has no mutex, because nothing else mutates it. Workers are other processes. They publish a few dozen bytes in a `memfd` mapping and wake the parent with `eventfd`. The file itself stays in the filesystem.

## Job states

```text
submitted --> queued --> starting --> running --> persisting --> completed
                 |          |           |
                 |          |           +--> failed / timed_out / cancelled
                 +--> cancelled
```

`completed` means the output file has already been `fsync`'d and renamed. `persisting` is the short window before that rename.

## Workspace

```text
forge-workspace/
  controller.lock     flock, held for the life of `forge run`
  controller.pid
  controller.log
  control.fifo        CLI writes one line, controller reads it
  next_id             fcntl record lock around the increment
  status.txt          telemetry, allowed to tear
  jobs/12/meta        system of record, atomic rename
  jobs/12/log
  jobs/12/output
```

## Operations and limits

```text
--io mmap      file pages become part of the worker address space
--io read      pread copies out of the page cache
--io direct    O_DIRECT, 4096-aligned; no silent fallback
--mem-mb       RLIMIT_AS   (this includes the mmap)
--cpu-sec      RLIMIT_CPU
--max-fds      RLIMIT_NOFILE
--max-out-mb   RLIMIT_FSIZE
--deadline     one timerfd per job
--cpu          sched_setaffinity
--nice         setpriority (larger nice = less CPU)
--priority     Forge's own queue order (larger = sooner). Not nice.
```

An external program is a normal job:

```bash
./forge submit --operation exec --exec ./scripts/sleep60.sh --input dataset.bin --deadline 2 --wait
```

`execve` receives `PROGRAM INPUT OUTPUT`.

## Where each kernel idea shows up

| Idea | Where to read it |
| --- | --- |
| `fork`, process groups, `waitid` | `controller.cpp` `launch_job`, `reap` |
| `execve` | `worker.cpp` `run_exec` |
| `signalfd`, blocked signals | `store.cpp` `block_supervisor_signals`, `controller.cpp` `setup` |
| `timerfd` | `controller.cpp` deadline arming and the 200ms tick |
| `eventfd` + `epoll` | `controller.cpp` `on_event`, `worker.cpp` `publish_chunk` |
| futex start gate | `worker.cpp` `wait_for_start_gate`, `open_start_gate` |
| `memfd` + `MAP_SHARED` | `controller.cpp` `launch_job` |
| `mmap` / `madvise` / `posix_fadvise` | `worker.cpp` `feed_range` |
| `O_DIRECT` | `worker.cpp` `feed_range` |
| `setrlimit`, affinity, nice | `worker.cpp` `apply_resource_limits`, `apply_scheduler_hints` |
| `openat`, `statx`, `renameat2`, `fsync` | `store.cpp` `atomic_write_file` |
| `flock` vs `fcntl` locks | `store.cpp` `acquire_controller_lock`, `allocate_job_id` |
| `/proc`, `/sys` | `main.cpp` `inspect`, `device`, `storage` |
| pid reuse | `controller.cpp` `recover` |

`make debug` rebuilds at `-O0 -g` for stepping. The default is `-O2 -g`.
