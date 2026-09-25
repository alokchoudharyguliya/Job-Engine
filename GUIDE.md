# Reading one job

This is the walkthrough to keep open beside the source. The job is:

```bash
./forge run --workspace /tmp/forge-ws --slots 2
./forge submit --workspace /tmp/forge-ws \
    --input /tmp/sample.bin \
    --operation count \
    --workers 2 \
    --io mmap \
    --wait
```

`sample.bin` is 8 bytes: `aabbccdd`. Two workers, so `split_ranges` in `forge.hpp` produces `[0, 4)` and `[4, 4)`.

## 1. The CLI writes a file, then a line

`main.cpp` `cmd_submit`

```text
realpath(sample.bin)          the controller has its own cwd
allocate_job_id               fcntl lock on next_id, then fsync
save_meta                     jobs/1/meta  state=submitted
send_fifo                     "SUBMIT 1\n"
```

`save_meta` calls `atomic_write_file` before the fifo write. The controller must not observe a SUBMIT for a meta file that is still a partial write.

```text
jobs/1/meta.tmp.<pid>
        | write
        | fsync          inode data is on disk
        | renameat2      directory entry swaps in one step
        | fsync(dir)     the new name survives power loss
        v
jobs/1/meta
```

That function is the VFS chapter. Read it before the event loop.

## 2. The controller wakes up

`controller.cpp` is blocked in `epoll_wait`. The fifo was opened `O_RDWR | O_NONBLOCK` so it does not deliver EOF every time a CLI closes its end.

```text
epoll_wait
    |
    fifo readable
    |
on_fifo -> handle_line("SUBMIT 1") -> enqueue
```

`enqueue` loads the meta, moves the state to `queued`, and calls `schedule`. The queue is a `vector` in the controller process. No mutex: the workers never touch it.

`pick_next` chooses the highest `--priority`, then the smallest id. `--priority` is Forge's order. `--nice` is a hint to CFS and has the opposite sign. Both are stored on `Meta` so you can see them in the same file and not mix them up.

## 3. fork

`launch_job` is the process chapter.

```text
statx(input)                 size, blocks, birth time if the fs has one
memfd_create + ftruncate
mmap(MAP_SHARED)             parent and children will share these pages
eventfd                      child writes 1, parent is asleep in epoll
timerfd                      only if --deadline was set
fork  (twice, for two slices)
setpgid                      each child is its own process group
open_start_gate              store 1, FUTEX_WAKE
epoll_ctl both new fds
state = running
```

`MAP_PRIVATE` on that memfd would be a silent bug: the child would copy-on-write and the parent would read zeros. The input file, later, is mapped `MAP_PRIVATE` on purpose. We only read it. We must not write it back.

The child closes the controller's epoll fd, signalfd, fifo, and the other jobs' eventfds. `CLOEXEC` does not fire on `fork`. It fires on `execve`. The close list is the fork case. See `fds_to_close`.

Then the child `_exit`s when it is done. Not `exit()`. `exit()` would flush stdio buffers duplicated by `fork` and run destructors that `close()` the child's copy of the controller's fds.

## 4. The futex gate

Both sides are in `worker.cpp`.

```text
parent                         child
start_gate = 0                 load start_gate
fork                           if 0: FUTEX_WAIT(addr, 0)
store 1 (release)
FUTEX_WAKE(INT_MAX)            wake, load again, see 1
```

If the parent stores 1 before the child sleeps, `FUTEX_WAIT` returns `EAGAIN` because the word is no longer 0. The child treats that as "look again", not as failure. The 30s timeout exists so a crashed parent does not leave workers asleep until reboot.

There is no `FUTEX_PRIVATE_FLAG`. That flag is for threads in one process. These are processes.

## 5. Reading the file

`feed_range` in `worker.cpp`. For this job, `--io mmap`.

```text
file pages:   [ 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 ]
chunk 1:                  [ 4 | 5 | 6 | 7 ]

mmap offset must be page-aligned, the chunk need not be.
map from the page that contains byte 4
hash/count starts (offset - map_off) bytes into the mapping
```

`count` adds every byte into `ChunkResult.aux`. If it only trusted the length, the mapping would never fault, and a comparison against `--io read` would be fake. `rchar` versus `read_bytes` and `minflt` versus `majflt` are written to the job log from `/proc/self` before and after the work.

`--io direct` is the third path. The buffer, the offset, and the length have to be multiples of 4096. The leftover tail uses a second open of the same inode without `O_DIRECT`. Two file descriptions, one inode. If the filesystem rejects `O_DIRECT` (tmpfs often does), the job fails and the log says so. There is no fallback.

## 6. Publishing the chunk

```text
write chunks[i]                     only this child writes slot i
fetch_add(chunks_done, release)    the publish
eventfd_write(1)
_exit(0)
```

The parent in `on_event` does the matching acquire load. `eventfd` means "the slot is visible", not "the chunk succeeded". Success is `ok == 1` in the slot, plus `exited:0` from `waitid`.

`SIGCHLD` arrives through `signalfd` because those signals were blocked in the controller. `waitid(P_ALL, WNOHANG)` reaps without stalling the loop. When both children are reaped and both slots are good, `finalize` writes `jobs/1/output` with the same rename protocol as the meta file, then sets `completed`.

`forge submit --wait` is not in that loop. It polls `meta` every 200ms. The event loop stays in the controller.

## 7. Failure

```text
deadline timerfd fires
        |
request_stop("timed_out")
        |
kill(-pid, SIGTERM)          the process group, so a shell's child dies too
        |
200ms tick, still alive after 1s
        |
kill(-pid, SIGKILL)
        |
SIGCHLD -> waitid -> finalize -> state timed_out
```

A worker that exits 0 with a published result still counts as `completed` if it won the race against the signal.

`RLIMIT_CPU` is different: the kernel signals the child, the child dies, the parent only sees `SIGCHLD`.

On the next `forge run`, `recover` will not `kill` a pid just because the number matches. It compares `starttime` from `/proc/<pid>/stat` with the value stored in `meta`. A recycled pid belongs to someone else.

## Experiments

Same file, three I/O paths. Compare `jobs/<id>/log` (`minflt`, `majflt`, `rchar`, `read_bytes`).

```bash
./forge submit --input big.bin --operation sha256 --io mmap --wait
./forge submit --input big.bin --operation sha256 --io read --wait
./forge submit --input big.bin --operation sha256 --io direct --wait
```

Run the mmap job twice. The second run's major faults usually drop, because the pages are still cached. `forge storage stats` shows `Cached`, `Dirty`, and `Writeback` while you do it.

Put the workspace on two filesystems:

```bash
./forge run --workspace /var/tmp/forge-ext4
./forge run --workspace /dev/shm/forge-tmpfs
```

`O_DIRECT` on tmpfs should fail the job. That is the result you want to see.

Queue versus slots:

```bash
./forge run --slots 1
# submit several count jobs without --wait, then forge jobs
```

One runs, the rest stay `queued`, in priority order.

Lock versus exec:

```bash
# while forge run is up, including during an exec job:
./forge run --workspace <the same path>
```

The second process must exit immediately. `flock` is on the open file description the parent still holds. The worker's `execve` does not release it.

## What was left out on purpose

Heartbeats, cgroups, namespaces, and eBPF are the next layer, not a second program. A heartbeat is another timestamp in the shared region plus the 200ms tick you already have. A cgroup would replace the `setrlimit` jail with one the kernel enforces on the whole process group. The event loop does not have to change shape for either of them.
