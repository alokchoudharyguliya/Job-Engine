#ifndef FORGE_HPP
#define FORGE_HPP

// Linux-only names (memfd, epoll, futex, statx) live in os/linux.
// The Darwin equivalents live in os/mac. This header stays the shared glossary.
#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <time.h>
#include <unistd.h>

static_assert(sizeof(void*) == 8, "Forge is written for 64-bit");

// =============================================================================
// Forge — a local job execution engine.
//
// This header is the glossary. The four .cpp files are one program, split
// only so each file has one job. Read them in this order:
//
//   README.md        what the program is, and the picture of the four files
//   forge.hpp        the nouns: job, workspace, shared region (you are here)
//   store.cpp        files: workspace, locks, atomic rename, /proc, /sys
//   worker.cpp       the child: limits, mmap, hash/count/copy, futex, exec
//   controller.cpp   the parent: epoll, schedule, fork, reap, deadlines
//   main.cpp         the commands you type
//   GUIDE.md         one job, followed function by function
//
// How a command crosses the files:
//
//   main.cpp                         controller.cpp
//   forge submit                     epoll_wait
//      | allocate id, write meta        | read "SUBMIT <id>" from the fifo
//      | write "SUBMIT <id>"            | fork
//      v                                v
//   store.cpp  <---- meta, output ----  worker.cpp
//   jobs/<id>/meta                      mmap / hash / copy
//   jobs/<id>/output                    eventfd_write, then _exit
//
// The dataset never moves through the fifo or through this header's shared
// region. Big bytes stay in the input file (read or mmap). The shared region
// holds a few dozen bytes of "what did my chunk produce".
// =============================================================================

namespace forge {

// A running controller will not start more worker processes than this, no
// matter how many jobs are queued. The cap is a safety rail against a typo
// like --slots 100000. The queue exists so extra jobs wait instead.
constexpr int kMaxSlots = 256;

// One job may fan out into this many child processes. Each one gets a
// disjoint byte range of the input and a private slot in SharedRegion.
constexpr int kMaxWorkers = 16;

// O_DIRECT on Linux requires the buffer, the file offset, and the length
// to be multiples of the logical block size. 4096 is the safe value for
// the filesystems this project is meant to be tried on (ext4, xfs).
constexpr uint64_t kDirectAlign = 4096;

// -----------------------------------------------------------------------------
// Job states, stored as text in jobs/<id>/meta so you can `cat` them.
//
//   submitted --> queued --> starting --> running --> persisting --> completed
//                    |          |           |            |
//                    |          |           |            +--> failed
//                    |          |           +--> timed_out / cancelled / failed
//                    +--> cancelled
//
// "persisting" is the short window where the result file is being fsync'd
// and renamed. You will rarely see it. It exists so a crash in that window
// is not reported as "completed" with a missing file.
// "completed" means the output file is already durable.
// -----------------------------------------------------------------------------

inline bool is_terminal_state(std::string_view state) {
  return state == "completed" || state == "failed" || state == "timed_out" ||
         state == "cancelled";
}

// Everything the controller needs to launch a job, in the form that gets
// written to jobs/<id>/meta. Strings are used so the file and the memory
// agree without a second encoding.
struct Meta {
  uint64_t id = 0;
  std::string state = "submitted";
  std::string op;             // sha256, chunk-sha256, count, copy, exec
  std::string input;          // absolute path, or the exec program's argv[1]
  std::string exec_path;      // absolute path of the program when op == exec
  std::string io = "mmap";    // mmap | read | direct
  int workers = 1;
  int priority = 0;           // larger number runs first (this is NOT nice)
  int nice = 0;               // the Unix nice value; larger means less CPU
  int cpu = -1;               // -1 = no affinity; otherwise pin to that CPU
  uint64_t mem_mb = 0;        // RLIMIT_AS; 0 = do not set
  uint64_t cpu_sec = 0;       // RLIMIT_CPU
  uint64_t max_fds = 0;       // RLIMIT_NOFILE
  uint64_t max_out_mb = 0;    // RLIMIT_FSIZE
  uint64_t deadline_s = 0;    // timerfd one-shot; 0 = no deadline
  std::string procs;          // "pid@starttime,pid@starttime" once running
  std::string exits;          // "exited:0,killed:9" once reaped
  uint64_t bytes = 0;         // input size for built-in ops
  std::string digest;         // short result summary
  std::string error;
  int64_t submitted_ms = 0;
  int64_t started_ms = 0;
  int64_t finished_ms = 0;
};

// One slice of the input. `offset` and `length` are in bytes.
// Ranges produced by split_ranges are contiguous and cover [0, size).
struct ByteRange {
  uint64_t offset = 0;
  uint64_t length = 0;
};

// Split `size` bytes across `workers` processes.
//
// align == 1: byte-granular. The first (size % workers) slices are one byte
// longer, so the work is as even as it can be.
//
// align == 4096 (O_DIRECT): every slice except the tail starts on an aligned
// boundary and has an aligned length. The leftover size % 4096 bytes stick
// to the last slice and are read through the page cache, because the kernel
// rejects an unaligned O_DIRECT read.
//
// An empty file is one empty slice, not N empty processes.
inline std::vector<ByteRange> split_ranges(uint64_t size, int workers,
                                           uint64_t align) {
  std::vector<ByteRange> out;
  if (workers < 1) {
    workers = 1;
  }
  if (size == 0 || workers == 1) {
    out.push_back(ByteRange{0, size});
    return out;
  }
  if (align <= 1) {
    const uint64_t nworkers = static_cast<uint64_t>(workers);
    const uint64_t base = size / nworkers;
    const uint64_t rem = size % nworkers;
    uint64_t off = 0;
    for (int i = 0; i < workers; ++i) {
      const uint64_t n = base + (static_cast<uint64_t>(i) < rem ? 1 : 0);
      out.push_back(ByteRange{off, n});
      off += n;
    }
    return out;
  }

  const uint64_t prefix = size - (size % align);
  if (prefix == 0) {
    // Smaller than one aligned block: one buffered read, no O_DIRECT.
    out.push_back(ByteRange{0, size});
    return out;
  }

  const uint64_t blocks = prefix / align;
  const uint64_t nworkers = static_cast<uint64_t>(workers);
  const uint64_t base_blocks = blocks / nworkers;
  const uint64_t rem_blocks = blocks % nworkers;
  uint64_t off = 0;
  for (int i = 0; i < workers; ++i) {
    const uint64_t b =
        base_blocks + (static_cast<uint64_t>(i) < rem_blocks ? 1 : 0);
    uint64_t n = b * align;
    if (i == workers - 1) {
      n += size - prefix;
    }
    out.push_back(ByteRange{off, n});
    off += n;
  }
  return out;
}

// -----------------------------------------------------------------------------
// The small shared mapping between the controller and its workers.
//
//   memfd_create  -->  an inode with no directory entry (a tmpfs file)
//   mmap MAP_SHARED in the parent
//   fork          -->  the child inherits the mapping
//
// MAP_SHARED is the whole point. MAP_PRIVATE would give the child a
// copy-on-write page, and the parent would never see the result.
//
// Layout, one cache line per word that more than one core writes.
// Your false-sharing labs are about a hot counter sharing a line with other
// data. This counter is touched once per chunk, so it is not a benchmark,
// but the layout is the same idea: don't park the counter on top of chunk 0.
//
//   line 0: magic, nchunks          written once, before fork
//   line 1: start_gate              parent publishes 1, children wait
//   line 2: chunks_done             each child release-increments
//   then:   chunks[i]               only child i writes slot i
//
// Protocol (both sides live in worker.cpp; the parent triggers the gate
// from controller.cpp):
//
//   1. Parent zeroes the region and forks. start_gate is 0.
//   2. Child writes nothing shared until it sees start_gate == 1.
//   3. Parent stores 1 with release, then FUTEX_WAKE.
//   4. Child fills chunks[i], then release-increments chunks_done,
//      then eventfd_write. The event means "the slot is published",
//      not "the chunk succeeded".
//   5. Parent acquire-loads chunks_done before reading the slot.
//      Process exit + waitid is the other channel: the parent does not
//      treat the job as finished until every child has been reaped.
// -----------------------------------------------------------------------------

constexpr uint32_t kShmMagic = 0x464F5247u;  // 'FORG'

struct alignas(64) ChunkResult {
  uint32_t index = 0;
  uint32_t ok = 0;
  uint64_t offset = 0;
  uint64_t length = 0;
  uint64_t bytes = 0;
  uint64_t aux = 0;     // count: sum of the bytes, so the read cannot be elided
  char hex[80] = {};    // sha256 of this slice, NUL-terminated
  char error[160] = {};
};

struct SharedRegion {
  uint32_t magic = 0;
  uint32_t nchunks = 0;
  alignas(64) uint32_t start_gate = 0;
  alignas(64) uint32_t chunks_done = 0;
  ChunkResult chunks[kMaxWorkers] = {};
};

static_assert(std::atomic_ref<uint32_t>::is_always_lock_free,
              "the futex word has to be a plain 32-bit atomic, not a lock");
static_assert(std::atomic_ref<uint32_t>::required_alignment <= 4);

// -----------------------------------------------------------------------------
// RAII for a file descriptor.
//
// A file descriptor is an integer index into this process's fd table.
// The thing it points at — the open file description — is a kernel object
// with its own offset, status flags, and reference count. dup and fork
// create a second index onto the same object.
//
// close() drops one reference. On Linux, don't retry close() after EINTR:
// the fd number is already released, and a retry can close a number that
// another thread (or a signal handler) has reused.
//
// After fork(), the child has its own copy of this C++ object and its own
// fd table entry. Destroying the child's object closes the child's entry
// only. We still don't rely on that in the child: the child _exit()s and
// closes the inherited controller fds by number. See worker_entry.
// -----------------------------------------------------------------------------

class UniqueFd {
 public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) : fd_(fd) {}
  ~UniqueFd() { reset(); }

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      reset();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  int get() const { return fd_; }
  explicit operator bool() const { return fd_ >= 0; }

  int release() {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }

  void reset(int fd = -1) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

 private:
  int fd_ = -1;
};

// -----------------------------------------------------------------------------
// Small helpers used by every file. Definitions are inline so a learner can
// read them here; they don't hide any syscall worth a chapter.
// -----------------------------------------------------------------------------

std::string errno_string();

inline int64_t realtime_ms() {
  timespec ts{};
  clock_gettime(CLOCK_REALTIME, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

inline uint64_t mono_ms() {
  timespec ts{};
  // Monotonic: deadlines and "how long has this job run" must not jump
  // backwards if someone sets the wall clock.
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000 +
         static_cast<uint64_t>(ts.tv_nsec / 1000000);
}

bool write_all(int fd, const void* data, size_t n);
bool write_line(int fd, std::string_view line);

bool parse_u64(std::string_view text, uint64_t& out);
bool parse_i64(std::string_view text, int64_t& out);

// -----------------------------------------------------------------------------
// store.cpp — the file abstraction.
//
// Our workspace, /proc, and /sys are all filesystems. The controller does
// not have a separate "database client". Job state is a directory tree.
// -----------------------------------------------------------------------------

// ./forge-workspace, or $FORGE_WORKSPACE, or an explicit flag. Always
// returned as an absolute path so the CLI and the controller name the same
// directory even if their current directories differ.
std::string resolve_workspace(const std::string& flag);

bool ensure_workspace(const std::string& root, std::string& err);

// flock() on controller.lock, non-blocking. The returned fd IS the lock.
// Hold it until the controller exits. A second controller gets a clear error
// instead of waiting, which is why this is LOCK_NB and the job-id lock is not.
UniqueFd acquire_controller_lock(const std::string& root, std::string& err);

bool write_pid_file(const std::string& root, int pid, std::string& err);
bool read_pid_file(const std::string& root, int& pid, std::string& err);
void remove_pid_file(const std::string& root);

// True when /proc/<pid> looks like a live forge controller.
bool pid_looks_like_forge(int pid);

// POSIX record lock (fcntl F_SETLKW) around next_id. Contrast with the flock
// above: this one waits, and it is per-process. The CLI holds it only for
// the increment. It is never held across fork.
bool allocate_job_id(const std::string& root, uint64_t& id, std::string& err);

std::string job_directory(const std::string& root, uint64_t id);
std::string control_fifo_path(const std::string& root);

bool validate_meta(const Meta& meta, std::string& err);
std::string meta_to_text(const Meta& meta);
bool text_to_meta(std::string_view text, Meta& meta, std::string& err);
bool save_meta(const std::string& root, const Meta& meta, std::string& err);
bool load_meta(const std::string& root, uint64_t id, Meta& meta, std::string& err);

bool append_job_log(const std::string& root, uint64_t id, std::string_view line,
                    std::string& err);
// O_APPEND log fd, inherited across fork. -1 on error.
int open_job_log(const std::string& root, uint64_t id, std::string& err);

std::vector<uint64_t> list_job_ids(const std::string& root);

bool read_text_file(const std::string& path, std::string& out, std::string& err);

// Crash-safe publish of a small file: write temp, fsync, renameat2, fsync
// the directory. This is the function to read when you want the VFS story.
bool atomic_write_file(const std::string& path, std::string_view data,
                       std::string& err);

// Same protocol for a file that was already written (the copy output).
// `filefd` is the temp file. It is fsync'd here. The caller closes it.
bool publish_file(int dirfd, int filefd, const char* tmp_name,
                  const char* final_name, std::string& err);

struct FileInfo {
  uint64_t size = 0;
  bool regular = false;
  uint64_t blocks_512 = 0;
  bool have_btime = false;
  int64_t btime_sec = 0;
};

// statx, with a stat() fallback if the kernel is old enough to return ENOSYS.
bool inspect_file(const std::string& path, FileInfo& info, std::string& err);

// Field 22 of /proc/<pid>/stat: start time in clock ticks since boot.
// Compared, never converted. Used so a restarted controller does not kill
// a recycled pid.
bool proc_starttime(int pid, uint64_t& ticks);

// -----------------------------------------------------------------------------
// worker.cpp — code that runs in the child, plus the futex gate the parent
// opens, plus the hash self-test.
// -----------------------------------------------------------------------------

struct WorkerLaunch {
  Meta meta;
  std::string job_dir;
  SharedRegion* shm = nullptr;  // null for --operation exec
  int event_fd = -1;            // -1 for exec; the child only writes it
  int log_fd = -1;
  int chunk_index = 0;
  uint64_t offset = 0;
  uint64_t length = 0;
  // Controller fds the child must close. Fork copies the fd table. CLOEXEC
  // does not fire on fork, only on exec. The list is the explicit fix.
  std::vector<int> close_fds;
};

// Never returns. Success and failure both end in _exit, so the child does
// not run the parent's C++ destructors.
[[noreturn]] void worker_entry(const WorkerLaunch& launch);

// Parent side of the start gate. Release-store 1, then FUTEX_WAKE.
void open_start_gate(SharedRegion* shm);

bool self_test(std::string& err);

// -----------------------------------------------------------------------------
// controller.cpp
// -----------------------------------------------------------------------------

// Foreground event loop. Returns 0 on a clean shutdown, 1 if it could not
// even take the lock or build the epoll set.
int run_controller(const std::string& workspace, int slots);

}  // namespace forge

#endif
