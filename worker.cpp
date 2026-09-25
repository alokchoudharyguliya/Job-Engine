#include "forge.hpp"
#include "os/platform.hpp"

// worker.cpp runs in the child after fork, except for open_start_gate and
// self_test, which the parent calls.
//
//   controller.cpp                    this file
//   fork ----------------------------> worker_entry
//   open_start_gate --futex---------> wait_for_start_gate
//                                     feed_range (mmap / read / O_DIRECT)
//                                     eventfd_write
//                                     _exit
//
// The input file is not copied into the shared region. mmap of a file is a
// view of the page cache. The memfd region is only the small result slot.

#include <fcntl.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <climits>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <vector>

extern char** environ;

namespace forge {
namespace {

void log_fd_line(int fd, std::string_view line) {
  if (fd >= 0) {
    write_line(fd, line);
  }
}

// FIPS 180-4 SHA-256. Straight implementation so a job's digest can be
// checked with sha256sum. Not a performance exercise.
class Sha256 {
 public:
  Sha256() { reset(); }

  void update(const uint8_t* data, size_t len) {
    bits_ += static_cast<uint64_t>(len) * 8;
    while (len > 0) {
      const size_t room = 64 - buf_len_;
      const size_t take = len < room ? len : room;
      std::memcpy(buf_ + buf_len_, data, take);
      buf_len_ += take;
      data += take;
      len -= take;
      if (buf_len_ == 64) {
        transform(buf_);
        buf_len_ = 0;
      }
    }
  }

  void final(uint8_t out[32]) {
    // Snapshot the bit length first. Padding bytes must not be counted
    // as part of the message.
    const uint64_t bit_len = bits_;
    uint8_t one = 0x80;
    update_raw(&one, 1);
    uint8_t zero = 0;
    while (buf_len_ != 56) {
      update_raw(&zero, 1);
    }
    uint8_t lenb[8];
    for (int i = 0; i < 8; ++i) {
      lenb[7 - i] = static_cast<uint8_t>((bit_len >> (8 * i)) & 0xff);
    }
    update_raw(lenb, 8);
    for (int i = 0; i < 8; ++i) {
      out[i * 4 + 0] = static_cast<uint8_t>((h_[i] >> 24) & 0xff);
      out[i * 4 + 1] = static_cast<uint8_t>((h_[i] >> 16) & 0xff);
      out[i * 4 + 2] = static_cast<uint8_t>((h_[i] >> 8) & 0xff);
      out[i * 4 + 3] = static_cast<uint8_t>(h_[i] & 0xff);
    }
  }

  static std::string hex_of(const uint8_t* data, size_t len) {
    Sha256 h;
    h.update(data, len);
    uint8_t dig[32];
    h.final(dig);
    return to_hex(dig);
  }

 private:
  void reset() {
    h_[0] = 0x6a09e667u;
    h_[1] = 0xbb67ae85u;
    h_[2] = 0x3c6ef372u;
    h_[3] = 0xa54ff53au;
    h_[4] = 0x510e527fu;
    h_[5] = 0x9b05688cu;
    h_[6] = 0x1f83d9abu;
    h_[7] = 0x5be0cd19u;
    bits_ = 0;
    buf_len_ = 0;
  }

  // Padding path: do not add to bits_.
  void update_raw(const uint8_t* data, size_t len) {
    while (len > 0) {
      const size_t room = 64 - buf_len_;
      const size_t take = len < room ? len : room;
      std::memcpy(buf_ + buf_len_, data, take);
      buf_len_ += take;
      data += take;
      len -= take;
      if (buf_len_ == 64) {
        transform(buf_);
        buf_len_ = 0;
      }
    }
  }

  static uint32_t rotr(uint32_t x, uint32_t n) {
    return (x >> n) | (x << (32 - n));
  }

  static uint32_t load_be(const uint8_t* p) {
    return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) |
           (uint32_t(p[2]) << 8) | uint32_t(p[3]);
  }

  void transform(const uint8_t block[64]) {
    static const uint32_t K[64] = {
        0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu,
        0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u, 0xd807aa98u, 0x12835b01u,
        0x243185beu, 0x550c7dc3u, 0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u,
        0xc19bf174u, 0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu,
        0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau, 0x983e5152u,
        0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u,
        0x06ca6351u, 0x14292967u, 0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu,
        0x53380d13u, 0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
        0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u, 0xd192e819u,
        0xd6990624u, 0xf40e3585u, 0x106aa070u, 0x19a4c116u, 0x1e376c08u,
        0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu,
        0x682e6ff3u, 0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
        0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u};
    uint32_t w[64];
    for (int i = 0; i < 16; ++i) {
      w[i] = load_be(block + i * 4);
    }
    for (int i = 16; i < 64; ++i) {
      const uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
      const uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
      w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3];
    uint32_t e = h_[4], f = h_[5], g = h_[6], h = h_[7];
    for (int i = 0; i < 64; ++i) {
      const uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
      const uint32_t ch = (e & f) ^ ((~e) & g);
      const uint32_t temp1 = h + S1 + ch + K[i] + w[i];
      const uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
      const uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
      const uint32_t temp2 = S0 + maj;
      h = g;
      g = f;
      f = e;
      e = d + temp1;
      d = c;
      c = b;
      b = a;
      a = temp1 + temp2;
    }
    h_[0] += a;
    h_[1] += b;
    h_[2] += c;
    h_[3] += d;
    h_[4] += e;
    h_[5] += f;
    h_[6] += g;
    h_[7] += h;
  }

  static std::string to_hex(const uint8_t dig[32]) {
    static const char* kHex = "0123456789abcdef";
    std::string out(64, '0');
    for (int i = 0; i < 32; ++i) {
      out[i * 2] = kHex[dig[i] >> 4];
      out[i * 2 + 1] = kHex[dig[i] & 0xf];
    }
    return out;
  }

  uint32_t h_[8]{};
  uint64_t bits_ = 0;
  uint8_t buf_[64]{};
  size_t buf_len_ = 0;
};

using Sink = bool (*)(void* ctx, const uint8_t* data, size_t n, std::string& err);

bool feed_range(const std::string& io, const std::string& path, uint64_t offset,
                uint64_t length, Sink sink, void* ctx, int log_fd, std::string& err) {
  // Three ways to read the same bytes. The operation (hash, count, copy)
  // doesn't change; only the path into the kernel does.
  //
  //   mmap   the file's page-cache pages become part of this address space.
  //          The first touch of each page is a fault. A major fault reads
  //          from storage; a minor fault finds the page already cached.
  //   read   pread copies from the page cache into our buffer. The cache is
  //          still there; we just don't map it.
  //   direct O_DIRECT skips the page cache. The buffer, offset, and length
  //          have to be multiples of 4096. The tail of the file is read
  //          normally and the log says so — we do not pretend it was direct.
  if (length == 0) {
    log_fd_line(log_fd, "io range empty");
    return true;
  }

  if (io == "mmap") {
    // open returns a file descriptor: an index in this process's fd table.
    // O_CLOEXEC marks that index so a later execve drops it. The inode and
    // the page cache live behind the descriptor, not in the integer itself.
    UniqueFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!fd) {
      err = "open input: " + errno_string();
      return false;
    }
    // Hint on the file (fadvise) and on the mapping (madvise). Both are
    // hints. The kernel may ignore them. WILLNEED would prefetch; it can
    // also stampede the disk, so the default is SEQUENTIAL.
    os::advise_sequential(fd.get(), offset, length);
    const long page = ::sysconf(_SC_PAGESIZE);
    const uint64_t page_u = page > 0 ? static_cast<uint64_t>(page) : 4096;
    // mmap offset must be page-aligned even when the chunk is not.
    //
    //   file:  [........|chunk|........]
    //                 ^page        ^page
    //   map:         [##############]
    //   hash starts at (offset - map_off) inside that mapping
    const uint64_t map_off = offset & ~(page_u - 1);
    const uint64_t prefix = offset - map_off;
    const uint64_t map_len = prefix + length;
    // mmap installs a VMA. PROT_READ is the only permission. MAP_PRIVATE means
    // a write would copy the page; we never write, so the pages stay the
    // page-cache pages of this file. The first touch of each page faults.
    void* mapped = ::mmap(nullptr, static_cast<size_t>(map_len), PROT_READ,
                          MAP_PRIVATE, fd.get(), static_cast<off_t>(map_off));
    if (mapped == MAP_FAILED) {
      err = "mmap input: " + errno_string() +
            ". RLIMIT_AS includes this mapping, so a small --mem-mb on a large file fails here.";
      return false;
    }
    os::advise_mapping(mapped, static_cast<size_t>(map_len));
    const bool ok = sink(ctx, static_cast<uint8_t*>(mapped) + prefix,
                         static_cast<size_t>(length), err);
    // munmap removes the VMA. The page-cache pages stay; only this mapping goes.
    ::munmap(mapped, static_cast<size_t>(map_len));
    return ok;
  }

  if (io == "direct") {
    if ((offset % kDirectAlign) != 0) {
      err = "O_DIRECT range is not 4096-aligned (offset " + std::to_string(offset) + ")";
      return false;
    }
    UniqueFd direct(os::open_readonly(path.c_str(), true, err));
    if (!direct) return false;
    // Second open file description of the same inode, without O_DIRECT, for
    // the unaligned tail. Two fds, one inode, different status flags: the
    // page cache is a property of how you open the file, not of the inode.
    UniqueFd cached(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!cached) {
      err = "open input tail: " + errno_string();
      return false;
    }
    const uint64_t aligned = length - (length % kDirectAlign);
    if (aligned == 0) {
      log_fd_line(log_fd, "range shorter than 4096; O_DIRECT skipped for this slice");
    }
    void* mem = nullptr;
    if (aligned > 0 && ::posix_memalign(&mem, kDirectAlign, 1 << 20) != 0) {
      err = "posix_memalign failed";
      return false;
    }
    uint64_t done = 0;
    bool ok = true;
    while (done < aligned) {
      const size_t want = static_cast<size_t>(
          std::min<uint64_t>(1 << 20, aligned - done));
      size_t got = 0;
      auto* buf = static_cast<uint8_t*>(mem);
      while (got < want) {
        const ssize_t r = ::pread(direct.get(), buf + got, want - got,
                                  static_cast<off_t>(offset + done + got));
        if (r < 0) {
          if (errno == EINTR) continue;
          err = "pread O_DIRECT: " + errno_string();
          ok = false;
          break;
        }
        if (r == 0) {
          err = "unexpected EOF during O_DIRECT read";
          ok = false;
          break;
        }
        got += static_cast<size_t>(r);
      }
      if (!ok) break;
      if (!sink(ctx, buf, want, err)) {
        ok = false;
        break;
      }
      done += want;
    }
    std::free(mem);
    if (!ok) return false;
    if (aligned < length) {
      const uint64_t tail = length - aligned;
      log_fd_line(log_fd, "tail " + std::to_string(tail) +
                              " bytes read via the page cache (O_DIRECT length must be a multiple of 4096)");
      std::vector<uint8_t> buf(static_cast<size_t>(tail));
      size_t got = 0;
      while (got < buf.size()) {
        const ssize_t r = ::pread(cached.get(), buf.data() + got, buf.size() - got,
                                  static_cast<off_t>(offset + aligned + got));
        if (r < 0) {
          if (errno == EINTR) continue;
          err = "pread tail: " + errno_string();
          return false;
        }
        if (r == 0) {
          err = "unexpected EOF in tail";
          return false;
        }
        got += static_cast<size_t>(r);
      }
      if (!sink(ctx, buf.data(), buf.size(), err)) return false;
    }
    return true;
  }

  // Default buffered path: "read".
  UniqueFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (!fd) {
    err = "open input: " + errno_string();
    return false;
  }
  os::advise_sequential(fd.get(), offset, length);
  std::vector<uint8_t> buf(1 << 20);
  uint64_t left = length;
  uint64_t at = offset;
  while (left > 0) {
    const size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), left));
    // pread reads at an absolute offset. It does not move the fd's cursor,
    // and the bytes come from the page cache unless the fd was opened direct.
    const ssize_t r = ::pread(fd.get(), buf.data(), want, static_cast<off_t>(at));
    if (r < 0) {
      if (errno == EINTR) continue;
      err = "pread: " + errno_string();
      return false;
    }
    if (r == 0) {
      err = "unexpected EOF";
      return false;
    }
    if (!sink(ctx, buf.data(), static_cast<size_t>(r), err)) return false;
    at += static_cast<uint64_t>(r);
    left -= static_cast<uint64_t>(r);
  }
  return true;
}

struct HashCtx {
  Sha256 hash;
};

bool hash_sink(void* ctx, const uint8_t* data, size_t n, std::string&) {
  static_cast<HashCtx*>(ctx)->hash.update(data, n);
  return true;
}

struct CountCtx {
  uint64_t bytes = 0;
  uint64_t sum = 0;
};

bool count_sink(void* ctx, const uint8_t* data, size_t n, std::string&) {
  // Touch every byte. A count that only used `n` would not fault a single
  // mmap page, and the mmap-vs-read comparison would be a lie.
  auto* c = static_cast<CountCtx*>(ctx);
  uint64_t sum = c->sum;
  for (size_t i = 0; i < n; ++i) {
    sum += data[i];
  }
  c->sum = sum;
  c->bytes += n;
  return true;
}

struct CopyCtx {
  int fd = -1;
  uint64_t bytes = 0;
};

bool copy_sink(void* ctx, const uint8_t* data, size_t n, std::string& err) {
  auto* c = static_cast<CopyCtx*>(ctx);
  if (!write_all(c->fd, data, n)) {
    err = "write output: " + errno_string();
    return false;
  }
  c->bytes += n;
  return true;
}

void apply_one_limit(int resource, uint64_t value, const char* name, int log_fd,
                     std::string& err, bool& ok) {
  if (value == 0) return;
  rlimit rl {};
  rl.rlim_cur = static_cast<rlim_t>(value);
  rl.rlim_max = static_cast<rlim_t>(value);
  // setrlimit installs a rlimit on this process. The child calls it before
  // work or execve, so the new image inherits the ceiling. RLIMIT_AS counts
  // every mapping, including the input mmap.
  if (::setrlimit(resource, &rl) < 0) {
    err = std::string("setrlimit ") + name + ": " + errno_string();
    ok = false;
    log_fd_line(log_fd, err);
  } else {
    log_fd_line(log_fd, std::string("limit ") + name + "=" + std::to_string(value));
  }
}

bool apply_resource_limits(const Meta& meta, int log_fd, std::string& err) {
  // Applied in the child, before work or execve, so the new image inherits
  // them. The parent could instead call prlimit(pid) after fork; doing it
  // here keeps the jail next to the code it constrains.
  // RLIMIT_AS covers every mapping, including mmap of the input. A 4GB cap
  // on a 20GB mmap fails at mmap time. That is the limit working.
  bool ok = true;
  if (meta.mem_mb > 0) {
    apply_one_limit(RLIMIT_AS, meta.mem_mb * 1024ull * 1024ull, "RLIMIT_AS", log_fd, err, ok);
  }
  if (meta.cpu_sec > 0) {
    apply_one_limit(RLIMIT_CPU, meta.cpu_sec, "RLIMIT_CPU", log_fd, err, ok);
  }
  if (meta.max_fds > 0) {
    apply_one_limit(RLIMIT_NOFILE, meta.max_fds, "RLIMIT_NOFILE", log_fd, err, ok);
  }
  if (meta.max_out_mb > 0) {
    apply_one_limit(RLIMIT_FSIZE, meta.max_out_mb * 1024ull * 1024ull, "RLIMIT_FSIZE", log_fd, err, ok);
  }
  return ok;
}

bool apply_scheduler_hints(const Meta& meta, int log_fd, std::string& err) {
  log_fd_line(log_fd, "scheduler " + os::scheduler_name());
  if (meta.cpu >= 0) {
    const int pinned = os::pin_cpu(meta.cpu, err);
    if (pinned < 0) return false;
    if (pinned > 0) log_fd_line(log_fd, err);
    else log_fd_line(log_fd, "affinity cpu " + std::to_string(meta.cpu));
  }
  if (meta.nice != 0) {
    // Larger nice => less CPU. Opposite sign from Forge's --priority.
    // Negative nice needs privilege; we warn and continue, because the job
    // is still valid without the hint. A failed affinity is fatal: the user
    // asked for a placement and did not get it.
    // setpriority writes the nice value the scheduler uses as a weight.
    // Larger nice means less CPU. It is not Forge's --priority, which only
    // orders the user-space queue. Negative nice needs privilege (EPERM).
    if (::setpriority(PRIO_PROCESS, 0, meta.nice) < 0) {
      log_fd_line(log_fd, "setpriority(" + std::to_string(meta.nice) +
                              ") failed: " + errno_string() +
                              ". Negative nice needs privileges; continuing.");
    } else {
      log_fd_line(log_fd, "nice " + std::to_string(meta.nice));
    }
  }
  return true;
}

bool wait_for_start_gate(SharedRegion* shm, int log_fd, std::string& err) {
  // Same word the parent wakes. Loop because a wake can be spurious and
  // because the parent may store 1 before we sleep:
  //
  //   child loads 0
  //   parent stores 1 and FUTEX_WAKE (nobody is asleep yet)
  //   child FUTEX_WAIT returns EAGAIN, because the word is no longer 0
  //   child loads 1 and continues
  std::atomic_ref<uint32_t> gate(shm->start_gate);
  for (;;) {
    if (gate.load(std::memory_order_acquire) == 1) {
      return true;
    }
    timespec ts {};
    ts.tv_sec = 30;
    const int rc = os::futex_wait(&shm->start_gate, 0, &ts);
    if (rc == 0) continue;
    if (errno == EAGAIN || errno == EINTR) continue;
    if (errno == ETIMEDOUT) {
      err = "start gate was not opened within 30s";
      log_fd_line(log_fd, err);
      return false;
    }
    err = "futex wait: " + errno_string();
    log_fd_line(log_fd, err);
    return false;
  }
}

void publish_chunk(SharedRegion* shm, const ChunkResult& chunk, int event_fd) {
  // Slot i has one writer. No lock. The release increment is the publish:
  // everything written to the slot happens-before an acquire load in the
  // parent that observes this new count.
  shm->chunks[chunk.index] = chunk;
  std::atomic_ref<uint32_t>(shm->chunks_done)
      .fetch_add(1, std::memory_order_release);
  if (event_fd >= 0) {
    const uint64_t one = 1;
    // eventfd is the wake. The parent is in epoll_wait, not polling.
    // write to the wake fd. On Linux this is eventfd and the kernel adds the
    // 8-byte value into a counter. On macOS it is a pipe; 8 bytes still wake
    // kqueue. The parent is blocked in the poller, not spinning.
    if (::write(event_fd, &one, sizeof(one)) < 0 && errno != EAGAIN) {
      // The parent still has SIGCHLD. Losing the event is logged by a
      // missing slot if we also failed to publish, which we didn't.
    }
  }
}

[[noreturn]] void fail_chunk(SharedRegion* shm, const WorkerLaunch& launch,
                             const std::string& err) {
  log_fd_line(launch.log_fd, err);
  if (shm != nullptr && launch.chunk_index >= 0 &&
      launch.chunk_index < kMaxWorkers) {
    ChunkResult chunk {};
    chunk.index = static_cast<uint32_t>(launch.chunk_index);
    chunk.ok = 0;
    chunk.offset = launch.offset;
    chunk.length = launch.length;
    std::snprintf(chunk.error, sizeof(chunk.error), "%s", err.c_str());
    publish_chunk(shm, chunk, launch.event_fd);
  }
  _exit(1);
}

void redirect_stdio(int log_fd) {
  // The child must not write the controller's terminal, and must not read
  // the user's keyboard. stdout/stderr become the job log. stdin is
  // /dev/null. dup2 does not copy FD_CLOEXEC onto the destination.
  if (log_fd >= 0) {
    ::dup2(log_fd, STDOUT_FILENO);
    ::dup2(log_fd, STDERR_FILENO);
  }
  UniqueFd devnull(::open("/dev/null", O_RDONLY | O_CLOEXEC));
  if (devnull) {
    ::dup2(devnull.get(), STDIN_FILENO);
  }
}

[[noreturn]] void run_exec(const WorkerLaunch& launch) {
  // execve replaces this process image. The limits, affinity, and stdio
  // redirects above survive. The parent's epoll fds are CLOEXEC and were
  // also closed explicitly, so the program we launch does not inherit the
  // controller's event loop.
  const std::string out = launch.job_dir + "/output";
  const char* argv[] = {launch.meta.exec_path.c_str(), launch.meta.input.c_str(),
                        out.c_str(), nullptr};
  // execve replaces this process image. The pid stays. Limits, the process
  // group, and the redirected stdout survive. CLOEXEC fds do not.
  ::execve(launch.meta.exec_path.c_str(), const_cast<char**>(argv), environ);
  const std::string err = "execve " + launch.meta.exec_path + ": " + errno_string();
  log_fd_line(launch.log_fd, err);
  _exit(127);
}

}  // namespace

void open_start_gate(SharedRegion* shm) {
  if (shm == nullptr) return;
  std::atomic_ref<uint32_t>(shm->start_gate).store(1, std::memory_order_release);
  os::futex_wake(&shm->start_gate, INT_MAX);
}

[[noreturn]] void worker_entry(const WorkerLaunch& launch) {
  // _exit, not exit. exit() flushes stdio buffers duplicated by fork (the
  // parent can print the same line twice) and runs static destructors.
  // Those destructors close the child's copy of controller fds. Combined
  // with flock living on the open file description, that is a bad place to
  // be clever. The kernel closes our fds when we _exit.
  for (int fd : launch.close_fds) {
    if (fd >= 0 && fd != launch.log_fd && fd != launch.event_fd) {
      ::close(fd);
    }
  }
  os::prepare_child_signals();
  // Own process group, so cancel/deadline can signal the worker and any
  // grandchild an exec'd shell starts. Both parent and child call setpgid;
  // whoever runs first wins, the other gets a harmless EACCES.
  // setpgid(0, 0) makes this process the leader of a new process group.
  // The parent does the same call with the child's pid. Whoever runs first wins.
  ::setpgid(0, 0);
  redirect_stdio(launch.log_fd);

  std::string err;
  if (!apply_resource_limits(launch.meta, launch.log_fd, err) ||
      !apply_scheduler_hints(launch.meta, launch.log_fd, err)) {
    if (launch.shm != nullptr) {
      fail_chunk(launch.shm, launch, err.empty() ? "limits/scheduler setup failed" : err);
    }
    log_fd_line(launch.log_fd, err);
    _exit(1);
  }

  if (launch.meta.op == "exec") {
    log_fd_line(launch.log_fd, "exec " + launch.meta.exec_path);
    run_exec(launch);
  }

  SharedRegion* shm = launch.shm;
  if (shm == nullptr || shm->magic != kShmMagic) {
    log_fd_line(launch.log_fd, "shared region missing or bad magic");
    _exit(2);
  }
  if (!wait_for_start_gate(shm, launch.log_fd, err)) {
    fail_chunk(shm, launch, err);
  }

  log_fd_line(launch.log_fd, "chunk " + std::to_string(launch.chunk_index) + " offset " +
                                 std::to_string(launch.offset) + " length " +
                                 std::to_string(launch.length) + " io " + launch.meta.io);
  os::log_vm_snapshot(launch.log_fd, "before");

  ChunkResult chunk {};
  chunk.index = static_cast<uint32_t>(launch.chunk_index);
  chunk.offset = launch.offset;
  chunk.length = launch.length;
  chunk.ok = 1;

  if (launch.meta.op == "sha256" || launch.meta.op == "chunk-sha256") {
    HashCtx ctx;
    if (launch.length > 0 &&
        !feed_range(launch.meta.io, launch.meta.input, launch.offset, launch.length,
                    hash_sink, &ctx, launch.log_fd, err)) {
      fail_chunk(shm, launch, err);
    }
    uint8_t dig[32];
    ctx.hash.final(dig);
    static const char* kHex = "0123456789abcdef";
    for (int i = 0; i < 32; ++i) {
      chunk.hex[i * 2] = kHex[dig[i] >> 4];
      chunk.hex[i * 2 + 1] = kHex[dig[i] & 0xf];
    }
    chunk.hex[64] = '\0';
    chunk.bytes = launch.length;
  } else if (launch.meta.op == "count") {
    CountCtx ctx;
    if (launch.length > 0 &&
        !feed_range(launch.meta.io, launch.meta.input, launch.offset, launch.length,
                    count_sink, &ctx, launch.log_fd, err)) {
      fail_chunk(shm, launch, err);
    }
    chunk.bytes = ctx.bytes;
    chunk.aux = ctx.sum;
    if (ctx.bytes != launch.length) {
      fail_chunk(shm, launch, "count shortfall: got " + std::to_string(ctx.bytes) +
                                  " expected " + std::to_string(launch.length));
    }
  } else if (launch.meta.op == "copy") {
    UniqueFd dirfd(::open(launch.job_dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (!dirfd) {
      fail_chunk(shm, launch, "open job dir: " + errno_string());
    }
    const std::string tmp =
        "output.tmp." + std::to_string(::getpid());
    UniqueFd out(::openat(dirfd.get(), tmp.c_str(),
                          O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644));
    if (!out) {
      fail_chunk(shm, launch, "open output temp: " + errno_string());
    }
    CopyCtx ctx;
    ctx.fd = out.get();
    if (launch.length > 0 &&
        !feed_range(launch.meta.io, launch.meta.input, launch.offset, launch.length,
                    copy_sink, &ctx, launch.log_fd, err)) {
      ::unlinkat(dirfd.get(), tmp.c_str(), 0);
      fail_chunk(shm, launch, err);
    }
    if (!publish_file(dirfd.get(), out.get(), tmp.c_str(), "output", err)) {
      ::unlinkat(dirfd.get(), tmp.c_str(), 0);
      fail_chunk(shm, launch, err);
    }
    chunk.bytes = ctx.bytes;
  } else {
    fail_chunk(shm, launch, "worker does not implement " + launch.meta.op);
  }

  os::log_vm_snapshot(launch.log_fd, "after");
  log_fd_line(launch.log_fd, "chunk " + std::to_string(launch.chunk_index) + " done bytes " +
                                 std::to_string(chunk.bytes));
  publish_chunk(shm, chunk, launch.event_fd);
  _exit(0);
}

bool self_test(std::string& err) {
  const std::string empty = Sha256::hex_of(nullptr, 0);
  if (empty != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") {
    err = "sha256(\"\") mismatch: " + empty;
    return false;
  }
  const char abc[] = "abc";
  const std::string abc_hex = Sha256::hex_of(reinterpret_cast<const uint8_t*>(abc), 3);
  if (abc_hex != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad") {
    err = "sha256(\"abc\") mismatch: " + abc_hex;
    return false;
  }
  auto ranges = split_ranges(10, 3, 1);
  if (ranges.size() != 3 || ranges[0].length != 4 || ranges[1].length != 3 ||
      ranges[2].length != 3) {
    err = "split_ranges byte split failed";
    return false;
  }
  auto aligned = split_ranges(8202, 4, 4096);
  uint64_t sum = 0;
  uint64_t prev = 0;
  for (size_t i = 0; i < aligned.size(); ++i) {
    if (aligned[i].offset != prev) {
      err = "aligned split is not contiguous";
      return false;
    }
    prev = aligned[i].offset + aligned[i].length;
    sum += aligned[i].length;
  }
  if (sum != 8202) {
    err = "aligned split length mismatch";
    return false;
  }
  return true;
}

}  // namespace forge
