#include "os/platform.hpp"

// Linux side of the platform line.
//
//   controller  -->  Poller::wait
//                      epoll
//                       |-- signalfd   SIGCHLD / SIGTERM / SIGINT
//                       |-- timerfd    tick and per-job deadlines
//                       `-- eventfd / fifo / pipes the controller still reads
//
//   launch_job  -->  memfd_create + MAP_SHARED
//   worker      -->  futex, O_DIRECT, sched_setaffinity
//   store       -->  renameat2

#include <fcntl.h>
#include <linux/futex.h>
#include <sched.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/signalfd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/timerfd.h>

#include <dirent.h>

#include <cstring>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <utility>

namespace forge::os {
namespace {

void supervisor_mask(sigset_t* mask) {
  sigemptyset(mask);
  sigaddset(mask, SIGCHLD);
  sigaddset(mask, SIGTERM);
  sigaddset(mask, SIGINT);
}

struct TimerSlot {
  int fd = -1;
  uint64_t token = 0;
};

}  // namespace

struct Poller::Impl {
  UniqueFd ep;
  UniqueFd sig;
  uint64_t signal_token = 0;
  bool signals_watched = false;
  int next_timer = 1;
  std::unordered_map<int, TimerSlot> timers;
  std::unordered_map<uint64_t, int> token_to_timer;
};

Poller::Poller() : impl_(new Impl) {}
Poller::~Poller() { delete impl_; }
Poller::Poller(Poller&& other) noexcept : impl_(other.impl_) { other.impl_ = nullptr; }
Poller& Poller::operator=(Poller&& other) noexcept {
  if (this != &other) {
    delete impl_;
    impl_ = other.impl_;
    other.impl_ = nullptr;
  }
  return *this;
}

bool Poller::open(std::string& err) {
  // Block first. If SIGTERM is not blocked, the default action kills the
  // process and signalfd never observes a pending signal.
  sigset_t mask;
  supervisor_mask(&mask);
  if (sigprocmask(SIG_BLOCK, &mask, nullptr) < 0) {
    err = "sigprocmask: " + errno_string();
    return false;
  }
  // signalfd turns queued signals into a readable fd. The signals must
  // already be blocked, or the default action runs and this fd stays empty.
  impl_->sig.reset(::signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC));
  // epoll_create1 allocates the interest set. EPOLL_CLOEXEC keeps it out of
  // a later execve. fork still duplicates it, so the child closes it by number.
  impl_->ep.reset(::epoll_create1(EPOLL_CLOEXEC));
  if (!impl_->sig || !impl_->ep) {
    err = "epoll/signalfd: " + errno_string();
    return false;
  }
  return true;
}

bool Poller::watch_signals(uint64_t token, std::string& err) {
  return watch_fd(impl_->sig.get(), token, err);
}

bool Poller::watch_fd(int fd, uint64_t token, std::string& err) {
  epoll_event ev {};
  ev.events = EPOLLIN;
  ev.data.u64 = token;
  // epoll_ctl registers an fd. Level-triggered EPOLLIN stays reported until
  // the fd is drained. data.u64 comes back from epoll_wait unchanged.
  if (epoll_ctl(impl_->ep.get(), EPOLL_CTL_ADD, fd, &ev) < 0) {
    err = "epoll_ctl: " + errno_string();
    return false;
  }
  if (fd == impl_->sig.get()) {
    impl_->signal_token = token;
    impl_->signals_watched = true;
  }
  return true;
}

void Poller::unwatch_fd(int fd) {
  if (fd < 0 || !impl_->ep) return;
  epoll_ctl(impl_->ep.get(), EPOLL_CTL_DEL, fd, nullptr);
}

int Poller::watch_timer(uint64_t token, uint64_t first_ms, uint64_t every_ms, std::string& err) {
  // timerfd is a clock the kernel turns into a readable fd. CLOCK_MONOTONIC
  // does not jump when the wall clock is set. A read consumes the expirations.
  UniqueFd tfd(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC));
  if (!tfd) {
    err = "timerfd_create: " + errno_string();
    return -1;
  }
  itimerspec its {};
  its.it_value.tv_sec = static_cast<time_t>(first_ms / 1000);
  its.it_value.tv_nsec = static_cast<long>((first_ms % 1000) * 1000000);
  if (its.it_value.tv_sec == 0 && its.it_value.tv_nsec == 0) its.it_value.tv_nsec = 1;
  if (every_ms > 0) {
    its.it_interval.tv_sec = static_cast<time_t>(every_ms / 1000);
    its.it_interval.tv_nsec = static_cast<long>((every_ms % 1000) * 1000000);
  }
  if (timerfd_settime(tfd.get(), 0, &its, nullptr) < 0) {
    err = "timerfd_settime: " + errno_string();
    return -1;
  }
  if (!watch_fd(tfd.get(), token, err)) return -1;
  const int id = impl_->next_timer++;
  impl_->token_to_timer[token] = id;
  impl_->timers.emplace(id, TimerSlot{tfd.release(), token});
  return id;
}

void Poller::unwatch_timer(int id) {
  auto it = impl_->timers.find(id);
  if (it == impl_->timers.end()) return;
  unwatch_fd(it->second.fd);
  impl_->token_to_timer.erase(it->second.token);
  ::close(it->second.fd);
  impl_->timers.erase(it);
}

int Poller::wait(WaitEvent* out, int cap, std::string& err) {
  epoll_event raw[16];
  const int ncap = cap < 16 ? cap : 16;
  // epoll_wait sleeps until at least one registered fd is readable.
  // Timeout -1 means forever. EINTR means a signal interrupted the sleep.
  const int n = ::epoll_wait(impl_->ep.get(), raw, ncap, -1);
  if (n < 0) {
    if (errno == EINTR) return 0;
    err = "epoll_wait: " + errno_string();
    return -1;
  }
  int produced = 0;
  for (int i = 0; i < n && produced < cap; ++i) {
    const uint64_t token = raw[i].data.u64;
    if (impl_->signals_watched && token == impl_->signal_token) {
      signalfd_siginfo info {};
      if (::read(impl_->sig.get(), &info, sizeof(info)) < 0) continue;
      out[produced].token = token;
      out[produced].signo = static_cast<int>(info.ssi_signo);
      ++produced;
      continue;
    }
    auto timer = impl_->token_to_timer.find(token);
    if (timer != impl_->token_to_timer.end()) {
      auto slot = impl_->timers.find(timer->second);
      if (slot != impl_->timers.end()) {
        uint64_t expirations = 0;
        ::read(slot->second.fd, &expirations, sizeof(expirations));
      }
    }
    out[produced].token = token;
    out[produced].signo = 0;
    ++produced;
  }
  return produced;
}

std::vector<int> Poller::inherited_fds() const {
  std::vector<int> fds;
  if (impl_->ep) fds.push_back(impl_->ep.get());
  if (impl_->sig) fds.push_back(impl_->sig.get());
  for (const auto& [id, slot] : impl_->timers) {
    (void)id;
    if (slot.fd >= 0) fds.push_back(slot.fd);
  }
  return fds;
}

bool create_shared_map(SharedMap& out, std::string& err) {
  // memfd is an inode with no directory entry. MAP_SHARED is required:
  // MAP_PRIVATE would copy-on-write and the parent would read zeros.
  // memfd_create makes an anonymous inode in tmpfs with no path. ftruncate
  // sets its size. The mapping, not the name, is what the child inherits.
  out.fd.reset(::memfd_create("forge-job", MFD_CLOEXEC));
  if (!out.fd || ::ftruncate(out.fd.get(), sizeof(SharedRegion)) < 0) {
    err = "memfd_create: " + errno_string();
    return false;
  }
  void* mapped = ::mmap(nullptr, sizeof(SharedRegion), PROT_READ | PROT_WRITE, MAP_SHARED,
                        out.fd.get(), 0);
  if (mapped == MAP_FAILED) {
    err = "mmap memfd: " + errno_string();
    return false;
  }
  out.ptr = static_cast<SharedRegion*>(mapped);
  std::memset(out.ptr, 0, sizeof(SharedRegion));
  return true;
}

void destroy_shared_map(SharedMap& map) {
  if (map.ptr != nullptr) {
    ::munmap(map.ptr, sizeof(SharedRegion));
    map.ptr = nullptr;
  }
  map.fd.reset();
}

bool create_notify(Notify& out, std::string& err) {
  // One counter fd. The child adds 1, the parent reads the total from epoll.
  // eventfd is a 64-bit counter in the kernel. write adds, read clears.
  // EFD_NONBLOCK makes a read with nothing pending return EAGAIN.
  out.held.reset(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
  if (!out.held) {
    err = "eventfd: " + errno_string();
    return false;
  }
  out.child_write = out.held.get();
  out.child_close = -1;
  return true;
}

bool notify_write(int fd) {
  const uint64_t one = 1;
  return ::write(fd, &one, sizeof(one)) == static_cast<ssize_t>(sizeof(one)) || errno == EAGAIN;
}

bool notify_read(int fd, uint64_t& count) {
  const ssize_t n = ::read(fd, &count, sizeof(count));
  if (n < 0) return errno == EAGAIN;
  return n == static_cast<ssize_t>(sizeof(count));
}

int futex_wait(uint32_t* addr, uint32_t expected, const timespec* timeout) {
  // No FUTEX_PRIVATE_FLAG: that flag is for threads in one process.
  // FUTEX_WAIT sleeps only if *addr is still `expected`. If the parent already
  // stored 1, the kernel returns EAGAIN and the caller loads the word again.
  return static_cast<int>(::syscall(SYS_futex, addr, FUTEX_WAIT, expected, timeout, nullptr, 0));
}

int futex_wake(uint32_t* addr, int nwaiters) {
  return static_cast<int>(::syscall(SYS_futex, addr, FUTEX_WAKE, nwaiters, nullptr, nullptr, 0));
}

void prepare_child_signals() {
  sigset_t mask;
  supervisor_mask(&mask);
  sigprocmask(SIG_UNBLOCK, &mask, nullptr);
}

bool replace_at(int dirfd, const char* from, const char* to, std::string& err) {
  // renameat2 changes one directory entry. Flag 0 is a plain rename: the
  // destination inode is replaced in one operation, so a reader never sees
  // a half-written file. ENOSYS means this kernel has no renameat2.
  if (::renameat2(dirfd, from, dirfd, to, 0) == 0) return true;
  if (errno == ENOSYS && ::renameat(dirfd, from, dirfd, to) == 0) return true;
  err = std::string("renameat2 ") + to + ": " + errno_string();
  return false;
}

void advise_sequential(int fd, uint64_t offset, uint64_t length) {
  ::posix_fadvise(fd, static_cast<off_t>(offset), static_cast<off_t>(length), POSIX_FADV_SEQUENTIAL);
}

void advise_mapping(void* addr, std::size_t length) {
  ::madvise(addr, length, MADV_SEQUENTIAL);
}

int open_readonly(const char* path, bool direct, std::string& err) {
  int flags = O_RDONLY | O_CLOEXEC;
  if (direct) flags |= O_DIRECT;
  const int fd = ::open(path, flags);
  if (fd < 0) {
    err = std::string(direct ? "open O_DIRECT: " : "open input: ") + errno_string();
    if (direct) {
      err += ". tmpfs often returns EINVAL. Forge does not fall back.";
    }
  }
  return fd;
}

int pin_cpu(int cpu, std::string& err) {
  if (cpu >= CPU_SETSIZE) {
    err = "cpu index is out of range";
    return -1;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  // sched_setaffinity restricts this process to the given CPUs. It does not
  // reserve the CPU; other tasks can still be scheduled there.
  if (::sched_setaffinity(0, sizeof(set), &set) < 0) {
    err = "sched_setaffinity: " + errno_string();
    return -1;
  }
  return 0;
}

std::string scheduler_name() {
  const int policy = ::sched_getscheduler(0);
  if (policy == SCHED_OTHER) return "SCHED_OTHER (CFS)";
  return "policy " + std::to_string(policy);
}

void log_vm_snapshot(int log_fd, const char* when) {
  auto dump = [&](const char* path) {
    std::string text;
    std::string err;
    if (!read_text_file(path, text, err)) {
      write_line(log_fd, std::string(when) + " " + path + " unreadable");
      return;
    }
    std::istringstream in(text);
    std::string line;
    while (std::getline(in, line)) {
      if (!line.empty()) write_line(log_fd, std::string(when) + " " + line);
    }
  };
  write_line(log_fd, std::string("--- vm snapshot ") + when + " ---");
  dump("/proc/self/io");
  std::string status;
  std::string err;
  if (read_text_file("/proc/self/status", status, err)) {
    std::istringstream in(status);
    std::string line;
    while (std::getline(in, line)) {
      if (line.rfind("VmRSS:", 0) == 0 || line.rfind("VmSize:", 0) == 0 ||
          line.rfind("VmPeak:", 0) == 0 || line.rfind("voluntary_ctxt_switches:", 0) == 0 ||
          line.rfind("nonvoluntary_ctxt_switches:", 0) == 0) {
        write_line(log_fd, std::string(when) + " " + line);
      }
    }
  }
  std::string stat;
  if (read_text_file("/proc/self/stat", stat, err)) {
    const auto rp = stat.rfind(')');
    if (rp != std::string::npos) {
      std::istringstream in(stat.substr(rp + 1));
      std::string skip;
      uint64_t minflt = 0, majflt = 0;
      for (int i = 0; i < 7; ++i) in >> skip;
      in >> minflt >> skip >> majflt;
      write_line(log_fd, std::string(when) + " minflt=" + std::to_string(minflt) +
                             " majflt=" + std::to_string(majflt));
    }
  }
}

void print_devices(std::ostream& out) {
  DIR* dp = ::opendir("/sys/block");
  if (dp == nullptr) {
    out << "opendir /sys/block: " << errno_string() << "\n";
    return;
  }
  out << "name        sectors     rotational  scheduler\n";
  while (dirent* ent = ::readdir(dp)) {
    if (ent->d_name[0] == '.') continue;
    const std::string name = ent->d_name;
    const std::string base = "/sys/block/" + name;
    auto slurp = [](const std::string& path) {
      std::string text;
      std::string err;
      if (!read_text_file(path, text, err)) return std::string("?");
      while (!text.empty() && (text.back() == '\n' || text.back() == ' ')) text.pop_back();
      return text;
    };
    const std::string kind =
        (name.rfind("loop", 0) == 0 || name.rfind("ram", 0) == 0) ? "virtual" : "disk";
    out << name << "  " << slurp(base + "/size") << "  rot=" << slurp(base + "/queue/rotational")
        << "  " << kind << "  " << slurp(base + "/queue/scheduler") << "\n";
  }
  ::closedir(dp);
}

void print_storage(std::ostream& out, const std::string& workspace) {
  out << "workspace " << workspace << "\n";
  std::string text;
  std::string err;
  std::string best_point, best_type, best_src;
  if (read_text_file("/proc/self/mountinfo", text, err)) {
    std::istringstream in(text);
    std::string line;
    while (std::getline(in, line)) {
      const auto dash = line.find(" - ");
      if (dash == std::string::npos) continue;
      std::istringstream lin(line.substr(0, dash));
      std::string id, parent, majmin, root, point;
      if (!(lin >> id >> parent >> majmin >> root >> point)) continue;
      if (workspace.rfind(point, 0) != 0) continue;
      if (point != "/" && workspace.size() > point.size() && workspace[point.size()] != '/' &&
          workspace != point) {
        continue;
      }
      if (point.size() < best_point.size()) continue;
      std::istringstream rin(line.substr(dash + 3));
      std::string fstype, src;
      if (!(rin >> fstype >> src)) continue;
      best_point = point;
      best_type = fstype;
      best_src = src;
    }
  }
  if (!best_type.empty()) {
    out << "filesystem " << best_type << " on " << best_point << " from " << best_src << "\n";
  }
  out << "\npage cache, from /proc/meminfo\n";
  out << "  Cached    file pages resident in the page cache\n";
  out << "  Dirty     cache pages waiting for writeback\n";
  out << "  Writeback cache pages being written now\n";
  out << "  Mapped    pages mapped with mmap\n";
  out << "  Shmem     shared memory, including memfd and tmpfs\n";
  if (read_text_file("/proc/meminfo", text, err)) {
    std::istringstream in(text);
    std::string line;
    while (std::getline(in, line)) {
      const char* keys[] = {"MemTotal:", "MemAvailable:", "Buffers:", "Cached:", "Dirty:",
                            "Writeback:", "Mapped:", "Shmem:", "Slab:"};
      for (const char* k : keys) {
        if (line.rfind(k, 0) == 0) out << "  " << line << "\n";
      }
    }
  }
  out << "\n/proc/diskstats\n";
  if (!read_text_file("/proc/diskstats", text, err)) return;
  std::istringstream in(text);
  std::string line;
  while (std::getline(in, line)) {
    std::istringstream lin(line);
    int major = 0, minor = 0;
    std::string name;
    uint64_t rd = 0, rd_merged = 0, rd_sec = 0, rd_ms = 0;
    uint64_t wr = 0, wr_merged = 0, wr_sec = 0, wr_ms = 0;
    if (!(lin >> major >> minor >> name >> rd >> rd_merged >> rd_sec >> rd_ms >> wr >> wr_merged >>
          wr_sec >> wr_ms)) {
      continue;
    }
    if (name.rfind("loop", 0) == 0) continue;
    out << "  " << name << "  reads " << rd << "  sectors " << rd_sec << "  writes " << wr
        << "  sectors " << wr_sec << "\n";
  }
}

}  // namespace forge::os

namespace forge {

bool inspect_file(const std::string& path, FileInfo& info, std::string& err) {
  struct statx stx {};
  if (::statx(AT_FDCWD, path.c_str(), 0, STATX_BASIC_STATS | STATX_BTIME, &stx) == 0) {
    info.size = stx.stx_size;
    info.regular = S_ISREG(stx.stx_mode);
    info.blocks_512 = stx.stx_blocks;
    if ((stx.stx_mask & STATX_BTIME) != 0) {
      info.have_btime = true;
      info.btime_sec = static_cast<int64_t>(stx.stx_btime.tv_sec);
    }
    return true;
  }
  if (errno != ENOSYS && errno != ENOTSUP) {
    err = "statx " + path + ": " + errno_string();
    return false;
  }
  struct stat st {};
  if (::stat(path.c_str(), &st) < 0) {
    err = "stat " + path + ": " + errno_string();
    return false;
  }
  info.size = static_cast<uint64_t>(st.st_size);
  info.regular = S_ISREG(st.st_mode);
  info.blocks_512 = static_cast<uint64_t>(st.st_blocks);
  return true;
}

bool proc_starttime(int pid, uint64_t& ticks) {
  std::string text;
  std::string err;
  if (!read_text_file("/proc/" + std::to_string(pid) + "/stat", text, err)) return false;
  const auto rparen = text.rfind(')');
  if (rparen == std::string::npos) return false;
  std::istringstream in(text.substr(rparen + 1));
  std::string skip;
  for (int i = 0; i < 19; ++i) {
    if (!(in >> skip)) return false;
  }
  return static_cast<bool>(in >> ticks);
}

bool pid_looks_like_forge(int pid) {
  if (pid <= 0 || ::kill(pid, 0) < 0) return false;
  std::string text;
  std::string err;
  if (!read_text_file("/proc/" + std::to_string(pid) + "/cmdline", text, err)) return false;
  for (char& ch : text) {
    if (ch == '\0') ch = ' ';
  }
  return text.find("forge") != std::string::npos;
}

}  // namespace forge
