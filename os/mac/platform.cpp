#include "os/platform.hpp"

// macOS side of the same platform line.
//
//   Linux                         macOS
//   epoll + signalfd + timerfd    one kqueue (EVFILT_READ / SIGNAL / TIMER)
//   eventfd                       a pipe
//   memfd + MAP_SHARED            MAP_ANON | MAP_SHARED (inherited by fork)
//   futex                         os_sync_wait_on_address, shared flag
//   O_DIRECT                      fcntl F_NOCACHE
//   sched_setaffinity             not available; pin_cpu reports that
//   /proc and /sys                libproc, statfs, sysctl, getmntinfo
//
// The controller still waits on one object and still forks workers.

#include <fcntl.h>
#include <sys/event.h>
#include <libproc.h>
#include <signal.h>
#include <sys/mount.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/types.h>

#include <mach/mach.h>

#include <os/os_sync_wait_on_address.h>

#include <cstring>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

namespace forge::os {
namespace {

struct TimerSlot {
  uint64_t token = 0;
};

void empty_signal(int) {}

}  // namespace

struct Poller::Impl {
  int kq = -1;
  uint64_t signal_token = 0;
  bool signals_watched = false;
  int next_timer = 1;
  std::unordered_map<int, TimerSlot> timers;
};

Poller::Poller() : impl_(new Impl) {}
Poller::~Poller() {
  if (impl_ != nullptr && impl_->kq >= 0) ::close(impl_->kq);
  delete impl_;
}
Poller::Poller(Poller&& other) noexcept : impl_(other.impl_) { other.impl_ = nullptr; }
Poller& Poller::operator=(Poller&& other) noexcept {
  if (this != &other) {
    if (impl_ != nullptr && impl_->kq >= 0) ::close(impl_->kq);
    delete impl_;
    impl_ = other.impl_;
    other.impl_ = nullptr;
  }
  return *this;
}

bool Poller::open(std::string& err) {
  // kqueue delivers EVFILT_SIGNAL and the process also receives the signal.
  // An empty handler keeps SIGTERM from killing the controller. SIG_IGN on
  // SIGCHLD would reap children automatically and waitid would see nothing.
  struct sigaction sa {};
  sa.sa_handler = empty_signal;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  if (sigaction(SIGCHLD, &sa, nullptr) < 0 || sigaction(SIGTERM, &sa, nullptr) < 0 ||
      sigaction(SIGINT, &sa, nullptr) < 0) {
    err = "sigaction: " + errno_string();
    return false;
  }
  // kqueue creates one kernel queue. Later kevent calls register filters
  // (readable fd, signal, timer) and also sleep until one of them fires.
  // This is the macOS object that epoll_wait is on Linux.
  impl_->kq = ::kqueue();
  if (impl_->kq < 0) {
    err = "kqueue: " + errno_string();
    return false;
  }
  return true;
}

bool Poller::watch_signals(uint64_t token, std::string& err) {
  impl_->signal_token = token;
  const int sigs[] = {SIGCHLD, SIGTERM, SIGINT};
  for (int sig : sigs) {
    struct kevent ev {};
    EV_SET(&ev, static_cast<uintptr_t>(sig), EVFILT_SIGNAL, EV_ADD, 0, 0,
           reinterpret_cast<void*>(static_cast<uintptr_t>(token)));
    // kevent with a non-NULL changelist registers a filter. EVFILT_SIGNAL
    // fires when that signal is delivered. ident is the signal number.
    // udata is our token, returned unchanged when the filter fires.
    if (::kevent(impl_->kq, &ev, 1, nullptr, 0, nullptr) < 0) {
      err = "kevent signal: " + errno_string();
      return false;
    }
  }
  impl_->signals_watched = true;
  return true;
}

bool Poller::watch_fd(int fd, uint64_t token, std::string& err) {
  struct kevent ev {};
  EV_SET(&ev, static_cast<uintptr_t>(fd), EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0,
         reinterpret_cast<void*>(static_cast<uintptr_t>(token)));
  if (::kevent(impl_->kq, &ev, 1, nullptr, 0, nullptr) < 0) {
    err = "kevent add fd: " + errno_string();
    return false;
  }
  return true;
}

void Poller::unwatch_fd(int fd) {
  if (impl_->kq < 0 || fd < 0) return;
  struct kevent ev {};
  EV_SET(&ev, static_cast<uintptr_t>(fd), EVFILT_READ, EV_DELETE, 0, 0, nullptr);
  ::kevent(impl_->kq, &ev, 1, nullptr, 0, nullptr);
}

int Poller::watch_timer(uint64_t token, uint64_t first_ms, uint64_t every_ms, std::string& err) {
  // EVFILT_TIMER has no separate "first" and "interval". The tick uses the
  // same period for both. A deadline is EV_ONESHOT.
  const int id = impl_->next_timer++;
  const uint64_t ms = first_ms == 0 ? 1 : first_ms;
  const uint16_t flags = EV_ADD | (every_ms == 0 ? EV_ONESHOT : 0);
  struct kevent ev {};
  EV_SET(&ev, static_cast<uintptr_t>(id), EVFILT_TIMER, flags, NOTE_USECONDS,
         static_cast<intptr_t>(ms * 1000),
         reinterpret_cast<void*>(static_cast<uintptr_t>(token)));
  if (::kevent(impl_->kq, &ev, 1, nullptr, 0, nullptr) < 0) {
    err = "kevent timer: " + errno_string();
    return -1;
  }
  impl_->timers.emplace(id, TimerSlot{token});
  return id;
}

void Poller::unwatch_timer(int id) {
  auto it = impl_->timers.find(id);
  if (it == impl_->timers.end() || impl_->kq < 0) return;
  struct kevent ev {};
  EV_SET(&ev, static_cast<uintptr_t>(id), EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
  ::kevent(impl_->kq, &ev, 1, nullptr, 0, nullptr);
  impl_->timers.erase(it);
}

int Poller::wait(WaitEvent* out, int cap, std::string& err) {
  struct kevent raw[16];
  const int ncap = cap < 16 ? cap : 16;
  // NULL changelist, non-NULL eventlist: this kevent sleeps until a filter
  // fires, then copies the ready events out. A timeout of NULL waits forever.
  const int n = ::kevent(impl_->kq, nullptr, 0, raw, ncap, nullptr);
  if (n < 0) {
    if (errno == EINTR) return 0;
    err = "kevent: " + errno_string();
    return -1;
  }
  int produced = 0;
  for (int i = 0; i < n && produced < cap; ++i) {
    out[produced].token = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(raw[i].udata));
    out[produced].signo = raw[i].filter == EVFILT_SIGNAL ? static_cast<int>(raw[i].ident) : 0;
    ++produced;
  }
  return produced;
}

std::vector<int> Poller::inherited_fds() const {
  std::vector<int> fds;
  if (impl_->kq >= 0) fds.push_back(impl_->kq);
  return fds;
}

bool create_shared_map(SharedMap& out, std::string& err) {
  // No memfd on macOS. An anonymous shared mapping is inherited by fork and
  // stays shared, which is the property the start gate and chunk slots need.
  // MAP_PRIVATE would copy on write and the parent would not see results.
  // MAP_ANONYMOUS: no file, the pages are just memory. MAP_SHARED: fork
  // does not copy them, so the child's stores are visible to the parent.
  // MAP_PRIVATE would silently give each process its own copy.
  void* mapped = ::mmap(nullptr, sizeof(SharedRegion), PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_SHARED, -1, 0);
  if (mapped == MAP_FAILED) {
    err = "mmap MAP_ANON|MAP_SHARED: " + errno_string();
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
  // eventfd does not exist here. A pipe is the same shape: the child writes
  // one byte, kqueue reports the read end. The parent holds both ends so
  // the read side never sees EOF when a child exits.
  int fds[2] = {-1, -1};
  // pipe allocates two fds on one kernel buffer. fds[0] reads, fds[1] writes.
  // A write wakes any kqueue watching the read end. This stands in for eventfd.
  if (::pipe(fds) < 0) {
    err = "pipe: " + errno_string();
    return false;
  }
  ::fcntl(fds[0], F_SETFL, O_NONBLOCK);
  ::fcntl(fds[1], F_SETFL, O_NONBLOCK);
  ::fcntl(fds[0], F_SETFD, FD_CLOEXEC);
  ::fcntl(fds[1], F_SETFD, FD_CLOEXEC);
  out.held.reset(fds[0]);
  out.write_end.reset(fds[1]);
  out.child_write = out.write_end.get();
  out.child_close = out.held.get();
  return true;
}

bool notify_write(int fd) {
  const char one = 1;
  const ssize_t n = ::write(fd, &one, 1);
  return n == 1 || errno == EAGAIN;
}

bool notify_read(int fd, uint64_t& count) {
  char buf[64];
  count = 0;
  for (;;) {
    const ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n < 0) return errno == EAGAIN || count > 0;
    if (n == 0) return count > 0;
    count += static_cast<uint64_t>(n);
  }
}

int futex_wait(uint32_t* addr, uint32_t expected, const timespec* timeout) {
  // Same protocol as a futex: sleep only if the word is still `expected`.
  // OS_SYNC_WAIT_ON_ADDRESS_SHARED is the cross-process flag. Without it
  // the parent and the child do not wake each other.
  const uint64_t value = expected;
  if (timeout == nullptr) {
    // os_sync_wait_on_address is the Mach equivalent of FUTEX_WAIT. The kernel
  // sleeps this thread only while *addr still equals value. SHARED is required
  // because the word lives in memory shared with another process.
  return ::os_sync_wait_on_address(addr, value, sizeof(uint32_t), OS_SYNC_WAIT_ON_ADDRESS_SHARED);
  }
  const uint64_t ns = static_cast<uint64_t>(timeout->tv_sec) * 1000000000ull +
                      static_cast<uint64_t>(timeout->tv_nsec);
  return ::os_sync_wait_on_address_with_timeout(addr, value, sizeof(uint32_t),
                                                OS_SYNC_WAIT_ON_ADDRESS_SHARED,
                                                OS_CLOCK_MACH_ABSOLUTE_TIME, ns);
}

int futex_wake(uint32_t* addr, int) {
  return ::os_sync_wake_by_address_any(addr, sizeof(uint32_t), OS_SYNC_WAKE_BY_ADDRESS_SHARED);
}

void prepare_child_signals() {
  // The controller installed empty handlers so kqueue could observe the
  // signals without dying. A worker must go back to the default action,
  // otherwise a deadline's SIGTERM is swallowed.
  signal(SIGCHLD, SIG_DFL);
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);
}

bool replace_at(int dirfd, const char* from, const char* to, std::string& err) {
  // renameat is atomic on the directory entry, same as Linux renameat2 with
  // a zero flag. macOS has no renameat2.
  if (::renameat(dirfd, from, dirfd, to) == 0) return true;
  err = std::string("renameat ") + to + ": " + errno_string();
  return false;
}

void advise_sequential(int, uint64_t, uint64_t) {
  // posix_fadvise is a Linux hint. The mapping path still calls madvise.
}

void advise_mapping(void* addr, std::size_t length) {
  ::madvise(addr, length, MADV_SEQUENTIAL);
}

int open_readonly(const char* path, bool direct, std::string& err) {
  const int fd = ::open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    err = std::string("open input: ") + errno_string();
    return -1;
  }
  // F_NOCACHE asks the unified buffer cache not to keep these pages.
  // It is the closest macOS call to Linux O_DIRECT, and it is not the same
  // contract: alignment rules differ, and the cache may still be involved.
  if (direct && ::fcntl(fd, F_NOCACHE, 1) < 0) {
    err = "F_NOCACHE: " + errno_string() + ". This is the macOS bypass of the unified buffer cache.";
    ::close(fd);
    return -1;
  }
  return fd;
}

int pin_cpu(int, std::string& err) {
  err = "macOS has no sched_setaffinity. Thread affinity policy is a hint, not a CPU jail, so Forge does not pretend the worker was pinned.";
  return 1;
}

std::string scheduler_name() { return "Mach scheduler (no CFS policy id)"; }

void log_vm_snapshot(int log_fd, const char* when) {
  // No /proc/self/io. rusage still splits minor and major faults, which is
  // the page-cache question: a second run of the same file should show
  // fewer major faults if the unified buffer cache held the pages.
  struct rusage ru {};
  if (::getrusage(RUSAGE_SELF, &ru) == 0) {
    write_line(log_fd, std::string(when) + " minflt=" + std::to_string(ru.ru_minflt) +
                           " majflt=" + std::to_string(ru.ru_majflt));
  }
  mach_task_basic_info info;
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info),
                &count) == KERN_SUCCESS) {
    write_line(log_fd, std::string(when) + " rss=" + std::to_string(info.resident_size) +
                           " virtual=" + std::to_string(info.virtual_size));
  }
}

void print_devices(std::ostream& out) {
  // There is no /sys/block. Mounts are the user-visible storage objects.
  struct statfs* mounts = nullptr;
  const int n = ::getmntinfo(&mounts, MNT_NOWAIT);
  if (n <= 0) {
    out << "getmntinfo: " << errno_string() << "\n";
    return;
  }
  out << "mount        fstype    device\n";
  for (int i = 0; i < n; ++i) {
    out << mounts[i].f_mntonname << "  " << mounts[i].f_fstypename << "  " << mounts[i].f_mntfromname
        << "\n";
  }
}

void print_storage(std::ostream& out, const std::string& workspace) {
  struct statfs st {};
  if (::statfs(workspace.c_str(), &st) == 0) {
    out << "workspace " << workspace << "\n";
    out << "filesystem " << st.f_fstypename << " on " << st.f_mntonname << " from " << st.f_mntfromname
        << "\n";
  } else {
    out << "statfs " << workspace << ": " << errno_string() << "\n";
  }
  uint64_t mem = 0;
  size_t len = sizeof(mem);
  if (::sysctlbyname("hw.memsize", &mem, &len, nullptr, 0) == 0) {
    out << "hw.memsize " << mem << "\n";
  }
  out << "macOS uses a unified buffer cache. There is no /proc/meminfo Cached line.\n";
  out << "Re-run a mmap job and compare majflt in the job log.\n";
}

}  // namespace forge::os

namespace forge {

bool inspect_file(const std::string& path, FileInfo& info, std::string& err) {
  struct stat st {};
  if (::stat(path.c_str(), &st) < 0) {
    err = "stat " + path + ": " + errno_string();
    return false;
  }
  info.size = static_cast<uint64_t>(st.st_size);
  info.regular = S_ISREG(st.st_mode);
  info.blocks_512 = static_cast<uint64_t>(st.st_blocks);
  info.have_btime = true;
  info.btime_sec = static_cast<int64_t>(st.st_birthtimespec.tv_sec);
  return true;
}

bool proc_starttime(int pid, uint64_t& ticks) {
  // Same role as field 22 of /proc/<pid>/stat: an identity that does not
  // match if the pid was recycled. Seconds since epoch, not boot ticks.
  // Compare it, don't convert it.
  struct proc_bsdinfo info {};
  const int n = ::proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &info, sizeof(info));
  if (n <= 0) return false;
  ticks = static_cast<uint64_t>(info.pbi_start_tvsec);
  return ticks != 0;
}

bool pid_looks_like_forge(int pid) {
  if (pid <= 0 || ::kill(pid, 0) < 0) return false;
  char path[PROC_PIDPATHINFO_MAXSIZE];
  if (::proc_pidpath(pid, path, sizeof(path)) <= 0) return false;
  return std::string(path).find("forge") != std::string::npos;
}

}  // namespace forge
