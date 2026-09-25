#ifndef FORGE_PLATFORM_HPP
#define FORGE_PLATFORM_HPP

// The engine (main, controller, worker, store) calls this header.
// One of the two folders is compiled in:
//
//   os/linux/platform.cpp   epoll, signalfd, timerfd, eventfd, memfd, futex
//   os/mac/platform.cpp     kqueue, pipes, anonymous shared mmap, os_sync_*
//
// The job queue, the hash, and the meta file do not change between them.

#include "../forge.hpp"

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

namespace forge::os {

struct WaitEvent {
  uint64_t token = 0;
  int signo = 0;  // set when this wake is a signal; 0 otherwise
};

// epoll set on Linux, kqueue on macOS. Timers and signals are registered
// here so the controller does not care which kernel object delivers them.
class Poller {
 public:
  Poller();
  ~Poller();
  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;
  Poller(Poller&& other) noexcept;
  Poller& operator=(Poller&& other) noexcept;

  bool open(std::string& err);
  // Linux: the signalfd is already open; this adds it to epoll.
  // macOS: EVFILT_SIGNAL for SIGCHLD, SIGTERM, and SIGINT.
  bool watch_signals(uint64_t token, std::string& err);
  bool watch_fd(int fd, uint64_t token, std::string& err);
  void unwatch_fd(int fd);
  // every_ms == 0 is a one-shot. Returns an id, or -1 on failure.
  int watch_timer(uint64_t token, uint64_t first_ms, uint64_t every_ms, std::string& err);
  void unwatch_timer(int id);
  // Drains timer and signal objects. Does not read ordinary fds (fifo, wake).
  int wait(WaitEvent* out, int cap, std::string& err);
  // Fds a forked child must close. CLOEXEC does not run on fork.
  std::vector<int> inherited_fds() const;

 private:
  struct Impl;
  Impl* impl_ = nullptr;
};

// Parent and children see the same bytes. Linux: memfd. macOS: MAP_ANON|MAP_SHARED.
struct SharedMap {
  UniqueFd fd;
  SharedRegion* ptr = nullptr;
};

struct Notify {
  UniqueFd held;          // parent reads this (eventfd, or the pipe's read end)
  UniqueFd write_end;     // macOS: parent also holds the pipe's write end
  int child_write = -1;   // child writes a wake here
  int child_close = -1;   // extra inherited fd the child must drop
};

bool create_shared_map(SharedMap& out, std::string& err);
void destroy_shared_map(SharedMap& map);

bool create_notify(Notify& out, std::string& err);
bool notify_write(int fd);
bool notify_read(int fd, uint64_t& count);

// Linux: futex. macOS: os_sync_wait_on_address / os_sync_wake_by_address_any,
// the cross-process wait on a memory word.
int futex_wait(uint32_t* addr, uint32_t expected, const timespec* timeout);
int futex_wake(uint32_t* addr, int nwaiters);

// Child: signals must kill the worker again. The parent may have blocked
// them (Linux signalfd) or installed an empty handler (macOS kqueue).
void prepare_child_signals();

bool replace_at(int dirfd, const char* from, const char* to, std::string& err);

void advise_sequential(int fd, uint64_t offset, uint64_t length);
void advise_mapping(void* addr, std::size_t length);
// direct == true: O_DIRECT on Linux, F_NOCACHE on macOS. No silent fallback.
int open_readonly(const char* path, bool direct, std::string& err);

// 0 pinned, 1 this OS cannot pin (caller logs and continues), -1 hard failure.
int pin_cpu(int cpu, std::string& err);
std::string scheduler_name();

void log_vm_snapshot(int log_fd, const char* when);

void print_devices(std::ostream& out);
void print_storage(std::ostream& out, const std::string& workspace);

}  // namespace forge::os

#endif
