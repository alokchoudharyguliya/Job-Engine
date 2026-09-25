#include "forge.hpp"
#include "os/platform.hpp"

// controller.cpp is the process that stays up. The wait itself lives in
// os/linux (epoll) or os/mac (kqueue). This file only reacts:
//
//   Poller::wait
//     |-- signal     SIGCHLD (reap), SIGTERM/SIGINT (shutdown)
//     |-- timer      200ms tick, and one deadline per job
//     |-- notify     a worker published its chunk
//     `-- fifo       "SUBMIT <id>", "CANCEL <id>", "SHUTDOWN"
//
// The queue lives only in this process. It does not need a mutex: nothing
// else mutates it. Workers are processes, and they talk through eventfd,
// the shared mapping, and their own exit.

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <vector>

namespace forge {
namespace {

enum class Wake : uint64_t { Signal = 1, Fifo = 2, Tick = 3, JobEvent = 4, JobDeadline = 5 };

uint64_t pack_wake(Wake kind, uint64_t job) {
  return (static_cast<uint64_t>(kind) << 56) | (job & 0x00ffffffffffffffULL);
}

Wake wake_kind(uint64_t packed) {
  return static_cast<Wake>(packed >> 56);
}

uint64_t wake_job(uint64_t packed) { return packed & 0x00ffffffffffffffULL; }

struct ProcRef {
  int pid = 0;
  uint64_t start = 0;
  bool reaped = false;
  bool grouped = false;
  std::string how;
};

struct LiveJob {
  Meta meta;
  std::vector<ProcRef> procs;
  os::Notify notify;
  os::SharedMap shared;
  int deadline_id = -1;
  UniqueFd log_fd;
  SharedRegion* shm = nullptr;
  bool uses_slot = false;
  bool finished = false;
  bool kill_escalated = false;
  std::string stop_reason;
  uint64_t stop_sent_ms = 0;
};

struct Controller {
  std::string root;
  int slots_max = 1;
  int slots_used = 0;
  bool shutting_down = false;
  uint64_t started_mono = 0;
  UniqueFd lock_fd;
  os::Poller poller;
  int tick_id = -1;
  UniqueFd fifo_fd;
  UniqueFd log_fd;
  std::string fifo_buf;
  std::unordered_map<uint64_t, LiveJob> live;
  std::vector<uint64_t> queue;
  std::unordered_map<int, uint64_t> pid_owner;
};

void say(Controller& c, std::string_view msg) {
  const std::string line = "[forge] " + std::string(msg);
  std::cout << line << "\n";
  std::cout.flush();
  if (c.log_fd) {
    write_line(c.log_fd.get(), line);
  }
}

bool set_state(Controller& c, LiveJob& job, const std::string& state) {
  job.meta.state = state;
  std::string err;
  if (!save_meta(c.root, job.meta, err)) {
    say(c, "job " + std::to_string(job.meta.id) + " could not save meta: " + err);
    return false;
  }
  return true;
}

std::vector<ProcRef> parse_procs(const std::string& text) {
  std::vector<ProcRef> out;
  std::string cur;
  auto flush = [&] {
    if (cur.empty()) return;
    const auto at = cur.find('@');
    ProcRef pr;
    uint64_t pid = 0;
    if (at == std::string::npos) {
      parse_u64(cur, pid);
    } else {
      parse_u64(cur.substr(0, at), pid);
      parse_u64(cur.substr(at + 1), pr.start);
    }
    pr.pid = static_cast<int>(pid);
    if (pr.pid > 0) out.push_back(pr);
    cur.clear();
  };
  for (char ch : text) {
    if (ch == ',') flush();
    else cur.push_back(ch);
  }
  flush();
  return out;
}

std::string format_procs(const std::vector<ProcRef>& procs) {
  std::string out;
  for (size_t i = 0; i < procs.size(); ++i) {
    if (i) out += ',';
    out += std::to_string(procs[i].pid) + "@" + std::to_string(procs[i].start);
  }
  return out;
}

void write_status_file(Controller& c) {
  // Telemetry, not the system of record. A torn status.txt is acceptable.
  // jobs/<id>/meta is the record, and it goes through atomic_write_file.
  int running = 0;
  for (const auto& [id, job] : c.live) {
    (void)id;
    if (job.meta.state == "running" || job.meta.state == "starting") ++running;
  }
  std::ostringstream body;
  body << "pid=" << ::getpid() << "\n"
       << "slots_used=" << c.slots_used << "\n"
       << "slots_max=" << c.slots_max << "\n"
       << "queued=" << c.queue.size() << "\n"
       << "running=" << running << "\n"
       << "unix_ms=" << realtime_ms() << "\n";
  const std::string path = c.root + "/status.txt";
  UniqueFd fd(::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644));
  if (fd) write_all(fd.get(), body.str().data(), body.str().size());
}

void release_runtime(Controller& c, LiveJob& job) {
  os::destroy_shared_map(job.shared);
  job.shm = nullptr;
  if (job.notify.held) c.poller.unwatch_fd(job.notify.held.get());
  job.notify = os::Notify{};
  if (job.deadline_id >= 0) {
    c.poller.unwatch_timer(job.deadline_id);
    job.deadline_id = -1;
  }
  job.log_fd.reset();
  for (const auto& pr : job.procs) {
    c.pid_owner.erase(pr.pid);
  }
  if (job.uses_slot) {
    c.slots_used -= 1;
    job.uses_slot = false;
  }
}

void signal_job(LiveJob& job, int sig) {
  for (const auto& pr : job.procs) {
    if (pr.reaped || pr.pid <= 0) continue;
    // kill(pid) signals one process. kill(-pid) signals the whole process
    // group whose leader is pid. The negative id is the kernel's group form.
    if (pr.grouped) ::kill(-pr.pid, sig);
    else ::kill(pr.pid, sig);
  }
}

void request_stop(Controller& c, LiveJob& job, const std::string& reason) {
  if (!job.stop_reason.empty()) return;
  job.stop_reason = reason;
  job.stop_sent_ms = mono_ms();
  say(c, "job " + std::to_string(job.meta.id) + " " + reason + ", signalling workers");
  std::string err;
  append_job_log(c.root, job.meta.id, "controller: " + reason, err);
  signal_job(job, SIGTERM);
}

std::vector<int> fds_to_close(Controller& c) {
  // Fork copies the fd table. CLOEXEC does not run on fork, only on exec.
  // The child closes these numbers so it cannot drain the parent's epoll,
  // read a sibling job's eventfd, or drop the controller lock via LOCK_UN
  // (it must not call LOCK_UN; closing its dup of an flock fd drops one
  // reference, and the parent's fd keeps the lock).
  std::vector<int> fds;
  auto add = [&](int fd) {
    if (fd >= 0) fds.push_back(fd);
  };
  for (int fd : c.poller.inherited_fds()) add(fd);
  add(c.fifo_fd.get());
  add(c.log_fd.get());
  add(c.lock_fd.get());
  for (auto& [id, job] : c.live) {
    (void)id;
    add(job.notify.held.get());
    add(job.notify.write_end.get());
    add(job.shared.fd.get());
    add(job.log_fd.get());
  }
  return fds;
}

std::string describe_wait(const siginfo_t& info) {
  if (info.si_code == CLD_EXITED) return "exited:" + std::to_string(info.si_status);
  if (info.si_code == CLD_KILLED) return "killed:" + std::to_string(info.si_status);
  if (info.si_code == CLD_DUMPED) return "dumped:" + std::to_string(info.si_status);
  return "wait:" + std::to_string(info.si_code) + ":" + std::to_string(info.si_status);
}

void finalize(Controller& c, uint64_t id);
void schedule(Controller& c);

void note_reap(Controller& c, int pid, const siginfo_t& info) {
  const auto owner = c.pid_owner.find(pid);
  if (owner == c.pid_owner.end()) {
    say(c, "reaped unknown pid " + std::to_string(pid) + " " + describe_wait(info));
    return;
  }
  const uint64_t id = owner->second;
  auto it = c.live.find(id);
  if (it == c.live.end()) return;
  LiveJob& job = it->second;
  bool all = true;
  bool any_bad = false;
  for (auto& pr : job.procs) {
    if (pr.pid == pid) {
      pr.reaped = true;
      pr.how = describe_wait(info);
    }
    if (!pr.reaped) all = false;
    if (pr.reaped && pr.how != "exited:0") any_bad = true;
  }
  say(c, "job " + std::to_string(id) + " pid " + std::to_string(pid) + " " + describe_wait(info));
  if (any_bad && job.stop_reason.empty()) {
    request_stop(c, job, "failed");
  }
  if (all) finalize(c, id);
}

void reap(Controller& c) {
  // waitid, not a blocking wait in the main loop. WNOHANG keeps the event
  // loop responsive. si_code tells us exit vs signal vs core dump.
  for (;;) {
    siginfo_t info {};
    // waitid reaps one zombie. WNOHANG returns immediately if none are ready
    // (si_pid == 0). P_ALL is every child of this controller. si_code says
    // whether the child exited, was killed, or dumped core.
    const int rc = ::waitid(P_ALL, 0, &info, WEXITED | WNOHANG);
    if (rc < 0) {
      if (errno == EINTR) continue;
      if (errno == ECHILD) break;
      say(c, std::string("waitid: ") + errno_string());
      break;
    }
    if (info.si_pid == 0) break;
    note_reap(c, info.si_pid, info);
  }
}

std::string join_exits(const LiveJob& job) {
  std::string out;
  for (size_t i = 0; i < job.procs.size(); ++i) {
    if (i) out += ',';
    out += job.procs[i].how.empty() ? "?" : job.procs[i].how;
  }
  return out;
}

// Clean success wins if every worker exited 0 and published a good result,
// even if a deadline fired in the same instant. Otherwise the stop reason
// (timed_out, cancelled, shutdown) names the state.
bool results_ok(Controller& c, const LiveJob& job, std::string& why, std::string& body) {
  if (job.meta.op == "exec") {
    if (job.procs.size() != 1 || job.procs[0].how != "exited:0") {
      why = job.procs.empty() ? "exec produced no child" : job.procs[0].how;
      return false;
    }
    body.clear();
    return true;
  }
  if (job.shm == nullptr) {
    why = "missing shared region";
    return false;
  }
  const uint32_t done =
      std::atomic_ref<uint32_t>(job.shm->chunks_done).load(std::memory_order_acquire);
  if (done < job.shm->nchunks) {
    why = "published " + std::to_string(done) + " of " + std::to_string(job.shm->nchunks) + " chunks";
    return false;
  }
  for (const auto& pr : job.procs) {
    if (pr.how != "exited:0") {
      why = "worker " + pr.how;
      return false;
    }
  }
  std::ostringstream out;
  if (job.meta.op == "sha256") {
    if (!job.shm->chunks[0].ok || job.shm->chunks[0].hex[0] == '\0') {
      why = job.shm->chunks[0].error[0] ? job.shm->chunks[0].error : "empty digest";
      return false;
    }
    out << job.shm->chunks[0].hex << "\n";
  } else if (job.meta.op == "chunk-sha256") {
    uint64_t bytes = 0;
    out << "bytes " << job.meta.bytes << "\nchunks " << job.shm->nchunks << "\n";
    out << "note each line is the sha256 of that slice, not of the whole file\n";
    for (uint32_t i = 0; i < job.shm->nchunks; ++i) {
      const auto& ch = job.shm->chunks[i];
      if (!ch.ok) {
        why = ch.error[0] ? ch.error : "chunk failed";
        return false;
      }
      bytes += ch.bytes;
      out << i << " " << ch.offset << " " << ch.length << " " << ch.hex << "\n";
    }
    if (bytes != job.meta.bytes) {
      why = "chunk bytes do not add up to the file size";
      return false;
    }
  } else if (job.meta.op == "count") {
    uint64_t sum = 0;
    for (uint32_t i = 0; i < job.shm->nchunks; ++i) {
      if (!job.shm->chunks[i].ok) {
        why = job.shm->chunks[i].error[0] ? job.shm->chunks[i].error : "chunk failed";
        return false;
      }
      sum += job.shm->chunks[i].bytes;
    }
    if (sum != job.meta.bytes) {
      why = "count " + std::to_string(sum) + " != file size " + std::to_string(job.meta.bytes);
      return false;
    }
    out << sum << "\n";
  } else if (job.meta.op == "copy") {
    if (!job.shm->chunks[0].ok) {
      why = job.shm->chunks[0].error[0] ? job.shm->chunks[0].error : "copy failed";
      return false;
    }
    FileInfo info;
    std::string err;
    const std::string path = job_directory(c.root, job.meta.id) + "/output";
    if (!inspect_file(path, info, err)) {
      why = "copy output missing: " + err;
      return false;
    }
    if (info.size != job.meta.bytes) {
      why = "output size " + std::to_string(info.size) + " != input " + std::to_string(job.meta.bytes);
      return false;
    }
  } else {
    why = "unknown operation";
    return false;
  }
  body = out.str();
  return true;
}

void finalize(Controller& c, uint64_t id) {
  auto it = c.live.find(id);
  if (it == c.live.end() || it->second.finished) return;
  LiveJob& job = it->second;
  job.finished = true;
  job.meta.exits = join_exits(job);
  job.meta.finished_ms = realtime_ms();

  std::string why;
  std::string body;
  const bool ok = results_ok(c, job, why, body);
  std::string state = "completed";
  if (!ok) {
    if (job.stop_reason == "timed_out") state = "timed_out";
    else if (job.stop_reason == "cancelled" || job.stop_reason == "shutdown") state = "cancelled";
    else state = "failed";
    job.meta.error = why.empty() ? state : why;
  } else if (job.meta.op == "sha256" && !body.empty()) {
    job.meta.digest = body;
    while (!job.meta.digest.empty() && job.meta.digest.back() == '\n') job.meta.digest.pop_back();
  } else if (job.meta.op == "count" && !body.empty()) {
    job.meta.digest = body;
    while (!job.meta.digest.empty() && job.meta.digest.back() == '\n') job.meta.digest.pop_back();
  }

  if (ok) {
    job.meta.error.clear();
  }
  if (ok && !body.empty()) {
    // persisting: the output name does not exist until renameat2 finishes.
    set_state(c, job, "persisting");
    std::string err;
    if (!atomic_write_file(job_directory(c.root, id) + "/output", body, err)) {
      state = "failed";
      job.meta.error = err;
    }
  }
  set_state(c, job, state);
  say(c, "job " + std::to_string(id) + " " + state +
             (job.meta.error.empty() ? "" : " (" + job.meta.error + ")"));
  release_runtime(c, job);
  c.live.erase(id);
  c.queue.erase(std::remove(c.queue.begin(), c.queue.end(), id), c.queue.end());
}

void launch_job(Controller& c, uint64_t id);

bool pick_next(Controller& c, uint64_t& id) {
  // Linear scan. A heap is the next step; a few hundred queued jobs do not
  // need one, and the policy is easier to see as a loop.
  int best_pri = 0;
  uint64_t best = 0;
  bool found = false;
  for (uint64_t cand : c.queue) {
    auto it = c.live.find(cand);
    if (it == c.live.end() || it->second.meta.state != "queued") continue;
    if (!found || it->second.meta.priority > best_pri ||
        (it->second.meta.priority == best_pri && cand < best)) {
      found = true;
      best = cand;
      best_pri = it->second.meta.priority;
    }
  }
  if (!found) return false;
  id = best;
  c.queue.erase(std::remove(c.queue.begin(), c.queue.end(), best), c.queue.end());
  return true;
}

void schedule(Controller& c) {
  if (c.shutting_down) return;
  while (c.slots_used < c.slots_max) {
    uint64_t id = 0;
    if (!pick_next(c, id)) return;
    launch_job(c, id);
  }
}

void enqueue(Controller& c, uint64_t id) {
  if (c.shutting_down) {
    Meta meta;
    std::string err;
    if (load_meta(c.root, id, meta, err) && !is_terminal_state(meta.state)) {
      meta.state = "cancelled";
      meta.error = "controller is shutting down";
      meta.finished_ms = realtime_ms();
      save_meta(c.root, meta, err);
    }
    return;
  }
  if (c.live.count(id)) return;
  LiveJob job;
  std::string err;
  if (!load_meta(c.root, id, job.meta, err)) {
    say(c, "SUBMIT " + std::to_string(id) + " has no readable meta: " + err);
    return;
  }
  if (is_terminal_state(job.meta.state)) return;
  if (!validate_meta(job.meta, err)) {
    job.meta.state = "failed";
    job.meta.error = err;
    job.meta.finished_ms = realtime_ms();
    save_meta(c.root, job.meta, err);
    say(c, "job " + std::to_string(id) + " rejected: " + job.meta.error);
    return;
  }
  job.meta.state = "queued";
  save_meta(c.root, job.meta, err);
  c.queue.push_back(id);
  c.live.emplace(id, std::move(job));
  say(c, "queued job " + std::to_string(id) + " " + c.live[id].meta.op);
  schedule(c);
}

void launch_job(Controller& c, uint64_t id) {
  auto it = c.live.find(id);
  if (it == c.live.end()) return;
  LiveJob& job = it->second;
  std::string err;

  if (job.meta.op != "exec") {
    FileInfo info;
    if (!inspect_file(job.meta.input, info, err) || !info.regular) {
      job.meta.error = err.empty() ? "input is not a regular file" : err;
      job.meta.state = "failed";
      job.meta.finished_ms = realtime_ms();
      save_meta(c.root, job.meta, err);
      say(c, "job " + std::to_string(id) + " " + job.meta.error);
      release_runtime(c, job);
      c.live.erase(id);
      return;
    }
    job.meta.bytes = info.size;
    say(c, "job " + std::to_string(id) + " input " + std::to_string(info.size) +
               " bytes, allocated blocks " + std::to_string(info.blocks_512) + "x512" +
               (info.have_btime ? ", btime present" : ""));
  }

  const int nworkers = job.meta.op == "exec" ? 1 : job.meta.workers;
  const uint64_t align = job.meta.io == "direct" ? kDirectAlign : 1;
  const auto ranges = split_ranges(job.meta.bytes, nworkers, align);
  const int chunks = static_cast<int>(ranges.size());

  UniqueFd log_fd(open_job_log(c.root, id, err));
  if (!log_fd) {
    job.meta.state = "failed";
    job.meta.error = err;
    save_meta(c.root, job.meta, err);
    c.live.erase(id);
    return;
  }

  os::SharedMap shared;
  os::Notify notify;
  SharedRegion* shm = nullptr;
  if (job.meta.op != "exec") {
    if (!os::create_shared_map(shared, err) || !os::create_notify(notify, err)) {
      os::destroy_shared_map(shared);
      job.meta.state = "failed";
      job.meta.error = err;
      save_meta(c.root, job.meta, err);
      c.live.erase(id);
      return;
    }
    shm = shared.ptr;
    shm->magic = kShmMagic;
    shm->nchunks = static_cast<uint32_t>(chunks);
  }

  set_state(c, job, "starting");
  job.meta.started_ms = realtime_ms();
  const std::string job_dir = job_directory(c.root, id);
  std::vector<int> close_fds = fds_to_close(c);
  // Created after fds_to_close. The child writes notify.child_write and
  // must drop the read end (macOS pipe) and the shared-memory fd (Linux memfd).
  if (notify.child_close >= 0) close_fds.push_back(notify.child_close);
  if (shared.fd) close_fds.push_back(shared.fd.get());

  std::vector<ProcRef> spawned;
  bool spawn_ok = true;
  std::string spawn_err;
  ::fflush(nullptr);
  for (int i = 0; i < chunks; ++i) {
    // fork copies this process. The child is a new pid with a copy of the
    // address space (copy-on-write) and a copy of the fd table. It does not
    // copy the kernel's open-file-description locks in the POSIX sense, but
    // it does share the flock object. The child must not return into this loop.
    const pid_t pid = ::fork();
    if (pid < 0) {
      spawn_ok = false;
      spawn_err = "fork: " + errno_string();
      break;
    }
    if (pid == 0) {
      WorkerLaunch launch;
      launch.meta = job.meta;
      launch.job_dir = job_dir;
      launch.shm = shm;
      launch.event_fd = notify.child_write;
      launch.log_fd = log_fd.get();
      launch.chunk_index = i;
      launch.offset = ranges[static_cast<size_t>(i)].offset;
      launch.length = ranges[static_cast<size_t>(i)].length;
      launch.close_fds = close_fds;
      worker_entry(launch);
    }
    ProcRef pr;
    pr.pid = pid;
    // setpgid makes the child its own process-group leader so a later
    // kill(-pid) hits the worker and any grandchild an exec'd shell starts.
    // EACCES means the child already called setpgid(0, 0) first. That is success.
    if (::setpgid(pid, pid) == 0 || errno == EACCES) pr.grouped = true;
    proc_starttime(pid, pr.start);
    spawned.push_back(pr);
  }

  if (!spawn_ok) {
    if (shm) open_start_gate(shm);
    for (const auto& pr : spawned) {
      if (pr.grouped) ::kill(-pr.pid, SIGKILL);
      else ::kill(pr.pid, SIGKILL);
      siginfo_t info {};
      while (::waitid(P_PID, static_cast<id_t>(pr.pid), &info, WEXITED) < 0 && errno == EINTR) {
      }
    }
    os::destroy_shared_map(shared);
    job.meta.state = "failed";
    job.meta.error = spawn_err;
    job.meta.finished_ms = realtime_ms();
    save_meta(c.root, job.meta, err);
    say(c, "job " + std::to_string(id) + " " + spawn_err);
    c.live.erase(id);
    return;
  }

  job.procs = std::move(spawned);
  job.meta.procs = format_procs(job.procs);
  job.shared = std::move(shared);
  job.shm = job.shared.ptr;
  job.notify = std::move(notify);
  job.log_fd = std::move(log_fd);
  for (const auto& pr : job.procs) c.pid_owner[pr.pid] = id;
  if (job.shm) open_start_gate(job.shm);
  if (job.notify.held &&
      !c.poller.watch_fd(job.notify.held.get(), pack_wake(Wake::JobEvent, id), err)) {
    say(c, "job " + std::to_string(id) + " watch notify: " + err);
  }
  if (job.meta.deadline_s > 0) {
    job.deadline_id = c.poller.watch_timer(pack_wake(Wake::JobDeadline, id),
                                           job.meta.deadline_s * 1000, 0, err);
    if (job.deadline_id < 0) say(c, "job " + std::to_string(id) + " deadline: " + err);
  }
  job.uses_slot = true;
  c.slots_used += 1;
  set_state(c, job, "running");
  say(c, "running job " + std::to_string(id) + " workers " + std::to_string(job.procs.size()) +
             " slots " + std::to_string(c.slots_used) + "/" + std::to_string(c.slots_max));
}

void on_cancel(Controller& c, uint64_t id) {
  auto it = c.live.find(id);
  if (it == c.live.end()) {
    Meta meta;
    std::string err;
    if (!load_meta(c.root, id, meta, err)) {
      say(c, "cancel: no job " + std::to_string(id));
      return;
    }
    if (!is_terminal_state(meta.state)) {
      meta.state = "cancelled";
      meta.error = "cancelled";
      meta.finished_ms = realtime_ms();
      save_meta(c.root, meta, err);
    }
    return;
  }
  if (it->second.meta.state == "queued") {
    it->second.meta.state = "cancelled";
    it->second.meta.error = "cancelled before start";
    it->second.meta.finished_ms = realtime_ms();
    std::string err;
    save_meta(c.root, it->second.meta, err);
    say(c, "cancelled queued job " + std::to_string(id));
    c.queue.erase(std::remove(c.queue.begin(), c.queue.end(), id), c.queue.end());
    c.live.erase(it);
    return;
  }
  request_stop(c, it->second, "cancelled");
}

void on_event(Controller& c, uint64_t id) {
  auto it = c.live.find(id);
  if (it == c.live.end() || !it->second.notify.held) return;
  uint64_t count = 0;
  // Level-triggered epoll stays readable until this counter is drained.
  if (!os::notify_read(it->second.notify.held.get(), count)) {
    say(c, "notify read: " + errno_string());
    return;
  }
  uint32_t done = 0;
  if (it->second.shm) {
    done = std::atomic_ref<uint32_t>(it->second.shm->chunks_done)
               .load(std::memory_order_acquire);
  }
  say(c, "job " + std::to_string(id) + " eventfd +" + std::to_string(count) +
             " chunks_done " + std::to_string(done));
}

void on_deadline(Controller& c, uint64_t id) {
  auto it = c.live.find(id);
  if (it == c.live.end() || it->second.deadline_id < 0) return;
  request_stop(c, it->second, "timed_out");
}

void on_tick(Controller& c) {
  const uint64_t now = mono_ms();
  std::vector<uint64_t> ids;
  for (const auto& [id, job] : c.live) ids.push_back(id);
  for (uint64_t id : ids) {
    auto it = c.live.find(id);
    if (it == c.live.end()) continue;
    LiveJob& job = it->second;
    if (!job.stop_reason.empty() && !job.kill_escalated && job.stop_sent_ms != 0 &&
        now - job.stop_sent_ms > 1000) {
      say(c, "job " + std::to_string(id) + " still alive 1s after SIGTERM, sending SIGKILL");
      signal_job(job, SIGKILL);
      job.kill_escalated = true;
    }
  }
  write_status_file(c);
  if (c.shutting_down) {
    bool busy = false;
    for (const auto& [id, job] : c.live) {
      (void)id;
      if (!job.procs.empty() && job.meta.state == "running") busy = true;
    }
    if (!busy) return;
  }
}

void handle_line(Controller& c, const std::string& line) {
  if (line == "SHUTDOWN") {
    say(c, "shutdown requested");
    c.shutting_down = true;
    std::vector<uint64_t> queued = c.queue;
    for (uint64_t id : queued) on_cancel(c, id);
    std::vector<uint64_t> ids;
    for (const auto& [id, job] : c.live) ids.push_back(id);
    for (uint64_t id : ids) {
      auto it = c.live.find(id);
      if (it == c.live.end()) continue;
      if (it->second.meta.state == "running" || it->second.meta.state == "starting") {
        request_stop(c, it->second, "shutdown");
      }
    }
    return;
  }
  if (line.rfind("SUBMIT ", 0) == 0) {
    uint64_t id = 0;
    if (!parse_u64(line.substr(7), id)) {
      say(c, "bad SUBMIT line");
      return;
    }
    enqueue(c, id);
    return;
  }
  if (line.rfind("CANCEL ", 0) == 0) {
    uint64_t id = 0;
    if (!parse_u64(line.substr(7), id)) {
      say(c, "bad CANCEL line");
      return;
    }
    on_cancel(c, id);
    return;
  }
  say(c, "ignored control line: " + line);
}

void on_fifo(Controller& c) {
  char buf[1024];
  for (;;) {
    const ssize_t n = ::read(c.fifo_fd.get(), buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN) break;
      say(c, std::string("fifo read: ") + errno_string());
      break;
    }
    if (n == 0) break;  // should not happen: we hold the fifo open O_RDWR
    c.fifo_buf.append(buf, static_cast<size_t>(n));
  }
  if (c.fifo_buf.size() > 65536) {
    say(c, "control fifo overflow, discarding");
    c.fifo_buf.clear();
  }
  for (;;) {
    const auto nl = c.fifo_buf.find('\n');
    if (nl == std::string::npos) break;
    std::string line = c.fifo_buf.substr(0, nl);
    c.fifo_buf.erase(0, nl + 1);
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (!line.empty()) handle_line(c, line);
  }
}

void recover(Controller& c) {
  // A dead controller's children were reparented to init. We can still
  // signal them if the pid AND the /proc starttime match. Pid reuse is why
  // the starttime is stored: kill(pid) alone might hit a new process.
  for (uint64_t id : list_job_ids(c.root)) {
    Meta meta;
    std::string err;
    if (!load_meta(c.root, id, meta, err)) continue;
    if (meta.state == "running" || meta.state == "starting" || meta.state == "persisting") {
      for (const auto& pr : parse_procs(meta.procs)) {
        uint64_t now = 0;
        if (pr.start != 0 && proc_starttime(pr.pid, now) && now == pr.start) {
          ::kill(-pr.pid, SIGKILL);
          ::kill(pr.pid, SIGKILL);
          say(c, "killed leftover pid " + std::to_string(pr.pid) + " from job " + std::to_string(id));
        }
      }
      meta.state = "failed";
      meta.error = "controller restarted while the job was in progress";
      meta.finished_ms = realtime_ms();
      save_meta(c.root, meta, err);
      continue;
    }
    if (meta.state == "submitted" || meta.state == "queued") {
      enqueue(c, id);
    }
  }
}

bool setup(Controller& c, std::string& err) {
  if (!ensure_workspace(c.root, err)) return false;
  c.lock_fd = acquire_controller_lock(c.root, err);
  if (!c.lock_fd) return false;
  if (!write_pid_file(c.root, ::getpid(), err)) return false;
  if (!c.poller.open(err)) return false;
  // O_RDWR so the read end never sees EOF when a CLI closes its write end.
  // A FIFO opened only for read reports EOF whenever the last writer
  // (the CLI) closes. O_RDWR keeps a writer reference inside the controller,
  // so the fifo stays readable. O_NONBLOCK stops open() from sleeping until
  // some other process connects. O_CLOEXEC drops it on exec, not on fork.
  c.fifo_fd.reset(::open(control_fifo_path(c.root).c_str(),
                         O_RDWR | O_NONBLOCK | O_CLOEXEC));
  c.log_fd.reset(::open((c.root + "/controller.log").c_str(),
                        O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644));
  if (!c.fifo_fd || !c.log_fd) {
    err = "controller fd setup: " + errno_string();
    return false;
  }
  if (!c.poller.watch_signals(pack_wake(Wake::Signal, 0), err) ||
      !c.poller.watch_fd(c.fifo_fd.get(), pack_wake(Wake::Fifo, 0), err)) {
    return false;
  }
  c.tick_id = c.poller.watch_timer(pack_wake(Wake::Tick, 0), 200, 200, err);
  if (c.tick_id < 0) return false;
  return true;
}

bool idle(const Controller& c) {
  return c.live.empty() && c.queue.empty();
}

int loop(Controller& c) {
  say(c, "controller pid " + std::to_string(::getpid()) + " workspace " + c.root +
             " slots " + std::to_string(c.slots_max));
  recover(c);
  uint64_t shutdown_since = 0;
  while (true) {
    if (c.shutting_down && idle(c)) break;
    if (c.shutting_down && shutdown_since == 0) shutdown_since = mono_ms();
    if (c.shutting_down && shutdown_since != 0 && mono_ms() - shutdown_since > 3000 && idle(c)) {
      break;
    }
    os::WaitEvent events[16];
    std::string err;
    const int n = c.poller.wait(events, 16, err);
    if (n < 0) {
      say(c, err);
      return 1;
    }
    if (n == 0) continue;
    for (int i = 0; i < n; ++i) {
      const uint64_t packed = events[i].token;
      switch (wake_kind(packed)) {
        case Wake::Signal: {
          if (events[i].signo == SIGCHLD) {
            reap(c);
            schedule(c);
          } else if (events[i].signo == SIGTERM || events[i].signo == SIGINT) {
            handle_line(c, "SHUTDOWN");
          }
          break;
        }
        case Wake::Fifo:
          on_fifo(c);
          break;
        case Wake::Tick:
          on_tick(c);
          if (c.shutting_down && idle(c)) return 0;
          break;
        case Wake::JobEvent:
          on_event(c, wake_job(packed));
          break;
        case Wake::JobDeadline:
          on_deadline(c, wake_job(packed));
          break;
      }
    }
    schedule(c);
  }
  return 0;
}

}  // namespace

int run_controller(const std::string& workspace, int slots) {
  // SIGPIPE's default action kills the process. We want EPIPE back from
  // write() instead. SIGCHLD and SIGTERM are owned by os::Poller.
  ::signal(SIGPIPE, SIG_IGN);
  Controller c;
  c.root = workspace;
  c.slots_max = slots;
  c.started_mono = mono_ms();
  std::string err;
  if (!setup(c, err)) {
    std::cerr << "forge: " << err << "\n";
    return 1;
  }
  const int rc = loop(c);
  remove_pid_file(c.root);
  say(c, "controller stopped");
  return rc;
}

}  // namespace forge
