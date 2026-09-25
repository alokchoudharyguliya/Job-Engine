#include "forge.hpp"
#include "os/platform.hpp"

// main.cpp is the part you type. It does not run jobs itself.
//
//   forge submit  ---- write jobs/<id>/meta, then "SUBMIT <id>" on the fifo
//   forge run     ---- controller.cpp event loop
//   forge jobs    ---- read the meta files back
//   forge status  ---- pid file + /proc, plus status.txt
//   forge logs    ---- the job log the worker appended
//   forge inspect ---- meta, and /proc/<pid> if the worker is still alive
//   forge cancel  ---- "CANCEL <id>" on the fifo
//   forge device  ---- /sys/block
//   forge storage ---- /proc/meminfo, /proc/diskstats, mount of the workspace
//
// device and storage do not need a running controller. They are reads of
// files the kernel publishes. That is the user/kernel boundary without a
// device driver.

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <sys/stat.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace forge {
namespace {

void usage(std::ostream& out) {
  out <<
      "Forge — local Linux job engine\n"
      "\n"
      "  forge run [--workspace DIR] [--slots N]\n"
      "  forge submit --input PATH --operation OP [options] [--wait]\n"
      "  forge jobs [--workspace DIR]\n"
      "  forge status [--workspace DIR]\n"
      "  forge logs ID [--workspace DIR]\n"
      "  forge inspect ID [--workspace DIR]\n"
      "  forge cancel ID [--workspace DIR]\n"
      "  forge device list\n"
      "  forge storage stats [--workspace DIR]\n"
      "  forge self-test\n"
      "\n"
      "Operations: sha256, chunk-sha256, count, copy, exec\n"
      "  sha256          one worker, digest matches sha256sum\n"
      "  chunk-sha256    N workers, one digest per slice (not the file digest)\n"
      "  count           N workers, byte count (every byte is touched)\n"
      "  copy            one worker, durable output via fsync + rename\n"
      "  exec            one worker, execve of --exec PROGRAM\n"
      "                  argv is: PROGRAM INPUT OUTPUT\n"
      "\n"
      "submit options:\n"
      "  --workers N       default 1, max 16\n"
      "  --io mmap|read|direct     default mmap\n"
      "  --priority N      larger runs first (not the same scale as nice)\n"
      "  --nice N          Unix nice, -20..19; larger means less CPU\n"
      "  --cpu N           pin the worker to that CPU\n"
      "  --mem-mb N        RLIMIT_AS (includes mmap of the input)\n"
      "  --cpu-sec N       RLIMIT_CPU\n"
      "  --max-fds N       RLIMIT_NOFILE\n"
      "  --max-out-mb N    RLIMIT_FSIZE\n"
      "  --deadline SEC    timerfd; 0 means none\n"
      "  --exec PATH       required for --operation exec\n"
      "  --wait            block until the job reaches a terminal state\n"
      "\n"
      "Start the controller in one terminal, submit from another.\n"
      "The workspace path decides the filesystem (ext4 vs tmpfs):\n"
      "  forge run --workspace /dev/shm/forge-ws\n";
}

struct Args {
  std::string workspace;
  std::vector<std::string> rest;
};

bool take_flag(std::vector<std::string>& args, const std::string& name, std::string& value) {
  for (size_t i = 0; i < args.size(); ++i) {
    if (args[i] == name && i + 1 < args.size()) {
      value = args[i + 1];
      args.erase(args.begin() + static_cast<long>(i), args.begin() + static_cast<long>(i) + 2);
      return true;
    }
  }
  return false;
}

bool has_flag(std::vector<std::string>& args, const std::string& name) {
  for (size_t i = 0; i < args.size(); ++i) {
    if (args[i] == name) {
      args.erase(args.begin() + static_cast<long>(i));
      return true;
    }
  }
  return false;
}

std::string flag_or(std::vector<std::string>& args, const std::string& name,
                    const std::string& fallback) {
  std::string value;
  if (take_flag(args, name, value)) return value;
  return fallback;
}

std::string real_path(const std::string& path, std::string& err) {
  char buf[PATH_MAX];
  if (::realpath(path.c_str(), buf) == nullptr) {
    err = "realpath " + path + ": " + errno_string();
    return {};
  }
  return buf;
}

bool send_fifo(const std::string& root, const std::string& line, std::string& err) {
  // O_NONBLOCK: if the controller is not holding the fifo open, open fails
  // with ENXIO instead of blocking forever.
  UniqueFd fd(::open(control_fifo_path(root).c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC));
  if (!fd) {
    if (errno == ENXIO) {
      err = "controller is not running for " + root + "\nStart it with:\n  forge run --workspace " + root;
    } else {
      err = "open fifo: " + errno_string();
    }
    return false;
  }
  const std::string msg = line.back() == '\n' ? line : line + "\n";
  if (!write_all(fd.get(), msg.data(), msg.size())) {
    err = "write fifo: " + errno_string();
    return false;
  }
  return true;
}

int cmd_run(std::vector<std::string> args) {
  const std::string ws = resolve_workspace(flag_or(args, "--workspace", ""));
  int slots = 1;
  const long ncpu = ::sysconf(_SC_NPROCESSORS_ONLN);
  if (ncpu > 0) slots = static_cast<int>(ncpu);
  std::string slot_text;
  if (take_flag(args, "--slots", slot_text)) {
    uint64_t n = 0;
    if (!parse_u64(slot_text, n) || n < 1 || n > static_cast<uint64_t>(kMaxSlots)) {
      std::cerr << "forge: --slots must be 1.." << kMaxSlots << "\n";
      return 1;
    }
    slots = static_cast<int>(n);
  }
  if (!args.empty()) {
    std::cerr << "forge: unknown argument " << args[0] << "\n";
    return 1;
  }
  return run_controller(ws, slots);
}

int wait_job(const std::string& root, uint64_t id) {
  // The CLI is not the event loop. It polls the meta file the controller
  // publishes. The controller is the one blocked in epoll_wait.
  for (;;) {
    Meta meta;
    std::string err;
    if (!load_meta(root, id, meta, err)) {
      std::cerr << "forge: " << err << "\n";
      return 1;
    }
    if (is_terminal_state(meta.state)) {
      std::cout << "job " << id << " " << meta.state << "\n";
      if (!meta.digest.empty()) std::cout << "digest " << meta.digest << "\n";
      if (!meta.error.empty()) std::cout << "error " << meta.error << "\n";
      std::cout << "output " << job_directory(root, id) << "/output\n";
      return meta.state == "completed" ? 0 : 1;
    }
    ::usleep(200000);
  }
}

int cmd_submit(std::vector<std::string> args) {
  const std::string ws = resolve_workspace(flag_or(args, "--workspace", ""));
  std::string err;
  if (!ensure_workspace(ws, err)) {
    std::cerr << "forge: " << err << "\n";
    return 1;
  }
  Meta meta;
  meta.op = flag_or(args, "--operation", "");
  meta.input = flag_or(args, "--input", "");
  meta.exec_path = flag_or(args, "--exec", "");
  meta.io = flag_or(args, "--io", "mmap");
  const bool wait = has_flag(args, "--wait");
  auto need_i = [&](const char* name, int& dest, int fallback) {
    std::string text = flag_or(args, name, "");
    if (text.empty()) {
      dest = fallback;
      return true;
    }
    int64_t n = 0;
    if (!parse_i64(text, n)) {
      err = std::string("bad ") + name;
      return false;
    }
    dest = static_cast<int>(n);
    return true;
  };
  auto need_u = [&](const char* name, uint64_t& dest) {
    std::string text = flag_or(args, name, "");
    if (text.empty()) return true;
    if (!parse_u64(text, dest)) {
      err = std::string("bad ") + name;
      return false;
    }
    return true;
  };
  if (!need_i("--workers", meta.workers, 1) || !need_i("--priority", meta.priority, 0) ||
      !need_i("--nice", meta.nice, 0) || !need_i("--cpu", meta.cpu, -1) ||
      !need_u("--mem-mb", meta.mem_mb) || !need_u("--cpu-sec", meta.cpu_sec) ||
      !need_u("--max-fds", meta.max_fds) || !need_u("--max-out-mb", meta.max_out_mb) ||
      !need_u("--deadline", meta.deadline_s)) {
    std::cerr << "forge: " << err << "\n";
    return 1;
  }
  if (!args.empty()) {
    std::cerr << "forge: unknown argument " << args[0] << "\n";
    return 1;
  }
  if (!validate_meta(meta, err)) {
    std::cerr << "forge: " << err << "\n";
    return 1;
  }
  if (meta.op != "exec") {
    meta.input = real_path(meta.input, err);
    if (meta.input.empty()) {
      std::cerr << "forge: " << err << "\n";
      return 1;
    }
    FileInfo info;
    if (!inspect_file(meta.input, info, err) || !info.regular) {
      std::cerr << "forge: input is not a regular file"
                << (err.empty() ? "" : " (" + err + ")") << "\n";
      return 1;
    }
  } else {
    meta.exec_path = real_path(meta.exec_path, err);
    if (meta.exec_path.empty()) {
      std::cerr << "forge: " << err << "\n";
      return 1;
    }
    if (!meta.input.empty() && meta.input[0] != '/') {
      char cwd[PATH_MAX];
      if (::getcwd(cwd, sizeof(cwd)) != nullptr) meta.input = std::string(cwd) + "/" + meta.input;
    }
  }
  if (!allocate_job_id(ws, meta.id, err)) {
    std::cerr << "forge: " << err << "\n";
    return 1;
  }
  meta.state = "submitted";
  meta.submitted_ms = realtime_ms();
  if (!save_meta(ws, meta, err)) {
    std::cerr << "forge: " << err << "\n";
    return 1;
  }
  if (!send_fifo(ws, "SUBMIT " + std::to_string(meta.id), err)) {
    std::cerr << "forge: " << err << "\n";
    return 1;
  }
  std::cout << "submitted job " << meta.id << "\n";
  if (wait) return wait_job(ws, meta.id);
  return 0;
}

int cmd_jobs(const std::string& ws) {
  const auto ids = list_job_ids(ws);
  if (ids.empty()) {
    std::cout << "no jobs in " << ws << "\n";
    return 0;
  }
  std::cout << "id        state        op             workers  digest/error\n";
  for (uint64_t id : ids) {
    Meta meta;
    std::string err;
    if (!load_meta(ws, id, meta, err)) {
      std::cout << id << " unreadable " << err << "\n";
      continue;
    }
    std::string tail = meta.digest.empty() ? meta.error : meta.digest;
    if (tail.size() > 48) tail.resize(48);
    std::cout << meta.id << "  " << meta.state << "  " << meta.op << "  " << meta.workers
              << "  " << tail << "\n";
  }
  return 0;
}

int cmd_status(const std::string& ws) {
  int pid = 0;
  std::string err;
  const bool have_pid = read_pid_file(ws, pid, err) && pid_looks_like_forge(pid);
  if (have_pid) std::cout << "controller: running pid " << pid << "\n";
  else std::cout << "controller: stopped\n";
  std::string status;
  if (read_text_file(ws + "/status.txt", status, err)) {
    std::cout << "status.txt (telemetry, may be stale if the controller was killed)\n";
    std::cout << status;
  }
  int running = 0, queued = 0, done = 0, failed = 0;
  for (uint64_t id : list_job_ids(ws)) {
    Meta meta;
    if (!load_meta(ws, id, meta, err)) continue;
    if (meta.state == "running" || meta.state == "starting") ++running;
    else if (meta.state == "queued" || meta.state == "submitted") ++queued;
    else if (meta.state == "completed") ++done;
    else ++failed;
  }
  std::cout << "jobs: " << running << " running, " << queued << " queued, " << done
            << " completed, " << failed << " not-completed\n";
  std::cout << "workspace: " << ws << "\n";
  return 0;
}

int cmd_logs(const std::string& ws, uint64_t id) {
  const std::string path = job_directory(ws, id) + "/log";
  UniqueFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (!fd) {
    std::cerr << "forge: open " << path << ": " << errno_string() << "\n";
    return 1;
  }
  char buf[4096];
  for (;;) {
    const ssize_t n = ::read(fd.get(), buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR) continue;
      std::cerr << "forge: read log: " << errno_string() << "\n";
      return 1;
    }
    if (n == 0) break;
    std::cout.write(buf, n);
  }
  return 0;
}

void print_filtered_proc(const std::string& path, const std::vector<std::string>& prefixes) {
  std::string text;
  std::string err;
  if (!read_text_file(path, text, err)) {
    std::cout << path << ": " << err << "\n";
    return;
  }
  std::cout << path << "\n";
  std::istringstream in(text);
  std::string line;
  while (std::getline(in, line)) {
    if (prefixes.empty()) {
      std::cout << "  " << line << "\n";
      continue;
    }
    for (const auto& p : prefixes) {
      if (line.rfind(p, 0) == 0) std::cout << "  " << line << "\n";
    }
  }
}

int cmd_inspect(const std::string& ws, uint64_t id) {
  Meta meta;
  std::string err;
  if (!load_meta(ws, id, meta, err)) {
    std::cerr << "forge: " << err << "\n";
    return 1;
  }
  std::cout << meta_to_text(meta);
  const std::string out = job_directory(ws, id) + "/output";
  FileInfo info;
  if (inspect_file(out, info, err) && info.regular && info.size < 4096 && meta.op != "copy") {
    std::string body;
    if (read_text_file(out, body, err)) {
      std::cout << "--- output ---\n" << body;
    }
  } else if (info.regular) {
    std::cout << "output bytes " << info.size << " at " << out << "\n";
  }
  if (meta.state == "running" || meta.state == "starting") {
    std::istringstream in(meta.procs);
    std::string tok;
    while (std::getline(in, tok, ',')) {
      const auto at = tok.find('@');
      uint64_t pid = 0;
      if (!parse_u64(at == std::string::npos ? tok : tok.substr(0, at), pid)) continue;
      std::cout << "--- /proc/" << pid << " ---\n";
      print_filtered_proc("/proc/" + std::to_string(pid) + "/status",
                          {"Name:", "State:", "Pid:", "PPid:", "VmRSS:", "VmSize:", "VmPeak:",
                           "Threads:", "voluntary_ctxt_switches:", "nonvoluntary_ctxt_switches:",
                           "Cpus_allowed_list:"});
      print_filtered_proc("/proc/" + std::to_string(pid) + "/io", {});
    }
  }
  std::cout << "log: " << job_directory(ws, id) << "/log\n";
  return 0;
}

int cmd_cancel(const std::string& ws, uint64_t id) {
  std::string err;
  if (!send_fifo(ws, "CANCEL " + std::to_string(id), err)) {
    std::cerr << "forge: " << err << "\n";
    return 1;
  }
  std::cout << "cancel requested for job " << id << "\n";
  for (int i = 0; i < 50; ++i) {
    Meta meta;
    if (load_meta(ws, id, meta, err) && is_terminal_state(meta.state)) {
      std::cout << "job " << id << " " << meta.state << "\n";
      return meta.state == "cancelled" || meta.state == "completed" ? 0 : 1;
    }
    ::usleep(100000);
  }
  std::cout << "cancel sent; job has not reached a terminal state yet\n";
  return 0;
}

int cmd_device() {
  os::print_devices(std::cout);
  return 0;
}

int cmd_storage(const std::string& ws) {
  os::print_storage(std::cout, ws);
  return 0;
}

uint64_t need_id(const std::vector<std::string>& args) {
  if (args.empty()) return 0;
  uint64_t id = 0;
  if (!parse_u64(args[0], id)) return 0;
  return id;
}

}  // namespace

int dispatch(int argc, char** argv) {
  ::signal(SIGPIPE, SIG_IGN);
  if (argc < 2) {
    usage(std::cerr);
    return 1;
  }
  const std::string cmd = argv[1];
  std::vector<std::string> args;
  for (int i = 2; i < argc; ++i) args.emplace_back(argv[i]);
  if (cmd == "help" || cmd == "-h" || cmd == "--help") {
    usage(std::cout);
    return 0;
  }
  if (cmd == "self-test") {
    std::string err;
    if (!self_test(err)) {
      std::cerr << "forge: " << err << "\n";
      return 1;
    }
    std::cout << "self-test ok\n";
    return 0;
  }
  if (cmd == "run") return cmd_run(std::move(args));
  if (cmd == "submit") return cmd_submit(std::move(args));
  if (cmd == "device") {
    if (args.size() == 1 && args[0] == "list") return cmd_device();
    usage(std::cerr);
    return 1;
  }
  std::string ws_flag;
  take_flag(args, "--workspace", ws_flag);
  const std::string root = resolve_workspace(ws_flag);
  if (cmd == "jobs") return cmd_jobs(root);
  if (cmd == "status") return cmd_status(root);
  if (cmd == "storage") {
    if (args.size() == 1 && args[0] == "stats") return cmd_storage(root);
    usage(std::cerr);
    return 1;
  }
  const uint64_t id = need_id(args);
  if (id == 0) {
    std::cerr << "forge: need a job id\n";
    return 1;
  }
  if (args.size() > 1) {
    std::cerr << "forge: unexpected argument " << args[1] << "\n";
    return 1;
  }
  if (cmd == "logs") return cmd_logs(root, id);
  if (cmd == "inspect") return cmd_inspect(root, id);
  if (cmd == "cancel") return cmd_cancel(root, id);
  usage(std::cerr);
  return 1;
}

}  // namespace forge

int main(int argc, char** argv) { return forge::dispatch(argc, argv); }
