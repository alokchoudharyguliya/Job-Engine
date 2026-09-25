#include "forge.hpp"
#include "os/platform.hpp"

// store.cpp is every place Forge treats the kernel as a filesystem.
//
//   main.cpp --commands--> controller.cpp --fork--> worker.cpp
//                              |                        |
//                              +---- this file ---------+
//                                        |
//                               forge-workspace/  /proc  /sys
//
// Two durability choices live next to each other on purpose:
//   - jobs/<id>/meta and jobs/<id>/output go through atomic_write_file
//     (temp, fsync, rename, fsync the directory). A crash must not leave
//     a half-written result with a finished name.
//   - status.txt, written by the controller, is plain telemetry. A torn
//     read there is annoying, not corrupting. See controller.cpp.

#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <sstream>
#include <vector>

namespace forge {

std::string errno_string() {
  const int err = errno;
  return std::string(std::strerror(err)) + " (errno " + std::to_string(err) + ")";
}

bool write_all(int fd, const void* data, size_t n) {
  const char* p = static_cast<const char*>(data);
  size_t left = n;
  while (left > 0) {
    const ssize_t w = ::write(fd, p, left);
    if (w < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (w == 0) {
      errno = EIO;
      return false;
    }
    p += w;
    left -= static_cast<size_t>(w);
  }
  return true;
}

bool write_line(int fd, std::string_view line) {
  std::string out(line);
  if (out.empty() || out.back() != '\n') {
    out.push_back('\n');
  }
  // One write for a short line so two processes appending to the same
  // O_APPEND log don't interleave mid-line. POSIX makes the write itself
  // atomic for regular files opened O_APPEND; keeping the line in one
  // write is what makes that guarantee useful.
  return write_all(fd, out.data(), out.size());
}

bool parse_u64(std::string_view text, uint64_t& out) {
  if (text.empty()) {
    return false;
  }
  uint64_t value = 0;
  for (char ch : text) {
    if (ch < '0' || ch > '9') {
      return false;
    }
    const uint64_t digit = static_cast<uint64_t>(ch - '0');
    if (value > (UINT64_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  out = value;
  return true;
}

bool parse_i64(std::string_view text, int64_t& out) {
  if (text.empty()) {
    return false;
  }
  int sign = 1;
  if (text[0] == '-') {
    sign = -1;
    text.remove_prefix(1);
  }
  uint64_t mag = 0;
  if (!parse_u64(text, mag)) {
    return false;
  }
  if (sign < 0) {
    out = -static_cast<int64_t>(mag);
  } else {
    out = static_cast<int64_t>(mag);
  }
  return true;
}

namespace {

bool mkdir_one(const std::string& path, std::string& err) {
  if (::mkdir(path.c_str(), 0755) == 0 || errno == EEXIST) {
    struct stat st {};
    if (::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
      return true;
    }
    err = path + " exists but is not a directory";
    return false;
  }
  err = "mkdir " + path + ": " + errno_string();
  return false;
}

std::string absolute_path(const std::string& path) {
  if (!path.empty() && path[0] == '/') {
    return path;
  }
  char cwd[4096];
  if (::getcwd(cwd, sizeof(cwd)) == nullptr) {
    return path;
  }
  if (path.empty() || path == ".") {
    return std::string(cwd);
  }
  return std::string(cwd) + "/" + path;
}

void split_last(const std::string& path, std::string& dir, std::string& name) {
  const auto slash = path.find_last_of('/');
  if (slash == std::string::npos) {
    dir = ".";
    name = path;
    return;
  }
  dir = slash == 0 ? "/" : path.substr(0, slash);
  name = path.substr(slash + 1);
}

}  // namespace

std::string resolve_workspace(const std::string& flag) {
  std::string path = flag;
  if (path.empty()) {
    if (const char* env = std::getenv("FORGE_WORKSPACE")) {
      path = env;
    } else {
      path = "forge-workspace";
    }
  }
  return absolute_path(path);
}

std::string job_directory(const std::string& root, uint64_t id) {
  return root + "/jobs/" + std::to_string(id);
}

std::string control_fifo_path(const std::string& root) {
  return root + "/control.fifo";
}

bool ensure_workspace(const std::string& root, std::string& err) {
  if (!mkdir_one(root, err)) {
    return false;
  }
  if (!mkdir_one(root + "/jobs", err)) {
    return false;
  }
  const std::string fifo = control_fifo_path(root);
  if (::mkfifo(fifo.c_str(), 0666) < 0 && errno != EEXIST) {
    err = "mkfifo " + fifo + ": " + errno_string();
    return false;
  }
  struct stat st {};
  if (::stat(fifo.c_str(), &st) < 0 || !S_ISFIFO(st.st_mode)) {
    err = fifo + " is not a FIFO. Remove it and start the controller again.";
    return false;
  }
  return true;
}

UniqueFd acquire_controller_lock(const std::string& root, std::string& err) {
  const std::string path = root + "/controller.lock";
  UniqueFd fd(::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644));
  if (!fd) {
    err = "open " + path + ": " + errno_string();
    return UniqueFd();
  }
  // flock is attached to the open file description, not to the process the
  // way a POSIX record lock is. fork shares that description. The lock drops
  // when the last fd referring to it is closed, which is this UniqueFd in
  // the parent — not when a worker exits. GUIDE.md has you prove that by
  // starting a second controller while an exec job is running.
  // flock is an advisory lock on the open file description. LOCK_EX is
  // exclusive. LOCK_NB returns EWOULDBLOCK instead of sleeping, so a second
  // controller fails immediately instead of queueing behind the first.
  if (::flock(fd.get(), LOCK_EX | LOCK_NB) < 0) {
    if (errno == EWOULDBLOCK || errno == EAGAIN) {
      err = "a controller is already running for " + root;
    } else {
      err = "flock " + path + ": " + errno_string();
    }
    return UniqueFd();
  }
  return fd;
}

bool write_pid_file(const std::string& root, int pid, std::string& err) {
  // Not the lock. A stale pid file after kill -9 is normal; status checks
  // /proc before trusting it. The flock above is what actually excludes a
  // second controller.
  return atomic_write_file(root + "/controller.pid",
                           std::to_string(pid) + "\n", err);
}

bool read_pid_file(const std::string& root, int& pid, std::string& err) {
  std::string text;
  if (!read_text_file(root + "/controller.pid", text, err)) {
    return false;
  }
  while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
    text.pop_back();
  }
  uint64_t value = 0;
  if (!parse_u64(text, value) || value > 0x7fffffffULL) {
    err = "controller.pid does not contain a pid";
    return false;
  }
  pid = static_cast<int>(value);
  return true;
}

void remove_pid_file(const std::string& root) {
  ::unlink((root + "/controller.pid").c_str());
}

bool allocate_job_id(const std::string& root, uint64_t& id, std::string& err) {
  const std::string path = root + "/next_id";
  UniqueFd fd(::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644));
  if (!fd) {
    err = "open next_id: " + errno_string();
    return false;
  }
  // F_SETLKW sleeps until the other submit releases the lock. Two terminals
  // can submit at once; the ids stay unique. This is a POSIX record lock:
  // it belongs to the process, and any close of this file in this process
  // would drop it. We do not fork while it is held.
  struct flock fl {};
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = 0;
  // fcntl F_SETLKW is a POSIX record lock. Unlike flock, it belongs to the
  // process and it waits (the W) until the other submit releases it.
  // Two terminals can submit at once; the ids stay unique.
  while (::fcntl(fd.get(), F_SETLKW, &fl) < 0) {
    if (errno == EINTR) {
      continue;
    }
    err = "lock next_id: " + errno_string();
    return false;
  }

  char buf[64] = {};
  const ssize_t n = ::pread(fd.get(), buf, sizeof(buf) - 1, 0);
  uint64_t next = 1;
  if (n > 0) {
    std::string text(buf, static_cast<size_t>(n));
    while (!text.empty() && (text.back() == '\n' || text.back() == '\0')) {
      text.pop_back();
    }
    uint64_t current = 0;
    if (!text.empty() && !parse_u64(text, current)) {
      err = "next_id is corrupt: " + text;
      return false;
    }
    if (!text.empty()) {
      next = current + 1;
    }
  }
  const std::string body = std::to_string(next) + "\n";
  if (::ftruncate(fd.get(), 0) < 0 ||
      ::pwrite(fd.get(), body.data(), body.size(), 0) < 0 ||
      ::fsync(fd.get()) < 0) {
    err = "update next_id: " + errno_string();
    return false;
  }
  fl.l_type = F_UNLCK;
  ::fcntl(fd.get(), F_SETLK, &fl);
  id = next;
  return true;
}

namespace {

bool assign_meta_field(Meta& meta, const std::string& key, const std::string& val,
                       std::string& err) {
  auto need_u64 = [&](uint64_t& dest) {
    if (!parse_u64(val, dest)) {
      err = "bad number for " + key;
      return false;
    }
    return true;
  };
  auto need_i64 = [&](int64_t& dest) {
    if (!parse_i64(val, dest)) {
      err = "bad number for " + key;
      return false;
    }
    return true;
  };
  if (key == "id") {
    return need_u64(meta.id);
  }
  if (key == "state") {
    meta.state = val;
    return true;
  }
  if (key == "op") {
    meta.op = val;
    return true;
  }
  if (key == "input") {
    meta.input = val;
    return true;
  }
  if (key == "exec") {
    meta.exec_path = val;
    return true;
  }
  if (key == "io") {
    meta.io = val;
    return true;
  }
  if (key == "workers") {
    uint64_t n = 0;
    if (!need_u64(n)) return false;
    meta.workers = static_cast<int>(n);
    return true;
  }
  if (key == "priority") {
    int64_t n = 0;
    if (!need_i64(n)) return false;
    meta.priority = static_cast<int>(n);
    return true;
  }
  if (key == "nice") {
    int64_t n = 0;
    if (!need_i64(n)) return false;
    meta.nice = static_cast<int>(n);
    return true;
  }
  if (key == "cpu") {
    int64_t n = 0;
    if (!need_i64(n)) return false;
    meta.cpu = static_cast<int>(n);
    return true;
  }
  if (key == "mem_mb") return need_u64(meta.mem_mb);
  if (key == "cpu_sec") return need_u64(meta.cpu_sec);
  if (key == "max_fds") return need_u64(meta.max_fds);
  if (key == "max_out_mb") return need_u64(meta.max_out_mb);
  if (key == "deadline_s") return need_u64(meta.deadline_s);
  if (key == "procs") {
    meta.procs = val;
    return true;
  }
  if (key == "exits") {
    meta.exits = val;
    return true;
  }
  if (key == "bytes") return need_u64(meta.bytes);
  if (key == "digest") {
    meta.digest = val;
    return true;
  }
  if (key == "error") {
    meta.error = val;
    return true;
  }
  if (key == "submitted_ms") return need_i64(meta.submitted_ms);
  if (key == "started_ms") return need_i64(meta.started_ms);
  if (key == "finished_ms") return need_i64(meta.finished_ms);
  return true;  // unknown keys are kept out of the way so old binaries can read new files
}

}  // namespace

bool validate_meta(const Meta& meta, std::string& err) {
  const auto bad_text = [](const std::string& s) {
    return s.find('\n') != std::string::npos || s.find('\r') != std::string::npos;
  };
  if (bad_text(meta.input) || bad_text(meta.exec_path) || bad_text(meta.op)) {
    err = "paths and operation names cannot contain newlines";
    return false;
  }
  if (meta.op != "sha256" && meta.op != "chunk-sha256" && meta.op != "count" &&
      meta.op != "copy" && meta.op != "exec") {
    err = "unknown operation '" + meta.op +
          "'. Use sha256, chunk-sha256, count, copy, or exec.";
    return false;
  }
  if (meta.io != "mmap" && meta.io != "read" && meta.io != "direct") {
    err = "unknown --io '" + meta.io + "'. Use mmap, read, or direct.";
    return false;
  }
  if (meta.workers < 1 || meta.workers > kMaxWorkers) {
    err = "workers must be between 1 and " + std::to_string(kMaxWorkers);
    return false;
  }
  if (meta.op == "sha256" && meta.workers != 1) {
    err = "sha256 is sequential, so it always uses 1 worker. "
          "Use chunk-sha256 to hash slices in parallel "
          "(that output is not the sha256 of the whole file).";
    return false;
  }
  if ((meta.op == "copy" || meta.op == "exec") && meta.workers != 1) {
    err = meta.op + " uses one worker process";
    return false;
  }
  if (meta.op == "exec" && meta.exec_path.empty()) {
    err = "exec requires --exec /path/to/program";
    return false;
  }
  if (meta.op != "exec" && meta.input.empty()) {
    err = "missing --input";
    return false;
  }
  if (meta.nice < -20 || meta.nice > 19) {
    err = "nice must be between -20 and 19";
    return false;
  }
  return true;
}

std::string meta_to_text(const Meta& meta) {
  std::ostringstream out;
  out << "id=" << meta.id << "\n"
      << "state=" << meta.state << "\n"
      << "op=" << meta.op << "\n"
      << "input=" << meta.input << "\n"
      << "exec=" << meta.exec_path << "\n"
      << "io=" << meta.io << "\n"
      << "workers=" << meta.workers << "\n"
      << "priority=" << meta.priority << "\n"
      << "nice=" << meta.nice << "\n"
      << "cpu=" << meta.cpu << "\n"
      << "mem_mb=" << meta.mem_mb << "\n"
      << "cpu_sec=" << meta.cpu_sec << "\n"
      << "max_fds=" << meta.max_fds << "\n"
      << "max_out_mb=" << meta.max_out_mb << "\n"
      << "deadline_s=" << meta.deadline_s << "\n"
      << "procs=" << meta.procs << "\n"
      << "exits=" << meta.exits << "\n"
      << "bytes=" << meta.bytes << "\n"
      << "digest=" << meta.digest << "\n"
      << "error=" << meta.error << "\n"
      << "submitted_ms=" << meta.submitted_ms << "\n"
      << "started_ms=" << meta.started_ms << "\n"
      << "finished_ms=" << meta.finished_ms << "\n";
  return out.str();
}

bool text_to_meta(std::string_view text, Meta& meta, std::string& err) {
  meta = Meta{};
  std::string current(text);
  std::istringstream in(current);
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line[0] == '#') {
      continue;
    }
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    const auto eq = line.find('=');
    if (eq == std::string::npos) {
      err = "meta line has no '=': " + line;
      return false;
    }
    if (!assign_meta_field(meta, line.substr(0, eq), line.substr(eq + 1), err)) {
      return false;
    }
  }
  if (meta.id == 0 || meta.op.empty() || meta.state.empty()) {
    err = "meta is missing id, op, or state";
    return false;
  }
  return true;
}

bool read_text_file(const std::string& path, std::string& out, std::string& err) {
  UniqueFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (!fd) {
    err = "open " + path + ": " + errno_string();
    return false;
  }
  out.clear();
  char buf[8192];
  for (;;) {
    const ssize_t n = ::read(fd.get(), buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      err = "read " + path + ": " + errno_string();
      return false;
    }
    if (n == 0) {
      break;
    }
    if (out.size() + static_cast<size_t>(n) > (4u << 20)) {
      err = path + " is larger than 4MB; refusing to load it as text";
      return false;
    }
    out.append(buf, static_cast<size_t>(n));
  }
  return true;
}

bool atomic_write_file(const std::string& path, std::string_view data,
                       std::string& err) {
  // Durable replace of a small file.
  //
  //   jobs/12/meta.tmp.<pid>          a different inode from meta
  //          | write all bytes
  //          | fsync the inode          data is on disk before the name moves
  //          v
  //   renameat2(meta.tmp -> meta)      one directory update, not a torn file
  //          |
  //          | fsync the directory      the new name itself survives power loss
  //          v
  //   jobs/12/meta
  //
  // A reader that opens "meta" sees the old inode or the new one, never a
  // mix of the two. rename replaces the directory entry; it does not follow
  // a symlink that happens to be sitting at the destination name.
  std::string dir;
  std::string name;
  split_last(path, dir, name);
  UniqueFd dirfd(::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  if (!dirfd) {
    err = "open directory " + dir + ": " + errno_string();
    return false;
  }
  const std::string tmp = name + ".tmp." + std::to_string(::getpid());
  // openat resolves the name relative to the directory fd, not the process
  // cwd. The temp inode is a different file from the final name until rename.
  UniqueFd fd(::openat(dirfd.get(), tmp.c_str(),
                       O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644));
  if (!fd) {
    err = "open " + tmp + ": " + errno_string();
    return false;
  }
  if (!write_all(fd.get(), data.data(), data.size()) ||
      ::fsync(fd.get()) < 0) {
    err = "write " + tmp + ": " + errno_string();
    ::unlinkat(dirfd.get(), tmp.c_str(), 0);
    return false;
  }
  fd.reset();
  if (!os::replace_at(dirfd.get(), tmp.c_str(), name.c_str(), err)) {
    ::unlinkat(dirfd.get(), tmp.c_str(), 0);
    return false;
  }
  // The directory is a file too. Its data is the list of names. Without
  // this fsync a power loss can forget the rename even though the inode
  // was durable.
  if (::fsync(dirfd.get()) < 0) {
    err = "fsync directory " + dir + ": " + errno_string();
    return false;
  }
  return true;
}

bool publish_file(int dirfd, int filefd, const char* tmp_name,
                  const char* final_name, std::string& err) {
  // Same protocol as atomic_write_file, for a payload we streamed (copy).
  // The output is a normal buffered file on purpose: fsync has a defined
  // meaning there. O_DIRECT on the output would be a different exercise.
  if (::fsync(filefd) < 0) {
    err = std::string("fsync ") + tmp_name + ": " + errno_string();
    return false;
  }
  if (!os::replace_at(dirfd, tmp_name, final_name, err)) return false;
  if (::fsync(dirfd) < 0) {
    err = std::string("fsync job directory: ") + errno_string();
    return false;
  }
  return true;
}

bool save_meta(const std::string& root, const Meta& meta, std::string& err) {
  const std::string dir = job_directory(root, meta.id);
  if (::mkdir(dir.c_str(), 0755) < 0 && errno != EEXIST) {
    err = "mkdir " + dir + ": " + errno_string();
    return false;
  }
  return atomic_write_file(dir + "/meta", meta_to_text(meta), err);
}

bool load_meta(const std::string& root, uint64_t id, Meta& meta, std::string& err) {
  std::string text;
  if (!read_text_file(job_directory(root, id) + "/meta", text, err)) {
    return false;
  }
  return text_to_meta(text, meta, err);
}

bool append_job_log(const std::string& root, uint64_t id, std::string_view line,
                    std::string& err) {
  const std::string path = job_directory(root, id) + "/log";
  UniqueFd fd(::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644));
  if (!fd) {
    err = "open log: " + errno_string();
    return false;
  }
  if (!write_line(fd.get(), line)) {
    err = "write log: " + errno_string();
    return false;
  }
  return true;
}

int open_job_log(const std::string& root, uint64_t id, std::string& err) {
  const std::string path = job_directory(root, id) + "/log";
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) {
    err = "open log: " + errno_string();
  }
  return fd;
}

std::vector<uint64_t> list_job_ids(const std::string& root) {
  std::vector<uint64_t> ids;
  const std::string dir = root + "/jobs";
  DIR* dp = ::opendir(dir.c_str());
  if (dp == nullptr) {
    return ids;
  }
  while (dirent* ent = ::readdir(dp)) {
    if (ent->d_name[0] == '.') {
      continue;
    }
    uint64_t id = 0;
    if (parse_u64(ent->d_name, id)) {
      ids.push_back(id);
    }
  }
  ::closedir(dp);
  std::sort(ids.begin(), ids.end());
  return ids;
}

}  // namespace forge
