#include "util.h"

namespace polar_race{

bool FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

static const int kA = 54059;  // a prime
static const int kB = 76963;  // another prime
static const int kFinish = 37;  // also prime
uint32_t StrHash(const char* s, int size) {
  uint32_t h = kFinish;
  while (size > 0) {
    h = (h * kA) ^ (s[0] * kB);
    s++;
    size--;
  }
  return h;
}

int LockOpe::LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

int LockOpe::LockFile(const std::string& fname, FileLock** lock) {
  *lock = NULL;
  int result = 0;
  int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    result = errno;
  } else if (LockOrUnlock(fd, true) == -1) {
    result = errno;
    close(fd);
  } else {
    FileLock* my_lock = new FileLock;
    my_lock->fd_ = fd;
    my_lock->name_ = fname;
    *lock = my_lock;
  }
  return result;
}

int LockOpe::UnlockFile(FileLock* lock) {
  int result = 0;
  if (LockOrUnlock(lock->fd_, false) == -1) {
    result = errno;
  }
  close(lock->fd_);
  delete lock;
  return result;
}

int FileOpe::GetDirFiles(const std::string& name, std::vector<std::string>& files) {
  files.clear();
  DIR* d = opendir(name.c_str());
  if (d == nullptr) {
    std::cerr << "Get directory children failed: " << strerror(errno) << std::endl;
    return errno;
  }
  struct dirent* entry;
  while ((entry = readdir(d)) != nullptr) {
    if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
      continue;
    }
    files.push_back(entry->d_name);
  }
  closedir(d);
  return 0;
}

int FileOpe::GetFileLength(const std::string& file) {
  struct stat stat_buf;
  int rc = stat(file.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

int32_t FileOpe::FindMaximumId(const std::vector<std::string> &files) {
  int32_t max = 0;
  char c_max;
  for (auto file : files) {
    c_max = static_cast<char >(file[file.size()] - 1);
    if (max < static_cast<int >(c_max)) {
      max = static_cast<int >(c_max);
    }
  }
  return max;
}

bool FileOpe::FileExists(const std::string& name) {
  return access(name.c_str(), F_OK) == 0;
}

}
