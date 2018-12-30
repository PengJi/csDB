#ifndef ENGINE_RACE_UTIL_H_
#define ENGINE_RACE_UTIL_H_

#include <iostream>
#include <cstring>
#include <vector>
#include <fstream>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

namespace polar_race {

class Status;
class FileLock;
class LockOpe;
class FileOpe;

bool FileExists(const std::string& path);
// Hash
uint32_t StrHash(const char* s, int size);

/**
 * @brief      文件锁
 */
class FileLock {
 public:
  FileLock() { }
  ~FileLock() { }

    int fd_;
    std::string name_;
 private:
  // No copying allowd
  FileLock(const FileLock&);
  void operator=(const FileLock&);
};

/**
 * @brief      锁操作
 */
class LockOpe{
public:
  int LockOrUnlock(int fd, bool lock);

  int LockFile(const std::string& fname, FileLock** lock);

  int UnlockFile(FileLock* lock);

};

/**
 * @brief      文件操作
 */
class FileOpe{
public:
  time_t TimeStamp() {
    time_t timer;
    time(&timer);
    return timer; 
  }

  //得到目录下的文件
  int GetDirFiles(const std::string& name, std::vector<std::string>& files);

  //得到文件的大小（字节）
  int GetFileLength(const std::string& file);

  //判断文件是否存在
  bool FileExists(const std::string& name);

  //找到文件最大
  int32_t FindMaximumId(const std::vector<std::string>& files);

private:
  FileOpe(const FileOpe& fileope);
  void operator=(const FileOpe& fileope);
};

/**
 * @brief      状态
 */
class Status {
 public:
  Status() : code_(kOk) { }
  virtual ~Status() { }
  
  static Status OK() { return Status(); }

  static Status NotFound(const std::string& msg) {
    return Status(kNotFound, msg); 
  }
  static Status IOError(const std::string& msg) {
    return Status(kIOError, msg); 
  }

  bool ok() const { return (code_ == kOk); }

  bool IsNotFound() const { return code() == kNotFound; }

  bool IsIOError() const { return code() == kIOError; }

  std::string ToString() const {
      return msg_;
  }
 private:
  enum Code {
    kOk = 0,
    kNotFound = 1,
    kIOError = 2
  };

  Code code_;
  std::string msg_;

  Code code() const {
    return code_;
  }

  Status(Code code, const std::string& msg)
          : code_(code),
          msg_(msg) { }
};

}

#endif
