// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_

#include <string>
#include <iostream>
#include <cstring>
#include <vector>
#include <fstream>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>

#include "include/engine.h"
#include "door_plate.h"
#include "util.h"

namespace polar_race {

/**
 * typedef signed char           int8_t;
 * typedef signed short          int16_t;
 * typedef signed int            int32_t;
 * typedef signed long long      int64_t;
 * typedef unsigned char         uint8_t;
 * typedef unsigned short        uint16_t;
 * typedef unsigned int          uint32_t;
 * typedef unsigned long long    uint64_t;
 */

const std::string HintDirectory = "/hint";
const std::string DataDirectory = "/data";
const std::string DataFileName = "data";
const std::string HintFileName = "hint";
const std::string LOCK = "/LOCK";

// Key-value
struct BitcaskData {
  time_t timestamp;
  int64_t key_len;
  std::string key;
  int64_t value_len;
  std::string value;
};

struct Options {
  Options() :
    max_file_size(1073741824),  //字节，1G
    max_index_size(1073741824), //字节，1G
    read_write(true) { }

  // The size of data file
  size_t max_file_size;
 
  // The size of index file 
  size_t max_index_size;

  // If this process is going to be a writer and not just a reader
  bool read_write;
};

class EngineRace : public Engine  {
 public:
  explicit EngineRace(const std::string& dir) 
  : db_lock_(NULL), plate_(dir), lock_(NULL), file_(NULL){
  }

  ~EngineRace();

  static RetCode Open(const std::string& name, Engine** eptr);

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  /*
   * NOTICE: Implement 'Range' in quarter-final,
   *         you can skip it in preliminary.
   */
  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;

  // 新添加
  //RetCode Delete(const std::string& key) override;

  // 新添加
  RetCode Close();

 private: 
  pthread_mutex_t mu_;
  std::string dbname_;
  Options options_;
  FileOpe* file_;
  LockOpe* lock_;
  FileLock* db_lock_;  //锁
  DoorPlate plate_;  //查找 hint file
  //std::unordered_map<std::string, BitcaskIndex> index_;

  // Active data file
  int32_t active_id_;
  int64_t active_file_size_;
  std::ofstream active_file_;

  // Hint file
  int32_t hint_id_;
  int64_t hint_file_size_;
  std::ofstream hint_file_;

  RetCode Init(const std::string dbname);

  RetCode NewFileStream(std::ofstream& file_stream,
                       int32_t& id,
                       int64_t& file_size,
                       const std::string& directory,
                       const std::string& filename);

  int64_t SyncData(const BitcaskData& data);

  RetCode SyncHint(const BitcaskIndex& index);

  RetCode Retrieve(const std::string& key,
                   const int32_t file_id,
                   const int64_t data_pos,
                   time_t* timestamp,
                   std::string* value);

  RetCode LoadHint(const std::string& file, BitcaskIndex &index);

  EngineRace(const EngineRace& er);
  void operator=(const EngineRace& er);

};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
