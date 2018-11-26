// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_DOOR_PLATE_H_
#define ENGINE_RACE_DOOR_PLATE_H_
#include <string.h>
#include <stdint.h>
#include <map>
#include <string>

#include "include/engine.h"

namespace polar_race {

static const int kMaxKeyLen = 8;  //key 固定 8B，value 为 4KB

// 索引
struct BitcaskIndex {
  BitcaskIndex() : key_len(0), in_use(0){
  }
  time_t timestamp;
  int64_t key_len;
  std::string key;
  int32_t file_id;  //所在物理文件
  int64_t data_pos;  //value在物理文件中的位置
  bool vaild;
  uint8_t in_use;
};

// Hash index for key
class DoorPlate  {
 public:
    explicit DoorPlate(const std::string& path);
    ~DoorPlate();

    RetCode Init();

    RetCode AddOrUpdate(const std::string& key, const BitcaskIndex& l);

    RetCode Find(const std::string& key, BitcaskIndex *location_hint);

    // RetCode GetRangeLocation(const std::string& lower, const std::string& upper,
    //     std::map<std::string, Location> *locations);

 private:
    std::string dir_;
    int fd_;
    BitcaskIndex *bi_;


    int CalcIndex(const std::string& key);
};

}  // namespace polar_race

#endif  // ENGINE_EXAMPLE_DOOR_PLATE_H_
