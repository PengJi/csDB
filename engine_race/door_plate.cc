// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <cstring>
#include <iostream>
#include <utility>

#include "util.h"
#include "door_plate.h"

namespace polar_race {

//static const uint32_t kMaxDoorCnt = 1024 * 1024 * 32;
static const uint32_t kMaxDoorCnt = 1024 * 1024 * 16;  //索引 BitcaskIndex 的最大数目
static const char kHashFile[] = "INDEX";
static const int kMaxRangeBufCount = kMaxDoorCnt;

// 判断key是否相同
static bool ItemKeyMatch(const BitcaskIndex &item, const std::string& target) {
  // std::cerr << "target.size(): "<<target.size() << std::endl;
  // std::cerr << "item.key_len: "<<item.key_len << std::endl;
  // std::cerr << "item.key: "<<item.key << std::endl;
  // std::cerr << "target: "<<target << std::endl;
  if (target.size() != item.key_len || item.key != target) {
  //if (true) {
    // Conflict
    std::cerr << "ItemKeyMatch false " << std::endl;
    return false;
  }
  std::cerr << "ItemKeyMatch true " << std::endl;
  return true;
}

// 哈希表冲突时，通过in_use 判断下一个位置是否在使用
static bool ItemTryPlace(const BitcaskIndex &item, const std::string& target) {
  if (item.in_use == 0) {  //没有使用
    return true;
  }
  return ItemKeyMatch(item, target); //已经使用了，则判断key是否相同
}

DoorPlate::DoorPlate(const std::string& path)
  : dir_(path),
  fd_(-1),
  bi_(NULL) {
  }

RetCode DoorPlate::Init() {
  bool new_create = false;
  const int map_size = kMaxDoorCnt * sizeof(BitcaskIndex);

  if (!FileExists(dir_)
      && 0 != mkdir(dir_.c_str(), 0755)) {
    return kIOError;
  }

  std::string path = dir_ + "/" + kHashFile;
  int fd = open(path.c_str(), O_RDWR, 0644);
  if (fd < 0 && errno == ENOENT) {
    // 不存在，则创建
    fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd >= 0) {
      new_create = true;
      if (posix_fallocate(fd, 0, map_size) != 0) {  //索引文件对应的内存一次性分配
        std::cerr << "posix_fallocate failed: " << strerror(errno) << std::endl;
        close(fd);
        return kIOError;
      }
    }
  }
  if (fd < 0) {
    return kIOError;
  }
  fd_ = fd;

  void* ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    std::cerr << "MAP_FAILED: " << strerror(errno) << std::endl;
    close(fd);
    return kIOError;
  }
  if (new_create) {
    memset(ptr, 0, map_size);
  }

  //items_ = reinterpret_cast<Item*>(ptr);
  bi_ = reinterpret_cast<BitcaskIndex*>(ptr);
  return kSucc;
}

DoorPlate::~DoorPlate() {
  if (fd_ > 0) {
    //const int map_size = kMaxDoorCnt * sizeof(Item);
    const int map_size = kMaxDoorCnt * sizeof(BitcaskIndex);
    //munmap(items_, map_size);
    munmap(bi_, map_size);
    close(fd_);
  }
}

// 哈希表，处理冲突是通过判断下一个位置是否可用
int DoorPlate::CalcIndex(const std::string& key) {
  uint32_t jcnt = 0;
  int index = StrHash(key.data(), key.size()) % kMaxDoorCnt;

  std::cerr << "CalcIndex bi_: "<< bi_ <<std::endl;
  std::cerr << "CalcIndex index: " << index << std::endl;
  //std::cerr << "CalcIndex bi_+index: " << (bi_ + index) << std::endl;
  std::cerr << "CalcIndex key: " << (bi_ + index)->key << std::endl;

  while (!ItemTryPlace(*(bi_ + index), key) && ++jcnt < kMaxDoorCnt) {
    // index处已经有bitcaskindex，计算下一个位置
    index = (index + 1) % kMaxDoorCnt;
    std::cerr << "CalcIndex index while: " << index << std::endl;
  }

  std::cerr << "CalcIndex index next: " << index << std::endl;

  if (jcnt == kMaxDoorCnt) {  //索引数目满
    // full
    return -1;
  }
  return index;
}

RetCode DoorPlate::AddOrUpdate(const std::string& key, const BitcaskIndex& b) {
  if (key.size() > kMaxKeyLen) {
    std::cerr << "key.size > kMaxKeyLen \n" << std::endl;
    return kInvalidArgument;
  }

  int index = CalcIndex(key);
  std::cerr << "AddOrUpdate CalcIndex: " << index << std::endl;

  if (index < 0) {  //索引数满
    return kFull;
  }

  BitcaskIndex* iptr = bi_ + index;
  if (iptr->in_use == 0) {  //新的索引 BitcaskIndex
    // new item
    //memcpy(const_cast<char*>(iptr->key.c_str()), key.data(), key.size());
    iptr->key = key;
    iptr->key_len = static_cast<int64_t >(key.size());
    iptr->in_use = 1;  // Place
  }

  iptr->timestamp = b.timestamp;
  iptr->file_id = b.file_id;
  iptr->data_pos = b.data_pos;
  iptr->vaild = true;

  return kSucc;
}

// 查找 BitcaskIndex
RetCode DoorPlate::Find(const std::string& key, BitcaskIndex *location_hint) {
  int index = CalcIndex(key);
  if (index < 0 || !ItemKeyMatch(*(bi_ + index), key)) {
    return kNotFound;
  }

  *location_hint = *(bi_ + index);
  return kSucc;
}

// RetCode DoorPlate::GetRangeLocation(const std::string& lower,
//     const std::string& upper,
//     std::map<std::string, Location> *locations) {
//   int count = 0;
//   for (Item *it = items_ + kMaxDoorCnt - 1; it >= items_; it--) {
//     if (!it->in_use) {
//       continue;
//     }
//     std::string key(it->key, it->key_size);
//     if ((key >= lower || lower.empty())
//         && (key < upper || upper.empty())) {
//       locations->insert(std::pair<std::string, Location>(key, it->location));
//       if (++count > kMaxRangeBufCount) {
//         return kOutOfMemory;
//       }
//     }
//   }
//   return kSucc;
// }

}  // namespace polar_race
