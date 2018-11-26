// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include "door_plate.h"

namespace polar_race {

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);

  polar_race::Options options;
  engine_race->options_ = options;

  // 初始化
  RetCode ret = engine_race->Init(name); 
  if(ret != kSucc){
    delete engine_race;
    return ret;
  }

  ret = engine_race->plate_.Init();
  if(ret != kSucc){
    delete engine_race;
    return ret;
  }

  *eptr = engine_race;
  return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
  // 释放锁
  if (db_lock_) {
    lock_->UnlockFile(db_lock_);
  }
}

// 初始化
RetCode EngineRace::Init(std::string dbname){
  RetCode ret = kSucc;
  int res;  //结果状态

  dbname_ = dbname;

  if(!file_ -> FileExists(dbname_)){
    //file_ -> CreateDir(dbname_);
    res =  mkdir(dbname_.c_str(), 0755);
    if(res != 0){
      std::cerr << "lock file failed. errno:" << errno << " " << strerror(errno) << std::endl;
    }
  }

  if(options_.read_write){
    res = lock_->LockFile(dbname_ + LOCK, &db_lock_);
    if(res != kSucc){
        std::cerr << "lock file failed " << strerror(errno) << std::endl;
        return kIncomplete;
    }
  }

  std::vector<std::string> hint_files;
  if(file_ -> FileExists(dbname_ + HintDirectory)){
    res = file_ -> GetDirFiles(dbname_ + HintDirectory, hint_files);
    if(res != kSucc){
        std::cerr << "get files failed " << strerror(errno) << std::endl;
        return kIncomplete;
    }
  }else{
    //file_ -> CreateDir(dbname_ + HintDirectory);
    res = mkdir((dbname_ + HintDirectory).c_str(), 0755);
    if (res != 0){
      std::cerr << "lock file failed. errno:" << errno << " " << strerror(errno) << std::endl;
    }
  }

  //找到 hint file 的最大id
  hint_id_ = file_ -> FindMaximumId(hint_files);

  //如果存在索引文件，则加载到内存
  BitcaskIndex hint_file;
  for(auto file : hint_files){
    this -> LoadHint(file, hint_file);
  }

  //找到 active file 的最大id
  if(file_ -> FileExists(dbname_ + DataDirectory)){
    std::vector<std::string> db_files;
    res = file_ -> GetDirFiles(dbname_ + DataDirectory, db_files);
    if(res != kSucc){
        std::cerr << "get files failed " << strerror(errno) << std::endl;
        return kIncomplete;
    }else{
        active_id_ = file_ -> FindMaximumId(db_files);
    }
  }else{
    //file_ -> CreateDir(dbname_ + DataDirectory);
    res = mkdir((dbname_ + DataDirectory).c_str(), 0755);
    if(res != 0){
      std::cerr << "lock file failed. errno:" << errno << " " << strerror(errno) << std::endl;
    }
  }

  ret = this->NewFileStream(active_file_, active_id_, active_file_size_, 
                            DataDirectory, DataFileName);
  if(ret != kSucc)
    return ret;

  ret = this->NewFileStream(hint_file_, hint_id_, hint_file_size_, 
                            HintDirectory, HintFileName);
  if(ret != kSucc)
    return ret;

  return ret;
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  pthread_mutex_lock(&mu_);

  RetCode ret;

  BitcaskData data;  //数据
  data.timestamp = file_->TimeStamp();
  data.key_len = static_cast<int64_t >(key.size());
  data.key = key.ToString();
  data.value_len = static_cast<int64_t >(value.size());
  data.value = value.ToString();

  // 写到文件，落盘
  int64_t pos = this->SyncData(data);  //返回数据的开始位置
  if (pos < 0) {
    //Write data failed.
    pthread_mutex_unlock(&mu_);
    return kIOError;
  }

  BitcaskIndex index;  //索引
  index.timestamp = data.timestamp;
  index.key_len = static_cast<int64_t >(key.size());
  index.key = key.ToString();
  index.file_id = active_id_;
  index.data_pos = pos;
  index.vaild = true;  //数据是否过期（删改）可用

  //std::cerr << "index.file_id: " << active_id_ << std::endl;
  //std::cerr << "index.data_pos: " << pos << std::endl;
  
  // 更新索引
  //index_[key.ToString()] = index;
  ret = plate_.AddOrUpdate(key.ToString(), index);
  if(ret != kSucc){
    pthread_mutex_unlock(&mu_);
    return kIOError;
  }

  // hint 落盘
  // ret = this->SyncHint(index);
  // if (ret != kSucc) {
  //   pthread_mutex_unlock(&mu_);
  //   return kIOError;
  // }

  pthread_mutex_unlock(&mu_);
  return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  pthread_mutex_lock(&mu_);

  //auto find = index_.find(key.ToString()); //改进
  BitcaskIndex index;
  RetCode ret;

  ret = plate_.Find(key.ToString(), &index);

  //if (find != index_.end() && find->second.vaild) {
  if (ret == kSucc) {
    int32_t file_id = index.file_id;
    int64_t data_pos = index.data_pos;
    time_t timestamp;
    //s = this->Retrieve(key, file_id, data_pos, &timestamp, value);
    pthread_mutex_unlock(&mu_);
    return this->Retrieve(key.ToString(), file_id, data_pos, &timestamp, value);
  } else {
    //return s.NotFound("Not Found"); 
    pthread_mutex_unlock(&mu_);
    return kNotFound;
  }

  return kSucc;
}

/*
// 删除
RetCode EngineRace::Delete(const std::string& key){

  return kSucc;
}
*/

//打开文件
RetCode EngineRace::NewFileStream(std::ofstream& file_stream,
                              int32_t& id,
                              int64_t& file_size,
                              const std::string& directory,
                              const std::string& filename) {
  if (options_.read_write) { // Writable and readable
    file_stream.open(dbname_ + directory + "/" + filename + std::to_string(id),
                      std::ios::in |
                      std::ios::out|
                      std::ios::app|
                      std::ios::ate|
                      std::ios::binary);
    file_size = file_stream.tellp();
  } else {  // Only readable
    file_stream.open(dbname_ + directory + "/" + filename + std::to_string(id),
                      std::ios::out|
                      std::ios::binary);
  }
  
  return kSucc;
}

// 刷数据
int64_t EngineRace::SyncData(const BitcaskData& data) {
  RetCode ret;
  int64_t pos = active_file_size_;

  // 判断文件大小
  if (active_file_size_ < options_.max_file_size) {
    active_file_.write((char *)(&data.timestamp), sizeof(time_t));
    active_file_.write((char *)(&data.key_len), sizeof(int64_t));
    active_file_.write(data.key.c_str(), data.key_len);
    active_file_.write((char *)(&data.value_len), sizeof(int64_t));
    active_file_.write(data.value.c_str(), data.value_len);
    
    active_file_size_ += sizeof(time_t) + sizeof(int64_t) * 2 + data.key_len + data.value_len;
    std::cerr << "SyncData, active_file_size_: " << active_file_size_ << std::endl;

    //std::cerr << "data, active_id_: " << active_id_ << std::endl;

    active_file_.flush();
    return pos;
  } else {  //超过最大文件长度，新建文件
    active_file_.close();

    // Open new data file
    active_id_++;
    ret = this->NewFileStream(active_file_, active_id_, active_file_size_, 
        DataDirectory, DataFileName);
    if(ret != kSucc)
        return kIOError;

    std::cerr << "data, new active_id_: " << active_id_ << std::endl;

    return this->SyncData(data);
  }
}

// 刷 hint file 到磁盘
RetCode EngineRace::SyncHint(const BitcaskIndex& index) {
  Status s;
  RetCode ret;

  // 判断 hint file 大小
  if(hint_file_size_ < options_.max_index_size) {
    hint_file_.write((char* )(&index.timestamp), sizeof(time_t));
    hint_file_.write((char* )(&index.key_len), sizeof(int64_t));
    hint_file_.write(index.key.c_str(), index.key_len);
    hint_file_.write((char* )(&index.file_id), sizeof(int32_t));
    hint_file_.write((char* )(&index.data_pos), sizeof(int64_t));
    
    hint_file_size_ += sizeof(time_t) + sizeof(int32_t) + sizeof(int64_t) * 2 + index.key_len;
    hint_file_.flush();
    return kSucc;
  } else {  //操作文件最大长度
    hint_file_.close();

    // Open new index file
    hint_id_++;
    //ret = this->NewFileStream(hint_file_, hint_id_, hint_file_size_, 
    //    HintDirectory, HintFileName);
    ret = kSucc;
    if (ret != kSucc) {
      return kIOError;
    }

    return this->SyncHint(index);
  }
}

// 读取文件数据
RetCode EngineRace::Retrieve(const std::string& key,
                         const int32_t file_id,
                         const int64_t data_pos,
                         time_t* timestamp,
                         std::string* value) {
  std::ifstream file_stream;

  file_stream.open(dbname_ + DataDirectory + "/" + DataFileName + std::to_string(file_id),
                    std::ios::out|
                    std::ios::binary);
  file_stream.seekg(data_pos, std::ios_base::beg);

  int64_t key_len = 0, value_len = 0;
  file_stream.read((char* )(&timestamp), sizeof(time_t));
  file_stream.read((char* )(&key_len), sizeof(int64_t));

  auto read_key = new char[key_len];
  file_stream.read(read_key, key_len);

  file_stream.read((char* )(&value_len), sizeof(int64_t));

  auto read_value = new char[value_len];
  file_stream.read(read_value, value_len);
  value->append(read_value, static_cast<unsigned long>(value_len));

  std::cerr << "Retrieve value: "<< *value <<std::endl;

  file_stream.close();

  return kSucc;
}

// 启动时，根据hint文件，创建 hash index
RetCode EngineRace::LoadHint(const std::string &file, BitcaskIndex &index) {
  std::ifstream file_stream;

  file_stream.open(dbname_ + DataDirectory + "/" + file,
                    std::ios::out|
                    std::ios::binary);
  file_stream.seekg(0, std::ios_base::beg);

  while (file_stream.eof()) {
    //BitcaskIndex index;
    file_stream.read((char* )(&index.timestamp), sizeof(time_t));
    file_stream.read((char* )(&index.key_len), sizeof(int64_t));
    auto read_key = new char[index.key_len];
    file_stream.read(read_key, index.key_len);
    file_stream.read((char* )(&index.file_id), sizeof(int32_t));
    file_stream.read((char* )(&index.data_pos), sizeof(int64_t));
    file_stream.read((char* )(&index.vaild), sizeof(bool));

    // if (index.vaild) {
    //   index_[index.key] = index;
    // }
  }
  
  file_stream.close();
  return kSucc;
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  return kSucc;
}

}  // namespace polar_race
