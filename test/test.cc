#include <assert.h>
#include <stdio.h>
#include <string>
#include "include/engine.h"

using namespace polar_race;

class DumpVisitor : public Visitor{
public:
  DumpVisitor(int *kcnt): key_cnt_(kcnt){}

  ~DumpVisitor(){}

  void Visitor(const PolarString& key, const PolarString& value){
    printf("Visit %s --> %s\n", key.data(), value.data());
    (*key_cnt_)++;
  }

private:
  int* key_cnt_;
};

int main(){
  Engine *engine = NULL;
  std::string value;

  RetCode ret = Engine::Open("mydb", &engine);
  assert(ret == kSucc);

  ret = engine->Write("aaa", "1111111");
  assert(ret == kSucc);
  ret = engine->Write("aaa", "2222222222");
  ret = engine->Write("aaa", "333333");
  ret = engine->Write("aaa", "444");

  ret = engine->Write("bbb", "55555555");
  assert(ret == kSucc);

  ret = engine->Write("ccd", "cbbbbbbb");

  ret = engine->Read("aaa", &value);
  printf("Read aaa value: %s\n", value.c_str());

  value.clear();
  ret = engine->Read("bbb", &value);
  assert(ret == kSucc);
  printf("Read bbb value: %s\n", value.c_str());

  /*
  int key_cnt = 0;
  DumpVisitor visitor(&key_cnt);
  ret = engine->Range("b", "", vistor);
  assert(ret == kSucc);
  printf("Range key cnt: %d\n", key_cnt);
  */

  return kSucc;
}