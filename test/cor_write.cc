#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <string>
#include "include/engine.h"

using namespace polar_race;

int main(){
  RetCode ret;
  Engine *engine = NULL;
  std::string value;

  ret = Engine::Open("mydb", &engine);
  assert(ret == kSucc);
  printf("open mydb completed\n");

  ret = engine->Write("aaa", "1111111");
  assert(ret == kSucc);
  printf("write aaa 1 completed\n");

  ret = engine->Write("aaa", "2222222222");
  printf("write aaa 2 completed\n");
  sleep(1);

  ret = engine->Write("aaa", "333333");
  printf("write aaa 3 completed\n");
  ret = engine->Write("aaa", "444");
  printf("write aaa 4 completed\n");
  sleep(2);

  ret = engine->Write("bbb", "55555555");
  assert(ret == kSucc);
  printf("write bbb 5 completed\n");
  sleep(2);

  ret = engine->Write("ccc", "666666666");
  assert(ret == kSucc);
  printf("write ccc 6 completed\n");
  sleep(2);

  ret = engine->Write("ddd", "77777");
  assert(ret == kSucc);
  printf("write ddd 7 completed\n");
  sleep(2);
 
  sleep(1);

  return kSucc;
}
