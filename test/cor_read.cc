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

  ret = engine->Read("aaa", &value);
  if(ret == kNotFound)
    printf("aaa kNotFound\n");
  else
    printf("aaa value: %s\n", value.c_str());

  value.clear();
  ret = engine->Read("bbb", &value);
  if(ret == kNotFound)
    printf("bbb kNotFound\n");
  else
    printf("bbb value: %s\n", value.c_str());

  value.clear();
  ret = engine->Read("ccc", &value);
  if(ret == kNotFound)
    printf("ccc kNotFound\n");
  else
    printf("ccc value: %s\n", value.c_str());

  value.clear();
  ret = engine->Read("ddd", &value);
  if(ret == kNotFound)
    printf("ddd kNotFound\n");
  else
    printf("ddd value: %s\n", value.c_str());


  return kSucc;
}
