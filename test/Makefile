CPP = g++ -std=c++11

CFLAGS = -g
LDFLAGS = -lengine -lpthread -lrt -lz
INC = -I../
LIB = -L../lib

all: test_with_kill

.PHONY: clean

test_with_kill:
	$(CPP) -o test_with_kill.o $(CFLAGS) $(INC) $(LIB) test_with_kill.cc $(LDFLAGS)

test_rw:
	$(CPP) -o test_rw.o $(CFLAGS) $(INC) $(LIB) test_rw.cc $(LDFLAGS)

test:
	$(CPP) -o test.o $(CFLAGS) $(INC) $(LIB) test.cc $(LDFLAGS)

write:
	$(CPP) -o write.o $(CFLAGS) $(INC) $(LIB) cor_write.cc $(LDFLAGS)

read:
	$(CPP) -o read.o $(CFLAGS) $(INC) $(LIB) cor_read.cc $(LDFLAGS)

clean:
	rm -rf *.o 
