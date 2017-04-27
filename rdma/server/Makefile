.PHONY: clean

CFLAGS  := -Wall -Werror -g
LD      := g++
CXXFLAGS := -std=c++11
LDFLAGS := ${LDFLAGS} -libverbs -lrdmacm -lpthread

APPS    := rdma-server

all: ${APPS}

rdma-server: memoryserver.hpp memoryserver.o
	${LD} -o $@ $^ ${CXXFLAGS} ${LDFLAGS}

clean:
	rm -f *.o ${APPS}

