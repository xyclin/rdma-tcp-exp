.PHONY: clean

CFLAGS  := -Wall -Werror -g
LD      := g++
CXXFLAGS := -std=c++11
LDFLAGS := ${LDFLAGS} -libverbs -lrdmacm -lpthread

APPS    := rdma-client

all: ${APPS}

rdma-client: coordinatorclient.hpp coordinatorclient.o
	${LD} -o $@ $^ ${CXXFLAGS} ${LDFLAGS}

clean:
	rm -f *.o ${APPS}

