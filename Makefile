# -O2 so the hash and the copy are not dominated by a debug build.
# -g so gdb and perf can still name functions. Use `make debug` while
# stepping through the controller.
UNAME_S := $(shell uname -s)
CXX      ?= g++
CXXFLAGS ?= -std=c++20 -Wall -Wextra -Wpedantic -O2 -g -pthread -I.
LDFLAGS  ?= -pthread

ifeq ($(UNAME_S),Linux)
  OS_SRC := os/linux/platform.cpp
  CXXFLAGS += -D_GNU_SOURCE
else ifeq ($(UNAME_S),Darwin)
  OS_SRC := os/mac/platform.cpp
else
  $(error Forge has an OS folder for Linux and macOS, not for $(UNAME_S))
endif

SRCS := main.cpp controller.cpp worker.cpp store.cpp $(OS_SRC)
OBJS := $(SRCS:.cpp=.o)

.PHONY: all clean debug

all: forge

forge: $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(LDFLAGS)

%.o: %.cpp forge.hpp os/platform.hpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

debug:
	$(MAKE) clean
	$(MAKE) all CXXFLAGS="-std=c++20 -Wall -Wextra -Wpedantic -O0 -g -pthread"

clean:
	rm -f $(OBJS) forge
