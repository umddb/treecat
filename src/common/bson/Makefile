CXX := g++
INCLUDES := -I../../../thirdparty/install/include -I../../../thirdparty -I../..
CXXFLAGS := -Wall -std=c++20 -MD 

SRCS := $(wildcard *.cpp)
OBJS := $(patsubst %.cpp,%.o,$(SRCS))

RELEASE_CXXFLAGS := -g -O2
DEBUG_CXXFLAGS := -g -O0

all: release

release: CXXFLAGS += $(RELEASE_CXXFLAGS) $(INCLUDES)
debug: CXXFLAGS += $(DEBUG_CXXFLAGS) $(INCLUDES)

release: $(OBJS)
debug: $(OBJS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -rf *.o *.d
	
.PHONY: all clean release debug
	
-include $(OBJS:.o=.d)