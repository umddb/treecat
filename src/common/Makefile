CXX := g++
INCLUDES := -I../../thirdparty/install/include -I..
CXXFLAGS := -Wall -std=c++20 -MD 

SRCS := $(wildcard *.cc)
OBJS := $(patsubst %.cc,%.o,$(SRCS))

RELEASE_CXXFLAGS := -g -O2
DEBUG_CXXFLAGS := -g -O0

SUBDIRS := bson

all: release 

release: CXXFLAGS += $(RELEASE_CXXFLAGS) $(INCLUDES)
debug: CXXFLAGS += $(DEBUG_CXXFLAGS) $(INCLUDES)

release: $(OBJS) $(SUBDIRS)
debug: $(OBJS) $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -rf *.o *.d
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done
	
.PHONY: all clean release debug $(SUBDIRS)
	
-include $(OBJS:.o=.d)