include ../src/storage/make_config.mk

export PKG_CONFIG_PATH = $(realpath ../thirdparty/install/lib/pkgconfig)
CXX := g++
INSTALL_PATH := ../thirdparty/install
LIBS := "-I$(INSTALL_PATH)/lib"
INCLUDES := -I$(INSTALL_PATH)/include -I../src
CXXFLAGS := -Wall -std=c++20 -MD $(INCLUDES)

SRCS := $(wildcard *.cc)
OBJS := $(patsubst %.cc,%.o,$(SRCS))

RELEASE_CXXFLAGS := -g -O2
DEBUG_CXXFLAGS := -g -O0

LDFLAGS += -L/usr/local/lib -L$(LIBS) `pkg-config --libs --static protobuf grpc++`\
           -pthread\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl

EXEC_LDFLAGS := -L/usr/local/lib -L$(LIBS) $(EXEC_LDFLAGS) -lrocksdb

BSON_LDFLAGS := $(INSTALL_PATH)/../IntelRDFPMathLib20U2/LIBRARY/libbid.a\
			$(LIBS)/libabsl_raw_hash_set.a\
			$(LIBS)/libabsl_raw_logging_internal.a\
			$(LIBS)/libabsl_hash.a\
			$(LIBS)/lib/libabsl_city.a\
			$(LIBS)/lib/libabsl_low_level_hash.a\
			-L/usr/local/lib\
			-L$(LIBS) -lfmt

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

GRPCOBJ = $(wildcard ../src/grpc/*.o)

BACKENDOBJ = $(wildcard ../src/backend/*.o)

STORAGEOBJ = $(wildcard ../src/storage/*.o)

BSONOBJ := $(wildcard ../src/common/bson/*.o)

COMMONOBJ := $(wildcard ../src/common/*.o)

EXECOBJ := $(wildcard ../src/exec/*.o)

CONCOBJ := $(wildcard ../src/concurrency/v1/*.o) $(wildcard ../src/concurrency/v2/*.o)\
			$(wildcard ../src/concurrency/v3/*.o)

CLIENTOBJ := $(wildcard ../src/client/*.o)


all: release

release: CXXFLAGS += $(RELEASE_CXXFLAGS) $(INCLUDES)
debug: CXXFLAGS += $(DEBUG_CXXFLAGS) $(INCLUDES)

release: runtree
debug: runtree

runtree: runtree.o
	$(CXX) -o runtree $(STORAGEOBJ) $(BSONOBJ) $(BACKENDOBJ) $(GRPCOBJ) $(COMMONOBJ) \
	$(EXECOBJ) $(CONCOBJ) runtree.o $(CXXFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) $(BSON_LDFLAGS) \
	$(LDFLAGS) $(PLATFORM_LDFLAGS) 

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -rf runtree *.o *.d
	
.PHONY: all clean release debug
	
-include $(OBJS:.o=.d)