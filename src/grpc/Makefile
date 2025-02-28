CXX := g++
INCLUDES := -I../../thirdparty/install/include -I..
CXXFLAGS := -Wall -std=c++20 -MD 

SRCS := $(wildcard *.cc) grpccatalog.pb.cc grpccatalog.grpc.pb.cc
OBJS := $(patsubst %.cc,%.o,$(SRCS)) 
PROTO_BIN := ../../thirdparty/install/bin
PROTO_SRC := grpccatalog.proto
PROTO_GEN := grpccatalog.pb.cc grpccatalog.pb.h grpccatalog.grpc.pb.cc grpccatalog.grpc.pb.h 

RELEASE_CXXFLAGS := -g -O2
DEBUG_CXXFLAGS := -g -O0

all: release

release: CXXFLAGS += $(RELEASE_CXXFLAGS) $(INCLUDES)
debug: CXXFLAGS += $(DEBUG_CXXFLAGS) $(INCLUDES)

release: $(PROTO_GEN) $(OBJS)
debug: $(PROTO_GEN) $(OBJS)

$(PROTO_GEN): $(PROTO_SRC)
	$(PROTO_BIN)/protoc -I=. --grpc_out=. --plugin=protoc-gen-grpc=$(PROTO_BIN)/grpc_cpp_plugin ./grpccatalog.proto
	$(PROTO_BIN)/protoc -I=. --cpp_out=. ./grpccatalog.proto

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -rf *.o *.d
	
.PHONY: all clean release debug
	
-include $(OBJS:.o=.d)