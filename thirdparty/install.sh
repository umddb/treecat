#!/bin/bash

# List of libraries that need to be installed
# grpc
# re2
# boost >= 1.83.0
# jemmalloc
# rocksdb with USE_RTTI=1
# fmt 7.1.3 (https://github.com/fmtlib/fmt/releases/tag/7.1.3)
# libpqxx master
# python3 faker
# python3 ct3 (Cheetah)

set -e

THIRDPARTY_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR=$THIRDPARTY_DIR/install
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$INSTALL_DIR

cd $THIRDPARTY_DIR
# Make the destination directory for all thirdparty libraries
mkdir -p $INSTALL_DIR





# grpc (& re2 for pkg-conf resolution)
if [ ! -e $INSTALL_DIR/lib/libgrpc.a ]; then
    sudo apt install -y cmake
    sudo apt install -y build-essential autoconf libtool pkg-config libssl-dev libtbb-dev libnuma-dev
    if [ ! -e "grpc" ]; then
          git clone --recurse-submodules -b v1.64.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
    fi
    cd grpc
    mkdir -p cmake/build
    pushd cmake/build
    cmake -DgRPC_INSTALL=ON \
          -DgRPC_BUILD_TESTS=OFF \
          -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
          ../..
    make -j 4
    make install
    popd
    cd third_party/re2
    sed -i 's|prefix=/usr/local|prefix?=/usr/local|' Makefile
    make prefix=$INSTALL_DIR
    make install prefix=$INSTALL_DIR
    cd ../../.. 
fi


# jemmalloc
if [ ! -e $INSTALL_DIR/lib/libjemalloc.a ]; then
    sudo apt-get install autoconf
    if [ ! -e jemalloc ]; then
        git clone https://github.com/jemalloc/jemalloc.git
    fi
    cd jemalloc
    ./autogen.sh --prefix=$INSTALL_DIR
    make
    make install
    cd ..
fi


# boost
if [ ! -e $INSTALL_DIR/lib/libboost_atomic.a ]; then
    if [ ! -e boost_1_85_0 ]; then
        wget https://archives.boost.io/release/1.85.0/source/boost_1_85_0.tar.gz
        tar -xzf boost_1_85_0.tar.gz
        rm boost_1_85_0.tar.gz
    fi
    cd boost_1_85_0
    ./bootstrap.sh --prefix=$INSTALL_DIR
    ./b2 install
    cd ..
fi


# rocksdb
if [ ! -e $INSTALL_DIR/lib/librocksdb.a ]; then 
    sudo apt-get install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
    if [ ! -e rocksdb ]; then
        git clone https://github.com/facebook/rocksdb.git
    fi
    cd rocksdb
    make install USE_RTTI=1 PREFIX=$INSTALL_DIR -j4
    cd ..
fi


# IntelRDFPMathLib
if [ ! -e IntelRDFPMathLib20U2 ]; then
    wget http://www.netlib.org/misc/intel/IntelRDFPMathLib20U2.tar.gz
    tar -xzf IntelRDFPMathLib20U2.tar.gz
    rm IntelRDFPMathLib20U2.tar.gz
fi
cd IntelRDFPMathLib20U2/LIBRARY
make CC=gcc CALL_BY_REF=0 GLOBAL_RND=0 GLOBAL_FLAGS=0 UNCHANGED_BINARY_FLAGS=0
cd ../..


# fmt
if [ ! -e $INSTALL_DIR/lib/libfmt.a ]; then
    if [ ! -e fmt-7.1.3 ]; then
        wget https://github.com/fmtlib/fmt/releases/download/7.1.3/fmt-7.1.3.zip
        unzip fmt-7.1.3.zip
        rm fmt-7.1.3.zip
    fi
    cd fmt-7.1.3
    mkdir -p build
    cd build
    cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR ..
    make
    make install
    cd ../..
fi

# python packages (Faker and Cheetah)
sudo apt install python3
sudo apt install python3-pip
pip install Faker
pip install CT3


# Worry about libpqxx later
# libpqxx

echo "Installed all third party libraries!"
