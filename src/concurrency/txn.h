// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef TXN_H
#define TXN_H

#include <type_traits>

#include "common/path.h"
#include "grpc/grpccatalog.pb.h"


class Transaction {
    public:
        class ReadSet;

        virtual ReadSet * readSet() = 0;
        virtual uint64_t readVid() = 0;
        virtual void addQueryRequest(ExecuteQueryRequest * query_request) = 0;
        virtual ~Transaction() {}

        class ReadSet {
            public:
                virtual ~ReadSet() {}
        };
        
};

#endif