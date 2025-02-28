// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef EXEC_NODE_H
#define EXEC_NODE_H

#include <chrono>

#include "exec/execbuffer.h"
#include "concurrency/txn.h"
#include "grpc/grpccatalog.pb.h"

class ExecNode;

struct QuerySession {
    ExecNode * exec_node_;
    uint64_t vid_;
    std::chrono::time_point<std::chrono::steady_clock> start_;
    std::chrono::nanoseconds diff_;

    QuerySession(): exec_node_(nullptr), vid_(0), diff_(0) {  }

};

class ExecNode {
    public:
        enum class Type {
            INNER = 0x01,
            LEAF = 0x02,
            INNER_LEAF = 0x03
        };

        virtual ~ExecNode() {}
        virtual bool next() = 0;
        virtual ExecBuffer * getOutputBuffer() = 0;

        virtual const Predicate & getPred() = 0;
        virtual ExecNode * getChild() = 0;

        virtual void setLowerBound(const std::string & lower_bound) = 0;
        virtual void setUpperBound(const std::string & upper_bound) = 0;
        virtual void setTightBound(const std::string & tight_bound) = 0;
        

};

inline ExecNode::Type operator |(ExecNode::Type lhs, ExecNode::Type rhs){
    using T = std::underlying_type_t <ExecNode::Type>;
    return static_cast<ExecNode::Type>(static_cast<T>(lhs) | static_cast<T>(rhs));
}
    
inline ExecNode::Type& operator |=(ExecNode::Type& lhs, ExecNode::Type rhs){
    lhs = lhs | rhs;
    return lhs;
}

inline ExecNode::Type operator &(ExecNode::Type lhs, ExecNode::Type rhs){
    using T = std::underlying_type_t <ExecNode::Type>;
    return static_cast<ExecNode::Type>(static_cast<T>(lhs) & static_cast<T>(rhs));
}
    
inline ExecNode::Type& operator &=(ExecNode::Type& lhs, ExecNode::Type rhs){
    lhs = lhs & rhs;
    return lhs;
}

#endif