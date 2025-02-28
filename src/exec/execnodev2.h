// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef EXEC_NODE_V2_H
#define EXEC_NODE_V2_H

#include "exec/execnode.h"
#include "exec/execbuffer.h"
#include "exec/evalexpr.h"
#include "grpc/grpccatalog.pb.h"
#include "storage/objstore.h"

#include "concurrency/txn.h"
#include "concurrency/v2/txnv2.h"


class ExecNodeV2 : public ExecNode {
    
    public:

        enum class LockOption {
            NONE,
            PARENT,
            RANGE,
            POINT
        };

        ExecNodeV2(ObjStore * obj_store, ExecNode* child_node, const Predicate & pred, bool is_last, bool base_only,
            unsigned int buf_size, uint64_t vid, uint32_t level, Type type, Transaction::ReadSet * read_set);
        ~ExecNodeV2();
        bool next() override;
        ExecBuffer * getOutputBuffer() override;
       
        const Predicate & getPred() override;
        ExecNode * getChild() override;

        void setLowerBound(const std::string & lower_bound) override;
        void setUpperBound(const std::string & upper_bound) override;
        void setTightBound(const std::string & tight_bound) override;
        
    private:
        ObjStore * obj_store_; 
        ExecNode * child_node_;
        const Predicate & pred_;
        bool is_last_;
        bool base_only_;
        unsigned int buf_size_;
        uint64_t vid_;
        uint32_t  level_;
        bool valid_;
        bool is_leaf_;
        bool init_;
        ExecBuffer * input_buffer_;
        ExecBuffer::Iterator input_buffer_iter_;
        ExecBuffer output_buffer_;
        LeafObjStore::Iterator * leaf_obj_iter_;
        InnerObjStore::Iterator * inner_obj_iter_;
        Type type_;
        TransactionV2::ReadSet * read_set_;
        // lock option is of 3 types: parent, range, and point. 
        LockOption lock_option_;
        std::unique_ptr<EvalExpr::EvalInfo> eval_info_;
        // lower bound and upper bound on the oid
        // lower bound is inclusive, upper_bound is exclusive
        LeafObjStore::ReadOptions leaf_obj_ro_;
        InnerObjStore::ReadOptions inner_obj_ro_;
        // Bound to a single oid 
        std::string tight_bound_;
        
        void processSingleInput();
        void processInput();
        void resetInputBufferIter();
        void resetObjIter(const Path & parent_path);
        void deleteObjIter();

};

#endif