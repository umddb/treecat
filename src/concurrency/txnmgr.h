// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef TXN_MGR_H
#define TXN_MGR_H

#include "concurrency/txn.h"
#include "common/clienttask.h"
#include "grpc/grpccatalog.pb.h"

class TransactionManager {
    public:
        virtual ~TransactionManager() {}
        virtual uint64_t getReadVid() = 0;
        virtual Transaction * getTransaction(uint64_t txn_id) = 0;
        // leaky abstraction on grpc to avoid deadlock on resource (# threads) for locking scheme
        virtual Transaction * startTransaction() = 0;
        virtual void startTransaction(ClientTask * task) = 0;
        virtual void removeTransaction(uint64_t txn_id) = 0;
        virtual Transaction * popTransaction(uint64_t txn_id) = 0;
        virtual void commit(CommitRequest * commit_request, CommitResponse * commit_response) = 0;
        virtual bool commit(Transaction* txn) = 0;
};

#endif