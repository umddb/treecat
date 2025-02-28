// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef BACKEND_SERVICE_H
#define BACKEND_SERVICE_H

#include <memory>

#include "boost/asio/thread_pool.hpp"


#include "common/config.h"
#include "common/clienttask.h"
#include "concurrency/txnmgr.h"
#include "storage/objstore.h"
#include "exec/execnode.h"
#include "exec/loader.h"

class BackendService {

    public:
        BackendService(Config* config);
        // disable copy constructor
        BackendService(const BackendService&) = delete;
        // disable assign constructor
        BackendService& operator=(const BackendService&) = delete;
        ~BackendService();
        void run();
        void enqueueTask(ClientTask* task, bool ok);
        void handleClientTask(ClientTask * task, bool ok);
        void handleStartTxn(ClientTask * task, bool ok);
        void handleExecuteQuery(ClientTask * task, bool ok);
        void handleCommit(ClientTask * task, bool ok);
        void handleBulkLoad(ClientTask * task, bool ok);
        void handleSnapshot(ClientTask * task, bool ok);
        void handleClone(ClientTask * task, bool ok);
        TransactionManager * transactionManager();


    private:
        const Config* config_;
        unsigned int thread_pool_size_;
        std::unique_ptr<boost::asio::thread_pool> thread_pool_;
        std::string db_path_;
        std::unique_ptr<ObjStore> obj_store_;
        std::unique_ptr<TransactionManager> txn_mgr_;
        uint32_t exec_buf_size_;
        int txn_mgr_version_;
        BufCompression compression_;

        // void executePlan(ExecNode * plan, ExecuteQueryResponse * response);

};

#endif //BACKEND_SERVICE_H