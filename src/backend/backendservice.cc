// Author: Keonwoo Oh (koh3@umd.edu)

#include <thread>
#include <iostream>
#include <string_view>
#include <filesystem>

#include "snappy.h"
#include "boost/asio/post.hpp"

#include "backend/backendservice.h"
#include "concurrency/txnmgr.h"
#include "concurrency/v1/txnmgrv1.h"
#include "concurrency/v2/txnmgrv2.h"
#include "concurrency/v3/txnmgrv3.h"
#include "exec/clone.h"
#include "exec/planner.h"
#include "storage/objstore.h"


BackendService::BackendService(Config* config) : config_(config) {
    std::string thread_pool_size_str;
    std::string exec_buf_size_str;
    std::string txn_mgr_version_str;
    std::string compression_str;

    if (config_ != nullptr) {
        thread_pool_size_str = config_->getParam("backend.thread_pool_size");
        exec_buf_size_str = config_->getParam("backend.exec_buf_size");
        db_path_ = config_->getParam("backend.db_path");
        txn_mgr_version_str = config->getParam("backend.txn_mgr_version");
        compression_str = config->getParam("backend.compression");
    }
    
    if (!thread_pool_size_str.empty()) {
        try {
            thread_pool_size_ = std::stoul(thread_pool_size_str);
        }
        catch(...) {
            // Same as the default number of threads in the thread pool of GRPServer
            std::cout << "backend.thread_pool_size is not set to a valid integer. Using " << std::thread::hardware_concurrency() << " threads." << std::endl;
            thread_pool_size_ = std::thread::hardware_concurrency();
        }
    }
    else {
        std::cout << "backend.thread_pool_size is not set. Using " << std::thread::hardware_concurrency() << " threads." << std::endl;
        thread_pool_size_ = std::thread::hardware_concurrency();
    }

    if (!exec_buf_size_str.empty()) {
        try {
            exec_buf_size_ = std::stoul(exec_buf_size_str);
        }
        catch(...) {
            // Same as the default number of threads in the thread pool of GRPServer
            std::cout << "backend.exec_buf_size is not set to a valid integer. Using " << 32 * 1024 << " bytes." << std::endl;
            exec_buf_size_ = 32 * 1024;
        }
    }
    else {
        std::cout << "backend.exec_buf_size is not set. Using " << 32 * 1024 << " bytes." << std::endl;
        exec_buf_size_ = 32 * 1024;
    }

    if (db_path_.empty()) {
        db_path_ = "/tmp/catalogstorage/";
        std::cout << "backend.db_path is not set. Using /tmp/catalogstorage/" << std::endl;
    }

    // create obj_store
    bool create = !std::filesystem::exists(db_path_);
    obj_store_ = std::make_unique<ObjStore>(db_path_, config_, create);


    // intialize txn manager. Default is version 1
    if (!txn_mgr_version_str.empty()) {
        try {
            txn_mgr_version_ = std::stoi(txn_mgr_version_str);
            switch (txn_mgr_version_) {
                case 1:
                    txn_mgr_ = std::unique_ptr<TransactionManager>(static_cast<TransactionManager*>(new TransactionManagerV1(config, obj_store_.get(), db_path_, thread_pool_size_)));
                    break;
                case 2:
                    txn_mgr_ = std::unique_ptr<TransactionManager>(static_cast<TransactionManager*>(new TransactionManagerV2(config, obj_store_.get(), db_path_, thread_pool_size_)));
                    break;
                case 3:
                    txn_mgr_ = std::unique_ptr<TransactionManager>(static_cast<TransactionManager*>(new TransactionManagerV3(config, obj_store_.get(), thread_pool_size_)));
                    break;
                default:
                    txn_mgr_ = std::unique_ptr<TransactionManager>(static_cast<TransactionManager*>(new TransactionManagerV1(config, obj_store_.get(), db_path_, thread_pool_size_)));
                    break;
            }
        }
        catch(...) {
            txn_mgr_version_ = 1;
            txn_mgr_ = std::unique_ptr<TransactionManager>(static_cast<TransactionManager*>(new TransactionManagerV1(config, obj_store_.get(), db_path_, thread_pool_size_)));
        }
    }
    else {
        txn_mgr_version_ = 1;
        txn_mgr_ = std::unique_ptr<TransactionManager>(static_cast<TransactionManager*>(new TransactionManagerV1(config, obj_store_.get(), db_path_, thread_pool_size_)));
    }

    if (!compression_str.empty()) {
        if (compression_str == "snappy") {
            compression_ = BufCompression::BUF_SNAPPY_COMPRESSION;
        }
        else {
            compression_ = BufCompression::BUF_NO_COMPRESSION;
        }
    }
    else {
        compression_ = BufCompression::BUF_NO_COMPRESSION;
    }
    
}

BackendService::~BackendService() {
    thread_pool_->join();
} 

void BackendService::run() {
    thread_pool_ = std::make_unique<boost::asio::thread_pool>(thread_pool_size_);
}

void BackendService::enqueueTask(ClientTask* task, bool ok) {
    boost::asio::post(*thread_pool_, [this, task = task, ok = ok](){ 
        handleClientTask(task, ok);     
    });
}

//TODO: Currently a dummy handler for testing purposes, but will have to change
void BackendService::handleClientTask(ClientTask * task, bool ok) {
    task->times_++;
    switch (task->type_) {
        case ClientTaskType::START_TXN:
            handleStartTxn(task, ok);
            break;
        case ClientTaskType::SNAPSHOT:
            handleSnapshot(task, ok);
            break;
        case ClientTaskType::CLONE:
            handleClone(task, ok);
            break;
        case ClientTaskType::GET_GARBAGE:
            if (!ok) {
                delete task;
                return;
            }
            
            task->status_ = ClientTaskStatus::FINISH;
            static_cast<grpc::ServerAsyncResponseWriter<GetGarbageResponse>*>
                    (task->writer_)->Finish(*static_cast<GetGarbageResponse*>(task->response_), grpc::Status::OK, task);
            break;
        case ClientTaskType::CLEAR_GARBAGE:
            if (!ok) {
                delete task;
                return;
            }
            
            static_cast<ClearGarbageResponse*>(task->response_)->set_success(true);
            task->status_ = ClientTaskStatus::FINISH;
            static_cast<grpc::ServerAsyncResponseWriter<ClearGarbageResponse>*>
                    (task->writer_)->Finish(*static_cast<ClearGarbageResponse*>(task->response_), grpc::Status::OK, task);
            break;
        case ClientTaskType::DEFINE_TYPE:
            if (!ok) {
                delete task;
                return;
            }
            static_cast<DefineTypeResponse*>(task->response_)->set_success(true);
            task->status_ = ClientTaskStatus::FINISH;
            static_cast<grpc::ServerAsyncResponseWriter<DefineTypeResponse>*>
                    (task->writer_)->Finish(*static_cast<DefineTypeResponse*>(task->response_), grpc::Status::OK, task);
            break;
        case ClientTaskType::EXECUTE_QUERY:
            // TODO pass in txn object for adding elements to read set 
            handleExecuteQuery(task, ok);
            break;
        case ClientTaskType::COMMIT:
            handleCommit(task, ok);
            break;
        case ClientTaskType::PRE_COMMIT:
            if (!ok) {
                delete task;
                return;
            }
            static_cast<PreCommitResponse*>(task->response_)->set_success(true);
            task->status_ = ClientTaskStatus::FINISH;
            static_cast<grpc::ServerAsyncResponseWriter<PreCommitResponse>*>
                    (task->writer_)->Finish(*static_cast<PreCommitResponse*>(task->response_), grpc::Status::OK, task);
            break;
        case ClientTaskType::BULK_LOAD:
            handleBulkLoad(task, ok);
            break;
        default:
            //TODO handle error
            break;
    }

}

void BackendService::handleStartTxn(ClientTask * task, bool ok) {
    if (!ok) {
        delete task;
        return;
    }

    txn_mgr_->startTransaction(task);

}

void BackendService::handleExecuteQuery(ClientTask * task, bool ok) {
    if (!ok && task->times_ <= 1) {
        delete task;
        return;
    }
    QuerySession * query_session;
    auto writer = static_cast<grpc::ServerAsyncWriter<ExecuteQueryResponse>*> (task->writer_);
    ExecuteQueryRequest * query_request = static_cast<ExecuteQueryRequest*>(task->request_);
    Transaction * txn = nullptr;

    //start a new query session
    if (task->times_ <= 1) {
        auto start = std::chrono::steady_clock::now();
        query_session = new QuerySession();
        query_session->start_ = start;
        task->session_ = query_session;
        if (query_request->has_txn_id() && (txn = txn_mgr_->getTransaction(query_request->txn_id())) != nullptr) {
            query_request->set_vid(txn->readVid());
            // Query path expressions cannot be deleted until txn is committed. 
            txn->addQueryRequest(query_request);  
        }
        else if (!query_request->has_vid()) {
        //get the latest
            uint64_t read_vid = txn_mgr_->getReadVid();
            query_request->set_vid(read_vid);
        }
        
        query_session->exec_node_ = constructPlan(obj_store_.get(), exec_buf_size_, query_request, txn, txn_mgr_.get(), txn_mgr_version_);
        
    }
    else {
        if (query_request->has_txn_id()) {
            txn = txn_mgr_->getTransaction(query_request->txn_id());
        }
        query_session = static_cast<QuerySession*>(task->session_);
    }

    //reallocate a new query response to send back
    delete static_cast<ExecuteQueryResponse*>(task->response_);
    ExecuteQueryResponse * query_response = new ExecuteQueryResponse();
    task->response_ = query_response;
    
    ExecNode * plan = query_session->exec_node_;
    query_response->set_vid(query_request->vid());
    query_response->set_base_only(query_request->base_only());
    
    bool valid = false;
    if (plan != nullptr) {
        auto start = std::chrono::steady_clock::now();
        valid = plan->next();
        auto end = std::chrono::steady_clock::now();
        query_session->diff_ += (end - start);
        if (valid) {
            ExecBuffer * exec_buffer = plan->getOutputBuffer();
            switch (compression_) {
                case BufCompression::BUF_SNAPPY_COMPRESSION: {
                    std::string compressed_str;
                    snappy::Compress(exec_buffer->data(), std::min(exec_buffer->size(), exec_buffer->capacity()), &compressed_str);
                    query_response->set_obj_list(compressed_str);
                    query_response->set_compression(BufCompression::BUF_SNAPPY_COMPRESSION);
                }
                    break;
                default:
                    query_response->set_obj_list(exec_buffer->data(), std::min(exec_buffer->size(), exec_buffer->capacity()));
                    query_response->set_compression(BufCompression::BUF_NO_COMPRESSION);
                    break;  
            }

            writer->Write(*query_response, task);
        }
    }

    if (!valid) {
        // print the final execution time

        // std::cout << "Executor Duration: "
        //    << std::chrono::duration<double, std::milli>(query_session->diff_).count() << " ms" << std::endl;
        task->status_ = ClientTaskStatus::FINISH;
        // txn may abort due to deadlock
        if (txn_mgr_version_ == 3 && txn != nullptr && static_cast<TransactionV3*>(txn)->abort()) {
            query_response->set_abort(true);
            query_response->set_compression(BufCompression::BUF_NO_COMPRESSION);
            writer->WriteAndFinish(*query_response, grpc::WriteOptions(), grpc::Status::OK, task);
        }
        else {
        // If not a read-write txn, delete txn request
            writer->Finish(grpc::Status::OK, task);
        }

        if (txn == nullptr || txn_mgr_version_ != 1) {
            delete query_request;
        }
        auto duration = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now()
             - query_session->start_);
        // std::cout << "Server End-to-End Duration: " << duration.count() << " ms" << std::endl;
        
        delete query_session;
        delete plan;
    }
    
}

void BackendService::handleCommit(ClientTask * task, bool ok) {
    if (!ok) {
        delete task;
        return;
    }
    CommitRequest * commit_request = static_cast<CommitRequest*>(task->request_);
    CommitResponse * commit_response = static_cast<CommitResponse*>(task->response_);
    txn_mgr_->commit(commit_request, commit_response);
    task->status_ = ClientTaskStatus::FINISH;
    static_cast<grpc::ServerAsyncResponseWriter<CommitResponse>*>
            (task->writer_)->Finish(*commit_response, grpc::Status::OK, task);

}

void BackendService::handleBulkLoad(ClientTask * task, bool ok) {
    if (!ok && task->times_ <= 1) {
        delete task;
        return;
    }

    LoadSession * load_session;
    auto reader = static_cast<grpc::ServerAsyncReader<BulkLoadResponse, BulkLoadRequest>*> (task->writer_);
    BulkLoadRequest * load_request = static_cast<BulkLoadRequest*>(task->request_);
    BulkLoadResponse * load_response = static_cast<BulkLoadResponse*>(task->response_);
    
    if (task->times_ <= 1) {
        load_session = new LoadSession();
        task->session_ = load_session;
        load_session->obj_store_ = obj_store_.get();
        std::stringstream temp_db_path_ss;
        temp_db_path_ss << "/tmp/catalog" << std::hex << reinterpret_cast<std::uintptr_t>(task);
        load_session->temp_db_path_ = temp_db_path_ss.str();

        rocksdb::Options options;
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;
        
        rocksdb::Status s = rocksdb::DB::Open(options, load_session->temp_db_path_, &load_session->temp_db_);
        // TODO: handle when s is not ok: assert(s.ok());

    }
    else {
        load_session = static_cast<LoadSession *>(task->session_);
    }

    // end of client stream, write response back
    if (!ok) {
        //TODO: we assume there is no concurrent txn
        finishBulkLoad(task, txn_mgr_->getReadVid());
        return;
    }
    
    if (!loadToTemp(load_session, load_request, load_response)) {
        terminateBulkLoad(task, load_session, load_response);
        return;
    }

    task->request_ = new BulkLoadRequest();
    reader->Read(static_cast<BulkLoadRequest*>(task->request_), task);

    delete load_request;

}


void BackendService::handleSnapshot(ClientTask * task, bool ok) {
    if (!ok) {
        delete task;
        return;
    }
    SnapshotRequest * snapshot_request = static_cast<SnapshotRequest*>(task->request_);
    SnapshotResponse * snapshot_response = static_cast<SnapshotResponse*>(task->response_);
    SnapshotMap * snapshot_map = obj_store_->snapshotMap();
    uint64_t vid = snapshot_request->has_vid() ? snapshot_request->vid() : txn_mgr_->getReadVid();
    
    if (vid > txn_mgr_->getReadVid()) {
        snapshot_response->set_success(false);
    }
    else {
        bool success = true;
        if (!snapshot_map->contains(snapshot_request->name()) ||
            (snapshot_request->has_override() && snapshot_request->override())) {
            success = success && snapshot_map->put(snapshot_request->name(), vid);
        }

        success = success && snapshot_map->get(snapshot_request->name(), &vid);
        snapshot_response->set_success(success);
        snapshot_response->set_vid(vid);
    }
    
    task->status_ = ClientTaskStatus::FINISH;
    static_cast<grpc::ServerAsyncResponseWriter<SnapshotResponse>*>
            (task->writer_)->Finish(*snapshot_response, grpc::Status::OK, task);
}


void BackendService::handleClone(ClientTask * task, bool ok) {
    if (!ok) {
        delete task;
        return;
    }
    CloneRequest * clone_request = static_cast<CloneRequest*>(task->request_);
    CloneResponse * clone_response = static_cast<CloneResponse*>(task->response_);
    clone(clone_request, clone_response, obj_store_.get(), txn_mgr_.get(), txn_mgr_version_);
    
    task->status_ = ClientTaskStatus::FINISH;
    static_cast<grpc::ServerAsyncResponseWriter<CloneResponse>*>
            (task->writer_)->Finish(*clone_response, grpc::Status::OK, task);
}



TransactionManager * BackendService::transactionManager() {
    return txn_mgr_.get();
}