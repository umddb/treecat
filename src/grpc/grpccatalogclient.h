// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef GRPC_CATALOG_CLIENT_H
#define GRPC_CATALOG_CLIENT_H

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <optional>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include "common/config.h"
#include "grpc/grpccatalog.grpc.pb.h"

class GRPCCatalogClient {

    public:
        GRPCCatalogClient(Config * config);
        // disable copy constructor
        GRPCCatalogClient(const GRPCCatalogClient&) = delete;
        // disable assign constructor
        GRPCCatalogClient& operator=(const GRPCCatalogClient&) = delete;
        ~GRPCCatalogClient();
        void run();
        static void handleCQ(grpc::CompletionQueue * cq);
        //Dummy rpc calls.
       
        SnapshotResponse * snapShot(const std::string & name, std::optional<uint64_t> vid, bool override);    
        CloneResponse * clone(const std::string & src_path, const std::string & dest_path, std::optional<uint64_t> vid);
        StartTxnResponse * startTxn(TxnMode txn_mode);
        std::vector<ExecuteQueryResponse*> executeQuery(ExecuteQueryRequest * query_request);
        BulkLoadResponse * bulkLoad(const std::string & file_path);
        CommitResponse * commit(CommitRequest * commit_request);
        
        /* TODO: implement the dummy versions of the rest
        bool clone();
        bool getGarbage();
        bool clearGarbage();
        bool defineType();
        
        bool Commit();
        bool preCommit();
        */
    private:

        enum class GRPCClientCallType { START_TXN, 
                            SNAPSHOT,
                            CLONE,
                            GET_GARBAGE,
                            CLEAR_GARBAGE,
                            DEFINE_TYPE,
                            EXECUTE_QUERY,
                            COMMIT,
                            PRE_COMMIT, 
                            BULK_LOAD };

        struct GRPCClientCall {
            grpc::CompletionQueue* cq_;
            grpc::ClientContext ctx_;
            grpc::Status status_;
            GRPCClientCallType type_;
            void *request_;
            void *response_;
            void *reader_;
            std::mutex mtx_;
            std::condition_variable cond_;
            bool processed_;
            bool ok_;

            GRPCClientCall(GRPCCatalog::Stub * stub, grpc::CompletionQueue* cq, GRPCClientCallType type, void * request = nullptr) : 
                    cq_(cq), type_(type), processed_(false), ok_(true) {
                
                switch (type_) {
                    case GRPCClientCallType::START_TXN:
                        // request is passed in by the caller
                        request_ = request;
                        response_ = new StartTxnResponse();
                        reader_ = stub->PrepareAsyncStartTxn(&ctx_, *static_cast<StartTxnRequest*>(request_), cq_).release();
                        break;
                    case GRPCClientCallType::SNAPSHOT:
                        request_ = request;
                        response_ = new SnapshotResponse();
                        reader_ = stub->PrepareAsyncSnapshot(&ctx_, *static_cast<SnapshotRequest*>(request_), cq_).release();
                        break;
                    case GRPCClientCallType::CLONE:
                        request_ = request;
                        response_ = new CloneResponse();
                        reader_ = stub->PrepareAsyncClone(&ctx_, *static_cast<CloneRequest*>(request_), cq_).release();
                        break;
                    case GRPCClientCallType::GET_GARBAGE:
                        request_ = new GetGarbageRequest();
                        response_ = new GetGarbageResponse();
                        reader_ = stub->PrepareAsyncGetGarbage(&ctx_, *static_cast<GetGarbageRequest*>(request_), cq_).release();
                        break;
                    case GRPCClientCallType::CLEAR_GARBAGE:
                        request_ = new ClearGarbageRequest();
                        response_ = new ClearGarbageResponse();
                        reader_ = stub->PrepareAsyncClearGarbage(&ctx_, *static_cast<ClearGarbageRequest*>(request_), cq_).release();
                        break;
                    case GRPCClientCallType::DEFINE_TYPE:
                        request_ = new DefineTypeRequest();
                        response_ = new DefineTypeResponse();
                        reader_ = stub->PrepareAsyncDefineType(&ctx_, *static_cast<DefineTypeRequest*>(request_), cq_).release();
                        break;
                    case GRPCClientCallType::EXECUTE_QUERY:
                        // request is passed in by the caller
                        request_ = request;
                        response_ = nullptr;
                        reader_ = stub->PrepareAsyncExecuteQuery(&ctx_, *static_cast<ExecuteQueryRequest*>(request_), cq_).release();
                        break;
                    case GRPCClientCallType::COMMIT:
                        // request is passed in by the caller
                        request_ = request;
                        response_ = new CommitResponse();
                        reader_ = stub->PrepareAsyncCommit(&ctx_, *static_cast<CommitRequest*>(request_), cq_).release();
                        break;
                    case GRPCClientCallType::PRE_COMMIT:
                        request_ = new PreCommitRequest();
                        response_ = new PreCommitResponse();
                        reader_ = stub->PrepareAsyncPreCommit(&ctx_, *static_cast<PreCommitRequest*>(request_), cq_).release();
                        break;
                    case GRPCClientCallType::BULK_LOAD:
                        request_ = nullptr;
                        response_ = new BulkLoadResponse();
                        reader_ = stub->PrepareAsyncBulkLoad(&ctx_, static_cast<BulkLoadResponse*>(response_), cq_).release();
                        break;
                    default:
                        //TODO handle error
                        break;
                }
            }



            ~GRPCClientCall(){
                switch (type_) {
                    case GRPCClientCallType::START_TXN:
                        delete static_cast<grpc::ClientAsyncResponseReader<StartTxnResponse>*>(reader_);
                        break;
                    case GRPCClientCallType::SNAPSHOT:
                        delete static_cast<grpc::ClientAsyncResponseReader<SnapshotResponse>*>(reader_);
                        break;
                    case GRPCClientCallType::CLONE:
                        delete static_cast<grpc::ClientAsyncResponseReader<CloneResponse>*>(reader_);
                        break;
                    case GRPCClientCallType::GET_GARBAGE:
                        delete static_cast<GetGarbageRequest*>(request_);
                        delete static_cast<GetGarbageResponse*>(response_);
                        delete static_cast<grpc::ClientAsyncResponseReader<GetGarbageResponse>*>(reader_);
                        break;
                    case GRPCClientCallType::CLEAR_GARBAGE:
                        delete static_cast<ClearGarbageRequest*>(request_);
                        delete static_cast<ClearGarbageResponse*>(response_);
                        delete static_cast<grpc::ClientAsyncResponseReader<ClearGarbageResponse>*>(reader_);
                        break;
                    case GRPCClientCallType::DEFINE_TYPE:
                        delete static_cast<DefineTypeRequest*>(request_);
                        delete static_cast<DefineTypeResponse*>(response_);
                        delete static_cast<grpc::ClientAsyncResponseReader<DefineTypeResponse>*>(reader_);
                        break;
                    case GRPCClientCallType::EXECUTE_QUERY:
                        delete static_cast<grpc::ClientAsyncReader<ExecuteQueryResponse>*>(reader_);
                        break;
                    case GRPCClientCallType::COMMIT:
                        delete static_cast<grpc::ClientAsyncResponseReader<CommitResponse>*>(reader_);
                        break;
                    case GRPCClientCallType::PRE_COMMIT:
                        delete static_cast<PreCommitRequest*>(request_);
                        delete static_cast<PreCommitResponse*>(response_);
                        delete static_cast<grpc::ClientAsyncResponseReader<PreCommitResponse>*>(reader_);
                        break;
                    case GRPCClientCallType::BULK_LOAD:
                        delete static_cast<BulkLoadRequest*>(request_);
                        delete static_cast<grpc::ClientAsyncWriter<BulkLoadRequest>*>(reader_);
                        break;
                    default:
                        //TODO handle error
                        break;
                }

            }
        };

        const Config * config_;
        std::string server_address_;
        std::unique_ptr<GRPCCatalog::Stub> stub_;
        std::thread cq_handler_;
        std::mutex mtx_;
        std::condition_variable cond_;
        // completion set
        std::unique_ptr<grpc::CompletionQueue> cq_;
        // completion set
        std::unordered_set<void*> cs_;
        
        void waitCall(GRPCClientCall * grpc_client_call);

};

#endif //GRPC_CATALOG_CLIENT_H 