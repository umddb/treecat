// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef CLIENT_TASK_H
#define CLIENT_TASK_H

#include <grpcpp/grpcpp.h>
#include "grpc/grpccatalog.grpc.pb.h"


enum class ClientTaskType { START_TXN, 
                            SNAPSHOT,
                            CLONE,
                            GET_GARBAGE,
                            CLEAR_GARBAGE,
                            DEFINE_TYPE,
                            EXECUTE_QUERY,
                            COMMIT,
                            PRE_COMMIT,
                            BULK_LOAD };

enum class ClientTaskStatus { PROCESS, 
                              FINISH };

struct ClientTask {
    GRPCCatalog::AsyncService* service_;
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;
    ClientTaskType type_;
    ClientTaskStatus status_;
    void *request_;
    void *response_;
    void *writer_;
    // rpc specific session data in case of streaming service
    void * session_;
    unsigned int times_;
    
    // disable copy constructor
    ClientTask(const ClientTask&) = delete;
    
    // disable assign constructor
    ClientTask& operator=(const ClientTask&) = delete;
    
    ClientTask(GRPCCatalog::AsyncService* service, grpc::ServerCompletionQueue* cq, 
               ClientTaskType type) : service_(service), cq_(cq), type_(type), 
               status_(ClientTaskStatus::PROCESS), times_(0) {
        // Call different types of request, response constructors depending on the task type
        switch (type_) {
            case ClientTaskType::START_TXN:
                request_ = new StartTxnRequest();
                response_ = new StartTxnResponse();
                writer_ = new grpc::ServerAsyncResponseWriter<StartTxnResponse>(&ctx_);
                service_->RequestStartTxn(&ctx_, static_cast<StartTxnRequest*>(request_), 
                                          static_cast<grpc::ServerAsyncResponseWriter<StartTxnResponse>*>(writer_),
                                          cq_, cq_, this);
                break;
            case ClientTaskType::SNAPSHOT:
                request_ = new SnapshotRequest();
                response_ = new SnapshotResponse();
                writer_ = new grpc::ServerAsyncResponseWriter<SnapshotResponse>(&ctx_);
                service_->RequestSnapshot(&ctx_, static_cast<SnapshotRequest*>(request_),
                                          static_cast<grpc::ServerAsyncResponseWriter<SnapshotResponse>*>(writer_),
                                          cq_, cq_, this);
                break;
            case ClientTaskType::CLONE:
                request_ = new CloneRequest();
                response_ = new CloneResponse();
                writer_ = new grpc::ServerAsyncResponseWriter<CloneResponse>(&ctx_);
                service_->RequestClone(&ctx_, static_cast<CloneRequest*>(request_), 
                                       static_cast<grpc::ServerAsyncResponseWriter<CloneResponse>*>(writer_),
                                       cq_, cq_, this);
                break;
            case ClientTaskType::GET_GARBAGE:
                request_ = new GetGarbageRequest();
                response_ = new GetGarbageResponse();
                writer_ = new grpc::ServerAsyncResponseWriter<GetGarbageResponse>(&ctx_);
                service_->RequestGetGarbage(&ctx_, static_cast<GetGarbageRequest*>(request_), 
                                            static_cast<grpc::ServerAsyncResponseWriter<GetGarbageResponse>*>(writer_),
                                            cq_, cq_, this);
                break;
            case ClientTaskType::CLEAR_GARBAGE:
                request_ = new ClearGarbageRequest();
                response_ = new ClearGarbageResponse();
                writer_ = new grpc::ServerAsyncResponseWriter<ClearGarbageResponse>(&ctx_);
                service_->RequestClearGarbage(&ctx_, static_cast<ClearGarbageRequest*>(request_), 
                                              static_cast<grpc::ServerAsyncResponseWriter<ClearGarbageResponse>*>(writer_),
                                              cq_, cq_, this);
                break;
            case ClientTaskType::DEFINE_TYPE:
                request_ = new DefineTypeRequest();
                response_ = new DefineTypeResponse();
                writer_ = new grpc::ServerAsyncResponseWriter<DefineTypeResponse>(&ctx_);
                service_->RequestDefineType(&ctx_, static_cast<DefineTypeRequest*>(request_), 
                                            static_cast<grpc::ServerAsyncResponseWriter<DefineTypeResponse>*>(writer_),
                                            cq_, cq_, this);
                break;
            case ClientTaskType::EXECUTE_QUERY:
                request_ = new ExecuteQueryRequest();
                response_ = new ExecuteQueryResponse();
                writer_ = new grpc::ServerAsyncWriter<ExecuteQueryResponse>(&ctx_);
                service_->RequestExecuteQuery(&ctx_, static_cast<ExecuteQueryRequest*>(request_), 
                                              static_cast<grpc::ServerAsyncWriter<ExecuteQueryResponse>*>(writer_),
                                              cq_, cq_, this);
                break;
            case ClientTaskType::COMMIT:
                request_ = new CommitRequest();
                response_ = new CommitResponse();
                writer_ = new grpc::ServerAsyncResponseWriter<CommitResponse>(&ctx_);
                service_->RequestCommit(&ctx_, static_cast<CommitRequest*>(request_), 
                                        static_cast<grpc::ServerAsyncResponseWriter<CommitResponse>*>(writer_),
                                        cq_, cq_, this);
                break;
            case ClientTaskType::PRE_COMMIT:
                request_ = new PreCommitRequest();
                response_ = new PreCommitResponse();
                writer_ = new grpc::ServerAsyncResponseWriter<PreCommitResponse>(&ctx_);
                service_->RequestPreCommit(&ctx_, static_cast<PreCommitRequest*>(request_), 
                                           static_cast<grpc::ServerAsyncResponseWriter<PreCommitResponse>*>(writer_),
                                           cq_, cq_, this);
                break;
            case ClientTaskType::BULK_LOAD:
                request_ = new BulkLoadRequest();
                response_ = new BulkLoadResponse();
                writer_ = new grpc::ServerAsyncReader<BulkLoadResponse, BulkLoadRequest>(&ctx_);
                service_->RequestBulkLoad(&ctx_, static_cast<grpc::ServerAsyncReader<BulkLoadResponse, BulkLoadRequest>*>(writer_),
                                           cq_, cq_, this);
                break;
            default:
                //TODO handle error
                break;
        }

    }
        
    ~ClientTask(){
        switch (type_) {
            case ClientTaskType::START_TXN:
                delete static_cast<StartTxnRequest*>(request_);
                delete static_cast<StartTxnResponse*>(response_);
                delete static_cast<grpc::ServerAsyncResponseWriter<StartTxnResponse>*>(writer_);
                break;
            case ClientTaskType::SNAPSHOT:
                delete static_cast<SnapshotRequest*>(request_);
                delete static_cast<SnapshotResponse*>(response_);
                delete static_cast<grpc::ServerAsyncResponseWriter<SnapshotResponse>*>(writer_);
                break;
            case ClientTaskType::CLONE:
                delete static_cast<CloneRequest*>(request_);
                delete static_cast<CloneResponse*>(response_);
                delete static_cast<grpc::ServerAsyncResponseWriter<CloneResponse>*>(writer_);
                break;
            case ClientTaskType::GET_GARBAGE:
                delete static_cast<GetGarbageRequest*>(request_);
                delete static_cast<GetGarbageResponse*>(response_);
                delete static_cast<grpc::ServerAsyncResponseWriter<GetGarbageResponse>*>(writer_);
                break;
            case ClientTaskType::CLEAR_GARBAGE:
                delete static_cast<ClearGarbageRequest*>(request_);
                delete static_cast<ClearGarbageResponse*>(response_);
                delete static_cast<grpc::ServerAsyncResponseWriter<ClearGarbageResponse>*>(writer_);
                break;
            case ClientTaskType::DEFINE_TYPE:
                delete static_cast<DefineTypeRequest*>(request_);
                delete static_cast<DefineTypeResponse*>(response_);
                delete static_cast<grpc::ServerAsyncResponseWriter<DefineTypeResponse>*>(writer_);
                break;
            case ClientTaskType::EXECUTE_QUERY:
                //delete static_cast<ExecuteQueryRequest*>(request_);
                delete static_cast<ExecuteQueryResponse*>(response_);
                delete static_cast<grpc::ServerAsyncWriter<ExecuteQueryResponse>*>(writer_);
                break;
            case ClientTaskType::COMMIT:
                delete static_cast<CommitRequest*>(request_);
                delete static_cast<CommitResponse*>(response_);
                delete static_cast<grpc::ServerAsyncResponseWriter<CommitResponse>*>(writer_);
                break;
            case ClientTaskType::PRE_COMMIT:
                delete static_cast<PreCommitRequest*>(request_);
                delete static_cast<PreCommitResponse*>(response_);
                delete static_cast<grpc::ServerAsyncResponseWriter<PreCommitResponse>*>(writer_);
                break;
            case ClientTaskType::BULK_LOAD:
                delete static_cast<BulkLoadRequest*>(request_);
                delete static_cast<BulkLoadResponse*>(response_);
                delete static_cast<grpc::ServerAsyncReader<BulkLoadResponse, BulkLoadRequest>*>(writer_);
                break;
            default:
                //TODO handle error
                break;
        }

    }

};
#endif // CLIENT_TASK_H