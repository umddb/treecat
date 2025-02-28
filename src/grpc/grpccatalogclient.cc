// Author: Keonwoo Oh (koh3@umd.edu)

#include <fstream>

#include "common/bson/json.h"

#include "grpc/grpccatalogclient.h"



GRPCCatalogClient::GRPCCatalogClient(Config * config) : config_(config) {
    if (config_ != nullptr) {
        server_address_ = config_->getParam("grpc.server_address");
    }    

    if (server_address_.empty()) {
        server_address_ = "0.0.0.0:9876";
        std::cout << "grpc.server_address is not set. Using " << server_address_ << "." << std::endl;
        
    }
}

GRPCCatalogClient::~GRPCCatalogClient() {
    cq_->Shutdown();
    cq_handler_.join();
}

void GRPCCatalogClient::run() {
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxReceiveMessageSize(-1);
    channel_args.SetMaxSendMessageSize(-1);
    auto channel = grpc::CreateCustomChannel(server_address_, grpc::InsecureChannelCredentials(), channel_args);

    stub_ = GRPCCatalog::NewStub(channel);
    cq_ = std::make_unique<grpc::CompletionQueue>();
    cq_handler_ = std::thread([cq = cq_.get()](){ handleCQ(cq); });
}

// TODO this could probably be cleaner.
void GRPCCatalogClient::handleCQ(grpc::CompletionQueue * cq) {
    void * tag;
    bool ok;
    while (cq->Next(&tag, &ok)) {
        GRPCClientCall * grpc_client_call = static_cast<GRPCClientCall*>(tag);
        {   std::lock_guard<std::mutex> lock(grpc_client_call->mtx_);
            grpc_client_call->processed_ = true;
            grpc_client_call->ok_ = ok;
        }
        grpc_client_call->cond_.notify_all();
    }
}


StartTxnResponse * GRPCCatalogClient::startTxn(TxnMode txn_mode){
    std::unique_ptr<StartTxnRequest> start_txn_request = std::make_unique<StartTxnRequest>();
    start_txn_request->set_txn_mode(txn_mode);
    std::unique_ptr<GRPCClientCall> call = std::make_unique<GRPCClientCall>(stub_.get(), cq_.get(),
            GRPCClientCallType::START_TXN, start_txn_request.get());

    auto reader = static_cast<grpc::ClientAsyncResponseReader<StartTxnResponse>*>(call->reader_);
    reader->StartCall();
    reader->Finish(static_cast<StartTxnResponse*>(call->response_), &call->status_, (void*)call.get());
    waitCall(call.get());
    if (call->status_.ok()) {
        return static_cast<StartTxnResponse*>(call->response_);
    }
    else {
        return nullptr;
    }
}


CommitResponse * GRPCCatalogClient::commit(CommitRequest * commit_request) {
    std::unique_ptr<GRPCClientCall> call = std::make_unique<GRPCClientCall>(stub_.get(), cq_.get(),
            GRPCClientCallType::COMMIT, commit_request);
    auto reader = static_cast<grpc::ClientAsyncResponseReader<CommitResponse>*>(call->reader_);
    reader->StartCall();
    reader->Finish(static_cast<CommitResponse*>(call->response_), &call->status_, (void*)call.get());
    waitCall(call.get());
    if (call->status_.ok()) {
        return static_cast<CommitResponse*>(call->response_);
    }
    else {
        return nullptr;
    }

}

SnapshotResponse * GRPCCatalogClient::snapShot(const std::string & name, std::optional<uint64_t> vid, 
        bool override = false){
    // initialize SnapshotRequest
    std::unique_ptr<SnapshotRequest> snapshot_request = std::make_unique<SnapshotRequest>();
    snapshot_request->set_name(name);
    if (vid.has_value()) {
        snapshot_request->set_vid(vid.value());
    }
    if (override) {
        snapshot_request->set_override(true);
    }

    std::unique_ptr<GRPCClientCall> call = std::make_unique<GRPCClientCall>(stub_.get(), cq_.get(),
            GRPCClientCallType::SNAPSHOT, snapshot_request.get());
    auto reader = static_cast<grpc::ClientAsyncResponseReader<SnapshotResponse>*>(call->reader_);
    reader->StartCall();
    reader->Finish(static_cast<SnapshotResponse*>(call->response_), &call->status_, (void*)call.get());
    waitCall(call.get());
    if (call->status_.ok()) {
        return static_cast<SnapshotResponse*>(call->response_);
    }
    else {
        return nullptr;
    }

}

CloneResponse * GRPCCatalogClient::clone(const std::string & src_path, const std::string & dest_path, 
        std::optional<uint64_t> vid = std::optional<uint64_t>()){
    // initialize CloneRequest
    std::unique_ptr<CloneRequest> clone_request = std::make_unique<CloneRequest>();
    clone_request->set_src_path(src_path);
    clone_request->set_dest_path(dest_path);
    if (vid.has_value()) {
        clone_request->set_vid(vid.value());
    }

    std::unique_ptr<GRPCClientCall> call = std::make_unique<GRPCClientCall>(stub_.get(), cq_.get(),
            GRPCClientCallType::CLONE, clone_request.get());
    auto reader = static_cast<grpc::ClientAsyncResponseReader<CloneResponse>*>(call->reader_);
    reader->StartCall();
    reader->Finish(static_cast<CloneResponse*>(call->response_), &call->status_, (void*)call.get());
    waitCall(call.get());
    if (call->status_.ok()) {
        return static_cast<CloneResponse*>(call->response_);
    }
    else {
        return nullptr;
    }

}




std::vector<ExecuteQueryResponse*> GRPCCatalogClient::executeQuery(ExecuteQueryRequest * query_request){
    //vector of exec responses to return, user is responsible for deallocating the responses
    std::vector<ExecuteQueryResponse*> exec_responses;
    std::unique_ptr<GRPCClientCall> call = std::make_unique<GRPCClientCall>(stub_.get(), cq_.get(),
            GRPCClientCallType::EXECUTE_QUERY, query_request);
    auto reader = static_cast<grpc::ClientAsyncReader<ExecuteQueryResponse>*>(call->reader_);
    call->processed_ = false;
    reader->StartCall(call.get());
    waitCall(call.get());
    bool ok = call->ok_;
    
    ExecuteQueryResponse * exec_response = nullptr;
    if (ok) {
        exec_response = new ExecuteQueryResponse();
        call->processed_ = false;
        reader->Read(exec_response, call.get());
        waitCall(call.get());
        ok = call->ok_;
    } 
    while (ok) {
        exec_responses.push_back(exec_response);
        exec_response = new ExecuteQueryResponse();
        call->processed_ = false;
        reader->Read(exec_response, call.get());
        waitCall(call.get());
        ok = call->ok_;
    }
    // invalid, so deallocate
    delete exec_response;
    call->processed_ = false;
    reader->Finish(&call->status_, (void*) call.get());
    waitCall(call.get());
    
    return exec_responses;

}

BulkLoadResponse * GRPCCatalogClient::bulkLoad(const std::string & file_path){
    std::unique_ptr<GRPCClientCall> call = std::make_unique<GRPCClientCall>(stub_.get(), cq_.get(),
            GRPCClientCallType::BULK_LOAD);
    std::ifstream input_file(file_path);

    if (!input_file.is_open()) {
        std::cerr << "Failed to open the file." << std::endl;
        return nullptr;
    }

    auto writer = static_cast<grpc::ClientAsyncWriter<BulkLoadRequest>*>(call->reader_);
    call->processed_ = false;
    writer->StartCall(call.get());
    
    std::string line;
    bool eof = !std::getline(input_file, line);
    bool ok = true;
    while(!eof && ok) {
        BulkLoadRequest* request = new BulkLoadRequest();
        while(!eof && request->obj_list_size() < 1000) {
            mongo::BSONObj bson_obj = mongo::fromjson(mongo::StringData(line));
            request->add_obj_list(bson_obj.objdata(), bson_obj.objsize());
            eof = !std::getline(input_file, line);
        }
        waitCall(call.get());
        delete static_cast<BulkLoadRequest*>(call->request_); 
        call->request_ = request;
        call->processed_ = false;
        ok = call->ok_;
        writer->Write(*request, call.get());
            
    }
    
    waitCall(call.get());
    call->processed_ = false;
    writer->WritesDone(call.get());
    waitCall(call.get());
    call->processed_ = false;
    writer->Finish(&call->status_, (void*) call.get());
    waitCall(call.get());
    if (call->status_.ok()) {
        return static_cast<BulkLoadResponse*>(call->response_);
    }
    else {
        return nullptr;
    }

}
/*
        bool GRPCCatalogClient::clone(){}
        bool GRPCCatalogClient::getGarbage(){}
        bool GRPCCatalogClient::clearGarbage(){}
        bool GRPCCatalogClient::defineType(){}
        bool GRPCCatalogClient::executeQuery(){}
        bool GRPCCatalogClient::Commit(){}
        bool GRPCCatalogClient::preCommit(){}

*/

void GRPCCatalogClient::waitCall(GRPCClientCall * grpc_client_call) {
    std::unique_lock<std::mutex> lock(grpc_client_call->mtx_);
    grpc_client_call->cond_.wait(lock, [grpc_client_call=grpc_client_call](){ return grpc_client_call->processed_; });
}