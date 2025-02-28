// Author: Keonwoo Oh (koh3@umd.edu)

#include <thread>

#include "boost/asio/post.hpp"

#include "grpc/grpccatalogserver.h"

GRPCCatalogServer::GRPCCatalogServer(Config * config, BackendService* backend): config_(config), 
    backend_(backend), stop_(true) {
    std::string thread_pool_size_str;

    if (config_ != nullptr) {
        thread_pool_size_str = config_->getParam("grpc.thread_pool_size");
        server_address_ = config_->getParam("grpc.server_address");
    }    

    if (!thread_pool_size_str.empty()) {
        try {
            thread_pool_size_ = std::stoul(thread_pool_size_str);
        }
        catch(...) {
            //GRPC documentation suggests having numcpu's threads
            std::cout << "grpc.thread_pool_size is not set to a valid integer. Using " << std::thread::hardware_concurrency() << " threads." << std::endl;
            thread_pool_size_ = std::thread::hardware_concurrency();
        }
    }
    else {
        std::cout << "grpc.thread_pool_size is not set. Using " << std::thread::hardware_concurrency() << " threads." << std::endl;
        thread_pool_size_ = std::thread::hardware_concurrency();
    }

    
    if (server_address_.empty()) {
        server_address_ = "0.0.0.0:9876";
        std::cout << "grpc.server_address is not set. Using " << server_address_ << "." << std::endl;
        
    }
    
}


GRPCCatalogServer::~GRPCCatalogServer() {
    stop();
}

void GRPCCatalogServer::run() {
    // Build grpc server and run it
    std::lock_guard<std::mutex> lock(mtx_);
    stop_ = false;
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    builder.SetMaxReceiveMessageSize(50 * 1024 * 1024);
    // 2 threads per completition queue
    for (unsigned int i = 0; i < (thread_pool_size_/2); i ++) {
        cq_.push_back(std::unique_ptr<grpc::ServerCompletionQueue>(builder.AddCompletionQueue()));
    }
    server_ = builder.BuildAndStart();

    // Start executing threads in the thread pool
    thread_pool_ = std::make_unique<boost::asio::thread_pool>(thread_pool_size_);
    for (auto &iter: cq_) {
        auto cq = iter.get();
        // add every type of client task to completion queue 
        for (int i = static_cast<int>(ClientTaskType::START_TXN); i <= 
            static_cast<int>(ClientTaskType::BULK_LOAD); i++) {
            new ClientTask(&service_, cq, static_cast<ClientTaskType>(i));
        }
        // 2 threads per completion queue.
        boost::asio::post(*thread_pool_, [cq = cq, backend = backend_](){ handleClientTasks(cq, backend); });
        boost::asio::post(*thread_pool_, [cq = cq, backend = backend_](){ handleClientTasks(cq, backend); });
    }
    

}

void GRPCCatalogServer::stop() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (!stop_) {
        stop_ = true;
        server_->Shutdown();
        for (auto &iter : cq_) {
            iter->Shutdown();
        }
        thread_pool_->join();
    }
    
}

void GRPCCatalogServer::handleClientTasks(grpc::ServerCompletionQueue *cq, BackendService *backend) {
    void * tag;
    bool ok;

    while (cq->Next(&tag, &ok)) {
        ClientTask* client_task = static_cast<ClientTask*>(tag);
        
        switch (client_task->status_) {
            case ClientTaskStatus::PROCESS:
                if (ok && client_task->times_ == 0) {
                    // create new client task of the same type for concurrency
                    new ClientTask(client_task->service_, client_task->cq_, client_task->type_);
                }
                // offload work to the backend 
                backend->enqueueTask(client_task, ok);
                break;
            case ClientTaskStatus::FINISH:
                delete client_task;
                break;
            default:
                // TODO handle error
                break;
        }
        
        
    }

}
