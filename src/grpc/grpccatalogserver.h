// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef GRPC_CATALOG_SERVER_H
#define GRPC_CATALOG_SERVER_H

#include <memory>
#include <mutex>
#include <string>

#include "boost/asio/thread_pool.hpp"
#include <grpcpp/grpcpp.h>

#include "backend/backendservice.h"
#include "common/config.h"
#include "common/clienttask.h"
#include "grpc/grpccatalog.grpc.pb.h"

class GRPCCatalogServer {
    public:
        GRPCCatalogServer(Config* config, BackendService* backend);
        // disable copy constructor
        GRPCCatalogServer(const GRPCCatalogServer&) = delete;
        // disable assign constructor
        GRPCCatalogServer& operator=(const GRPCCatalogServer&) = delete;
        ~GRPCCatalogServer();
        void run();
        void stop();
        static void handleClientTasks(grpc::ServerCompletionQueue * cq, BackendService *backend);

    private:
        const Config* config_;
        BackendService* backend_; 
        std::string server_address_;
        unsigned int thread_pool_size_;
        std::unique_ptr<boost::asio::thread_pool> thread_pool_;
        GRPCCatalog::AsyncService service_;
        std::unique_ptr<grpc::Server> server_;
        std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cq_;
        bool stop_;
        std::mutex mtx_;
        
};

#endif //GRPC_CATALOG_SERVER_H