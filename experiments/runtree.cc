#include <iostream>
#include <fstream>
#include <memory>
#include <chrono>
#include <fstream>
#include <filesystem>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "backend/backendservice.h"
#include "grpc/grpccatalogclient.h"
#include "grpc/grpccatalogserver.h"

void loadTree(Config * config, const std::string & file_path) {
    std::string catalog_dir = config->getParam("backend.db_path");
    if (catalog_dir.empty()) {
        system("rm -rf /tmp/catalogstorage/");
    }
    else {
        if (std::filesystem::exists(catalog_dir)) {
            std::cout << "Make sure to remove " << catalog_dir << std::endl;
            exit(1);
        }
    }
    
    BackendService* backend = new BackendService(config);
    GRPCCatalogServer* grpccatalogserver = new GRPCCatalogServer(config, backend);
    backend->run();
    grpccatalogserver->run();
    auto grpccatalogclient = new GRPCCatalogClient(config);
    grpccatalogclient->run();
    grpccatalogclient->bulkLoad(file_path);

    grpccatalogserver->stop();
    delete grpccatalogclient;
    delete grpccatalogserver;
    delete backend;
    std::cout << "Finished loading data" << std::endl;
}


int main(int argc, char** argv) {
    if (argc != 3) {
        std::cout << "./runtree <config_file> <data_file>" << std::endl;
        return 1;
    }

    Config config(argv[1]);
    
    std::string file_path(argv[2]);
     
    loadTree(&config, file_path); 

    std::ofstream version_output(config.getParam("version_output"), std::ios::app);
    version_output << config.getParam("backend.txn_mgr_version") << "\n";
    version_output.close();

    BackendService* backend = new BackendService(&config);
    GRPCCatalogServer* grpccatalogserver = new GRPCCatalogServer(&config, backend);    
    backend->run();
    grpccatalogserver->run();

    while(true) {
        sleep(100);
    }

    

    return 0;
}