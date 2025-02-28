// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef LOADER_H
#define LOADER_H

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "grpc/grpccatalog.pb.h"

#include "storage/objstore.h"

#include "common/clienttask.h"
#include "common/objkind.h"

struct LoadSession {
    rocksdb::DB * temp_db_;
    std::string temp_db_path_;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;
    ObjStore* obj_store_;
    //txn_manager_
};



bool loadToTemp(LoadSession * load_session, BulkLoadRequest * load_request, BulkLoadResponse * load_response);

void finishBulkLoad(ClientTask * task, uint64_t vid);

void terminateBulkLoad(ClientTask * task, LoadSession * load_session, BulkLoadResponse * load_response);


#endif