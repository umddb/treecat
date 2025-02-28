// Author: Keonwoo Oh (koh3@umd.edu)

#include <algorithm>

#include "common/bson/bsonobj.h"

#include "exec/loader.h"

static bool checkPath(LoadSession * load_session, BulkLoadResponse * load_response, std::string_view path_str, ObjKind obj_kind);

bool loadToTemp(LoadSession * load_session, BulkLoadRequest * load_request, BulkLoadResponse * load_response) {
    const char null_val = 0; 
    for (int i = 0; i < load_request->obj_list_size(); i++) {
        const std::string & obj = load_request->obj_list(i);
        mongo::BSONObj obj_meta = mongo::BSONObj(obj.data()).firstElement().Obj();
        mongo::BSONObj obj_val = mongo::BSONObj(obj.data()).secondElement().Obj();
        mongo::BSONObj paths = obj_meta.getField("paths").Obj();
        ObjKind obj_kind = static_cast<ObjKind>(obj_meta.getField("obj_kind").numberLong());

        int index = 0;
        for (mongo::BSONElement path : paths) {
            mongo::StringData path_str = path.checkAndGetStringData();
            if (!checkPath(load_session, load_response, std::string_view(path_str.rawData(), path_str.size()), obj_kind)) {
                return false;
            }
            // insert the actual object if the primary path
            rocksdb::Status status;
            if (index == 0) {
                status = load_session->temp_db_->Put(load_session->write_options_, rocksdb::Slice(path_str.rawData(), path_str.size()), rocksdb::Slice(obj.data(), obj.size()));
            }
            // otherwise null value, so secondary paths can at least be checked for existence. 
            else {
                status = load_session->temp_db_->Put(load_session->write_options_, rocksdb::Slice(path_str.rawData(), path_str.size()), rocksdb::Slice(&null_val, 1));
            }

            if (!status.ok()) {
                return false;
            } 
            index++;
        }

        if (obj_kind <= ObjKind::INNER_LEAF && index > 1) {
            load_response->add_err("Following inner object has multiple paths: " + paths.firstElement().String());
            return false;
        }
    }

    return true;

}

bool checkPath(LoadSession * load_session, BulkLoadResponse * load_response, std::string_view path_str, ObjKind obj_kind) {
    
    Path path(path_str);
    
    // object of depth 1 has no parent
    if (path.depth() > 1) {
        // the max function is to handle the root
        size_t last_slash_idx = std::max(path_str.find_last_of('/'), std::size_t(1));
        if (last_slash_idx == std::string::npos || (last_slash_idx >= path_str.size() - 1 &&  path_str.size() != 2)) {
            load_response->add_err("Invalid Path: " + std::string(path_str));
            return false;
        }

        std::string_view parent_path_str(path_str.data(), last_slash_idx);
        Path parent_path(parent_path_str);
        // TODO: put read lock on the parent
        // check if parent exists
        rocksdb::PinnableSlice temp_val; 
        if (!load_session->obj_store_->innerObjStore()->contains(parent_path, ULLONG_MAX) && 
                !load_session->temp_db_->Get(load_session->read_options_, load_session->temp_db_->DefaultColumnFamily(), rocksdb::Slice(parent_path_str.data(), parent_path_str.size()), &temp_val).ok()) {
            load_response->add_err("Parent path does not exist:" + std::string(parent_path_str));
            return false;
        }
        temp_val.Reset();
    }

    //check if path exists
    bool exists = false;
    switch (obj_kind) {
        case ObjKind::INNER:
        case ObjKind::INNER_LEAF:
            exists = load_session->obj_store_->innerObjStore()->contains(path, ULLONG_MAX);
            break;
        case ObjKind::LEAF:
            exists = load_session->obj_store_->leafObjStore()->contains(path, ULLONG_MAX);
            break;
        default:
            break;
    }

    if (exists) {
        load_response->add_err("Path already exists:" + std::string(path_str));
    }

    return !exists;
}

void finishBulkLoad(ClientTask * task, uint64_t vid) {
    //TODO have to take care of WAL, concurrency control etc.
    LoadSession * load_session = static_cast<LoadSession*>(task->session_);
    BulkLoadResponse * response = static_cast<BulkLoadResponse*>(task->response_);
    std::unique_ptr<rocksdb::Iterator> iter(load_session->temp_db_->NewIterator(load_session->read_options_));
    iter->SeekToFirst();
    bool status = true;
    auto version_map = load_session->obj_store_->versionMap();
    auto inner_obj_store = load_session->obj_store_->innerObjStore();
    auto leaf_obj_store = load_session->obj_store_->leafObjStore();

    VersionMap::Header header;
    header.delta_vid_ = vid;
    while (iter->Valid()) {
        if (iter->value().size() >= mongo::BSONObj::kMinBSONLength) {
            rocksdb::Slice path_slice = iter->key();
            Path path(std::string_view(path_slice.data(), path_slice.size()));
            mongo::BSONObj obj = mongo::BSONObj(iter->value().data());
            // load the kv pair to obj_store_
            mongo::BSONObj obj_meta = obj.firstElement().Obj();
            mongo::BSONObj obj_val = obj.secondElement().Obj();
            
            switch (static_cast<ObjKind>(obj_meta.getField("obj_kind").numberLong())) {
                case ObjKind::INNER:
                case ObjKind::INNER_LEAF:
                    header.children_vid_ = vid;
                    status = version_map->put(path, header);
                    status = status && inner_obj_store->put(path, obj, vid);
                    break;
                case ObjKind::LEAF:
                    header.children_vid_ = 0;
                    status = version_map->put(path, header);
                    status =  status && leaf_obj_store->put(path, obj, vid);
                    break;
                default:
                    break;
            }

            if (!status) {
                response->add_err("Trouble loading object to obj_store: " + std::string(path_slice.data(), path_slice.size()));
                break;
            }
        }
        
        iter->Next();
        
    }

    response->set_success(status);
    response->set_vid(vid);
    task->status_ = ClientTaskStatus::FINISH;
    static_cast<grpc::ServerAsyncReader<BulkLoadResponse, BulkLoadRequest>*>(task->writer_
            )->Finish(*response, grpc::Status::OK, task);

    // have to clean up the obj_store
    if (!status) {
        while (iter->Valid()) {
            rocksdb::Slice path_slice = iter->key();
            Path path(std::string_view(path_slice.data(), path_slice.size()));
            rocksdb::Slice obj = iter->value();
            mongo::BSONObj obj_meta = mongo::BSONObj(obj.data()).firstElement().Obj();

            bool status;
            switch (static_cast<ObjKind>(obj_meta.getField("obj_kind").numberLong())) {
                case ObjKind::INNER:
                case ObjKind::INNER_LEAF:
                    status = version_map->remove(path);
                    status = status && inner_obj_store->remove(path, vid, false);
                    break;
                case ObjKind::LEAF:
                    status = version_map->remove(path);
                    status = status && leaf_obj_store->remove(path, vid, false);
                    break;
                default:
                    break;
            }
            
            iter->Prev();
            
        }

    }

    delete iter.release();
    delete load_session->temp_db_;
    // Careful!
    std::system(("rm -rf " + load_session->temp_db_path_).c_str());

    delete load_session;
}

void terminateBulkLoad(ClientTask * task, LoadSession * load_session, BulkLoadResponse * load_response) {
    load_response->set_success(false);
    task->status_ = ClientTaskStatus::FINISH;
    static_cast<grpc::ServerAsyncReader<BulkLoadResponse, BulkLoadRequest>*>(task->writer_
            )->Finish(*load_response, grpc::Status::OK, task);

    delete load_session->temp_db_;
    // Careful!
    std::system(("rm -rf " + load_session->temp_db_path_).c_str());
    delete load_session;
}