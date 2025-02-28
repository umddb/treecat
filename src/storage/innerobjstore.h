// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef INNER_OBJ_STORE_H
#define INNER_OBJ_STORE_H

#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "common/bson/bsonobj.h"
#include "common/bson/document.h"
#include "common/path.h"
#include "storage/deltastore.h"
#include "storage/snapshotstore.h"

namespace mmb = mongo::mutablebson;

class InnerObjStore {
    
    public:
        // lower bound is inclusive, upper_bound is exclusive
        struct ReadOptions {
            std::string lower_bound_;
            std::string upper_bound_;
        };

        class Iterator {
            
            public:
                Iterator(const Path &path, uint64_t vid, rocksdb::DB *db, rocksdb::ColumnFamilyHandle * delta_store, 
                        rocksdb::ColumnFamilyHandle *snapshot_store, const ReadOptions & read_options);
                ~Iterator();
                void reset(const Path &path, uint64_t vid, const ReadOptions & read_options);
                
                void next();
                rocksdb::Slice key();
                
                mongo::BSONObj value();
                uint64_t vid();
                // uint64_t lastVid();
                bool valid();
                SnapshotStore::Header snapshotHeader();
                bool removed();

            private:
                uint64_t vid_;
                bool valid_;
                std::string path_str_;
                rocksdb::Slice path_;
                rocksdb::DB *db_;
                rocksdb::ColumnFamilyHandle *delta_store_;
                rocksdb::ColumnFamilyHandle *snapshot_store_;
                rocksdb::ReadOptions read_options_;
                std::unique_ptr<rocksdb::Iterator> delta_iter_;
                std::unique_ptr<rocksdb::Iterator> snapshot_iter_;
                rocksdb::Slice snapshot_;
                std::string upper_bound_str_;
                std::unique_ptr<rocksdb::Slice> upper_bound_;

                bool prefixMatch();

        };

        InnerObjStore(rocksdb::DB * db, rocksdb::ColumnFamilyHandle * delta_store, rocksdb::ColumnFamilyHandle *snapshot_store);
        ~InnerObjStore();
        bool get(const Path &path, uint64_t vid, mongo::BSONObj * value);
        bool contains(const Path &path, uint64_t vid);
        bool getCurSnapshot(const Path &path, mongo::BSONObj * val, SnapshotStore::Header* header);
        bool put(const Path &path);
        bool put(const Path &path, const mongo::BSONObj & val, uint64_t vid);
        bool remove(const Path &path, uint64_t vid, bool is_delta);
      
        //TODO pass in predicate
        Iterator * newIterator(const Path &path, uint64_t vid, const ReadOptions & read_options);

    private:
        rocksdb::DB *db_;
        rocksdb::ColumnFamilyHandle *delta_store_;
        rocksdb::ColumnFamilyHandle *snapshot_store_;
        rocksdb::ReadOptions read_options_;
        rocksdb::WriteOptions write_options_;

};

#endif // INNER_OBJ_STORE