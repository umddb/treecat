// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef SNAPSHOT_MAP_H
#define SNAPSHOT_MAP_H

#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "common/path.h"

class SnapshotMap {
    public:
        SnapshotMap(rocksdb::DB * db, rocksdb::ColumnFamilyHandle * snapshot_map_store);
        ~SnapshotMap();
        bool get(const std::string& name, uint64_t * value);
        bool contains(const std::string& name);
        bool put(const std::string& name, uint64_t value);
        bool remove(const std::string& name);

    private:
        rocksdb::DB *db_;
        rocksdb::ColumnFamilyHandle *snapshot_map_store_;
        rocksdb::ReadOptions read_options_;
        rocksdb::WriteOptions write_options_;

};

#endif