// Author: Keonwoo Oh (koh3@umd.edu)

#include <cstring>
#include <endian.h>

#include "storage/snapshotmap.h"

SnapshotMap::SnapshotMap(rocksdb::DB * db, rocksdb::ColumnFamilyHandle * snapshot_map_store) : 
    db_(db), snapshot_map_store_(snapshot_map_store) {
    
}


SnapshotMap::~SnapshotMap() { }


bool SnapshotMap::get(const std::string& name, uint64_t * value) {
    rocksdb::Slice name_slice(name.data(), name.size());
    rocksdb::PinnableSlice pinned_value;
    rocksdb::Status status;

    status = db_->Get(read_options_, snapshot_map_store_, name_slice, &pinned_value);

    if (!status.ok() || pinned_value.size() != sizeof(uint64_t)) {
        return false;
    }

    memcmp(value, pinned_value.data(), sizeof(uint64_t));
    *value = le64toh(*value);
    
    return true;
}


bool SnapshotMap::contains(const std::string& name) {
    rocksdb::Slice name_slice(name.data(), name.size());
    rocksdb::PinnableSlice pinned_value;
    rocksdb::Status status;

    status = db_->Get(read_options_, snapshot_map_store_, name_slice, &pinned_value);

    return (status.ok() && pinned_value.size() == sizeof(uint64_t));

}


bool SnapshotMap::put(const std::string& name, uint64_t value) {
    uint64_t le_value = htole64(value);

    auto status = db_->Put(write_options_ , snapshot_map_store_ , rocksdb::Slice(name.data(), name.size()),
        rocksdb::Slice(reinterpret_cast<char*>(&le_value), sizeof(uint64_t)));
    
    return status.ok() && db_->SyncWAL().ok();
}


bool SnapshotMap::remove(const std::string& name) {
    auto status = db_->Delete(write_options_, snapshot_map_store_, rocksdb::Slice(name.data(), name.size()));
    return status.ok(); 
}