// Author: Keonwoo Oh (koh3@umd.edu)

#include <cstring>
#include <endian.h>

#include "storage/versionmap.h"


VersionMap::Iterator::Iterator(rocksdb::DB *db, rocksdb::ColumnFamilyHandle * version_map_store, 
        const Path & lower_bound, const Path & upper_bound) : db_(db), version_map_store_(version_map_store) {
    reset(lower_bound, upper_bound);
}

VersionMap::Iterator::~Iterator() { }
                
void VersionMap::Iterator::reset(const Path & lower_bound, const Path & upper_bound) {
    rocksdb::ReadOptions ro_with_upper_bound;
    upper_bound_str_.clear();
    upper_bound_str_.append(upper_bound.data_, upper_bound.size_);
    upper_bound_.reset(new rocksdb::Slice(upper_bound_str_.data(), upper_bound_str_.size()));
    ro_with_upper_bound.iterate_upper_bound = upper_bound_.get();
    version_map_iter_.reset(db_->NewIterator(ro_with_upper_bound, version_map_store_));
    version_map_iter_->Seek(rocksdb::Slice(lower_bound.data_, lower_bound.size_));
    valid_ = version_map_iter_->Valid();
}

void VersionMap::Iterator::next() {
    version_map_iter_->Next();
    valid_ = version_map_iter_->Valid();
}

rocksdb::Slice VersionMap::Iterator::key() {
    return version_map_iter_->key();
}

VersionMap::Header VersionMap::Iterator::value() {
    return Header(version_map_iter_->value().data_);
}

bool VersionMap::Iterator::valid() {
    return valid_;
}


VersionMap::VersionMap(rocksdb::DB * db, rocksdb::ColumnFamilyHandle * version_map_store) : 
    db_(db), version_map_store_(version_map_store) {
    
}

VersionMap::~VersionMap() { }

bool VersionMap::get(const Path &path, Header * value) {
    rocksdb::Slice path_slice(path.data_, path.size_);
    rocksdb::PinnableSlice pinned_value;
    rocksdb::Status status;

    status = db_->Get(read_options_, version_map_store_, path_slice, &pinned_value);

    if (!status.ok() || pinned_value.size() != sizeof(Header)) {
        return false;
    }

    *value = Header(pinned_value.data());
    
    return true;
}

bool VersionMap::contains(const Path &path) {
    rocksdb::Slice path_slice(path.data_, path.size_);
    rocksdb::PinnableSlice pinned_value;
    rocksdb::Status status;

    status = db_->Get(read_options_, version_map_store_, path_slice, &pinned_value);

    return (status.ok() && pinned_value.size() == sizeof(Header));

}

bool VersionMap::put(const Path &path, const Header & val) {
    Header header;
    header.delta_vid_ = htole64(val.delta_vid_);
    header.children_vid_ = htole64(val.children_vid_);
    
    return db_->Put(write_options_ , version_map_store_ , rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(reinterpret_cast<char*>(&header), sizeof(Header))).ok();
}

bool VersionMap::remove(const Path &path) {
    auto status = db_->Delete(write_options_, version_map_store_, rocksdb::Slice(path.data_, path.size_));
    return status.ok(); 
}

VersionMap::Iterator * VersionMap::newIterator(const Path & lower_bound, const Path & upper_bound) {
   return new VersionMap::Iterator(db_, version_map_store_, lower_bound, upper_bound);
}
