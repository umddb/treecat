// Author: Keonwoo Oh (koh3@umd.edu)

#include <cstring>
#include <endian.h>

#include "storage/innerobjstore.h"


InnerObjStore::InnerObjStore(rocksdb::DB * db, rocksdb::ColumnFamilyHandle * delta_store, rocksdb::ColumnFamilyHandle *snapshot_store) : 
    db_(db), delta_store_(delta_store), snapshot_store_(snapshot_store){
    read_options_.async_io = true;
    read_options_.readahead_size = 0;
    read_options_.adaptive_readahead = true;
}

InnerObjStore::~InnerObjStore() { }

bool InnerObjStore::get(const Path &path, uint64_t vid, mongo::BSONObj * value) {
    rocksdb::Slice path_slice(path.data_, path.size_);
    rocksdb::PinnableSlice cur_snapshot;
    rocksdb::Status status;

    status = db_->Get(read_options_, snapshot_store_, path_slice, &cur_snapshot);

    if (!status.ok() || cur_snapshot.size() < sizeof(SnapshotStore::Header) + mongo::BSONObj::kMinBSONLength) {
        return false;
    }
    // if current snapshot is visible, return
    if (SnapshotStore::visible(vid, cur_snapshot)) {
        *value = mongo::BSONObj(&cur_snapshot.data()[sizeof(SnapshotStore::Header)]).getOwned();
        return true; 
    }
    else if (SnapshotStore::removed(vid, cur_snapshot) || !SnapshotStore::hasDelta(cur_snapshot)) {
        return false;
    }
    // else traverse delta store and rollback the deltas
    std::unique_ptr<rocksdb::Iterator> delta_iter(db_->NewIterator(read_options_, delta_store_));
    // seek to the last modified version
    rocksdb::Slice last_delta_key = DeltaStore::lastDeltaKey(path_slice, cur_snapshot);
    
    delta_iter->Seek(last_delta_key);
    free(const_cast<char*>(last_delta_key.data()));

    bool valid_delta;
    DeltaStore::Header delta_vids(0,0);
    
    valid_delta = delta_iter->Valid() && DeltaStore::validPath(delta_iter->key(), path_slice);
    if (valid_delta) {
        delta_vids = DeltaStore::Header(delta_iter->key().data());
    }
    while(valid_delta && delta_vids.start_vid_ > vid) {
        delta_iter->Next();
        valid_delta = delta_iter->Valid() && DeltaStore::validPath(delta_iter->key(), path_slice);
        if (valid_delta) {
            delta_vids = DeltaStore::Header(delta_iter->key().data());
        }
    }

    if (vid >= delta_vids.start_vid_ && vid < delta_vids.end_vid_ 
            && delta_iter->value().size() > mongo::BSONObj::kMinBSONLength) {
        *value = mongo::BSONObj(delta_iter->value().data()).getOwned();
        return true;
    }

    return false;
}

bool InnerObjStore::contains(const Path &path, uint64_t vid) {
    rocksdb::Slice path_slice(path.data_, path.size_);
    rocksdb::PinnableSlice cur_snapshot;
    rocksdb::Status status;

    status = db_->Get(read_options_, snapshot_store_, path_slice, &cur_snapshot);

    if (!status.ok() || cur_snapshot.size() < sizeof(SnapshotStore::Header) + mongo::BSONObj::kMinBSONLength) {
        return false;
    }
    // if current snapshot is visible, return
    if (SnapshotStore::visible(vid, cur_snapshot)) {
        return true; 
    }
    else if (SnapshotStore::removed(vid, cur_snapshot) || !SnapshotStore::hasDelta(cur_snapshot)) {
        return false;
    }
    // else traverse delta store and rollback the deltas
    std::unique_ptr<rocksdb::Iterator> delta_iter(db_->NewIterator(read_options_, delta_store_));
    // seek to the last modified version
    rocksdb::Slice last_delta_key = DeltaStore::lastDeltaKey(path_slice, cur_snapshot);
    
    delta_iter->Seek(last_delta_key);
    free(const_cast<char*>(last_delta_key.data()));

    bool valid_delta;
    DeltaStore::Header delta_vids(0,0);
    
    valid_delta = delta_iter->Valid() && DeltaStore::validPath(delta_iter->key(), path_slice);
    if (valid_delta) {
        delta_vids = DeltaStore::Header(delta_iter->key().data());
    }
    while(valid_delta && delta_vids.start_vid_ > vid) {
        delta_iter->Next();
        valid_delta = delta_iter->Valid() && DeltaStore::validPath(delta_iter->key(), path_slice);
        if (valid_delta) {
            delta_vids = DeltaStore::Header(delta_iter->key().data());
        }
    }

    if (vid >= delta_vids.start_vid_ && vid < delta_vids.end_vid_ 
            && delta_iter->value().size() > mongo::BSONObj::kMinBSONLength) {
        return true;
    }

    return false;
}

bool InnerObjStore::getCurSnapshot(const Path &path, mongo::BSONObj * val, SnapshotStore::Header* header) {
    rocksdb::PinnableSlice cur_snapshot;
    rocksdb::Status status = db_->Get(read_options_, snapshot_store_, rocksdb::Slice(path.data_, path.size_), &cur_snapshot);
    if (status.ok()) {
        *header = SnapshotStore::Header(cur_snapshot.data()); 
        *val = mongo::BSONObj(&cur_snapshot.data()[sizeof(SnapshotStore::Header)]).getOwned();
        return true;
    }

    return false;
    
}

bool InnerObjStore::put(const Path &path) {
    return db_->Put(write_options_ , snapshot_store_ , rocksdb::Slice(path.data_, path.size_), rocksdb::Slice()).ok();
}

// This for adding an object for the first time. Upsert function is to be implemented.
bool InnerObjStore::put(const Path &path, const mongo::BSONObj & val, uint64_t vid) {
    SnapshotStore::Header header;
    header.cur_vid_ = htole64(vid);
    header.delta_vid_ = htole64(0);

    size_t final_size = sizeof(SnapshotStore::Header) + val.objsize();
    char * buf = static_cast<char*>(malloc(final_size));
    memcpy(buf, &header, sizeof(SnapshotStore::Header));
    memcpy(&buf[sizeof(SnapshotStore::Header)], val.objdata(), val.objsize());
    auto status = db_->Put(write_options_, snapshot_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(buf, final_size));
    free(buf);
    return status.ok();
}

bool InnerObjStore::remove(const Path &path, uint64_t vid, bool is_delta) {
    if (is_delta) {

    }
    else {
        auto status = db_->Delete(write_options_, snapshot_store_, rocksdb::Slice(path.data_, path.size_));
        return status.ok();
    }

    return false;
}

InnerObjStore::Iterator* InnerObjStore::newIterator(const Path &path, uint64_t vid, const ReadOptions & read_options) {
    return new Iterator(path, vid, db_, delta_store_, snapshot_store_, read_options);
}

InnerObjStore::Iterator::Iterator(const Path &path, uint64_t vid, rocksdb::DB *db, rocksdb::ColumnFamilyHandle * delta_store, 
                        rocksdb::ColumnFamilyHandle *snapshot_store, const ReadOptions & read_options) : 
                        vid_(vid), db_(db), delta_store_(delta_store), snapshot_store_(snapshot_store) {
    
    snapshot_iter_.reset(db_->NewIterator(read_options_, snapshot_store_));
    delta_iter_.reset(db_->NewIterator(read_options_, delta_store_));
    reset(path, vid, read_options);   
}

InnerObjStore::Iterator::~Iterator() { } 

void InnerObjStore::Iterator::reset(const Path &path, uint64_t vid, const ReadOptions & read_options) {
    vid_ = vid;
    //clear path string first
    path_str_.clear();
    //if the path does not end with a slash, append it and update the depth
    if (path.data_[path.size_ - 1] != '/') {
        path_str_.reserve(path.size_ + 1);
        path_str_.append(path.data_, path.size_);
        path_str_.append(1, '/');

        //update depth
        uint32_t depth;
        memcpy(&depth, path.data_, sizeof(uint32_t));
        depth = le32toh(depth);
        depth++;
        depth = htole32(depth);
        memcpy(const_cast<char*>(path_str_.data()), &depth, sizeof(uint32_t));
    }
    else {
        path_str_.append(path.data_, path.size_);
    }

    path_ =  rocksdb::Slice(path_str_.data(), path_str_.size());

    //if upper_bound is set, create new iterator with upper bound
    if (!read_options.upper_bound_.empty()) {
        rocksdb::ReadOptions ro_with_upper_bound;
        upper_bound_str_.clear();
        upper_bound_str_.reserve(path_str_.size() + read_options.upper_bound_.size());
        upper_bound_str_.append(path_str_);
        upper_bound_str_.append(read_options.upper_bound_);
        upper_bound_.reset(new rocksdb::Slice(upper_bound_str_.data(), upper_bound_str_.size()));
        ro_with_upper_bound.iterate_upper_bound = upper_bound_.get();
        snapshot_iter_.reset(db_->NewIterator(ro_with_upper_bound, snapshot_store_));
    }
    // if there is no upper bound, but one was set previously,
    // creat new default iterator and clear out the upper bound
    else if (!upper_bound_str_.empty()) {
        upper_bound_str_.clear();
        snapshot_iter_.reset(db_->NewIterator(read_options_, snapshot_store_));
    }

    // if lower bound is set, seek to the lower bound
    if (!read_options.lower_bound_.empty()) {
        std::string lower_bound_str;
        lower_bound_str.reserve(path_str_.size() + read_options.lower_bound_.size());
        lower_bound_str.append(path_str_);
        lower_bound_str.append(read_options.lower_bound_);
        snapshot_iter_->Seek(lower_bound_str);
    }
    // else seek to the first child
    else {
        snapshot_iter_->Seek(path_);
    }

    // pretty much same logic as next() without calling next on the rocksdb::iterator
    // bit messy, but saw no way around this. 
    bool found = false;
    valid_ = snapshot_iter_->Valid() && prefixMatch();
    
    if (valid_) {
        rocksdb::Slice cur_path = snapshot_iter_->key();
        rocksdb::Slice cur_snapshot = snapshot_iter_->value();
        // The current snapshot is visible
        if (SnapshotStore::visible(vid_, cur_snapshot)) {
            found = true;
            snapshot_ = rocksdb::Slice(&cur_snapshot.data()[sizeof(SnapshotStore::Header)], 
                    cur_snapshot.size() - sizeof(SnapshotStore::Header)); 
        }
        // If the object is not removed and has delta, roll back deltas to recover the right version
        else if (!SnapshotStore::removed(vid_, cur_snapshot) && SnapshotStore::hasDelta(cur_snapshot)) {
            rocksdb::Slice last_delta_key = DeltaStore::lastDeltaKey(cur_path, cur_snapshot);
            delta_iter_->Seek(last_delta_key);
            free(const_cast<char*>(last_delta_key.data()));
            
            bool valid_delta;
            DeltaStore::Header delta_vids(0,0);
            valid_delta = delta_iter_->Valid() && DeltaStore::validPath(delta_iter_->key(), cur_path);
            if (valid_delta) {
                delta_vids = DeltaStore::Header(delta_iter_->key().data());
            }
            while(valid_delta && delta_vids.start_vid_ > vid_) {
                delta_iter_->Next();
                valid_delta = delta_iter_->Valid() && DeltaStore::validPath(delta_iter_->key(), cur_path);
                if (valid_delta) {
                    delta_vids = DeltaStore::Header(delta_iter_->key().data());
                }
            }

            if (vid_ >= delta_vids.start_vid_ && vid_ < delta_vids.end_vid_ 
                    && delta_iter_->value().size() > mongo::BSONObj::kMinBSONLength) {
                snapshot_ = delta_iter_->value();
                found = true;
            }
        }    

    }

    if (!found && valid_) {
        next();
    }
    
}

SnapshotStore::Header InnerObjStore::Iterator::snapshotHeader() {
    return SnapshotStore::Header(snapshot_iter_->value().data_);
}

bool InnerObjStore::Iterator::removed() {
    return SnapshotStore::removed(vid_, snapshot_iter_->value());
}

void InnerObjStore::Iterator::next() {
    bool found = false;
    while (!found && valid_) {
        snapshot_iter_->Next();
        valid_ = snapshot_iter_->Valid() && prefixMatch();
        if (valid_) {
            rocksdb::Slice cur_path = snapshot_iter_->key();
            rocksdb::Slice cur_snapshot = snapshot_iter_->value();
            // The current snapshot is visible
            if (SnapshotStore::visible(vid_, cur_snapshot)) {
                found = true;
                snapshot_ = rocksdb::Slice(&cur_snapshot.data()[sizeof(SnapshotStore::Header)], 
                        cur_snapshot.size() - sizeof(SnapshotStore::Header)); 
            }
            // If the object is not removed and has delta, roll back deltas to recover the right version
            else if (!SnapshotStore::removed(vid_, cur_snapshot) && SnapshotStore::hasDelta(cur_snapshot)) {
                rocksdb::Slice last_delta_key = DeltaStore::lastDeltaKey(cur_path, cur_snapshot);
                delta_iter_->Seek(last_delta_key);
                free(const_cast<char*>(last_delta_key.data()));
                
                bool valid_delta;
                DeltaStore::Header delta_vids(0,0);
                valid_delta = delta_iter_->Valid() && DeltaStore::validPath(delta_iter_->key(), cur_path);
                if (valid_delta) {
                    delta_vids = DeltaStore::Header(delta_iter_->key().data());
                }
                while(valid_delta && delta_vids.start_vid_ > vid_) {
                    delta_iter_->Next();
                    valid_delta = delta_iter_->Valid() && DeltaStore::validPath(delta_iter_->key(), cur_path);
                    if (valid_delta) {
                        delta_vids = DeltaStore::Header(delta_iter_->key().data());
                    }
                }

                if (vid_ >= delta_vids.start_vid_ && vid_ < delta_vids.end_vid_ 
                        && delta_iter_->value().size() > mongo::BSONObj::kMinBSONLength) {
                    snapshot_ = delta_iter_->value();
                    found = true;
                }
            }    

        }
    
    }

}

bool InnerObjStore::Iterator::prefixMatch() {
    rocksdb::Slice path = snapshot_iter_->key();
    if (path.size() <= path_.size()) {
        return false;
    }

    return !memcmp(path.data(), path_.data(), path_.size());
}

rocksdb::Slice InnerObjStore::Iterator::key() {
    if (valid_) {
        return snapshot_iter_->key();
    }

    return rocksdb::Slice(nullptr, 0);

}


mongo::BSONObj InnerObjStore::Iterator::value() {
    if (valid_) {
        return mongo::BSONObj(snapshot_.data());
    }

    return mongo::BSONObj();
}

uint64_t InnerObjStore::Iterator::vid() {
    return vid_;
}

// uint64_t InnerObjStore::Iterator::lastVid() {
//     if (valid_) {
//         return snapshot_.lastVid();
//     }

//     return vid_;
// }

bool InnerObjStore::Iterator::valid() {
    return valid_;
}
