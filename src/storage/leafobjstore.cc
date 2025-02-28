// Author: Keonwoo Oh (koh3@umd.edu)

#include <cstring>
#include <endian.h>

#include "storage/leafobjstore.h"

LeafObjStore::LeafObjStore(rocksdb::DB * db, rocksdb::ColumnFamilyHandle * leaf_store) : 
    db_(db), leaf_store_(leaf_store) {
    read_options_.async_io = true;
    read_options_.readahead_size = 0;
    read_options_.adaptive_readahead = true;
}

LeafObjStore::~LeafObjStore() { }

bool LeafObjStore::get(const Path &path, uint64_t vid, mongo::BSONObj * value) {
    rocksdb::Slice path_slice(path.data_, path.size_);
    rocksdb::PinnableSlice path_or_value;
    rocksdb::PinnableSlice real_value;
    rocksdb::Status status;

    status = db_->Get(read_options_, leaf_store_, path_slice, &path_or_value);

    if (!status.ok() || path_or_value.size() < sizeof(Header) + mongo::BSONObj::kMinBSONLength) {
        return false;
    }
    
    Header leaf_header(path_or_value.data());
    // if visible in the given vid
    if (LeafObjStore::visible(vid, leaf_header)) {
        if (static_cast<bool>(leaf_header.is_primary_)) {
            // the retrieved value has the object
            *value = mongo::BSONObj(&path_or_value.data()[sizeof(Header)]).getOwned();
            return true;
        }
        else {
            // the retrieved value was the primary path
            status = db_->Get(read_options_, leaf_store_, rocksdb::Slice(&path_or_value.data()[sizeof(Header)], 
                    path_or_value.size() - sizeof(Header)), &real_value);
            if (status.ok() && real_value.size() >= sizeof(Header) + mongo::BSONObj::kMinBSONLength) {
               *value = mongo::BSONObj(&real_value.data()[sizeof(Header)]).getOwned(); 
                return true;
            }
        }

    }

    return false;    

}

Path LeafObjStore::getPrimaryPath(const Path &path, uint64_t vid) {
    rocksdb::Slice path_slice(path.data_, path.size_);
    rocksdb::PinnableSlice path_or_value;
    rocksdb::PinnableSlice real_value;
    rocksdb::Status status;

    status = db_->Get(read_options_, leaf_store_, path_slice, &path_or_value);

    if (!status.ok() || path_or_value.size() < sizeof(Header) + mongo::BSONObj::kMinBSONLength) {
        return Path();
    }
    
    Header leaf_header(path_or_value.data());
    // if visible in the given vid
    if (LeafObjStore::visible(vid, leaf_header)) {
        if (static_cast<bool>(leaf_header.is_primary_)) {
            return Path(path.data_, path.size_, true);
        }
        else {
            return Path(&path_or_value.data()[sizeof(Header)], path_or_value.size() - sizeof(Header), true);
        }

    }

    return Path();   

}




bool LeafObjStore::contains(const Path &path, uint64_t vid) {
    rocksdb::Slice path_slice(path.data_, path.size_);
    rocksdb::PinnableSlice path_or_value;
    rocksdb::PinnableSlice real_value;
    rocksdb::Status status;

    status = db_->Get(read_options_, leaf_store_, path_slice, &path_or_value);
    // coincidentally kMinBSONLength is equal to min path length
    if (!status.ok() || path_or_value.size() < sizeof(Header) + mongo::BSONObj::kMinBSONLength) {
        return false;
    }
    
    Header leaf_header(path_or_value.data());
    // if visible in the given vid
    if (LeafObjStore::visible(vid, leaf_header)) {
        if (static_cast<bool>(leaf_header.is_primary_)) {
            // the retrieved value has the object
            return true;
        }
        else {
            // the retrieved value was the primary path
            return db_->Get(read_options_, leaf_store_, rocksdb::Slice(&path_or_value.data()[sizeof(Header)], 
                    path_or_value.size() - sizeof(Header)), &real_value).ok();
        }
    }

    return false;    

}

//put function for dummy value
bool LeafObjStore::put(const Path &path) {
    return db_->Put(write_options_ , leaf_store_ , rocksdb::Slice(path.data_, path.size_), rocksdb::Slice()).ok();
}


bool LeafObjStore::put(const Path &primary_path, const mongo::BSONObj & val, uint64_t vid) {
    Header leaf_header;
    leaf_header.create_vid_ = htole64(vid);
    leaf_header.tombstone_vid_ = htole64(0);
    mongo::BSONObj paths = val.firstElement().Obj().getField("paths").Obj();
    //obj size is enough as it holds all the paths as its properties.
    char * buf = static_cast<char*>(malloc(sizeof(LeafObjStore::Header) + val.objsize()));
    
    bool status = true;
    int index = 0;
    for (mongo::BSONElement path_elm: paths) {
        mongo::StringData path_str = path_elm.checkAndGetStringData();
        Path path(std::string_view(path_str.rawData(), path_str.size()));

        size_t final_size;
        if (index == 0) {
            leaf_header.is_primary_ = static_cast<char>(true);
            final_size = sizeof(LeafObjStore::Header) + val.objsize();
            memcpy(buf, &leaf_header, sizeof(LeafObjStore::Header));
            memcpy(&buf[sizeof(LeafObjStore::Header)], val.objdata(), val.objsize());
        }
        else {
            leaf_header.is_primary_ = static_cast<char>(false);
            final_size = sizeof(LeafObjStore::Header) + primary_path.size_;
            memcpy(buf, &leaf_header, sizeof(LeafObjStore::Header));
            memcpy(&buf[sizeof(LeafObjStore::Header)], primary_path.data_, primary_path.size_);
        }

        status = status && db_->Put(write_options_ , leaf_store_ , rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(buf, final_size)).ok();
        index++;
    }

    free(buf);
    return status;
    
}

bool LeafObjStore::remove(const Path &path, uint64_t vid, bool is_delta) {
    if (is_delta) {

    }
    else {
        auto status = db_->Delete(write_options_, leaf_store_, rocksdb::Slice(path.data_, path.size_));
        return status.ok();
    }

    return false;
}

LeafObjStore::Iterator* LeafObjStore::newIterator(const Path &path, uint64_t vid, const ReadOptions & read_options) {
    return new Iterator(path, vid, db_, leaf_store_, read_options);
}

LeafObjStore::Iterator::Iterator(const Path &path, uint64_t vid, rocksdb::DB *db, 
                        rocksdb::ColumnFamilyHandle * leaf_store, const ReadOptions & read_options) : 
                        vid_(vid), db_(db), leaf_store_(leaf_store) {
    leaf_iter_.reset(db_->NewIterator(read_options_, leaf_store_));
    reset(path, vid, read_options);
}

LeafObjStore::Iterator::~Iterator() { } 

void LeafObjStore::Iterator::reset(const Path &path, uint64_t vid, const ReadOptions & read_options) {
    uint32_t depth;
    vid_ = vid;
    //clear out the previous path
    path_str_.clear();
    //if the path does not end with a slash, append it and update the depth
    if (path.data_[path.size_ - 1] != '/') {
        path_str_.reserve(path.size_ + 1);
        path_str_.append(path.data_, path.size_);
        path_str_.append(1, '/');

        //update depth
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
        leaf_iter_.reset(db_->NewIterator(ro_with_upper_bound, leaf_store_));
    }
    // if there is no upper bound, but one was set previously,
    // creat new default iterator and clear out the upper bound
    else if (!upper_bound_str_.empty()) {
        upper_bound_str_.clear();
        leaf_iter_.reset(db_->NewIterator(read_options_, leaf_store_));
    }

    // if lower bound is set, seek to the lower bound
    if (!read_options.lower_bound_.empty()) {
        std::string lower_bound_str;
        lower_bound_str.reserve(path_str_.size() + read_options.lower_bound_.size());
        lower_bound_str.append(path_str_);
        lower_bound_str.append(read_options.lower_bound_);
        leaf_iter_->Seek(lower_bound_str);
    }
    // else seek to the first child
    else {
        leaf_iter_->Seek(path_);
    }

    bool found = false;
    valid_ = leaf_iter_->Valid() && prefixMatch();
    // pretty much same logic as next() without calling next on the rocksdb::iterator
    // bit messy, but saw no way around this. 
    if (valid_) {
        rocksdb::Slice path_or_value = leaf_iter_->value();
        Header leaf_header(path_or_value.data());
        // The object is visible
        if (LeafObjStore::visible(vid_, leaf_header)) {
            if (static_cast<bool>(leaf_header.is_primary_)) {
                // the retrieved value has the object
                value_ = mongo::BSONObj(&path_or_value.data()[sizeof(Header)]);
                found =  true;
            }
            else {
                // the retrieved value was the primary path
                real_value_.Reset();
                rocksdb::Status status = db_->Get(read_options_, leaf_store_, rocksdb::Slice(&path_or_value.data()[sizeof(Header)], 
                        path_or_value.size() - sizeof(Header)), &real_value_);
                if (status.ok()) {
                    value_ = mongo::BSONObj(&real_value_.data()[sizeof(Header)]);
                    found = true;
                }
                
            }    
        }
    }

    if (!found && valid_) {
       next(); 
    }
    
}

void LeafObjStore::Iterator::next() {
    bool found = false;
    while (!found && valid_) {
        leaf_iter_->Next();
        valid_ = leaf_iter_->Valid() && prefixMatch();
        if (valid_) {
            rocksdb::Slice path_or_value = leaf_iter_->value();
            Header leaf_header(path_or_value.data());
            // The object is visible
            if (LeafObjStore::visible(vid_, leaf_header)) {
                if (static_cast<bool>(leaf_header.is_primary_)) {
                    // the retrieved value has the object
                    value_ = mongo::BSONObj(&path_or_value.data()[sizeof(Header)]);
                    found =  true;
                }
                else {
                    // the retrieved value was the primary path
                    real_value_.Reset();
                    rocksdb::Status status = db_->Get(read_options_, leaf_store_, rocksdb::Slice(&path_or_value.data()[sizeof(Header)], 
                            path_or_value.size() - sizeof(Header)), &real_value_);
                    if (status.ok()) {
                        value_ = mongo::BSONObj(&real_value_.data()[sizeof(Header)]);
                        found = true;
                    }
                    
                }
    
            }          

        }
    }

}

bool LeafObjStore::Iterator::prefixMatch() {
    rocksdb::Slice path = leaf_iter_->key();
    if (path.size() <= path_.size()) {
        return false;
    }

    return !memcmp(path.data(), path_.data(), path_.size());
}

bool LeafObjStore::visible(uint64_t vid, LeafObjStore::Header header) {
    if (vid >= header.create_vid_ && (header.tombstone_vid_ == 0 || vid < header.tombstone_vid_)) {
        return true;
    }
    return false;
}

rocksdb::Slice LeafObjStore::Iterator::key() {
    if (valid_) {
        return leaf_iter_->key();
    }

    return rocksdb::Slice(nullptr, 0);

}


mongo::BSONObj* LeafObjStore::Iterator::value() {
    if (valid_) {
        return &value_;
    }

    return nullptr;
}

uint64_t LeafObjStore::Iterator::vid() {
    return vid_;
}

bool LeafObjStore::Iterator::valid() {
    return valid_;
}