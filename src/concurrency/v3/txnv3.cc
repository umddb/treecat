// Author: Keonwoo Oh (koh3@umd.edu)

#include <cstdint>
#include <stack>
#include <endian.h>

#include "concurrency/v3/txnv3.h"
#include "concurrency/v3/txnmgrv3.h"
#include "common/mmbutil.h"

TransactionV3::TransactionV3(TransactionManagerV3 * txn_mgr, uint64_t txn_id, ObjStore * obj_store) : 
    txn_mgr_(txn_mgr), txn_id_(txn_id), processed_(false) {
    read_vid_ = UINT64_MAX;
    abort_.store(false);
    read_set_ = std::make_unique<ReadSet>(txn_mgr_->lockManager(), this);
    write_set_ = std::make_unique<WriteSet>(txn_mgr_->lockManager(), this, obj_store);
    owned_locks_ = std::make_unique<boost::unordered_flat_map<Path, LockMode>>();
}

TransactionV3::~TransactionV3() { }

uint64_t TransactionV3::readVid() {
    return read_vid_;
}

Transaction::ReadSet * TransactionV3::readSet() {
    return static_cast<Transaction::ReadSet*>(read_set_.get());
}

void TransactionV3::addQueryRequest(ExecuteQueryRequest * query_request) {  }

TransactionV3::WriteSet * TransactionV3::writeSet() {
    return write_set_.get();
}

//clear out read and write sets
void TransactionV3::clear() {
    read_set_.reset();
    write_set_.reset();
    owned_locks_.reset();
}

bool TransactionV3::abort() {
    return abort_.load();
}

TransactionV3::ReadSet::ReadSet(LockManager * lock_mgr, TransactionV3 * txn) :
    lock_mgr_(lock_mgr), txn_(txn) {
    
}

TransactionV3::ReadSet::~ReadSet() { }

bool TransactionV3::ReadSet::add(const Path & path, LockMode lock_mode) {
    bool abort;
    lock_mgr_->lock(path, txn_, lock_mode);
    {   std::lock_guard<std::mutex> txn_lock(txn_->mtx_);
        abort = txn_->abort_.load(); }
    if (abort) {
        lock_mgr_->unlock(path, txn_);
    }
    
    return !abort; 
}

boost::unordered_flat_map<Path, LockMode> * TransactionV3::ReadSet::ownedLocks() {
    return txn_->owned_locks_.get();
}

TransactionV3::WriteSet::WriteSet(LockManager * lock_mgr, TransactionV3 * txn, ObjStore * obj_store) :
    lock_mgr_(lock_mgr), txn_(txn), obj_store_(obj_store) {
    write_batch_ = std::make_unique<rocksdb::WriteBatchWithIndex>();
    merge_set_ = std::make_unique<boost::unordered_flat_map<Path, std::pair<size_t, size_t>>>();
}

TransactionV3::WriteSet::~WriteSet() { }


rocksdb::WriteBatchWithIndex * TransactionV3::WriteSet::getWriteBatch() { 
    return write_batch_.get();
}

boost::unordered_flat_map<Path, std::pair<size_t, size_t>>* TransactionV3::WriteSet::getMergeSet() {
    return merge_set_.get();
}

const std::string & TransactionV3::WriteSet::getBuf() {
    return buf_;
}

boost::unordered_flat_map<Path, LockMode> * TransactionV3::WriteSet::ownedLocks() {
    return txn_->owned_locks_.get();
}

bool TransactionV3::WriteSet::add(const Write & write) {
    Path path(write.path_str());
    Path parent_path = path.parent();
    if (!lockFullPath(path)) {
        return false;
    }

    mongo::BSONObj parent_val;
    // first, check if parent exists
    bool valid = (parent_path.size_ == 5 || getInnerObj(parent_path, &parent_val) || 
                        obj_store_->innerObjStore()->contains(parent_path, std::numeric_limits<uint64_t>::max()));
    // check if the object is already in the write batch as any non-null BSON object
    valid = (valid && !contains(path, write.is_leaf()));

    // if parent exists in either write batch or the database AND object itself does not exist in write batch
    if (valid) {
        // TODO for now, we assume there was no leaf object of the same name that was deleted, but still visible in 
        // some version of the catalog.
        valid = (write.is_leaf() && 
                !obj_store_->leafObjStore()->contains(path, std::numeric_limits<uint64_t>::max()));
        if (valid) {
            addToLeafStore(path, write.write_value());
        }
        else if (!write.is_leaf()) {
            mongo::BSONObj cur_obj;
            SnapshotStore::Header snapshot_header;
            valid = !obj_store_->innerObjStore()->getCurSnapshot(path, &cur_obj, &snapshot_header);
            if (valid) {
                addToSnapshot(path, write.write_value());
            }
            else {
                valid = cur_obj.isEmpty();
                if (valid) {
                    // update start vid for modification constraint
                    uint64_t start_vid = htole64(snapshot_header.cur_vid_);
                    std::string new_delta_path;
                    new_delta_path.reserve(sizeof(DeltaStore::Header) + path.size_);
                    new_delta_path.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
                    //Place holder for end_vid. Has to be updated later during validation
                    new_delta_path.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
                    new_delta_path.append(path.data_, path.size_);
                    // add to the current snapshot as delta
                    addToDelta(new_delta_path, cur_obj);
                    // add the new value as the new snapshot
                    // update the delta vid of the new snapshot to cur_vid
                    SnapshotStore::Header new_snapshot_header;
                    new_snapshot_header.delta_vid_ = start_vid;
                    addToSnapshot(path, write.write_value(), new_snapshot_header);
                } 
            }
        
        }

    }

    return valid;

}

bool TransactionV3::WriteSet::update(const Write & write) {
    Path path(write.path_str());
    Path parent_path = path.parent();
    if (!lockFullPath(path)) {
        return false;
    }

    mongo::BSONObj parent_val;
    // first, check if write is of type non-leaf and parent exists
    // there is a special condition for root object, which actually does not exist in the object store
    bool valid = (!write.is_leaf()) && (parent_path.size_ == 5 || getInnerObj(parent_path, &parent_val) || 
                        obj_store_->innerObjStore()->contains(parent_path, std::numeric_limits<uint64_t>::max()));
    
    // if parent exists in either write batch or the database 
    if (valid) {
        mongo::BSONObj cur_obj;
        SnapshotStore::Header snapshot_header;
        if (obj_store_->innerObjStore()->getCurSnapshot(path, &cur_obj, &snapshot_header)) {
            uint64_t start_vid = htole64(snapshot_header.cur_vid_);
            std::string new_delta_path;
            new_delta_path.reserve(sizeof(DeltaStore::Header) + path.size_);
            new_delta_path.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
            //Place holder for end_vid. Has to be updated later during validation
            new_delta_path.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
            new_delta_path.append(path.data_, path.size_);
            // add to the current snapshot as delta
            addToDelta(new_delta_path, cur_obj);
            // add the new value as the new snapshot
            // update the delta vid of the new snapshot to cur_vid
            SnapshotStore::Header new_snapshot_header;
            new_snapshot_header.delta_vid_ = start_vid;
            addToSnapshot(path, write.write_value(), new_snapshot_header);
        }
        else {
            addToSnapshot(path, write.write_value());
        }

    }

    return valid;

}

bool TransactionV3::WriteSet::merge(const Write & write) {
    Path path(write.path_str());
    Path parent_path = path.parent();
    if (!lockFullPath(path)) {
        return false;
    }

    mongo::BSONObj parent_val;
    // first, check if write is of type non-leaf and parent exists
    bool valid = (!write.is_leaf()) && (parent_path.size_ == 5 || getInnerObj(parent_path, &parent_val) || 
                        obj_store_->innerObjStore()->contains(parent_path, std::numeric_limits<uint64_t>::max()));
    
    mongo::BSONObj cur_obj;
    SnapshotStore::Header snapshot_header;
    // object must be in either write batch or in the object store
    valid = valid && (contains(path, write.is_leaf()) 
        || obj_store_->innerObjStore()->contains(path, std::numeric_limits<uint64_t>::max()));
    // if both parent and the object exist in either write batch or the database 
    if (valid) {
        addToMerge(path, write.write_value());
    }

    return valid;

}

bool TransactionV3::WriteSet::remove(const Write & write) {
    Path root_path(write.path_str());
    if (!lockFullPath(root_path)) {
        return false;
    }

    mongo::BSONObj root_obj;
    mongo::BSONObj empty_obj;
    std::string_view empty_obj_str(empty_obj.objdata(), empty_obj.objsize());
    
    InnerObjStore * inner_obj_store = obj_store_->innerObjStore();
    LeafObjStore * leaf_obj_store = obj_store_->leafObjStore();
    
    if (write.is_leaf()) {
        // leaf object exists, so remove
        if (leaf_obj_store->get(root_path, std::numeric_limits<uint64_t>::max(), &root_obj)) {
            std::string vid_str(sizeof(uint64_t), ' ');
            addToLeafStore(root_path, vid_str);
        }
        // TODO handle the case where the write set contains the leaf object
    }
    else {
        SnapshotStore::Header snapshot_header;
    
        if (inner_obj_store->getCurSnapshot(root_path, &root_obj, &snapshot_header) && 
            !root_obj.isEmpty()) {
            uint64_t start_vid = htole64(snapshot_header.cur_vid_);
            std::string new_delta_path;
            new_delta_path.reserve(sizeof(DeltaStore::Header) + root_path.size_);
            new_delta_path.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
            //Place holder for end_vid. Has to be updated later during validation
            new_delta_path.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
            new_delta_path.append(root_path.data_, root_path.size_);
            // add to the current snapshot as delta
            addToDelta(new_delta_path, root_obj);
            // add empty value as the new snapshot
            // update the delta vid of the new snapshot to cur_vid
            SnapshotStore::Header new_snapshot_header;
            new_snapshot_header.delta_vid_ = start_vid;
            addToSnapshot(root_path, empty_obj_str, new_snapshot_header);
            
            std::stack<Path> path_queue;
            path_queue.push(root_path);
            while (!path_queue.empty()) {
                Path cur_path = path_queue.top();
                path_queue.pop();
                // traverse all non-leaf children
                InnerObjStore::Iterator * inner_obj_iter = inner_obj_store->newIterator(cur_path, 
                    std::numeric_limits<uint64_t>::max(), InnerObjStore::ReadOptions());
                
                while(inner_obj_iter->valid()) {
                    Path child_path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true);
                    mongo::BSONObj child_obj = inner_obj_iter->value();
                    snapshot_header = inner_obj_iter->snapshotHeader();
                    start_vid = htole64(snapshot_header.cur_vid_);
                    
                    // if most recent snapshot is empty (removed), no op
                    if (!inner_obj_iter->removed()) {
                        // The value of the iterator is pointing to the most recent snapshot, which 
                        // we can append as the last delta version.
                        new_delta_path.clear();
                        new_delta_path.reserve(sizeof(DeltaStore::Header) + child_path.size_);
                        new_delta_path.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
                        //Place holder for end_vid. Has to be updated later during validation
                        new_delta_path.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
                        new_delta_path.append(child_path.data_, child_path.size_);
                        // add to the current snapshot as delta
                        addToDelta(new_delta_path, child_obj);
                        // add empty value as the new snapshot
                        // update the delta vid of the new snapshot to cur_vid
                        new_snapshot_header.delta_vid_ = start_vid;
                        addToSnapshot(child_path, empty_obj_str, new_snapshot_header);
                    }
                    
                    path_queue.push(Path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true));
                    inner_obj_iter->next();
                }
                delete inner_obj_iter;
                // traverse all leaf children
                LeafObjStore::Iterator * leaf_obj_iter = leaf_obj_store->newIterator(cur_path, 
                    std::numeric_limits<uint64_t>::max(), LeafObjStore::ReadOptions());
                while(leaf_obj_iter->valid()) {
                    Path child_path(leaf_obj_iter->key().data(), leaf_obj_iter->key().size(), true);
                    std::string vid_str(sizeof(uint64_t), ' ');
                    addToLeafStore(child_path, vid_str);
                    // no need to add constraint checks as any conflicts are captured by the scan set
                    leaf_obj_iter->next();
                }
                delete leaf_obj_iter;
                
            }

        }
    
    }

    return true;

}


bool TransactionV3::WriteSet::getInnerObj(const Path & path, mongo::BSONObj * bson_obj_val) {
    // check writebatch    
    std::string obj_val;
    rocksdb::Status status = write_batch_->GetFromBatch(obj_store_->snapshot_store_, rocksdb::DBOptions(), 
        rocksdb::Slice(path.data_, path.size_), &obj_val);
    if (status.ok() && obj_val.size() > sizeof(SnapshotStore::Header) + mongo::BSONObj::kMinBSONLength){
        *bson_obj_val = mongo::BSONObj(&obj_val.data()[sizeof(SnapshotStore::Header)]).getOwned();
        return true;
    }
    
    return false;

}

bool TransactionV3::WriteSet::contains(const Path & path, bool is_leaf) {
    // check writebatch first 
    std::string obj_val;
    if (is_leaf) {
        rocksdb::Status status = write_batch_->GetFromBatch(obj_store_->leaf_store_, rocksdb::DBOptions(),
            rocksdb::Slice(path.data_, path.size_), &obj_val);
        // if the status is not merge-in-progress, the action is add object, rather than removal, 
        // which only merges tombstone_vid. We do not handle case where object was added and then
        // removed in the same transaction...
        if (status.ok()) {
            return true;
        }
    }
    else {
        // check writebatch and make sure that it is not removal (check slice size)
        // if not found, check merge_set_ and return true if it is there
        rocksdb::Status status = write_batch_->GetFromBatch(obj_store_->snapshot_store_, rocksdb::DBOptions(),
            rocksdb::Slice(path.data_, path.size_), &obj_val);
        if (status.ok() && obj_val.size() > sizeof(SnapshotStore::Header) + mongo::BSONObj::kMinBSONLength){
            return true;
        }

        if (merge_set_->contains(path)) {
            return true;
        }
    }
    
    return false;

}

void TransactionV3::WriteSet::addToLeafStore(const Path & path, const Path & primary_path) {
    std::string final_obj_val;
    LeafObjStore::Header header;
    header.is_primary_ = false;
    final_obj_val.reserve(sizeof(LeafObjStore::Header) + primary_path.size_);
    final_obj_val.append(reinterpret_cast<char*>(&header), sizeof(LeafObjStore::Header));
    final_obj_val.append(primary_path.data_, primary_path.size_);
    write_batch_->Put(obj_store_->leaf_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_obj_val.data(), final_obj_val.size()));   
}

void TransactionV3::WriteSet::addToLeafStore(const Path & path, std::string_view obj_val) {
    if (obj_val.size() == sizeof(uint64_t)) {
        write_batch_->Merge(obj_store_->leaf_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(obj_val.data(), obj_val.size()));
    }
    else {
        std::string final_obj_val;
        LeafObjStore::Header header;
        final_obj_val.reserve(sizeof(LeafObjStore::Header) + obj_val.size());
        final_obj_val.append(reinterpret_cast<char*>(&header), sizeof(LeafObjStore::Header));
        final_obj_val.append(obj_val);
        write_batch_->Put(obj_store_->leaf_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_obj_val.data(), final_obj_val.size()));
    }
}

void TransactionV3::WriteSet::addToSnapshot(const Path & path, std::string_view obj_val) {
    std::string final_obj_val;
    SnapshotStore::Header header;
    final_obj_val.reserve(sizeof(SnapshotStore::Header) + obj_val.size());
    final_obj_val.append(reinterpret_cast<char*>(&header), sizeof(SnapshotStore::Header));
    final_obj_val.append(obj_val);
    write_batch_->Put(obj_store_->snapshot_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_obj_val.data(), final_obj_val.size())); 
}

void TransactionV3::WriteSet::addToSnapshot(const Path & path, std::string_view obj_val, SnapshotStore::Header header) {
    std::string final_obj_val;
    final_obj_val.reserve(sizeof(SnapshotStore::Header) + obj_val.size());
    final_obj_val.append(reinterpret_cast<char*>(&header), sizeof(SnapshotStore::Header));
    final_obj_val.append(obj_val);
    write_batch_->Put(obj_store_->snapshot_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_obj_val.data(), final_obj_val.size()));
    
}

void TransactionV3::WriteSet::addToMerge(const Path & path, std::string_view merge_val) {
    std::string base_val;
    rocksdb::Status status = write_batch_->GetFromBatch(obj_store_->snapshot_store_, rocksdb::DBOptions(), rocksdb::Slice(path.data_, path.size_), &base_val);
    if (status.ok()) {
        mongo::BSONObj bson_base_val(&(base_val.data()[sizeof(SnapshotStore::Header)]));
        mongo::BSONObj bson_merge_val(merge_val.data());
        mongo::BSONObj bson_final_val = MmbUtil::mergeDeltaToBase(bson_base_val, bson_merge_val);
        std::string final_snapshot_val;
        final_snapshot_val.reserve(sizeof(SnapshotStore::Header) + bson_final_val.objsize()); 
        final_snapshot_val.append(base_val.data(), sizeof(SnapshotStore::Header));
        final_snapshot_val.append(bson_final_val.objdata(), bson_final_val.objsize());
        write_batch_->Put(obj_store_->snapshot_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_snapshot_val.data(), final_snapshot_val.size()));
    }
    else if (merge_set_->contains(path)) {
        mongo::BSONObj bson_delta1(&buf_.data()[merge_set_->at(path).first]);
        mongo::BSONObj bson_delta2(merge_val.data());
        mongo::BSONObj bson_final_delta = MmbUtil::mergeDeltas(bson_delta1, bson_delta2);
        merge_set_->insert_or_assign(path, std::pair<size_t, size_t>(buf_.size(), bson_final_delta.objsize()));
        buf_.append(bson_final_delta.objdata(), bson_final_delta.objsize());
    }
    else {
        merge_set_->emplace(path, std::pair<size_t, size_t>(buf_.size(), merge_val.size()));
        buf_.append(merge_val.data(), merge_val.size());
    }    
}

void TransactionV3::WriteSet::addToDelta(std::string_view path_str, const mongo::BSONObj & obj_val) {
    Path path(&path_str.data()[sizeof(DeltaStore::Header)], path_str.size() - sizeof(DeltaStore::Header), false);
    write_batch_->Put(obj_store_->delta_store_, rocksdb::Slice(path_str.data(), path_str.size()), rocksdb::Slice(obj_val.objdata(), obj_val.objsize()));   
}

// locks the full path, including all the ancestors up to the parent
// parent is locked in X mode while the rest are locked in IX mode. 
// The operation modifies the depth of the path and restores it back, 
// so the path is not const. 
bool TransactionV3::WriteSet::lockFullPath(Path & path) {
    uint32_t path_depth;
    memcpy(&path_depth, path.data_, sizeof(uint32_t));
    path_depth = le32toh(path_depth);
    bool granted = true;
    uint32_t cur_depth = 0;
    uint32_t cur_size = 4;
    // lock all the way up to the parent path
    while (path_depth > cur_depth && !txn_->abort_.load()) {
        if (path.data_[cur_size] == '/') {
            // difference between path depth and lock level is 1, instead of 2 as 
            // root path is also locked and has one slash. 
            LockMode lock_mode = ((path_depth - cur_depth) > 1) ? LOCK_MODE_IX : LOCK_MODE_X;
            // exception for the root path, which only consists of a slash
            if (cur_depth == 0) {
                Path cur_path("/");
                if (!lockSinglePath(cur_path, lock_mode)) {
                    granted = false;
                    break;
                }
            }
            else {
                cur_depth = htole32(cur_depth);
                memcpy(path.data_, &cur_depth, sizeof(uint32_t));
                Path cur_path(path.data_, cur_size, false);
                if (!lockSinglePath(cur_path, lock_mode)) {
                    granted = false;
                    break;
                }
                cur_depth = le32toh(cur_depth);
            }
            cur_depth++;
        }
        cur_size++;
    }
    // restores the path depth 
    path_depth = htole32(path_depth);
    memcpy(path.data_, &cur_depth, sizeof(uint32_t));

    // lock the object path itself in X mode at last
    // if (!txn_->abort_.load()) {
    //     lockSinglePath(path, LOCK_MODE_X);
    // }

    return granted;

}

bool TransactionV3::WriteSet::lockSinglePath(const Path & path, LockMode lock_mode) {
    bool abort;
    lock_mgr_->lock(path, txn_, lock_mode);
    {   std::lock_guard<std::mutex> txn_lock(txn_->mtx_);
        abort = txn_->abort_.load(); }
    if (abort) {
        lock_mgr_->unlock(path, txn_);
    }
    return !abort;
}


