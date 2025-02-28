// Author: Keonwoo Oh (koh3@umd.edu)

#include <stack>
#include <endian.h>
#include <boost/functional/hash.hpp>

#include "concurrency/v2/txnv2.h"
#include "common/mmbutil.h"
#include "grpc/grpccatalog.pb.h"

using ConstraintKey = TransactionV2::ConstraintKey;
using ConstraintType = TransactionV2::ConstraintType;
using LiveTxnKey = TransactionV2::LiveTxnKey;

TransactionV2::TransactionV2(unsigned int num_partitions, ObjStore * obj_store, uint64_t read_vid) :
        old_read_vid_(read_vid), processed_(false) {
    votes_.store(0);
    abort_.store(false);
    read_set_ = std::make_unique<ReadSet>(num_partitions, this);
    write_set_ = std::make_unique<WriteSet>(num_partitions, obj_store, this);
}

TransactionV2::~TransactionV2() {

}

uint64_t TransactionV2::readVid() {
    return new_read_vid_;
}

Transaction::ReadSet * TransactionV2::readSet() {
    return static_cast<Transaction::ReadSet*>(read_set_.get());
}

void TransactionV2::addQueryRequest(ExecuteQueryRequest * query_request) {
    
}

TransactionV2::WriteSet * TransactionV2::writeSet() {
    return write_set_.get();
}

//clear out read and write sets
void TransactionV2::clear() {
    read_set_.reset();
    write_set_.reset();
}


TransactionV2::ReadSet::ReadSet(unsigned int num_partitions, TransactionV2 * txn) : txn_(txn) {
    for (unsigned int i = 0; i < num_partitions; i++) {
        parent_locks_.push_back(std::make_unique<std::vector<Path>>());
        range_locks_.push_back(std::make_unique<std::vector<std::pair<ConstraintKey, ConstraintKey>>>());
        constraint_checks_.push_back(std::make_unique<std::vector<std::pair<ConstraintKey, ConstraintType>>>());
        constraint_check_vids_.push_back(std::make_unique<std::vector<uint64_t>>());
    }
}

TransactionV2::ReadSet::~ReadSet() {

}

void TransactionV2::ReadSet::addParentLock(const Path & path) {
    unsigned int partition_id = std::hash<Path>()(path) % parent_locks_.size();
    parent_locks_[partition_id]->push_back(path);
}


void TransactionV2::ReadSet::addPointLock(const Path & path) {
    unsigned int partition_id = std::hash<Path>()(path) % constraint_checks_.size();
    constraint_checks_[partition_id]->emplace_back(ConstraintKey(std::numeric_limits<uint64_t>::max(), path), ConstraintType::NOT_MODIFIED);
    constraint_check_vids_[partition_id]->push_back(txn_->new_read_vid_);
}

void TransactionV2::ReadSet::addRangeLock(const Path & lower_bound,const Path & upper_bound) {
    size_t hash_val = std::hash<Path>()(lower_bound) % constraint_checks_.size();
    boost::hash_combine(hash_val, upper_bound);
    unsigned int partition_id = hash_val % constraint_checks_.size();
    range_locks_[partition_id]->emplace_back(ConstraintKey(std::numeric_limits<uint64_t>::max(), lower_bound),
        ConstraintKey(0, upper_bound));
}

void TransactionV2::ReadSet::addConstraintCheck(const Path & path, ConstraintType constraint_type, uint64_t vid) {
    unsigned int partition_id = std::hash<Path>()(path) % constraint_checks_.size();
    constraint_checks_[partition_id]->emplace_back(ConstraintKey(std::numeric_limits<uint64_t>::max(), path), constraint_type);
    constraint_check_vids_[partition_id]->push_back(vid);
}

std::vector<Path> * TransactionV2::ReadSet::getParentLocks(unsigned int partition_id) {
    return parent_locks_[partition_id].get();
}

std::vector<std::pair<ConstraintKey, ConstraintKey>> * TransactionV2::ReadSet::getRangeLocks(unsigned int partition_id) {
    return range_locks_[partition_id].get();
}

std::vector<std::pair<ConstraintKey, ConstraintType>> * TransactionV2::ReadSet::getConstraintChecks(unsigned int partition_id) {
    return constraint_checks_[partition_id].get();
}

std::vector<uint64_t> * TransactionV2::ReadSet::getConstraintCheckVids(unsigned int partition_id) {
    return constraint_check_vids_[partition_id].get();
}

TransactionV2::WriteSet::WriteSet(unsigned int num_partitions, ObjStore * obj_store, TransactionV2 * txn) 
        : obj_store_(obj_store), txn_(txn) {
    for (unsigned int i = 0; i < num_partitions; i++) {
        write_batch_.push_back(std::make_unique<rocksdb::WriteBatchWithIndex>());
        merge_set_.push_back(std::make_unique<boost::unordered_flat_map<Path, std::pair<size_t, size_t>>>());
        version_map_updates_.push_back(std::make_unique<boost::unordered_flat_set<Path>>());
        constraint_updates_.push_back(std::make_unique<std::vector<LFSkipList<ConstraintKey,ConstraintType>::Node*>>());
        constraint_updates_keys_.push_back(std::make_unique<std::vector<ConstraintKey>>());
    }
}

TransactionV2::WriteSet::~WriteSet() { }

rocksdb::WriteBatchWithIndex * TransactionV2::WriteSet::getWriteBatch(unsigned int partition_id) {
    return write_batch_[partition_id].get();
}

boost::unordered_flat_map<Path, std::pair<size_t, size_t>>* TransactionV2::WriteSet::getMergeSet(unsigned int partition_id) {
    return merge_set_[partition_id].get();
}

boost::unordered_flat_set<Path>* TransactionV2::WriteSet::getVersionMapUpdates(unsigned int partition_id) {
    return version_map_updates_[partition_id].get();
}

std::vector<LFSkipList<ConstraintKey,ConstraintType>::Node*> * TransactionV2::WriteSet::getConstraintUpdates(unsigned int partition_id) {
    return constraint_updates_[partition_id].get();
}

std::vector<ConstraintKey> * TransactionV2::WriteSet::getConstraintUpdatesKeys(unsigned int partition_id) {
    return constraint_updates_keys_[partition_id].get();
}

bool TransactionV2::WriteSet::add(const Write & write) {
    Path path(write.path_str());
    Path parent_path = path.parent();

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
            txn_->read_set_->addConstraintCheck(path, ConstraintType::NOT_EXIST);
        }
        else if (!write.is_leaf()) {
            mongo::BSONObj cur_obj;
            SnapshotStore::Header snapshot_header;
            valid = !obj_store_->innerObjStore()->getCurSnapshot(path, &cur_obj, &snapshot_header);
            if (valid) {
                addToSnapshot(path, write.write_value());
                txn_->read_set_->addConstraintCheck(path, ConstraintType::NOT_EXIST);
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
                    // As null object has to be continued in version chain
                    txn_->read_set_->addConstraintCheck(path, ConstraintType::NOT_MODIFIED, start_vid);
                } 
            }
        
        }

    }

    if (valid) {
        txn_->read_set_->addConstraintCheck(parent_path, ConstraintType::EXIST);
        updateConstraint(path, ConstraintType::EXIST);
        // unlike TxnManagerV1, the full path is added to the Version Map 
        updateVersionMap(parent_path);
    }

    return valid;

}

bool TransactionV2::WriteSet::update(const Write & write) {
    Path path(write.path_str());
    Path parent_path = path.parent();

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
            // have to continue the version chain
            txn_->read_set_->addConstraintCheck(path, ConstraintType::NOT_MODIFIED, start_vid);
        }
        else {
            addToSnapshot(path, write.write_value());
            // object was never inserted, so there is no pre image
            txn_->read_set_->addConstraintCheck(path, ConstraintType::NOT_EXIST);
        }

    }

    if (valid) {
        txn_->read_set_->addConstraintCheck(parent_path, ConstraintType::EXIST);
        updateConstraint(path, ConstraintType::EXIST);
        // unlike TxnManagerV1, the full path is added to the Version Map 
        updateVersionMap(parent_path);
        
    }

    return valid;

}

bool TransactionV2::WriteSet::merge(const Write & write) {
    Path path(write.path_str());
    Path parent_path = path.parent();

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
        // addToMerge, constraint checks for the object itself
        addToMerge(path, write.write_value());
        txn_->read_set_->addConstraintCheck(parent_path, ConstraintType::EXIST);
        
        updateConstraint(path, ConstraintType::EXIST);
        // unlike TxnManagerV1, the full path is added to the Version Map 
        updateVersionMap(parent_path);
    }

    return valid;

}

bool TransactionV2::WriteSet::remove(const Write & write) {
    Path root_path(write.path_str());
    mongo::BSONObj root_obj;
    mongo::BSONObj empty_obj;
    std::string_view empty_obj_str(empty_obj.objdata(), empty_obj.objsize());
    TransactionV2::ReadSet * read_set = txn_->read_set_.get();
    uint64_t read_vid = txn_->new_read_vid_;

    InnerObjStore * inner_obj_store = obj_store_->innerObjStore();
    LeafObjStore * leaf_obj_store = obj_store_->leafObjStore();
    // If the removed object is leaf, place minimum constraints, so that removal operation 
    // can be performed correctly.
    if (write.is_leaf()) {
        if (leaf_obj_store->get(root_path, std::numeric_limits<uint64_t>::max(), &root_obj)) {
            std::string vid_str(sizeof(uint64_t), ' ');
            addToLeafStore(root_path, vid_str);
            txn_->read_set_->addConstraintCheck(root_path, ConstraintType::EXIST);
            updateConstraint(root_path, ConstraintType::NOT_EXIST);
            updateVersionMap(root_path.parent());
        }
        // TODO handle the case where the write set contains the leaf object
        // No op, so no object should have been added in between
        else {
            txn_->read_set_->addConstraintCheck(root_path, ConstraintType::NOT_EXIST);
        }
    }
    else {
        SnapshotStore::Header snapshot_header;
    
        if (inner_obj_store->getCurSnapshot(root_path, &root_obj, &snapshot_header) && 
            !root_obj.isEmpty()) {
            // if object has been modified beyond the read_vid, abort
            uint64_t start_vid = htole64(snapshot_header.cur_vid_);
            if (start_vid > read_vid) {
                txn_->abort_.store(true);
                return false;
            }

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
            // have to continue the version chain
            read_set->addConstraintCheck(root_path, ConstraintType::NOT_MODIFIED, start_vid);
            updateConstraint(root_path, ConstraintType::NOT_EXIST);
            updateVersionMap(root_path.parent());
            
            std::stack<Path> path_queue;
            path_queue.push(root_path);
            while (!path_queue.empty()) {
                Path cur_path = path_queue.top();
                path_queue.pop();
                // traverse all non-leaf children
                InnerObjStore::Iterator * inner_obj_iter = inner_obj_store->newIterator(cur_path, read_vid, InnerObjStore::ReadOptions());
                
                read_set->addParentLock(cur_path);
                while(inner_obj_iter->valid()) {
                    Path child_path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true);
                    mongo::BSONObj child_obj = inner_obj_iter->value();
                    snapshot_header = inner_obj_iter->snapshotHeader();
                    start_vid = htole64(snapshot_header.cur_vid_);
                    // Either the object has been modified recently or has been removed
                    if (start_vid > read_vid) {
                        txn_->abort_.store(true);
                        delete inner_obj_iter;
                        return false;
                    }
                    // if most recent snapshot is empty (removed), no op
                    if (!inner_obj_iter->removed()) {
                        // since we checked that the snapshot header cur_vid is less than the read_vid, 
                        // the value of the iterator is pointing to the most recent snapshot, which 
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
                        updateConstraint(child_path, ConstraintType::NOT_EXIST);
                        updateVersionMap(cur_path);
                    }
                    
                    // no need to add constraint checks as any conflicts are captured by the scan set
                    path_queue.push(Path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true));
                    inner_obj_iter->next();
                }
                delete inner_obj_iter;
                // traverse all leaf children
                LeafObjStore::Iterator * leaf_obj_iter = leaf_obj_store->newIterator(cur_path, read_vid, LeafObjStore::ReadOptions());
                while(leaf_obj_iter->valid()) {
                    Path child_path(leaf_obj_iter->key().data(), leaf_obj_iter->key().size(), true);
                    std::string vid_str(sizeof(uint64_t), ' ');
                    addToLeafStore(child_path, vid_str);
                    updateConstraint(child_path, ConstraintType::NOT_EXIST);
                    updateVersionMap(cur_path);
                    // no need to add constraint checks as any conflicts are captured by the scan set
                    leaf_obj_iter->next();
                }
                delete leaf_obj_iter;
                
            }

        }
    
    }

    return true;

}


bool TransactionV2::WriteSet::getInnerObj(const Path & path, mongo::BSONObj * bson_obj_val) {
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();
    // check writebatch    
    std::string obj_val;
    rocksdb::Status status = write_batch_[partition_id]->GetFromBatch(obj_store_->snapshot_store_, rocksdb::DBOptions(), rocksdb::Slice(path.data_, path.size_), &obj_val);
    if (status.ok() && obj_val.size() > sizeof(SnapshotStore::Header) + mongo::BSONObj::kMinBSONLength){
        *bson_obj_val = mongo::BSONObj(&obj_val.data()[sizeof(SnapshotStore::Header)]).getOwned();
        return true;
    }
    
    return false;

}

bool TransactionV2::WriteSet::contains(const Path & path, bool is_leaf) {
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();
    // check writebatch first 
    std::string obj_val;
    if (is_leaf) {
        rocksdb::Status status = write_batch_[partition_id]->GetFromBatch(obj_store_->leaf_store_, rocksdb::DBOptions(), rocksdb::Slice(path.data_, path.size_), &obj_val);
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
        rocksdb::Status status = write_batch_[partition_id]->GetFromBatch(obj_store_->snapshot_store_, rocksdb::DBOptions(), rocksdb::Slice(path.data_, path.size_), &obj_val);
        if (status.ok() && obj_val.size() > sizeof(SnapshotStore::Header) + mongo::BSONObj::kMinBSONLength){
            return true;
        }

        if (merge_set_[partition_id]->contains(path)) {
            return true;
        }
    }
    
    return false;

}

const std::string & TransactionV2::WriteSet::getBuf() {
    return buf_;
}

void TransactionV2::WriteSet::addToLeafStore(const Path & path, const Path & primary_path) {
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();
    std::string final_obj_val;
    LeafObjStore::Header header;
    header.is_primary_ = false;
    final_obj_val.reserve(sizeof(LeafObjStore::Header) + primary_path.size_);
    final_obj_val.append(reinterpret_cast<char*>(&header), sizeof(LeafObjStore::Header));
    final_obj_val.append(primary_path.data_, primary_path.size_);
    write_batch_[partition_id]->Put(obj_store_->leaf_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_obj_val.data(), final_obj_val.size()));
    
}


void TransactionV2::WriteSet::addToLeafStore(const Path & path, std::string_view obj_val) {
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();

    if (obj_val.size() == sizeof(uint64_t)) {
        write_batch_[partition_id]->Merge(obj_store_->leaf_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(obj_val.data(), obj_val.size()));
    }
    else {
        std::string final_obj_val;
        LeafObjStore::Header header;
        final_obj_val.reserve(sizeof(LeafObjStore::Header) + obj_val.size());
        final_obj_val.append(reinterpret_cast<char*>(&header), sizeof(LeafObjStore::Header));
        final_obj_val.append(obj_val);
        write_batch_[partition_id]->Put(obj_store_->leaf_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_obj_val.data(), final_obj_val.size()));
    }
    
}

void TransactionV2::WriteSet::addToSnapshot(const Path & path, std::string_view obj_val) {
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();
    std::string final_obj_val;
    SnapshotStore::Header header;
    final_obj_val.reserve(sizeof(SnapshotStore::Header) + obj_val.size());
    final_obj_val.append(reinterpret_cast<char*>(&header), sizeof(SnapshotStore::Header));
    final_obj_val.append(obj_val);
    write_batch_[partition_id]->Put(obj_store_->snapshot_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_obj_val.data(), final_obj_val.size()));
    
}

void TransactionV2::WriteSet::addToSnapshot(const Path & path, std::string_view obj_val, SnapshotStore::Header header) {
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();
    std::string final_obj_val;
    final_obj_val.reserve(sizeof(SnapshotStore::Header) + obj_val.size());
    final_obj_val.append(reinterpret_cast<char*>(&header), sizeof(SnapshotStore::Header));
    final_obj_val.append(obj_val);
    write_batch_[partition_id]->Put(obj_store_->snapshot_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_obj_val.data(), final_obj_val.size()));
    
}

void TransactionV2::WriteSet::addToMerge(const Path & path, std::string_view merge_val) {
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();

    std::string base_val;
    rocksdb::Status status = write_batch_[partition_id]->GetFromBatch(obj_store_->snapshot_store_, rocksdb::DBOptions(), rocksdb::Slice(path.data_, path.size_), &base_val);
    if (status.ok()) {
        mongo::BSONObj bson_base_val(&(base_val.data()[sizeof(SnapshotStore::Header)]));
        mongo::BSONObj bson_merge_val(merge_val.data());
        mongo::BSONObj bson_final_val = MmbUtil::mergeDeltaToBase(bson_base_val, bson_merge_val);
        std::string final_snapshot_val;
        final_snapshot_val.reserve(sizeof(SnapshotStore::Header) + bson_final_val.objsize()); 
        final_snapshot_val.append(base_val.data(), sizeof(SnapshotStore::Header));
        final_snapshot_val.append(bson_final_val.objdata(), bson_final_val.objsize());
        write_batch_[partition_id]->Put(obj_store_->snapshot_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(final_snapshot_val.data(), final_snapshot_val.size()));
    }
    else if (merge_set_[partition_id]->contains(path)) {
        mongo::BSONObj bson_delta1(&buf_.data()[merge_set_[partition_id]->at(path).first]);
        mongo::BSONObj bson_delta2(merge_val.data());
        mongo::BSONObj bson_final_delta = MmbUtil::mergeDeltas(bson_delta1, bson_delta2);
        merge_set_[partition_id]->insert_or_assign(path, std::pair<size_t, size_t>(buf_.size(), bson_final_delta.objsize()));
        buf_.append(bson_final_delta.objdata(), bson_final_delta.objsize());
    }
    else {
        merge_set_[partition_id]->emplace(path, std::pair<size_t, size_t>(buf_.size(), merge_val.size()));
        buf_.append(merge_val.data(), merge_val.size());
        txn_->read_set_->addConstraintCheck(path, ConstraintType::EXIST);
    }    
}


void TransactionV2::WriteSet::addToDelta(std::string_view path_str, const mongo::BSONObj & obj_val) {
    Path path(&path_str.data()[sizeof(DeltaStore::Header)], path_str.size() - sizeof(DeltaStore::Header), false); 
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();
    write_batch_[partition_id]->Put(obj_store_->delta_store_, rocksdb::Slice(path_str.data(), path_str.size()), rocksdb::Slice(obj_val.objdata(), obj_val.objsize()));   
}

// vid of the constraint has to be updated to txn->commit_vid_ during the write phase
void TransactionV2::WriteSet::updateConstraint(const Path & path, ConstraintType constraint_type) {
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();
    constraint_updates_[partition_id]->push_back(
        new LFSkipList<TransactionV2::ConstraintKey, TransactionV2::ConstraintType>::Node
        (ConstraintKey(std::numeric_limits<uint64_t>::max(), path), constraint_type));
    constraint_updates_keys_[partition_id]->push_back(ConstraintKey(std::numeric_limits<uint64_t>::max(), path));     
}

void TransactionV2::WriteSet::updateVersionMap(const Path & path) {
    unsigned int partition_id = std::hash<Path>()(path) % write_batch_.size();
    version_map_updates_[partition_id]->emplace(path);
}
