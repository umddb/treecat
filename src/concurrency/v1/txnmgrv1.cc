// Author: Keonwoo Oh (koh3@umd.edu)

#include <filesystem>
#include <limits>

#include <endian.h>
#include <immintrin.h>

#include "concurrency/v1/txnmgrv1.h"

#include "boost/asio/post.hpp"
#include "common/mmbutil.h"
#include "common/config.h"
#include "exec/evalexpr.h"

using Constraint = TransactionV1::Constraint;
using ConstraintType = TransactionV1::ConstraintType; 
using LogIdxKey = TransactionV1::LogIdxKey;
using LogIdxValue = TransactionV1::LogIdxValue;
using LiveTxnKey = TransactionV1::LiveTxnKey;


TransactionManagerV1::Validator::Validator(unsigned int num_partitions, TransactionManagerV1 * txn_mgr, Writer * writer, ObjStore * obj_store) :
    num_partitions_(num_partitions), txn_mgr_(txn_mgr), writer_(writer), obj_store_(obj_store), stop_(true) {
    
    LogIdxKey head_key(0, Path("/"));
    LogIdxKey tail_key(std::numeric_limits<uint64_t>::max(), 
        Path("//////////////////////////////////////////////////////"));
    LogIdxValue dummy_idx_value(nullptr, 0,0); 
    
    // intialize all the partitioned data structures
    thread_pool_ = std::make_unique<boost::asio::thread_pool>(num_partitions_);
    for (unsigned int i = 0; i < num_partitions_; i++) {
        thread_states_.push_back(std::make_unique<ThreadState>());
        version_map_.push_back(std::make_unique<boost::concurrent_flat_map<Path, uint64_t>>());
        auto head = new LFSkipList<LogIdxKey,LogIdxValue>::Node(head_key, dummy_idx_value);
        auto tail = new LFSkipList<LogIdxKey,LogIdxValue>::Node(tail_key, dummy_idx_value); 
        log_idx_.push_back(std::make_unique<LFSkipList<LogIdxKey,LogIdxValue>>(head,tail));
        constraint_map_.push_back(std::make_unique<boost::concurrent_flat_map<Path,Constraint>>());
        merge_cache_.push_back(std::make_unique<boost::concurrent_flat_map<LogIdxKey,std::pair<mongo::BSONObj, mongo::BSONObj>>>());
    } 

    global_curr_txn_.store(nullptr);
    validation_queue_size_.store(0);
    
}

TransactionManagerV1::Validator::~Validator() {
    stop();
    
}


void TransactionManagerV1::Validator::validate(TransactionV1 * txn) {
    validation_queue_.enqueue(txn);
    uint64_t queue_size = validation_queue_size_.fetch_add(1);
    if (queue_size == 0) {
        {   std::lock_guard<std::mutex> lock(mtx_); }
        cond_.notify_all();
    }
}

// executors for validation
void TransactionManagerV1::Validator::handleTransactions(unsigned int rank) {
    ThreadState* thread_state = thread_states_[rank].get();

    log_idx_[rank]->registerThread();
    
    while (!stop_) {
        int counter = 0;
        // spin loop a little bit
        while (validation_queue_size_.load() == 0 && 
            thread_state->local_curr_txn_ == global_curr_txn_.load() && counter < 500) {
            _mm_pause();
            counter++;
        }

        // wait for either the queue to fill, some other thread to replace global curr txn, or stop 
        if (validation_queue_size_.load() == 0 && 
            thread_state->local_curr_txn_ == global_curr_txn_.load()) {
            std::unique_lock<std::mutex> lock(mtx_);
            cond_.wait(lock, [this, thread_state](){ 
                return (validation_queue_size_.load() > 0 
                || thread_state->local_curr_txn_ != global_curr_txn_.load() 
                || stop_); });
        }
        
        // if woken up to stop, don't proceed to next request 
        if (stop_) {
            continue;
        }

        // read the head of the queue and try CAS operation, if successful, 
        // remove the head from the queue, otherwise simply move on to handle the new txn
        TransactionV1 * next_txn;
        if (thread_state->local_curr_txn_ == global_curr_txn_.load()) {
            bool valid = validation_queue_.front(next_txn);
            if (valid) {
                bool success =  global_curr_txn_.compare_exchange_strong(thread_state->local_curr_txn_, next_txn);
                if (success) {
                    // commit_vid_ is not accessed until after a synchronization point 
                    // in the middle of validation, so this is fine
                    next_txn->commit_vid_ = txn_mgr_->commit_vid_.fetch_add(1);
                    validation_queue_size_.fetch_sub(1);
                    validation_queue_.dequeue(next_txn);
                }  
            }
        }
        
        thread_state->local_curr_txn_ = global_curr_txn_.load();
        handleTransaction(rank, thread_state->local_curr_txn_);
    }
}

void TransactionManagerV1::Validator::run() {
    if (stop_) {
        stop_ = false;
        //start running validator threads
        for (unsigned int i = 0; i < num_partitions_; i++) {
            boost::asio::post(*thread_pool_, [this, i]() { handleTransactions(i); });
        }
    }
}

void TransactionManagerV1::Validator::stop() {
    if (!stop_) {
        stop_ = true;
        {   std::lock_guard<std::mutex> lock(mtx_); }
            cond_.notify_all();
        thread_pool_->join();
    }
}

void TransactionManagerV1::Validator::registerThread() {
    for (auto & log_idx_part : log_idx_) {
        log_idx_part->registerThread();
    }
}

void TransactionManagerV1::Validator::collectInMemoryGarbage(TransactionV1 * txn) {
    if (!txn->abort_.load()) {
        for (unsigned int i = 0; i < num_partitions_; i++) {        
            boost::unordered_flat_set<Path>* version_map_updates = txn->write_set_->getVersionMapUpdates(i);
            boost::unordered_flat_map<Path, LFSkipList<LogIdxKey,LogIdxValue>::Node*>* log_idx_updates = txn->write_set_->getLogIdxUpdates(i);
            std::vector<std::pair<Path, Constraint>> * constraint_updates = txn->write_set_->getConstraintUpdates(i);
            boost::unordered_flat_map<Path, std::pair<size_t, size_t>>* merge_set = txn->write_set_->getMergeSet(i);

            for (auto & path: *version_map_updates) {
                version_map_[i]->erase_if(path, [&](const auto& x){ return (x.second == txn->commit_vid_); });
            }

            for (auto & log_idx_update : *log_idx_updates) {
                // Copy the idx key to avoid seg fault. This works since there is only one entry 
                // per path for each txn (so no duplicate entries unlike constraint_map in txn mgr v2). 
                LogIdxKey log_idx_key = log_idx_update.second->key_;
                log_idx_[i]->remove(log_idx_key);
            }

            for (auto & constraint_update : *constraint_updates) {
                constraint_map_[i]->erase_if(constraint_update.first, 
                    [&](const auto& x){ return (x.second.vid_ == txn->commit_vid_); });
            }

            for (auto & merge_op : *merge_set) {
                merge_cache_[i]->erase(LogIdxKey(txn->commit_vid_, merge_op.first));
            }
        }
    }
    
    // TODO clear the txn read set and write set
    txn->clear();
}

void TransactionManagerV1::Validator::handleTransaction(unsigned int rank, TransactionV1 * txn) {
    std::vector<std::pair<Path, const Predicate&>> * scan_set = txn->read_set_->getScanSet(rank);
    std::vector<std::pair<Path, Constraint>> * constraint_checks = txn->read_set_->getConstraintChecks(rank);
    rocksdb::WriteBatchWithIndex * write_batch = txn->write_set_->getWriteBatch(rank);
    boost::unordered_flat_set<Path>* version_map_updates = txn->write_set_->getVersionMapUpdates(rank);
    boost::unordered_flat_map<Path,LFSkipList<LogIdxKey,LogIdxValue>::Node*>* log_idx_updates = txn->write_set_->getLogIdxUpdates(rank);
    std::vector<std::pair<Path, Constraint>> * constraint_updates = txn->write_set_->getConstraintUpdates(rank);
    // pointers, instead of objects to save memory allocation time. The nodes in 
    // the skiplist will not be garbage collected until there is guarantee that they 
    // are not needed by the validator. 
    boost::unordered_flat_map<Path, LogIdxKey*> traversed_paths;

    uint64_t vid;
    for (size_t i = 0; i < scan_set->size() && !txn->abort_.load(); i++) {
        bool found = version_map_[rank]->visit((*scan_set)[i].first, [&](const auto& x){ vid = x.second; }  );
        if (found && vid > txn->new_read_vid_) {
            const Predicate & predicate = (*scan_set)[i].second;
            LFSkipList<LogIdxKey, LogIdxValue>::Iterator * log_idx_iter = log_idx_[rank]->newIterator();
            // Note that paths in scan set include slash at the end, so no need to further process the 
            // paths to increase path depth etc. 
            LogIdxKey seek_key(txn->new_read_vid_ + 1, (*scan_set)[i].first);
            log_idx_iter->seek(seek_key);
            while (log_idx_iter->isValid() && log_idx_iter->key().path_.hasPrefix(seek_key.path_) 
                && !txn->abort_.load()) {
                const LogIdxKey & log_idx_key = log_idx_iter->key(); 
                LogIdxValue log_idx_value = log_idx_iter->value();
                
                // eval info for obj_id predicate evaluation
                EvalExpr::EvalInfo eval_info;
                eval_info.path_ = rocksdb::Slice(log_idx_key.path_.data_, log_idx_key.path_.size_);
                mongo::BSONObj before_image;
                mongo::BSONObj after_image;
                /*
                 * The write operation is merge type, so before and after image has to be constructed.
                 * The key idea is to first look for any previously constructed images in merge_cache, 
                 * which is persisted across txns until it is safe to garbage collect. Then, look for appropriate before
                 * image to apply the merge operation, first in the merge_cache and log_idx, then obj_store. This is 
                 * frankly hard because merge operation as it is, simply specifies the operation to apply. So, we need an 
                 * additional data structure to keep track of the latest log_idx entry of the same path that have been 
                 * traversed so far, which, if it exists, has the corresponding before-image of the merge operation 
                 * in either the merge_cache or log_idx. This optimization is possible precisely because we 
                 * traverse the log_idx in the ascending order of vid. 
                 *
                 * 1) Check if (path, vid) is in merge cache, evaluate both before and after image if found in the cache
                 * 2) If 1 fails, check traversed_paths for path:
                 *      2.1) If entry exists, search for the right path,vid pair in merge_cache, 
                 *           apply merge operation, then put an entry to merge_cache
                 *      2.2) If entry exists, but not in merge_cache, search for the entry in log_idx,
                 *           check that the entry is not of merge type, apply the merge operation, then 
                 *           cache an entry in merge_cache  
                 * 3) Lastly, if all of the above fails, get the before-image from obj-store, apply merge operation, 
                 * cache the entry in merge_cache
                 */
                if (log_idx_value.after_ == 0) {
                    if (!merge_cache_[rank]->visit(log_idx_key, [&](const auto & x) {
                            before_image = x.second.first;
                            after_image = x.second.second;})) {
                        // if the path has been traversed, its before image is in either 
                        // merge cache or log_idx. Retrieve the before-image, construct 
                        // after-image and cache it. 
                        if (traversed_paths.contains(log_idx_key.path_)) {
                            LogIdxKey * prev_idx_key = traversed_paths.at(log_idx_key.path_);
                            // if not in merge_cache the image must be in log_idx
                            if (!merge_cache_[rank]->visit(*prev_idx_key, [&](const auto & x){ 
                                before_image = x.second.second; })) {
                                LogIdxValue prev_idx_value;
                                log_idx_[rank]->get(*prev_idx_key, prev_idx_value);
                                before_image = mongo::BSONObj(&prev_idx_value.buf_->data()[prev_idx_value.after_]);
                            }
                        }
                        // otherwise, retrieve the image from the object store and construct the after-image
                        else {
                            obj_store_->innerObjStore()->get(log_idx_key.path_, txn->new_read_vid_, &before_image);
                        }

                        after_image = MmbUtil::mergeDeltaToBase(before_image, 
                            mongo::BSONObj(&log_idx_value.buf_->data()[log_idx_value.before_]));
                        // cache the entry, so it can be used later
                        merge_cache_[rank]->insert_or_assign(log_idx_key, 
                            std::pair<mongo::BSONObj, mongo::BSONObj>(before_image, after_image));

                    }
                }
                else {
                    before_image = mongo::BSONObj(&log_idx_value.buf_->data()[log_idx_value.before_]);
                    after_image = mongo::BSONObj(&log_idx_value.buf_->data()[log_idx_value.after_]);
                }

                if (!precisionLock(before_image, after_image, predicate, &eval_info)) {
                    txn->abort_.store(true);
                    break;
                }

                // Merge operation on an object can be interspersed with other operations
                traversed_paths.insert_or_assign(log_idx_key.path_, const_cast<LogIdxKey*>(&log_idx_key));
                log_idx_iter->next();
            }

            delete log_idx_iter;

        }

    }

    // Check the basic write constraints
    for (size_t i = 0; i < constraint_checks->size() && !txn->abort_.load(); i++) {
        uint64_t vid;
        ConstraintType constraint_type;
        if (constraint_map_[rank]->visit(constraint_checks->at(i).first, [&](const auto & x){
            vid = x.second.vid_;
            constraint_type = x.second.type_;})) {
            switch (constraint_checks->at(i).second.type_) {
                case ConstraintType::EXIST:
                    if (constraint_type == ConstraintType::NOT_EXIST) {
                        txn->abort_.store(true);
                    }
                    break;
                case ConstraintType::NOT_EXIST:
                    if (constraint_type == ConstraintType::EXIST) {
                        txn->abort_.store(true);
                    }
                    break;
                case ConstraintType::NOT_MODIFIED:
                    if (vid > constraint_checks->at(i).second.vid_) {
                        txn->abort_.store(true);
                    }
                    break;
                default:
                    break;
            }
        }

        
    }

    uint64_t votes = txn->votes_.fetch_add(1);
    int count = 0;
    // spin wait for a bit, instead of blocking immediately
    while (txn->votes_.load() < num_partitions_ && count < 500) {
        _mm_pause();
        count++;
    }
    // synchronization point before updating data structures
    // if there are other threads still working, block until the last thread is done
    if (txn->votes_.load() < num_partitions_) {
        std::unique_lock<std::mutex> lock(thread_states_[rank]->mtx_);
        thread_states_[rank]->cond_.wait(lock, [this, txn](){
            return txn->votes_.load() >= num_partitions_; });
        lock.unlock();
    }
    // if the thread is the last to reach this point, wake other threads up
    else if (votes >= (num_partitions_ - 1)) {
        for (unsigned int i = 0; i < num_partitions_; i++) {
            if (i != rank) {
                std::unique_lock<std::mutex> lock(thread_states_[i]->mtx_);
                thread_states_[i]->ready_ = true;
                lock.unlock();
                thread_states_[i]->cond_.notify_one();
            }   
            
        }
    }
    
    if (!txn->abort_.load()) {
        // update the vid of each snapshot in inner_obj_store
        uint64_t le_commit_vid = htole64(txn->commit_vid_);
        rocksdb::WBWIIterator* iter = write_batch->NewIterator(obj_store_->snapshot_store_);
        iter->SeekToFirst();
        while (iter->Valid()) {
            Path path(iter->Entry().key.data(), iter->Entry().key.size(), false);
            // reset current vid
            memcpy(&const_cast<char*>(iter->Entry().value.data())[sizeof(uint64_t)], &le_commit_vid, sizeof(uint64_t));
            iter->Next();
        }

        delete iter;

        // update the vid of each new leaf object
        iter = write_batch->NewIterator(obj_store_->leaf_store_);
        iter->SeekToFirst();
        while (iter->Valid()) {
            auto entry = iter->Entry();
            Path path(entry.key.data(), entry.key.size(), false);
            // regardless of merge or put, set the first 64bit int
            // In case of merge, the int is merged as tombstone_vid_
            memcpy(const_cast<char*>(entry.value.data()), &le_commit_vid, sizeof(uint64_t));
            iter->Next();
        }

        delete iter;

        // only reset end_vid_ in writes to delta_store
        iter = write_batch->NewIterator(obj_store_->delta_store_);
        iter->SeekToFirst();
        while (iter->Valid()) {
            memcpy(&const_cast<char*>(iter->Entry().key.data())[sizeof(uint64_t)], &le_commit_vid, sizeof(uint64_t));
            iter->Next();
        } 
        delete iter;

        // update version map, log idx, constraint map
        for (auto & path: *version_map_updates) {
            version_map_[rank]->insert_or_assign(path, txn->commit_vid_);
        }

        for (auto & log_idx_update : *log_idx_updates) {
            log_idx_update.second->key_.vid_ = txn->commit_vid_;
            log_idx_[rank]->insert(log_idx_update.second);
        }

        for (auto & constraint_update : *constraint_updates) {
            constraint_update.second.vid_ = txn->commit_vid_;
            constraint_map_[rank]->insert_or_assign(constraint_update.first, constraint_update.second);
        }

    }

    votes = txn->votes_.fetch_add(1);
    if (votes == (2*num_partitions_ - 1)) {
        writer_->write(txn);
    }

}

bool TransactionManagerV1::Validator::precisionLock(mongo::BSONObj & before_image, 
    mongo::BSONObj & after_image, const Predicate & predicate, EvalExpr::EvalInfo * eval_info) {
    if (!before_image.isEmpty() && EvalExpr::evalPred<mongo::BSONObj>(&before_image, predicate, eval_info)) {
        return false;
    }
    else if (!after_image.isEmpty() && EvalExpr::evalPred(&after_image, predicate, eval_info)) {
        return false;
    }
    return true;
}

TransactionManagerV1::Writer::Writer(unsigned int num_partitions, TransactionManagerV1 * txn_mgr, ObjStore * obj_store) :
    num_partitions_(num_partitions), txn_mgr_(txn_mgr), obj_store_(obj_store), stop_(true) {
    thread_pool_ = std::make_unique<boost::asio::thread_pool>(num_partitions_ + 2);
    for (unsigned int i = 0; i < num_partitions_; i++) {
        thread_states_.push_back(std::make_unique<ThreadState>());
    }
    fsync_thread_ = std::make_unique<ThreadState>();
    in_memory_gc_thread_ = std::make_unique<ThreadState>();
    global_curr_txn_.store(nullptr);
    write_queue_size_.store(0);
    fsync_queue_size_.store(0);
    in_memory_gc_queue_size_.store(0);
}


TransactionManagerV1::Writer::~Writer() {
    stop();
}


void TransactionManagerV1::Writer::write(TransactionV1 * txn) {
    write_queue_.enqueue(txn);
    uint64_t queue_size = write_queue_size_.fetch_add(1);
    if (queue_size == 0) {
        {   std::lock_guard<std::mutex> lock(mtx_); }
        cond_.notify_all();
    }

}


void TransactionManagerV1::Writer::handleTransactions(unsigned int rank) {
    ThreadState* thread_state = thread_states_[rank].get();

    while (!stop_) {
        int counter = 0;
        // spin loop a little bit
        while (write_queue_size_.load() == 0 && 
            thread_state->local_curr_txn_ == global_curr_txn_.load() && counter < 500) {
            _mm_pause();
            counter++;
        }

        // wait for either the queue to fill, some other thread to replace global curr txn, or stop 
        if (write_queue_size_.load() == 0 && 
            thread_state->local_curr_txn_ == global_curr_txn_.load()) {
            std::unique_lock<std::mutex> lock(mtx_);
            cond_.wait(lock, [this, thread_state](){ 
                return (write_queue_size_.load() > 0 
                || thread_state->local_curr_txn_ != global_curr_txn_.load() 
                || stop_); });
        }
        
        // if woken up to stop, don't proceed to next request 
        if (stop_) {
            continue;
        }

        // read the head of the queue and try CAS operation, if successful, 
        // remove the head from the queue, otherwise simply move on to handle the new txn
        TransactionV1 * next_txn;
        if (thread_state->local_curr_txn_ == global_curr_txn_.load()) {
            bool valid = write_queue_.front(next_txn);
            if (valid) {
                bool success =  global_curr_txn_.compare_exchange_strong(thread_state->local_curr_txn_, next_txn);
                if (success) {
                    write_queue_size_.fetch_sub(1);
                    write_queue_.dequeue(next_txn);
                }
            }
        }
        
        thread_state->local_curr_txn_ = global_curr_txn_.load();
        thread_states_[rank]->ready_ = false;
        handleTransaction(rank, thread_state->local_curr_txn_);

    }

}

void TransactionManagerV1::Writer::fsync(TransactionV1 * txn) {
    fsync_queue_.enqueue(txn);
    uint64_t queue_size = fsync_queue_size_.fetch_add(1);
    if (queue_size == 0) {
        {   std::lock_guard<std::mutex> lock(fsync_thread_->mtx_); }
        fsync_thread_->cond_.notify_one();
    }
}

void TransactionManagerV1::Writer::fsyncTransactions() {
    TransactionV1 * input_txn;
    bool committed;
    std::vector<TransactionV1*> txns;

    while (!stop_) {
        int counter = 0;
        // spin loop a little bit
        while (fsync_queue_size_.load() == 0 && counter < 500) {
            _mm_pause();
            counter++;
        }

        // wait for the queue to fill
        if (fsync_queue_size_.load() == 0) {
            std::unique_lock<std::mutex> thread_lock(fsync_thread_->mtx_);
            fsync_thread_->cond_.wait(thread_lock, [this](){ return (fsync_queue_size_.load() > 0 || stop_); });
        }
        
        //batch commit size of 50000 for now
        while (fsync_queue_size_.load() > 0 && txns.size() < 50000) {
            fsync_queue_size_.fetch_sub(1);
            fsync_queue_.dequeue(input_txn);
            txns.push_back(input_txn);
        }
        // fsync the wal 
        committed = obj_store_->kv_store_->SyncWAL().ok();
        
        for (auto& txn : txns) {
            //notify the waiting backend thread, so it can return the result. 
            std::unique_lock<std::mutex> txn_lock(txn->mtx_);
            txn->abort_.store(txn->abort_.load() || !committed);
            txn->processed_ = true;
            txn_lock.unlock();
            txn->cond_.notify_one();
        }

        txns.clear();

    }
}

void TransactionManagerV1::Writer::requestInMemoryGC(TransactionV1 * txn) {
    in_memory_gc_queue_.enqueue(txn);
    uint64_t queue_size = in_memory_gc_queue_size_.fetch_add(1);
    if (queue_size == 0) {
        {   std::lock_guard<std::mutex> lock(in_memory_gc_thread_->mtx_); }
        in_memory_gc_thread_->cond_.notify_one();
    }
}

void TransactionManagerV1::Writer::handleInMemoryGCRequests() {
    txn_mgr_->validator_->registerThread();
    txn_mgr_->live_txns_->registerThread();

    // while not stoppped
    while (!stop_) {
        int counter = 0;
        // spin loop a little bit
        while (in_memory_gc_queue_size_.load() == 0 && counter < 500) {
            _mm_pause();
            counter++;
        }

        // wait for either the queue to fill or stop 
        if (in_memory_gc_queue_size_.load() == 0) {
            std::unique_lock<std::mutex> thread_lock(in_memory_gc_thread_->mtx_);
            in_memory_gc_thread_->cond_.wait(thread_lock, 
                [this](){ return (in_memory_gc_queue_size_.load() > 0 || stop_); });
        }
        
        TransactionV1 * txn;
        while (in_memory_gc_queue_size_.load() > 0 && !stop_) {
            in_memory_gc_queue_size_.fetch_sub(1);
            in_memory_gc_queue_.dequeue(txn);
            LFSLStatus status = txn_mgr_->live_txns_->remove(LiveTxnKey(txn->old_read_vid_, txn));
            // if txn was the head, it was blocking garbage collection.
            if ((status & LFSLStatus::WAS_HEAD) == LFSLStatus::WAS_HEAD) {
                uint64_t request_vid;
                LiveTxnKey live_txn_key;
                int value;
                // value is discarded and never used
                if (txn_mgr_->live_txns_->head(live_txn_key, value)) {
                    request_vid = live_txn_key.vid_;
                }
                // when there is no next head, it is safe to garbage collect up to the current read_vid
                else {
                    request_vid = txn_mgr_->read_vid_.load();
                }

                if (request_vid > txn_mgr_->watermark_.load()) {
                    txn_mgr_->watermark_.store(request_vid);
                }

                // read in the next head
                LiveTxnKey head_key;
                int head_value;
                if (txn_mgr_->live_txns_->head(head_key, head_value) && head_key.vid_ < txn_mgr_->watermark_.load()) {
                    continue;
                }
                
                TransactionV1 * committed_txn;
                while (txn_mgr_->committed_txns_.front(committed_txn) && committed_txn->commit_vid_ < request_vid) {
                    // Clean out every in-memory garbage related to this txn
                    txn_mgr_->committed_txns_.dequeue(committed_txn);
                    txn_mgr_->validator_->collectInMemoryGarbage(committed_txn);
                }

            }

        }

    }    
}


void TransactionManagerV1::Writer::run() {
    if (stop_) {
        stop_ = false;
        for (unsigned int i = 0; i < num_partitions_; i++) {
            boost::asio::post(*thread_pool_, [this, i]() { handleTransactions(i); });
        }
        boost::asio::post(*thread_pool_, [this](){ fsyncTransactions(); } );
        boost::asio::post(*thread_pool_, [this](){ handleInMemoryGCRequests(); } );
    }
}

void TransactionManagerV1::Writer::stop() {
    if (!stop_) {
        stop_ = true;
        // wake up all writer threads
        {   std::lock_guard<std::mutex> lock(mtx_); }
        cond_.notify_all();
        // wake up fsync thread
        {   std::lock_guard<std::mutex> lock(fsync_thread_->mtx_); }
        fsync_thread_->cond_.notify_one();
        // wake up gc thread 
        {   std::lock_guard<std::mutex> lock(in_memory_gc_thread_->mtx_); }
        in_memory_gc_thread_->cond_.notify_one();

        thread_pool_->join();
    }
}


void TransactionManagerV1::Writer::handleTransaction(unsigned int rank, TransactionV1 * txn) {
    rocksdb::WriteBatchWithIndex * write_batch = txn->write_set_->getWriteBatch(rank);
    boost::unordered::unordered_flat_map<Path, std::pair<uint64_t, uint64_t>>* merge_set = txn->write_set_->getMergeSet(rank);    
    
    if (!txn->abort_.load()) {
        for (auto & elem : *merge_set) {
            materializeMerge(write_batch, elem.first, rocksdb::Slice(&txn->write_set_->getBuf().data()[elem.second.first],
                elem.second.second), txn->commit_vid_);
        }
        //TODO handle when the final write fails. 
        auto status = obj_store_->kv_store_->Write(rocksdb::WriteOptions(), write_batch->GetWriteBatch());
    }
    
    uint64_t votes = txn->votes_.fetch_add(1);
    int count = 0;
    while (txn->votes_.load() < (3*num_partitions_) && count < 500) {
        _mm_pause();
        count++;
    }
    // synchronization point
    // if there are other threads still working, block until the last thread is done
    if (txn->votes_.load() < (3*num_partitions_)) {
        std::unique_lock<std::mutex> lock(thread_states_[rank]->mtx_);
        thread_states_[rank]->cond_.wait(lock, [this, txn](){ 
            return txn->votes_.load() >= (3*num_partitions_); });
        lock.unlock();
    }
    // if the thread is the last to reach this point, wake other threads up
    else if (votes >= (3*num_partitions_ - 1)) {
        txn_mgr_->read_vid_.store(txn->commit_vid_);
        if (!txn->abort_.load()) {
            fsync(txn);
        }
        //no need to fsync wal if txn was aborted
        else {
            std::unique_lock<std::mutex> txn_lock(txn->mtx_);
            txn->processed_ = true;
            txn_lock.unlock();
            txn->cond_.notify_one();
        }

        for (unsigned int i = 0; i < num_partitions_; i++) {
            if (i != rank) {
                std::unique_lock<std::mutex> lock(thread_states_[i]->mtx_);
                thread_states_[i]->ready_ = true;
                lock.unlock();
                thread_states_[i]->cond_.notify_one();
            }   
            
        }

        // collect inmemory garbage
        txn_mgr_->committed_txns_.enqueue(txn);
        requestInMemoryGC(txn);

    }

}

// Materialize merge, writing current snapshot to the delta store and the final snapshot 
// obtained from merging delta as the new snapshot (need to shift vids in both)
void TransactionManagerV1::Writer::materializeMerge(rocksdb::WriteBatchWithIndex * write_batch,
        const Path & path, rocksdb::Slice merge_val, uint64_t vid) {    
    
    uint64_t start_vid = 0;
    uint64_t end_vid = htole64(vid);
    mongo::BSONObj cur_obj;
    SnapshotStore::Header snapshot_header;
    //if object already exists, put current snapshot as new delta

    if (obj_store_->innerObjStore()->getCurSnapshot(path, &cur_obj, &snapshot_header)) {
        start_vid = htole64(snapshot_header.cur_vid_);
        std::string new_delta_path;
        new_delta_path.reserve(sizeof(DeltaStore::Header) + path.size_);
        new_delta_path.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
        new_delta_path.append(reinterpret_cast<char*>(&end_vid), sizeof(uint64_t));
        new_delta_path.append(path.data_, path.size_);
        write_batch->Put(obj_store_->delta_store_, rocksdb::Slice(new_delta_path.data(), new_delta_path.size()), rocksdb::Slice(cur_obj.objdata(), cur_obj.objsize()));

    }
    
    mongo::BSONObj new_obj = MmbUtil::mergeDeltaToBase(cur_obj, mongo::BSONObj(merge_val.data()));
    std::string new_snapshot_val;
    new_snapshot_val.reserve(sizeof(SnapshotStore::Header) + new_obj.objsize());
    new_snapshot_val.append(reinterpret_cast<char*>(&start_vid), sizeof(uint64_t));
    new_snapshot_val.append(reinterpret_cast<char*>(&end_vid), sizeof(uint64_t));
    new_snapshot_val.append(new_obj.objdata(), new_obj.objsize());
    write_batch->Put(obj_store_->snapshot_store_, rocksdb::Slice(path.data_, path.size_), rocksdb::Slice(new_snapshot_val.data(), new_snapshot_val.size()));

}

TransactionManagerV1::TransactionManagerV1(Config* config, ObjStore * obj_store, const std::string & db_path,
    unsigned int thread_pool_size) : obj_store_(obj_store) {

    std::string num_partitions_str;
    if (config != nullptr) {
        num_partitions_str = config->getParam("txnmgrv1.num_partitions");
    }

    if (!num_partitions_str.empty()) {
        try {
            num_partitions_ = std::stoul(num_partitions_str);
        }
        catch(...) {
            std::cout << "txnmgrv1.num_partitions is not set to a valid integer. Using " << std::thread::hardware_concurrency() << std::endl;
            num_partitions_ = std::thread::hardware_concurrency();
        }
    }
    else {
        std::cout << "txnmgrv1.num_partitions is not set. Using " << std::thread::hardware_concurrency() << std::endl;
        num_partitions_ = std::thread::hardware_concurrency();
    }

    //auto head = new LFSkipList<LogIdxKey,LogIdxValue>::Node(head_key, dummy_idx_value);
    auto head = new LFSkipList<LiveTxnKey,int>::Node(LiveTxnKey(0, nullptr), 0);
    auto tail = new LFSkipList<LiveTxnKey,int>::Node(LiveTxnKey(std::numeric_limits<uint64_t>::max(), nullptr), 0);
    live_txns_ = std::make_unique<LFSkipList<LiveTxnKey, int>>(head, tail);
    writer_ = std::make_unique<Writer>(num_partitions_, this, obj_store_);
    validator_ = std::make_unique<Validator>(num_partitions_, this, writer_.get(), obj_store_);
    //TODO this should be set during recovery process
    read_vid_.store(1);
    commit_vid_.store(2);
    next_txn_id_.store(1);
    // initialize txn slots
    for (unsigned int i = 0 ; i < thread_pool_size; i++) {
        txn_slots_.push_back(std::make_unique<TxnSlot>());
    }
    //start running the validation/write process
    writer_->run();
    validator_->run();

}

TransactionManagerV1::~TransactionManagerV1() {
    validator_->stop();
    writer_->stop();
    txn_map_.visit_all([](auto& x) { delete static_cast<TransactionV1 *>(x.second); });
}

uint64_t TransactionManagerV1::getReadVid() {
    return read_vid_.load();
}

Transaction * TransactionManagerV1::getTransaction(uint64_t txn_id) {
    std::optional<void*> txn;
    bool found = txn_map_.visit(txn_id, [&](const auto& x){ txn = x.second; });
    if (found) {
        return static_cast<Transaction*>(*txn);
    }
    return nullptr;
}

Transaction * TransactionManagerV1::startTransaction() {
    uint64_t txn_id = next_txn_id_.fetch_add(1);
    TransactionV1 * txn = new TransactionV1(num_partitions_, obj_store_, read_vid_.load());
    txn->txn_id_ = txn_id;
    txn_map_.insert_or_assign(txn_id, static_cast<void*>(txn));
    // live_txn_ is more of a set than a map
    live_txns_->registerThread();
    live_txns_->insert(LiveTxnKey(txn->old_read_vid_,txn), 0);
    // if watermark is higher than read_vid, (which is extremely unlikely), 
    // bump up the read_vid to the watermark
    txn->new_read_vid_ = (watermark_.load() > txn->old_read_vid_) ? watermark_.load() : txn->old_read_vid_;
    return static_cast<Transaction*>(txn);
}

void TransactionManagerV1::startTransaction(ClientTask * task) {
    StartTxnRequest * start_txn_request = static_cast<StartTxnRequest*>(task->request_);
    StartTxnResponse * start_txn_response = static_cast<StartTxnResponse*>(task->response_);
    
    switch (start_txn_request->txn_mode()) {
        case TXN_MODE_READ_WRITE: {
            std::minstd_rand * rand_gen = RandGen::getTLSInstance();
            static thread_local std::uniform_int_distribution<size_t> dist(0, txn_slots_.size() - 1);
            bool started_txn = false;
            int retry = 0;
            while(!started_txn) {
                size_t rand_val = (dist(*rand_gen));
                TxnSlot * txn_slot = txn_slots_[rand_val].get();
                retry++;
                std::unique_lock<std::mutex> lock(txn_slot->mtx_);
                started_txn = !txn_slot->occupied_;
                if (!started_txn) {
                    if (retry > 3) {
                        txn_slot->txn_queue_.push(task);
                        lock.unlock();
                        return;
                    }
                    else {
                        lock.unlock();
                    }
                }
                else {
                    txn_slot->occupied_ = true;
                    lock.unlock();

                    uint64_t txn_id = next_txn_id_.fetch_add(1);
                    TransactionV1 * txn = new TransactionV1(num_partitions_, obj_store_, read_vid_.load());
                    txn->txn_id_ = txn_id;
                    txn->txn_slot_ = rand_val;
                    txn_map_.insert_or_assign(txn_id, static_cast<void*>(txn));
                    // live_txn_ is more of a set than a map
                    live_txns_->registerThread();
                    live_txns_->insert(LiveTxnKey(txn->old_read_vid_,txn), 0);
                    // if watermark is higher than read_vid, (which is extremely unlikely), 
                    // bump up the read_vid to the watermark
                    txn->new_read_vid_ = (watermark_.load() > txn->old_read_vid_) ? watermark_.load() : txn->old_read_vid_;

                    start_txn_response->set_txn_id(txn_id);
                    start_txn_response->set_vid(txn->new_read_vid_);            
                    start_txn_response->set_success(true);

                }

            }
        }
            break;
        case TXN_MODE_READ_ONLY: {
            uint64_t read_vid = read_vid_.load();

            if (start_txn_request->has_read_vid()) {
                uint64_t requested_read_vid = start_txn_request->read_vid();
                // if there are commits to become visible, wait until read_vid is updated. 
                if (requested_read_vid > read_vid && requested_read_vid < commit_vid_.load()) {
                    while (requested_read_vid > read_vid_.load()) {
                        _mm_pause();
                    }
                    read_vid = requested_read_vid;
                }
                else {
                    read_vid = std::min(read_vid, requested_read_vid);
                }
            }
            

            start_txn_response->set_vid(read_vid);
            start_txn_response->set_success(true);
        }
            break;
        default:
            start_txn_response->set_success(false);
            break;

    }
    
    task->status_ = ClientTaskStatus::FINISH;
    static_cast<grpc::ServerAsyncResponseWriter<StartTxnResponse>*>
            (task->writer_)->Finish(*start_txn_response, grpc::Status::OK, task);
}

// not thread safe for the same txn
void TransactionManagerV1::removeTransaction(uint64_t txn_id) {
    std::optional<void*> txn;
    bool found = txn_map_.visit(txn_id, [&](const auto& x){ txn = x.second; });
    if (found) {
        txn_map_.erase(txn_id);
        delete static_cast<TransactionV1*>(*txn);
    }
}

// not thread safe for the same txn
Transaction * TransactionManagerV1::popTransaction(uint64_t txn_id) {
    std::optional<void*> txn;
    bool found = txn_map_.visit(txn_id, [&](const auto& x){ txn = x.second; });
    if (found) {
        txn_map_.erase(txn_id);
        return static_cast<Transaction*>(*txn);
    }
    return nullptr;
}

void TransactionManagerV1::commit(CommitRequest * commit_request, CommitResponse * commit_response) {
    uint64_t txn_id = commit_request->txn_id();
    //TODO, send the txn to completed txn queue. Don't delete the transaction for now
    TransactionV1 * txn = static_cast<TransactionV1*>(getTransaction(txn_id));
    if (txn == nullptr) {
        commit_response->set_success(false);
        return;
    }

    // logic for emptying txn slot so other waiting startTxn request can proceed
    TxnSlot * txn_slot = txn_slots_[txn->txn_slot_].get();
    std::unique_lock<std::mutex> lock(txn_slot->mtx_);
    if (txn_slot->txn_queue_.empty()) {
        txn_slot->occupied_ = false;
        lock.unlock();
    }
    else {
        ClientTask * task = txn_slot->txn_queue_.front();
        txn_slot->txn_queue_.pop();
        txn_slot->occupied_ = true;
        lock.unlock();
        
        StartTxnResponse * start_txn_response = static_cast<StartTxnResponse*>(task->response_);
        
        // txn only enters the slot queue if its mode is read-write
        uint64_t new_txn_id = next_txn_id_.fetch_add(1);
        TransactionV1 * new_txn = new TransactionV1(num_partitions_, obj_store_, read_vid_.load());
        new_txn->txn_id_ = new_txn_id;
        new_txn->txn_slot_ = txn->txn_slot_;
        txn_map_.insert_or_assign(new_txn_id, static_cast<void*>(new_txn));
        // live_txn_ is more of a set than a map
        live_txns_->registerThread();
        live_txns_->insert(LiveTxnKey(new_txn->old_read_vid_, new_txn), 0);
        // if watermark is higher than read_vid, (which is extremely unlikely), 
        // bump up the read_vid to the watermark
        new_txn->new_read_vid_ = (watermark_.load() > new_txn->old_read_vid_) ? watermark_.load() : new_txn->old_read_vid_;

        start_txn_response->set_txn_id(new_txn_id);
        start_txn_response->set_vid(new_txn->readVid());            
        start_txn_response->set_success(true);  
        
        task->status_ = ClientTaskStatus::FINISH;
        static_cast<grpc::ServerAsyncResponseWriter<StartTxnResponse>*>
                (task->writer_)->Finish(*start_txn_response, grpc::Status::OK, task);

    }

    preprocess(commit_request, txn);

    if (!txn->abort_.load()) {
        validator_->validate(txn);
        // wait for the commit request to be validated and written out
        std::unique_lock lock(txn->mtx_);
        txn->cond_.wait(lock, [txn](){ return txn->processed_; });
        lock.unlock();
    }
    
    commit_response->set_success(!txn->abort_.load());
    commit_response->set_commit_vid(txn->commit_vid_);
    
    //TODO make a queue of complete txns. Don't delete txn yet
    // but deallocate read and write sets
    // txn->clear();
    // delete txn;
}

bool TransactionManagerV1::commit(Transaction * txn) {
    TransactionV1* txn_v1 = static_cast<TransactionV1*>(txn);
    if (txn_v1 == nullptr) {
        return false;
    }

    if (!txn_v1->abort_.load()) {
        validator_->validate(txn_v1);
        // wait for the commit request to be validated and written out
        std::unique_lock lock(txn_v1->mtx_);
        txn_v1->cond_.wait(lock, [txn_v1](){ return txn_v1->processed_; });
        lock.unlock();
    }
    
    //TODO make a queue of complete txns. Don't delete txn yet
    // but deallocate read and write sets
    return !txn_v1->abort_.load();
    // delete txn;
}


void TransactionManagerV1::updateReadVid(uint64_t new_vid) {
    read_vid_.store(new_vid);
}

void TransactionManagerV1::preprocess(CommitRequest * commit_request, TransactionV1 * txn) {
    TransactionV1::WriteSet * write_set = txn->write_set_.get();
    std::string empty_str;

    if (commit_request->has_abort() && commit_request->abort()) {
        txn->abort_.store(commit_request->abort());
        return;
    }

    for (int i = 0; i < commit_request->write_set_size() && !txn->abort_.load(); i++) {
        const Write & write = commit_request->write_set(i);
        
        if (write.has_trigger()) {
            //UDFV1::invoke(write, txn, obj_store_);
        }
        switch(write.type()) {
            case WRITE_TYPE_ADD:
                if (!write_set->add(write)){
                    txn->abort_.store(true);
                }
                break;

            case WRITE_TYPE_REMOVE:
                if (!write_set->remove(write)) {
                    txn->abort_.store(true);
                }
                break;

            case WRITE_TYPE_MERGE:
                if (!write_set->merge(write)){
                    txn->abort_.store(true);
                }
                break;

            case WRITE_TYPE_UPDATE:
                if (!write_set->update(write)){
                    txn->abort_.store(true);
                }
                break;

            default:
                txn->abort_.store(true);
                break;
        }
    }
}