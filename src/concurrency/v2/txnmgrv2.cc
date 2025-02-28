// Author: Keonwoo Oh (koh3@umd.edu)

#include <filesystem>
#include <limits>

#include <endian.h>
#include <immintrin.h>

#include "concurrency/v2/txnmgrv2.h"

#include "boost/asio/post.hpp"
#include "common/mmbutil.h"
#include "common/config.h"
#include "exec/evalexpr.h"

using ConstraintKey = TransactionV2::ConstraintKey;
using ConstraintType = TransactionV2::ConstraintType;
using LiveTxnKey = TransactionV2::LiveTxnKey;


TransactionManagerV2::Validator::Validator(unsigned int num_partitions, TransactionManagerV2 * txn_mgr, Writer * writer, ObjStore * obj_store) :
    num_partitions_(num_partitions), txn_mgr_(txn_mgr), writer_(writer), obj_store_(obj_store), stop_(true) {

    ConstraintKey head_key(std::numeric_limits<uint64_t>::max(), Path("/"));
    ConstraintKey tail_key(0, Path("//////////////////////////////////////////////////////"));
    auto head = new LFSkipList<ConstraintKey,ConstraintType>::Node(head_key, ConstraintType::NOT_MODIFIED);
    auto tail = new LFSkipList<ConstraintKey,ConstraintType>::Node(tail_key, ConstraintType::NOT_MODIFIED); 
    constraint_map_ = std::make_unique<LFSkipList<ConstraintKey,ConstraintType>>(head,tail);

    // intialize all the partitioned data structures
    thread_pool_ = std::make_unique<boost::asio::thread_pool>(num_partitions_);
    for (unsigned int i = 0; i < num_partitions_; i++) {
        thread_states_.push_back(std::make_unique<ThreadState>());
        version_map_.push_back(std::make_unique<boost::concurrent_flat_map<Path, uint64_t>>());
        
    } 

    global_curr_txn_.store(nullptr);
    validation_queue_size_.store(0);
    
}

TransactionManagerV2::Validator::~Validator() {
    stop();
    
}


void TransactionManagerV2::Validator::validate(TransactionV2 * txn) {
    validation_queue_.enqueue(txn);
    uint64_t queue_size = validation_queue_size_.fetch_add(1);
    if (queue_size == 0) {
        {   std::lock_guard<std::mutex> lock(mtx_); }
        cond_.notify_all();
    }
}

// executors for validation
void TransactionManagerV2::Validator::handleTransactions(unsigned int rank) {
    ThreadState* thread_state = thread_states_[rank].get();
    constraint_map_->registerThread();
    
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
        TransactionV2 * next_txn;
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

void TransactionManagerV2::Validator::run() {
    if (stop_) {
        stop_ = false;
        //start running validator threads
        for (unsigned int i = 0; i < num_partitions_; i++) {
            boost::asio::post(*thread_pool_, [this, i]() { handleTransactions(i); });
        }
    }
}

void TransactionManagerV2::Validator::stop() {
    if (!stop_) {
        stop_ = true;
        {   std::lock_guard<std::mutex> lock(mtx_); }
            cond_.notify_all();
        thread_pool_->join();
    }
}

void TransactionManagerV2::Validator::registerThread() {
    constraint_map_->registerThread();
}

void TransactionManagerV2::Validator::collectInMemoryGarbage(TransactionV2 * txn) {
    if (!txn->abort_.load()) {
        for (unsigned int i = 0; i < num_partitions_; i++) {
            boost::unordered_flat_set<Path>* version_map_updates = txn->write_set_->getVersionMapUpdates(i);
            std::vector<ConstraintKey> * constraint_updates_keys = 
                txn->write_set_->getConstraintUpdatesKeys(i);

            for (auto & path: *version_map_updates) {
                version_map_[i]->erase_if(path, [&](const auto& x){ return (x.second == txn->commit_vid_); });
            }

            for (auto & constraint_updates_key : *constraint_updates_keys) {
                constraint_updates_key.vid_ = txn->commit_vid_;
                constraint_map_->remove(constraint_updates_key);
            }
        }
    }
    
    // TODO clear the txn read set and write set
    txn->clear();
}

void TransactionManagerV2::Validator::handleTransaction(unsigned int rank, TransactionV2 * txn) {
    std::vector<Path> * parent_locks = txn->read_set_->getParentLocks(rank);
    std::vector<std::pair<ConstraintKey, ConstraintKey>> * range_locks = txn->read_set_->getRangeLocks(rank);
    std::vector<std::pair<ConstraintKey, ConstraintType>> * constraint_checks = txn->read_set_->getConstraintChecks(rank);
    std::vector<uint64_t> *constraint_check_vids = txn->read_set_->getConstraintCheckVids(rank);

    rocksdb::WriteBatchWithIndex * write_batch = txn->write_set_->getWriteBatch(rank);
    boost::unordered_flat_set<Path>* version_map_updates = txn->write_set_->getVersionMapUpdates(rank);
    std::vector<LFSkipList<ConstraintKey,ConstraintType>::Node*> * constraint_updates = 
        txn->write_set_->getConstraintUpdates(rank);

    // optimistic parent locking
    uint64_t vid;
    for (size_t i = 0; i < parent_locks->size() && !txn->abort_.load(); i++) {
        bool found = version_map_[rank]->visit((*parent_locks)[i], [&](const auto& x){ vid = x.second; });
        if (found && vid > txn->new_read_vid_) {
            txn->abort_.store(true);
        }
    }

    // optimistic range locking
    LFSkipList<ConstraintKey, ConstraintType>::Iterator * constraint_iter = constraint_map_->newIterator();
    for (size_t i = 0; i < range_locks->size() && !txn->abort_.load(); i++) {
        constraint_iter->seek((*range_locks)[i].first);
        // while iterator is valid and is less than upper bound
        while (constraint_iter->isValid() && constraint_iter->key().path_ < (*range_locks)[i].second.path_
            && !txn->abort_.load()) {
            if (constraint_iter->key().vid_ > txn->new_read_vid_) {
                txn->abort_.store(true);
            }
        }
    }
    delete constraint_iter;

    // optimistic point locking + constraint checks
    for (size_t i = 0 ; i < constraint_checks->size() && !txn->abort_.load(); i++) {
        ConstraintKey constraint_key(0, Path());
        ConstraintType constraint_type;

        constraint_map_->tryGet(constraint_checks->at(i).first, constraint_key, constraint_type);

        if (constraint_checks->at(i).first.path_ == constraint_key.path_) {
            switch (constraint_checks->at(i).second) {
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
                    if (constraint_key.vid_ > constraint_check_vids->at(i)) {
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

        // insert the constraint updates to constraint map
        for (auto & constraint_update : *constraint_updates) {
            constraint_update->key_.vid_ = txn->commit_vid_;
            constraint_map_->insert(constraint_update);
        }

    }

    votes = txn->votes_.fetch_add(1);
    if (votes == (2*num_partitions_ - 1)) {
        writer_->write(txn);
    }

}

TransactionManagerV2::Writer::Writer(unsigned int num_partitions, TransactionManagerV2 * txn_mgr, ObjStore * obj_store) :
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


TransactionManagerV2::Writer::~Writer() {
    stop();
}


void TransactionManagerV2::Writer::write(TransactionV2 * txn) {
    write_queue_.enqueue(txn);
    uint64_t queue_size = write_queue_size_.fetch_add(1);
    if (queue_size == 0) {
        {   std::lock_guard<std::mutex> lock(mtx_); }
        cond_.notify_all();
    }

}


void TransactionManagerV2::Writer::handleTransactions(unsigned int rank) {
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
        TransactionV2 * next_txn;
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

void TransactionManagerV2::Writer::fsync(TransactionV2 * txn) {
    fsync_queue_.enqueue(txn);
    uint64_t queue_size = fsync_queue_size_.fetch_add(1);
    if (queue_size == 0) {
        {   std::lock_guard<std::mutex> lock(fsync_thread_->mtx_); }
        fsync_thread_->cond_.notify_one();
    }
}

void TransactionManagerV2::Writer::fsyncTransactions() {
    TransactionV2 * input_txn;
    bool committed;
    std::vector<TransactionV2*> txns;

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
        auto status = obj_store_->kv_store_->SyncWAL();
        if (status.ok()) {
            committed = true;
        }
        else {
            committed = false;
        }
        
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

void TransactionManagerV2::Writer::requestInMemoryGC(TransactionV2 * txn) {
    in_memory_gc_queue_.enqueue(txn);
    uint64_t queue_size = in_memory_gc_queue_size_.fetch_add(1);
    if (queue_size == 0) {
        {   std::lock_guard<std::mutex> lock(in_memory_gc_thread_->mtx_); }
        in_memory_gc_thread_->cond_.notify_one();
    }
}

void TransactionManagerV2::Writer::handleInMemoryGCRequests() {
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
        
        TransactionV2 * txn;
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
                
                TransactionV2 * committed_txn;
                while (txn_mgr_->committed_txns_.front(committed_txn) && committed_txn->commit_vid_ < request_vid) {
                    // Clean out every in-memory garbage related to this txn
                    txn_mgr_->committed_txns_.dequeue(committed_txn);
                    txn_mgr_->validator_->collectInMemoryGarbage(committed_txn);
                }

            }

        }

    }    
}


void TransactionManagerV2::Writer::run() {
    if (stop_) {
        stop_ = false;
        for (unsigned int i = 0; i < num_partitions_; i++) {
            boost::asio::post(*thread_pool_, [this, i]() { handleTransactions(i); });
        }
        boost::asio::post(*thread_pool_, [this](){ fsyncTransactions(); } );
        boost::asio::post(*thread_pool_, [this](){ handleInMemoryGCRequests(); } );
    }
}

void TransactionManagerV2::Writer::stop() {
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


void TransactionManagerV2::Writer::handleTransaction(unsigned int rank, TransactionV2 * txn) {
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
void TransactionManagerV2::Writer::materializeMerge(rocksdb::WriteBatchWithIndex * write_batch,
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

TransactionManagerV2::TransactionManagerV2(Config* config, ObjStore * obj_store, const std::string & db_path,
    unsigned int thread_pool_size) : obj_store_(obj_store) {

    std::string num_partitions_str;
    if (config != nullptr) {
        num_partitions_str = config->getParam("txnmgrv2.num_partitions");
    }

    if (!num_partitions_str.empty()) {
        try {
            num_partitions_ = std::stoul(num_partitions_str);
        }
        catch(...) {
            std::cout << "txnmgrv2.num_partitions is not set to a valid integer. Using " << std::thread::hardware_concurrency() << std::endl;
            num_partitions_ = std::thread::hardware_concurrency();
        }
    }
    else {
        std::cout << "txnmgrv2.num_partitions is not set. Using " << std::thread::hardware_concurrency() << std::endl;
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

TransactionManagerV2::~TransactionManagerV2() {
    validator_->stop();
    writer_->stop();
    txn_map_.visit_all([](auto& x) { delete static_cast<TransactionV2 *>(x.second); });
}

uint64_t TransactionManagerV2::getReadVid() {
    return read_vid_.load();
}

Transaction * TransactionManagerV2::getTransaction(uint64_t txn_id) {
    std::optional<void*> txn;
    bool found = txn_map_.visit(txn_id, [&](const auto& x){ txn = x.second; });
    if (found) {
        return static_cast<Transaction*>(*txn);
    }
    return nullptr;
}

Transaction * TransactionManagerV2::startTransaction() {
    uint64_t txn_id = next_txn_id_.fetch_add(1);
    TransactionV2 * txn = new TransactionV2(num_partitions_, obj_store_, read_vid_.load());
    txn->txn_id_ = txn_id;
    txn_map_.insert_or_assign(txn_id, static_cast<void*>(txn));
    return static_cast<Transaction*>(txn);
}

void TransactionManagerV2::startTransaction(ClientTask * task) {
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
                    TransactionV2 * txn = new TransactionV2(num_partitions_, obj_store_, read_vid_.load());
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
void TransactionManagerV2::removeTransaction(uint64_t txn_id) {
    std::optional<void*> txn;
    bool found = txn_map_.visit(txn_id, [&](const auto& x){ txn = x.second; });
    if (found) {
        txn_map_.erase(txn_id);
        delete static_cast<TransactionV2*>(*txn);
    }
}

// not thread safe for the same txn
Transaction * TransactionManagerV2::popTransaction(uint64_t txn_id) {
    std::optional<void*> txn;
    bool found = txn_map_.visit(txn_id, [&](const auto& x){ txn = x.second; });
    if (found) {
        txn_map_.erase(txn_id);
        return static_cast<Transaction*>(*txn);
    }
    return nullptr;
}

void TransactionManagerV2::commit(CommitRequest * commit_request, CommitResponse * commit_response) {
    uint64_t txn_id = commit_request->txn_id();
    //TODO, send the txn to completed txn queue. Don't delete the transaction for now
    TransactionV2 * txn = static_cast<TransactionV2*>(getTransaction(txn_id));
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
        TransactionV2 * new_txn = new TransactionV2(num_partitions_, obj_store_, read_vid_.load());
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

bool TransactionManagerV2::commit(Transaction * txn) {
    TransactionV2* txn_v2 = static_cast<TransactionV2*>(txn);
    if (txn_v2 == nullptr) {
        return false;
    }

    if (!txn_v2->abort_.load()) {
        validator_->validate(txn_v2);
        // wait for the commit request to be validated and written out
        std::unique_lock lock(txn_v2->mtx_);
        txn_v2->cond_.wait(lock, [txn_v2](){ return txn_v2->processed_; });
        lock.unlock();
    }
    
    //TODO make a queue of complete txns. Don't delete txn yet
    // but deallocate read and write sets
    return !txn_v2->abort_.load();
    // delete txn;
}


void TransactionManagerV2::updateReadVid(uint64_t new_vid) {
    read_vid_.store(new_vid);
}

void TransactionManagerV2::preprocess(CommitRequest * commit_request, TransactionV2 * txn) {
    TransactionV2::WriteSet * write_set = txn->write_set_.get();
    std::string empty_str;

    if (commit_request->has_abort() && commit_request->abort()) {
        txn->abort_.store(commit_request->abort());
        return;
    }

    for (int i = 0; i < commit_request->write_set_size() && !txn->abort_.load(); i++) {
        const Write & write = commit_request->write_set(i);
        
        if (write.has_trigger()) {
            //UDFV2::invoke(write, txn, obj_store_);
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