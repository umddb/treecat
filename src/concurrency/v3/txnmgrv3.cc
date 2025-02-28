// Author: Keonwoo Oh (koh3@umd.edu)

#include <stack>

#include "boost/unordered/unordered_flat_set.hpp"

#include "common/mmbutil.h"
#include "concurrency/v3/txnmgrv3.h"
#include "common/randgen.h"

TransactionManagerV3::TransactionManagerV3(Config* config, ObjStore * obj_store, unsigned int thread_pool_size) 
        : obj_store_(obj_store), stop_(false) {    
    //TODO this should be set during recovery process
    lock_mgr_ = std::make_unique<LockManager>();
    read_vid_.store(1);
    commit_vid_.store(2);
    next_txn_id_.store(1);
    fsync_thread_.thread_ = std::make_unique<std::thread>([this](){ fsyncTransactions(); });
    read_vid_thread_.thread_ = std::make_unique<std::thread>([this](){ updateReadVid(); });
    std::make_heap(vid_heap_.begin(), vid_heap_.end(), std::greater<>{});
    // initialize txn slots
    for (unsigned int i = 0 ; i < thread_pool_size; i++) {
        txn_slots_.push_back(std::make_unique<TxnSlot>());
    }
}

TransactionManagerV3::~TransactionManagerV3() { 
    { std::lock_guard<std::mutex>(fsync_thread_.mtx_);
        stop_ =  true; }
    { std::lock_guard<std::mutex>(read_vid_thread_.mtx_); }
    fsync_thread_.cond_.notify_all();
    read_vid_thread_.cond_.notify_all();
    fsync_thread_.thread_->join();
    read_vid_thread_.thread_->join();

}

uint64_t TransactionManagerV3::getReadVid() {
    //TODO add read_vid updater
    return read_vid_.load();
}

Transaction * TransactionManagerV3::getTransaction(uint64_t txn_id) {
    std::optional<void*> txn;
    bool found = txn_map_.visit(txn_id, [&](const auto& x){ txn = x.second; });
    if (found) {
        return static_cast<Transaction*>(*txn);
    }
    return nullptr;
}

Transaction * TransactionManagerV3::startTransaction() {
    uint64_t txn_id = next_txn_id_.fetch_add(1);
    TransactionV3 * txn = new TransactionV3(this, txn_id, obj_store_);
    txn_map_.emplace(txn_id, static_cast<void*>(txn));
    return static_cast<Transaction*>(txn);
}


void TransactionManagerV3::startTransaction(ClientTask * task) {   
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
                    TransactionV3 * txn = new TransactionV3(this, txn_id, obj_store_);
                    txn->txn_slot_ = rand_val;
                    txn_map_.emplace(txn_id, static_cast<void*>(txn));

                    start_txn_response->set_txn_id(txn_id);
                    start_txn_response->set_vid(txn->readVid());            
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
void TransactionManagerV3::removeTransaction(uint64_t txn_id) {
    std::optional<void*> txn;
    bool found = txn_map_.visit(txn_id, [&](const auto& x){ txn = x.second; });
    if (found) {
        txn_map_.erase(txn_id);
        delete static_cast<TransactionV3*>(*txn);
    }
}

// not thread safe for the same txn
Transaction * TransactionManagerV3::popTransaction(uint64_t txn_id) {
    std::optional<void*> txn;
    bool found = txn_map_.visit(txn_id, [&](const auto& x){ txn = x.second; });
    if (found) {
        txn_map_.erase(txn_id);
        return static_cast<Transaction*>(*txn);
    }
    return nullptr;
}

void TransactionManagerV3::commit(CommitRequest * commit_request, CommitResponse * commit_response) {
    uint64_t txn_id = commit_request->txn_id();
    //TODO, send the txn to completed txn queue. Don't delete the transaction for now
    TransactionV3 * txn = static_cast<TransactionV3*>(getTransaction(txn_id));
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
        TransactionV3 * new_txn = new TransactionV3(this, new_txn_id, obj_store_);
        new_txn->txn_slot_ = txn->txn_slot_;
        txn_map_.emplace(new_txn_id, static_cast<void*>(new_txn));

        start_txn_response->set_txn_id(new_txn_id);
        start_txn_response->set_vid(new_txn->readVid());            
        start_txn_response->set_success(true);  
        
        task->status_ = ClientTaskStatus::FINISH;
        static_cast<grpc::ServerAsyncResponseWriter<StartTxnResponse>*>
                (task->writer_)->Finish(*start_txn_response, grpc::Status::OK, task);

    }

    preprocess(commit_request, txn);
    if (txn->abort_.load()) {
        unlockAll(txn);
        commit_response->set_success(false);
        return;
    }
    
    txn->commit_vid_= commit_vid_.fetch_add(1);
    updateCommitVid(txn);
    //if txn not aborted
    if (!txn->abort_.load()) {
        // write the write batch to rocksdb
        rocksdb::WriteBatchWithIndex * write_batch = txn->write_set_->getWriteBatch();
        boost::unordered::unordered_flat_map<Path, std::pair<uint64_t, uint64_t>>* merge_set = txn->write_set_->getMergeSet();    
        
        for (auto & elem : *merge_set) {
            materializeMerge(write_batch, elem.first, rocksdb::Slice(&txn->write_set_->getBuf().data()[elem.second.first], elem.second.second), txn->commit_vid_);
        }
        // TODO handle when the final write fails. 
        auto status = obj_store_->kv_store_->Write(rocksdb::WriteOptions(), write_batch->GetWriteBatch());
    }

    unlockAll(txn);
    
    // schedule it to fsync && read_vid updater
    postprocess(txn);
    // wait for fsync if txn is committed (not aborted)
    if (!txn->abort_.load()) {
        std::unique_lock txn_lock(txn->mtx_);
        txn->cond_.wait(txn_lock, [txn](){ return txn->processed_; });
        txn_lock.unlock();
    }
    
    //txn succeeds if not aborted
    commit_response->set_success(!txn->abort_.load());
    commit_response->set_commit_vid(txn->commit_vid_);

    txn->clear();
}

bool TransactionManagerV3::commit(Transaction* txn) {
    TransactionV3 * txn_v3 = static_cast<TransactionV3*>(txn);
    if (txn_v3 == nullptr) {
        return false;
    }
    
    if (txn_v3->abort_.load()) {
        unlockAll(txn_v3);
        return false;
    }
    
    txn_v3->commit_vid_= commit_vid_.fetch_add(1);
    updateCommitVid(txn_v3);
    //if txn not aborted
    if (!txn_v3->abort_.load()) {
        // write the write batch to rocksdb
        rocksdb::WriteBatchWithIndex * write_batch = txn_v3->write_set_->getWriteBatch();
        boost::unordered::unordered_flat_map<Path, std::pair<uint64_t, uint64_t>>* merge_set = txn_v3->write_set_->getMergeSet();    
        
        for (auto & elem : *merge_set) {
            materializeMerge(write_batch, elem.first, rocksdb::Slice(&txn_v3->write_set_->getBuf().data()[elem.second.first], elem.second.second), txn_v3->commit_vid_);
        }
        // TODO handle when the final write fails. 
        auto status = obj_store_->kv_store_->Write(rocksdb::WriteOptions(), write_batch->GetWriteBatch());
    }

    unlockAll(txn_v3);
    
    //schedule it to fsync && read_vid updater
    postprocess(txn_v3);
    //wait for fsync if txn is committed (not aborted)
    if (!txn_v3->abort_.load()) {
        std::unique_lock txn_lock(txn_v3->mtx_);
        txn_v3->cond_.wait(txn_lock, [txn_v3](){ return txn_v3->processed_; });
        txn_lock.unlock();
    }
    
    txn_v3->clear();
    return !txn_v3->abort_.load();
}

LockManager * TransactionManagerV3::lockManager() {
    return lock_mgr_.get();
}

void TransactionManagerV3::unlockAll(TransactionV3 * txn) {
    if (txn->owned_locks_ != nullptr) {
        std::unique_lock<std::mutex> txn_lock(txn->mtx_);
        boost::unordered::unordered_flat_map<Path, LockMode> *owned_locks = txn->owned_locks_.get();
        auto lock_iter = owned_locks->begin();
        while (lock_iter != owned_locks->end()) {
            Path read_path(lock_iter->first.data_, lock_iter->first.size_, true);
            txn_lock.unlock();
            lock_mgr_->unlock(read_path, txn);
            txn_lock.lock();
            lock_iter = owned_locks->begin();
        }
    }    
}


void TransactionManagerV3::preprocess(CommitRequest * commit_request, TransactionV3 * txn) {
    TransactionV3::WriteSet * write_set = txn->write_set_.get();
    std::string empty_str;

    if (commit_request->has_abort() && commit_request->abort()) {
        txn->abort_.store(commit_request->abort());
        return;
    }

    for (int i = 0; i < commit_request->write_set_size() && !txn->abort_.load(); i++) {
        const Write & write = commit_request->write_set(i);
        
        if (write.has_trigger()) {
            // TODO 
            // UDFV3::invoke(write, txn, obj_store_);
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

void TransactionManagerV3::updateCommitVid(TransactionV3 * txn) {
    rocksdb::WriteBatchWithIndex * write_batch = txn->write_set_->getWriteBatch();

    uint64_t le_commit_vid = htole64(txn->commit_vid_);
    rocksdb::WBWIIterator* iter = write_batch->NewIterator(obj_store_->snapshot_store_);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // reset current vid
        memcpy(&const_cast<char*>(iter->Entry().value.data())[sizeof(uint64_t)], &le_commit_vid, sizeof(uint64_t));
        iter->Next();
    }

    delete iter;

    iter = write_batch->NewIterator(obj_store_->leaf_store_);
    iter->SeekToFirst();
    while (iter->Valid()) {
        auto entry = iter->Entry();
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

}

// Materialize merge, writing current snapshot to the delta store and the final snapshot 
// obtained from merging delta as the new snapshot (need to shift vids in both)
void TransactionManagerV3::materializeMerge(rocksdb::WriteBatchWithIndex * write_batch,
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

void TransactionManagerV3::postprocess(TransactionV3 * txn) {
    if (!txn->abort()) {
        fsync_thread_.task_queue_->enqueue(txn);
        uint64_t txn_queue_size = fsync_thread_.task_queue_size_.fetch_add(1);
        if (txn_queue_size == 0) {
            {   std::lock_guard<std::mutex> lock(fsync_thread_.mtx_); }
            fsync_thread_.cond_.notify_one();
        }
    }
    
    read_vid_thread_.task_queue_->enqueue(txn);
    uint64_t txn_queue_size = read_vid_thread_.task_queue_size_.fetch_add(1);
    if (txn_queue_size == 0) {
        {   std::lock_guard<std::mutex> lock(read_vid_thread_.mtx_); }
        read_vid_thread_.cond_.notify_one();
    }
}

void TransactionManagerV3::fsyncTransactions() {
    void * task;
    TransactionV3 * input_txn;
    std::vector<TransactionV3*> txns;

    while (!stop_) {
        int counter = 0;
        // spin loop a little bit
        while (fsync_thread_.task_queue_size_.load() == 0 && counter < 500) {
            _mm_pause();
            counter++;
        }

        // wait for the queue to fill
        if (fsync_thread_.task_queue_size_.load() == 0) {
            std::unique_lock<std::mutex> thread_lock(fsync_thread_.mtx_);
            fsync_thread_.cond_.wait(thread_lock, [this](){ return (fsync_thread_.task_queue_size_.load() > 0 || stop_); });
        }

        if (fsync_thread_.task_queue_size_.load() > 0) {
             //batch commit size of 50000 for now
            while (fsync_thread_.task_queue_size_.load() > 0 && txns.size() < 50000) {
                fsync_thread_.task_queue_size_.fetch_sub(1);
                fsync_thread_.task_queue_->dequeue(task);
                input_txn = static_cast<TransactionV3*>(task);
                txns.push_back(input_txn);
            }
            // fsync the wal 
            bool committed = obj_store_->kv_store_->SyncWAL().ok();
            
            for (auto& txn : txns) {
                //notify the waiting backend thread, so it can return the result. 
                std::unique_lock<std::mutex> txn_lock(txn->mtx_);
                txn->abort_.store(txn->abort_.load() || !committed);
                txn->processed_ = true;
                txn_lock.unlock();
                txn->cond_.notify_one();
            }
        }

        txns.clear();

    }
}


void TransactionManagerV3::updateReadVid() {
    void * task;
    TransactionV3 * txn;

    while (!stop_) {
        uint64_t txn_queue_size = read_vid_thread_.task_queue_size_.load();
        if (txn_queue_size > 0) {
            read_vid_thread_.task_queue_size_.fetch_sub(1);
            read_vid_thread_.task_queue_->dequeue(task);
            txn = static_cast<TransactionV3*>(task);

            if (txn->commit_vid_ == (read_vid_.load() + 1)) {
                read_vid_.store(txn->commit_vid_);
                while (!vid_heap_.empty() && vid_heap_[0] == (read_vid_.load() + 1)) {
                    std::pop_heap(vid_heap_.begin(), vid_heap_.end(), std::greater<>{});
                    read_vid_.store(vid_heap_.back());
                    vid_heap_.pop_back();
                }
            }
            else {
                vid_heap_.push_back(txn->commit_vid_);
                std::push_heap(vid_heap_.begin(), vid_heap_.end(), std::greater<>{});
            }

        }
        else {
            int counter = 0;
            // spin loop a little bit
            while (read_vid_thread_.task_queue_size_.load() == 0 && counter < 500) {
                _mm_pause();
                counter++;
            }

            if (read_vid_thread_.task_queue_size_.load() == 0) {
                std::unique_lock<std::mutex> lock(read_vid_thread_.mtx_);
                read_vid_thread_.cond_.wait(lock, [ this ](){ return (read_vid_thread_.task_queue_size_.load() > 0 || stop_); });
            }

        }
    }
}


LockManager::LockManager() {
    stop_.store(false);  
    deadlock_detector_ = std::thread( [ this ] { detectDeadlock(); });
}

//TODO delete lock manager first when deallocating txn_mgr
LockManager::~LockManager() {
    stop_.store(true);
    deadlock_detector_.join();
    //TODO destroy all 
}

void LockManager::detectDeadlock() {
    while (!stop_.load()) {
        boost::unordered_flat_set<TransactionV3*> cyclers = detectCycle();
        for (auto& cycler : cyclers) {
            { std::lock_guard<std::mutex> cycler_lock(cycler->mtx_);
              cycler->abort_.store(true); }
            cycler->cond_.notify_all();
        }
        //sleep for 10 milliseconds
        std::this_thread::sleep_for(std::chrono::microseconds(10000));
    }
}


void LockManager::lock(const Path & path, TransactionV3 * txn, LockMode lock_mode) {
    // latch the txn state first
    std::unique_lock<std::mutex> txn_latch(txn->mtx_);
    boost::unordered::unordered_flat_map<Path, LockMode> * owned_locks = txn->owned_locks_.get();

    // check if txn already holds some lock for the given path
    if (owned_locks->contains(path))  {
        // if txn already holds a stronger or equal lock, return
        if (stronger((*owned_locks)[path], lock_mode)) {
            return;
        }
        // lock is upgradable to the requested lock mode, so long as there is no
        // conflict with current lock holders. 
        else if (stronger(lock_mode, (*owned_locks)[path])) {
            upgradeLock(path, txn, lock_mode);
            return;
        }
        // the requested lock mode is not upgradable from the current lock mode
        else {
            txn->abort_.store(true);
            return;
        }
    }
    
    LockRequest lock_request(txn, lock_mode);
    std::unique_lock queue_latch(latches_[std::hash<Path>()(path) % SHARDS_COUNT].mtx_);

    bool queue_found = false;
    std::list<LockRequest>* lock_queue;
    // try inserting new lock queue at least 3 times, just in case
    for (int i = 0; i < 3 && !queue_found ; i++ ) {
        queue_found = lock_table_.visit(path, [&](const auto& x){ lock_queue = x.second; }  );
        if (!queue_found) {
            lock_queue = new std::list<LockRequest>();
            bool inserted = lock_table_.emplace(Path(path.data_,path.size_, true), lock_queue);
            if (!inserted) {
                delete lock_queue;
            }
        }
    }

    bool has_conflict = false;
    for (auto & lock_request : *lock_queue) {
        // the lock schedule policy is strictly FIFO for correctness etc, so as soon as a request is not granted, 
        // stop and wait for prior requests to be processed first
        has_conflict = (!lock_request.granted_) || conflicting(lock_request.lock_mode_, lock_mode);
        if (has_conflict) {
            // set the waiting txn
            txn->waiting_.store(lock_queue->front().txn_);
            break;
        } 
    }

    lock_request.granted_ = !has_conflict;
    
    // enqueue the lock request
    lock_queue->emplace_back(lock_request);
    queue_latch.unlock();

    if (has_conflict) {
        // insert txn to waiting txns
        waiting_txns_.emplace(txn);
        // wait
        txn->cond_.wait(txn_latch, [txn, owned_locks, &path, lock_mode ](){ return (owned_locks->contains(path) && 
            stronger((*owned_locks)[path], lock_mode)) || txn->abort_.load(); } );
        // remove txn from waiting txns
        waiting_txns_.erase(txn);
    }
    else {
        owned_locks->insert_or_assign(path, lock_mode);
    }

}

void LockManager::unlock(const Path & path, TransactionV3 * txn) {
    // remove the path from owned locks first
    {   std::lock_guard<std::mutex> txn_latch(txn->mtx_);
        txn->owned_locks_->erase(path); }

    bool seen_lock_modes[6] = { false, false, false, false, false, false };
    bool conflicting_lock_modes[6] = { false, false, false, false, false, false };
    // lock requests that are granted as a result of unlocking
    std::vector<LockRequest> granted_requests;

    // latch the lock queue
    std::unique_lock queue_latch(latches_[std::hash<Path>()(path) % SHARDS_COUNT].mtx_);

    std::list<LockRequest>* lock_queue;
    bool queue_found = lock_table_.visit(path, [&](const auto& x){ lock_queue = x.second; } );
    // if lock queue does not even exist, return
    if (!queue_found) {
        return;
    }
    
    auto txn_iter = std::find_if(lock_queue->begin(), lock_queue->end(), 
        [ txn ](LockRequest request){ return request.txn_ == txn; });
    // if the lock request is not even in the lock queue, return
    if (txn_iter == lock_queue->end()) {
        return;
    }

    bool was_head = (txn_iter == lock_queue->begin());

    // remove the lock request
    lock_queue->erase(txn_iter);
    bool grant = true;
    for (auto lock_iter = lock_queue->begin(); lock_iter != lock_queue->end(); lock_iter++) {
        if (!lock_iter->granted_) {
            // if the txn was the head, waiting_ has to be updated for txns still waiting
            if (was_head && lock_iter != lock_queue->begin()) {
                lock_iter->txn_->waiting_.store(lock_queue->front().txn_);
            }
            // If there are no conflicting prior requests, lock is granted
            // The lock scheduling policy is strictly FIFO for correctness reasons etc. Hence, 
            // stop granting locks as soon as one lock request fails being granted. 
            grant = grant && !conflicting_lock_modes[lock_iter->lock_mode_];
            if (grant) {
                lock_iter->granted_ = true;
                granted_requests.push_back(*lock_iter);
            }
        }
        // if the current lock mode has not been seen before, update the conflicting lock modes
        if (!seen_lock_modes[lock_iter->lock_mode_]) {
            seen_lock_modes[lock_iter->lock_mode_] = true;
            for (size_t i = 0; i < 6; i++) {
                conflicting_lock_modes[i] = conflicting_lock_modes[i] || conflicting(lock_iter->lock_mode_, static_cast<LockMode>(i));
            }
        }
    }

    queue_latch.unlock();

    // finally wake up the txns that have been waiting
    for (auto & request : granted_requests) {
        {   std::lock_guard waiting_txn_latch(request.txn_->mtx_);
            request.txn_->owned_locks_->insert_or_assign(path, request.lock_mode_);
        }
        request.txn_->cond_.notify_one();
    }

}

bool LockManager::descendable(LockMode lock_mode1, LockMode lock_mode2) {
    return lock_descendability_matrix_[lock_mode1][lock_mode2];
}

// txn mtx is already locked when this function is called
void LockManager::upgradeLock(const Path & path, TransactionV3 * txn, LockMode lock_mode) {
    std::unique_lock queue_latch(latches_[std::hash<Path>()(path) % SHARDS_COUNT].mtx_);
    
    std::list<LockRequest>* lock_queue;
    bool queue_found = lock_table_.visit(path, [&](const auto& x){ lock_queue = x.second; } );
    // if lock queue does not even exist, return
    if (!queue_found) {
        txn->abort_.store(true);
        return;
    }

    bool has_conflict = false;
    for (auto & lock_request : *lock_queue) {
        has_conflict = (lock_request.granted_ 
            && lock_request.txn_ != txn && conflicting(lock_request.lock_mode_, lock_mode));
        if (has_conflict) {
            txn->abort_.store(true);
            break;
        }
    }
    
    // upgrade the lock if there is no conflict
    if (!has_conflict) {
        auto txn_iter = std::find_if(lock_queue->begin(), lock_queue->end(), 
            [ txn ](LockRequest request){ return request.txn_ == txn; });
        if (txn_iter == lock_queue->end()) {
            txn->abort_.store(true);
            return;
        }     
        txn_iter->lock_mode_ = lock_mode;
        queue_latch.unlock();
        txn->owned_locks_->insert_or_assign(path, lock_mode);
    }

}


bool LockManager::conflicting(LockMode lock_mode1, LockMode lock_mode2) {
    return !lock_compatibility_matrix_[lock_mode1][lock_mode2];
}

bool LockManager::stronger(LockMode lock_mode1, LockMode lock_mode2) {
    return lock_strength_matrix_[lock_mode1][lock_mode2];
}

// detects cycle and returns all txns that are in a cycle
boost::unordered_flat_set<TransactionV3*> LockManager::detectCycle() {
    // First, construct the wait-for graph, which consists of vertices and vertex_idx.
    // Because each txn is waiting for exactly 1 txn, there is exactly 1 outgoing edge
    // for each vertex. Hence we only build vertices, a vector of outgoing edges, and
    // vertex_idx, index on the txn to its position in vertices. 
    std::vector<Vertex> vertices;
    boost::unordered_flat_map<TransactionV3*, size_t> vertex_idx;
    waiting_txns_.visit_all([ & ](const auto& x) { vertices.emplace_back(x, x->waiting_);  
                                                   vertex_idx.emplace(x, vertices.size() - 1); });

    // A variation of DFS to detect all cycles in the graph exactly once. To achieve this, 
    // we use a coloring scheme where we assign a distinct color for each DFS run. This way, 
    // we can prune out false positives that arise due to multiple runs on the same 
    // connected component, each starting from a different vertex. This scheme works 
    // because there is exactly one outgoing edge for every vertex (so there is no "overlapping" cycles). 
    boost::unordered_flat_set<TransactionV3*> cyclers;
    for (size_t i = 0; i < vertices.size(); i++) {
        if (vertices[i].color_ < 0) {
            int color = i;
            std::stack<TransactionV3*> queue;
            queue.push(vertices[i].vertex_);
            while (!queue.empty()) {
                TransactionV3 * cur_vertex = queue.top();
                queue.pop();
                vertices[vertex_idx[cur_vertex]].color_ = color;
                TransactionV3 * neighbor = vertices[vertex_idx[cur_vertex]].neighbor_;
                if (vertex_idx.contains(neighbor)) {
                    if (vertices[vertex_idx[neighbor]].color_ < 0) {
                        queue.push(neighbor);
                    }
                    else if (vertices[vertex_idx[neighbor]].color_ == color) {
                        cyclers.emplace(neighbor);
                    }
                }
            }
        }
    }

    return cyclers;
    
}




