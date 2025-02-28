// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef TXN_MGR_V3_H
#define TXN_MGR_V3_H

#include <atomic>
#include <vector>
#include <memory>
#include <queue>

#include "boost/asio/thread_pool.hpp"
#include "boost/unordered/concurrent_flat_map.hpp"
#include "boost/unordered/concurrent_flat_set.hpp"

#include "grpc/grpccatalog.grpc.pb.h"

#include "common/concurrentqueue.h"
#include "common/paddedmutex.h"
#include "storage/objstore.h"
#include "concurrency/v3/txnv3.h"
#include "concurrency/txnmgr.h"

class TransactionManagerV3 : public TransactionManager {

    public:

        struct alignas(64) ThreadState  {
            std::unique_ptr<std::thread> thread_;
            std::mutex mtx_;
            std::condition_variable cond_;
            std::unique_ptr<ConcurrentQueue<void*>> task_queue_;
            std::atomic<uint64_t> task_queue_size_;

            ThreadState() {
                task_queue_size_.store(0);
                task_queue_ = std::make_unique<ConcurrentQueue<void*>>();
            }
        };

        TransactionManagerV3(Config* config, ObjStore * obj_store, unsigned int thread_pool_size);
        ~TransactionManagerV3();
        uint64_t getReadVid() override;
        Transaction * getTransaction(uint64_t txn_id) override;
        Transaction * startTransaction() override;
        void startTransaction(ClientTask * task) override;
        void removeTransaction(uint64_t txn_id) override;
        Transaction * popTransaction(uint64_t txn_id) override;
        void commit(CommitRequest * commit_request, CommitResponse * commit_response) override;
        bool commit(Transaction* txn) override;
        LockManager * lockManager();
        void unlockAll(TransactionV3 * txn);
        // void updateReadVid(uint64_t new_vid);
        
    private:

        struct alignas(64) TxnSlot {
            std::queue<ClientTask*> txn_queue_;
            std::mutex mtx_;
            bool occupied_; 

            TxnSlot() : occupied_(false) {  }
        };

        void preprocess(CommitRequest * commit_request, TransactionV3 * txn);
        void updateCommitVid(TransactionV3 * txn);
        void materializeMerge(rocksdb::WriteBatchWithIndex * write_batch,
                const Path & path, rocksdb::Slice merge_val, uint64_t vid);
        void postprocess(TransactionV3 * txn);
        void fsyncTransactions();
        void updateReadVid();

        std::unique_ptr<LockManager> lock_mgr_;
        ObjStore * obj_store_;
        boost::concurrent_flat_map<uint64_t, void * > txn_map_;
        std::atomic<uint64_t> read_vid_;
        std::atomic<uint64_t> commit_vid_;
        std::atomic<uint64_t> next_txn_id_;
        std::vector<uint64_t> vid_heap_;
        ThreadState fsync_thread_;
        ThreadState read_vid_thread_;
        bool stop_;
        // variables for limiting # of current txn sessions
        std::vector<std::unique_ptr<TxnSlot>> txn_slots_;
    
};

class LockManager {
    public:
        LockManager();
        ~LockManager();

        void detectDeadlock();
        void lock(const Path & path, TransactionV3 * txn, LockMode lock_mode);
        void unlock(const Path & path, TransactionV3 * txn);

        // can descend lock mode from lock_mode1 to lock_mode2
        static bool descendable(LockMode lock_mode1, LockMode lock_mode2);
        // whether lock mode can be descended (e.g. from IX to X, but not IS to X)
        static constexpr bool lock_descendability_matrix_[6][6] = {
        /**         NL   IS   IX   S   SIX   X */
        /* NL */ {true, false, false, false, false, false},
        /* IS */ {true, true, false, true, false, false},
        /* IX  */{true, true, true, true, true, true},
        /* S  */ {true, true, false, true, false, false},
        /* SIX */{true, true, true, true, true, true},
        /* X  */ {true, true, true, true, true, true}};

    private:

        static constexpr bool lock_compatibility_matrix_[6][6] = {
        /**         NL   IS   IX   S   SIX   X */
        /* NL */ {true, true, true, true, true, true},
        /* IS */ {true, true, true, true, true, false},
        /* IX  */{true, true, true, false, false, false},
        /* S  */ {true, true, false, true, false, false},
        /* SIX */{true, true, false, false, false, false},
        /* X  */ {true, false, false, false, false, false}};

        static constexpr bool lock_strength_matrix_[6][6] = {
        /**         NL   IS   IX   S   SIX   X */
        /* NL */ {true, false, false, false, false, false},
        /* IS */ {true, true, false, false, false, false},
        /* IX  */{true, true, true, false, false, false},
        /* S  */ {true, true, false, true, false, false},
        /* SIX */{true, true, true, true, true, false},
        /* X  */ {true, true, true, true, true, true}};

        static constexpr size_t SHARDS_COUNT = 2048;

        struct LockRequest {
            TransactionV3 * txn_;
            LockMode lock_mode_;
            bool granted_;
            LockRequest(TransactionV3 * txn, LockMode lock_mode) : txn_(txn), lock_mode_(lock_mode), granted_(false) { }
        };

        struct Vertex {
            TransactionV3 * vertex_;
            TransactionV3 * neighbor_;
            int color_;
 
            Vertex(TransactionV3 * vertex, TransactionV3 * neighbor) : vertex_(vertex), neighbor_(neighbor), 
                color_(-1) { }
        };

        std::atomic<bool> stop_;
        // latches for controlling the lock state of a single path. Pooled by hash value of the path
        PaddedMutex latches_[SHARDS_COUNT];
        // queue of txns waiting for locks, 1 or more txns at the front hold the lock
        boost::concurrent_flat_map<Path, std::list<LockRequest>*> lock_table_;
        // txns that are waiting for lock to be granted
        boost::concurrent_flat_set<TransactionV3*> waiting_txns_;
        // background thread that periodically detects deadlock
        std::thread deadlock_detector_;

        void upgradeLock(const Path & path, TransactionV3 * txn, LockMode lock_mode);

        static bool conflicting(LockMode lock_mode1, LockMode lock_mode2);
        static bool stronger(LockMode lock_mode1, LockMode lock_mode2);

        boost::unordered_flat_set<TransactionV3*> detectCycle();

};


#endif