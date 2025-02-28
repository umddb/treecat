// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef TXN_MGR_V1_H
#define TXN_MGR_V1_H

#include <atomic>
#include <vector>
#include <memory>
#include <queue>

#include "boost/lockfree/queue.hpp"
#include "boost/asio/thread_pool.hpp"
#include "boost/unordered/concurrent_flat_map.hpp"

#include "rocksdb/file_system.h"

#include "common/config.h"
#include "common/objkind.h"
#include "common/concurrentqueue.h"
#include "common/lfskiplist.h"

#include "storage/versionmap.h"
#include "storage/objstore.h"
#include "concurrency/v1/txnv1.h"
#include "concurrency/txnmgr.h"
#include "grpc/grpccatalog.pb.h"
#include "exec/evalexpr.h"



class TransactionManagerV1 : public TransactionManager {

    public:
        class Writer;

        struct alignas(64) ThreadState {
            std::mutex mtx_;
            std::condition_variable cond_;
            TransactionV1* local_curr_txn_;
            bool ready_;
            
            ThreadState() : local_curr_txn_(nullptr), ready_(false) { }
        };

        class Validator {
            public:
                Validator(unsigned int num_partitions, TransactionManagerV1 * txn_mgr, Writer * writer, ObjStore * obj_store);
                ~Validator();
                void validate(TransactionV1 * txn);
                void run();
                void stop();
                void registerThread();
                void collectInMemoryGarbage(TransactionV1 * txn);

            private:
                std::vector<std::unique_ptr<ThreadState>> thread_states_;
                std::unique_ptr<boost::asio::thread_pool> thread_pool_;
                unsigned int num_partitions_;
                TransactionManagerV1 * txn_mgr_;
                Writer * writer_;
                ObjStore * obj_store_;

                // global current txn all threads are currently working on 
                std::atomic<TransactionV1*> global_curr_txn_;
                ConcurrentQueue<TransactionV1*> validation_queue_;
                std::atomic<uint64_t> validation_queue_size_;

                // partitioned data structures for validation
                std::vector<std::unique_ptr<boost::concurrent_flat_map<Path, uint64_t>>> version_map_;
                std::vector<std::unique_ptr<LFSkipList<TransactionV1::LogIdxKey,TransactionV1::LogIdxValue>>> log_idx_;
                std::vector<std::unique_ptr<boost::concurrent_flat_map<Path,TransactionV1::Constraint>>> constraint_map_;
                // cache for storing before and after image of merge operation
                std::vector<std::unique_ptr<boost::concurrent_flat_map<TransactionV1::LogIdxKey,std::pair<mongo::BSONObj, mongo::BSONObj>>>> merge_cache_;

                bool stop_;
                std::mutex mtx_;
                std::condition_variable cond_;

                void handleTransactions(unsigned int rank);
                void handleTransaction(unsigned int rank, TransactionV1 * txn);
                bool precisionLock(mongo::BSONObj & before_image, mongo::BSONObj & after_image, 
                    const Predicate & predicate, EvalExpr::EvalInfo * eval_info);
        };

        class Writer {
            public:
                Writer(unsigned int num_partitions, TransactionManagerV1 * txn_mgr, ObjStore * obj_store);
                ~Writer();
                void write(TransactionV1 * txn);
                void run();
                void stop();

            private:
                std::atomic<TransactionV1*> global_curr_txn_;
                ConcurrentQueue<TransactionV1*> write_queue_;
                std::atomic<uint64_t> write_queue_size_;
                ConcurrentQueue<TransactionV1*> fsync_queue_;
                std::atomic<uint64_t> fsync_queue_size_;
                ConcurrentQueue<TransactionV1*> in_memory_gc_queue_;
                std::atomic<uint64_t> in_memory_gc_queue_size_;


                std::vector<std::unique_ptr<ThreadState>> thread_states_;
                std::unique_ptr<ThreadState> fsync_thread_;
                std::unique_ptr<ThreadState> in_memory_gc_thread_;
                std::unique_ptr<boost::asio::thread_pool> thread_pool_;
                unsigned int num_partitions_;
                TransactionManagerV1 * txn_mgr_;
                ObjStore * obj_store_;
                bool stop_;
                std::mutex mtx_;
                std::condition_variable cond_;
      
                void handleTransactions(unsigned int rank);
                void fsyncTransactions();
                void handleInMemoryGCRequests();
                void handleTransaction(unsigned int rank, TransactionV1 * txn);
                void materializeMerge(rocksdb::WriteBatchWithIndex * write_batch, const Path & path, 
                        rocksdb::Slice merge_val, uint64_t vid);
                void fsync(TransactionV1 * txn);
                void requestInMemoryGC(TransactionV1 * txn);
        };

        TransactionManagerV1(Config* config, ObjStore * obj_store, const std::string & db_path,
            unsigned int thread_pool_size);
        ~TransactionManagerV1();
        uint64_t getReadVid() override;
        Transaction * getTransaction(uint64_t txn_id) override;
        Transaction * startTransaction() override;
        void startTransaction(ClientTask * task) override;
        void removeTransaction(uint64_t txn_id) override;
        Transaction * popTransaction(uint64_t txn_id) override;
        void commit(CommitRequest * commit_request, CommitResponse * commit_response) override;
        bool commit(Transaction * txn) override;

        void updateReadVid(uint64_t new_vid);
        
    private:
        struct alignas(64) TxnSlot {
            std::queue<ClientTask*> txn_queue_;
            std::mutex mtx_;
            bool occupied_; 

            TxnSlot() : occupied_(false) {  }
        };

        void preprocess(CommitRequest * commit_request, TransactionV1 * txn);

        std::unique_ptr<Validator> validator_;
        std::unique_ptr<Writer> writer_;
        ObjStore * obj_store_;
        boost::concurrent_flat_map<uint64_t, void * > txn_map_;
        

        std::atomic<uint64_t> read_vid_;
        std::atomic<uint64_t> commit_vid_;
        std::atomic<uint64_t> next_txn_id_;
        // watermark for keeping track of vid up to which garbage has been collected 
        std::atomic<uint64_t> watermark_;
        unsigned int num_partitions_;
        bool stop_;

        // data structures for garbage collection
        ConcurrentQueue<TransactionV1*> committed_txns_;
        std::unique_ptr<LFSkipList<TransactionV1::LiveTxnKey, int>> live_txns_;
        // variables for limiting # of current txn sessions
        std::vector<std::unique_ptr<TxnSlot>> txn_slots_;
        
};


#endif