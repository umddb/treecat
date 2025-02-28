// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef TXN_MGR_V2_H
#define TXN_MGR_V2_H

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
#include "concurrency/v2/txnv2.h"
#include "concurrency/txnmgr.h"
#include "grpc/grpccatalog.pb.h"
#include "exec/evalexpr.h"



class TransactionManagerV2 : public TransactionManager {

    public:
        class Writer;

        struct alignas(64) ThreadState {
            std::mutex mtx_;
            std::condition_variable cond_;
            TransactionV2* local_curr_txn_;
            bool ready_;
            
            ThreadState() : local_curr_txn_(nullptr), ready_(false) { }
        };

        class Validator {
            public:
                Validator(unsigned int num_partitions, TransactionManagerV2 * txn_mgr, Writer * writer, ObjStore * obj_store);
                ~Validator();
                void validate(TransactionV2 * txn);
                void run();
                void stop();
                void registerThread();
                void collectInMemoryGarbage(TransactionV2 * txn);

            private:
                std::vector<std::unique_ptr<ThreadState>> thread_states_;
                std::unique_ptr<boost::asio::thread_pool> thread_pool_;
                unsigned int num_partitions_;
                TransactionManagerV2 * txn_mgr_;
                Writer * writer_;
                ObjStore * obj_store_;

                // global current txn all threads are currently working on 
                std::atomic<TransactionV2*> global_curr_txn_;
                ConcurrentQueue<TransactionV2*> validation_queue_;
                std::atomic<uint64_t> validation_queue_size_;

                // partitioned data structures for validation
                std::vector<std::unique_ptr<boost::concurrent_flat_map<Path, uint64_t>>> version_map_;
                std::unique_ptr<LFSkipList<TransactionV2::ConstraintKey,TransactionV2::ConstraintType>> constraint_map_;

                bool stop_;
                std::mutex mtx_;
                std::condition_variable cond_;

                void handleTransactions(unsigned int rank);
                void handleTransaction(unsigned int rank, TransactionV2 * txn);

        };

        class Writer {
            public:
                Writer(unsigned int num_partitions, TransactionManagerV2 * txn_mgr, ObjStore * obj_store);
                ~Writer();
                void write(TransactionV2 * txn);
                void run();
                void stop();

            private:
                std::atomic<TransactionV2*> global_curr_txn_;
                ConcurrentQueue<TransactionV2*> write_queue_;
                std::atomic<uint64_t> write_queue_size_;
                ConcurrentQueue<TransactionV2*> fsync_queue_;
                std::atomic<uint64_t> fsync_queue_size_;
                ConcurrentQueue<TransactionV2*> in_memory_gc_queue_;
                std::atomic<uint64_t> in_memory_gc_queue_size_;


                std::vector<std::unique_ptr<ThreadState>> thread_states_;
                std::unique_ptr<ThreadState> fsync_thread_;
                std::unique_ptr<ThreadState> in_memory_gc_thread_;
                std::unique_ptr<boost::asio::thread_pool> thread_pool_;
                unsigned int num_partitions_;
                TransactionManagerV2 * txn_mgr_;
                ObjStore * obj_store_;
                bool stop_;
                std::mutex mtx_;
                std::condition_variable cond_;
      
                void handleTransactions(unsigned int rank);
                void fsyncTransactions();
                void handleInMemoryGCRequests();
                void handleTransaction(unsigned int rank, TransactionV2 * txn);
                void materializeMerge(rocksdb::WriteBatchWithIndex * write_batch, const Path & path, 
                        rocksdb::Slice merge_val, uint64_t vid);
                void fsync(TransactionV2 * txn);
                void requestInMemoryGC(TransactionV2 * txn);
        };

        TransactionManagerV2(Config* config, ObjStore * obj_store,
            const std::string & db_path, unsigned int thread_pool_size);
        ~TransactionManagerV2();
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


        void preprocess(CommitRequest * commit_request, TransactionV2 * txn);

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
        ConcurrentQueue<TransactionV2*> committed_txns_;
        std::unique_ptr<LFSkipList<TransactionV2::LiveTxnKey, int>> live_txns_;
        // variables for limiting # of current txn sessions
        std::vector<std::unique_ptr<TxnSlot>> txn_slots_;
        
};


#endif