// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef TXN_V3_H
#define TXN_V3_H

#include <condition_variable>
#include <unordered_map>
#include <vector>

#include "boost/unordered/unordered_flat_map.hpp"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "grpc/grpccatalog.grpc.pb.h"

#include "concurrency/txn.h"
#include "storage/objstore.h"

class TransactionManagerV3;
class LockManager;

class TransactionV3 : public Transaction {
    
    friend class TransactionManagerV3;
    friend class LockManager;

    public:
        
        class ReadSet : public Transaction::ReadSet {

            public:
                ReadSet(LockManager * lock_mgr, TransactionV3 * txn);
                ~ReadSet();
                bool add(const Path & path, LockMode lock_mode);
                boost::unordered_flat_map<Path, LockMode> * ownedLocks();

            private:
                LockManager * lock_mgr_;
                TransactionV3 * txn_;
        };

        class WriteSet {
            public:
                WriteSet(LockManager * lock_mgr, TransactionV3 * txn, ObjStore * obj_store);
                ~WriteSet();
                rocksdb::WriteBatchWithIndex * getWriteBatch();
                boost::unordered_flat_map<Path, std::pair<size_t, size_t>>* getMergeSet();
                const std::string & getBuf();
                boost::unordered_flat_map<Path, LockMode> * ownedLocks();
                
                bool add(const Write& write);
                bool merge(const Write& write);
                bool update(const Write& write);
                bool remove(const Write & write);

                bool getInnerObj(const Path & path, mongo::BSONObj * bson_obj_val);
                bool contains(const Path & path, bool is_leaf);
                
                void addToLeafStore(const Path & path, const Path & primary_path);
                void addToLeafStore(const Path & path, std::string_view obj_val);
                void addToSnapshot(const Path & path, std::string_view obj_val);
                void addToSnapshot(const Path & path, std::string_view obj_val, SnapshotStore::Header header);
                void addToMerge(const Path & path, std::string_view merge_val);
                void addToDelta(std::string_view path_str, const mongo::BSONObj & obj_val);
                bool lockFullPath(Path & path);
                bool lockSinglePath(const Path & path, LockMode lock_mode);

            private:
                // RocksDB write batch that can be atomically applied during commit
                std::unique_ptr<rocksdb::WriteBatchWithIndex> write_batch_;
                // merge operations that are evaluated during commit
                std::unique_ptr<boost::unordered_flat_map<Path, std::pair<size_t, size_t>>> merge_set_;
                
                std::string buf_;
                LockManager * lock_mgr_;
                TransactionV3 * txn_;
                ObjStore * obj_store_;
                
        };

        TransactionV3(TransactionManagerV3 * txn_mgr, uint64_t txn_id, ObjStore * obj_store);
        ~TransactionV3();
        uint64_t readVid() override;
        Transaction::ReadSet * readSet() override;
        void addQueryRequest(ExecuteQueryRequest * query_request) override;
        TransactionV3::WriteSet * writeSet();
        void clear();
        bool abort();

        TransactionManagerV3 * txn_mgr_;
        std::unique_ptr<ReadSet> read_set_;
        std::unique_ptr<WriteSet> write_set_;
        std::unique_ptr<boost::unordered_flat_map<Path, LockMode>> owned_locks_;
        uint64_t txn_id_;
        uint64_t read_vid_;
        uint64_t commit_vid_;
        std::mutex mtx_;
        std::condition_variable cond_;
        bool processed_;
        std::atomic<bool> abort_;
        // slot the txn belongs to 
        size_t txn_slot_;
        //the txn this txn is currently waiting for
        std::atomic<TransactionV3*> waiting_;

    private:
        
};

#endif