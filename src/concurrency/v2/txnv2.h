// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef TXN_V2_H
#define TXN_V2_H

#include <condition_variable>
#include <unordered_map>
#include <vector>


#include "rocksdb/utilities/write_batch_with_index.h"

#include "concurrency/txn.h"
#include "common/stringhash.h"
#include "common/lfskiplist.h"
#include "common/path.h"
#include "storage/objstore.h"
#include "grpc/grpccatalog.pb.h"

#include "boost/unordered/unordered_flat_map.hpp"
#include "boost/unordered/unordered_flat_set.hpp"
#include <boost/functional/hash.hpp>


class TransactionV2 : public Transaction {

    friend class TransactionManagerV2;

    public:
        enum class ConstraintType {
            EXIST,
            NOT_EXIST,
            // this field is not used for any entry that is actually persisted. 
            NOT_MODIFIED
        };

        struct ConstraintKey {
            uint64_t vid_;
            Path path_;

            ConstraintKey(uint64_t vid, const Path & path) : vid_(vid), path_(path) { }
            
            ConstraintKey(const ConstraintKey & other) : vid_(other.vid_), path_(other.path_){ }

            ConstraintKey& operator=(const ConstraintKey & other) {
                if (this != &other) {
                    vid_ = other.vid_;
                    path_ = other.path_;
                }
                
                return *this;
            }

            ConstraintKey(ConstraintKey && other) : vid_(other.vid_), path_(std::move(other.path_)){ }

            ConstraintKey& operator=(ConstraintKey&& other) {
                if (this != &other) {
                    vid_ = other.vid_;
                    path_ = std::move(other.path_);
                }
                
                return *this;
            }

            bool operator==(const ConstraintKey& other) const {
                return compare(other) == 0;
            }

            // Inequality operator (!=)
            bool operator!=(const ConstraintKey& other) const {
                return compare(other) != 0; 
            }

            // Less than operator (<)
            bool operator<(const ConstraintKey& other) const {
                return compare(other) < 0;
            }

            // Less than or equal to operator (<=)
            bool operator<=(const ConstraintKey& other) const {
                return compare(other) <= 0;
            }

            // Greater than operator (>)
            bool operator>(const ConstraintKey& other) const {
                return compare(other) > 0;
            }

            // Greater than or equal to operator (>=)
            bool operator>=(const ConstraintKey& other) const {
                return compare(other) >= 0;
            }

            // compare by path, then vid. vid is sorted in descending order
            int compare(const ConstraintKey & other) const {
                int r = path_.compare(other.path_);

                if (r == 0) {
                    if (vid_ > other.vid_) {
                        r = -1;
                    }
                    else if (vid_ < other.vid_) {
                        r = 1;
                    }
                }

                return r;

            }
            
        };
        
        struct LiveTxnKey {
            uint64_t vid_;
            TransactionV2 * txn_;

            LiveTxnKey() : vid_(0), txn_(nullptr) { }
            LiveTxnKey(uint64_t vid, TransactionV2 * txn) : vid_(vid), txn_(txn) { }

            bool operator==(const LiveTxnKey& other) const {
                return (vid_ == other.vid_ && txn_ == other.txn_);
            }

            // Inequality operator (!=)
            bool operator!=(const LiveTxnKey& other) const {
                return (vid_ != other.vid_ || txn_ != other.txn_);
            }

            // Less than operator (<)
            bool operator<(const LiveTxnKey& other) const {
                return (vid_ < other.vid_ || (vid_ == other.vid_ && txn_ < other.txn_));
            }

            // Less than or equal to operator (<=)
            bool operator<=(const LiveTxnKey& other) const {
                return (vid_ <= other.vid_ || (vid_ == other.vid_ && txn_ <= other.txn_));
            }

            // Greater than operator (>)
            bool operator>(const LiveTxnKey& other) const {
                return (vid_ > other.vid_ || (vid_ == other.vid_ && txn_ > other.txn_));
            }

            // Greater than or equal to operator (>=)
            bool operator>=(const LiveTxnKey& other) const {
                return (vid_ >= other.vid_ || (vid_ == other.vid_ && txn_ >= other.txn_));
            }

        };

        class ReadSet : public Transaction::ReadSet {
            public:
                ReadSet(unsigned int num_partitions, TransactionV2* txn);
                ~ReadSet();
                void addParentLock(const Path & path);
                // Note: we combine range locks and point lock for read queries with constraint 
                // checks for write operations for both efficiency and due to limitations of current 
                // skiplist implementation
                void addPointLock(const Path & path);
                void addRangeLock(const Path & lower_bound,const Path & upper_bound);
                void addConstraintCheck(const Path & path, ConstraintType constraint_type, 
                    uint64_t vid = std::numeric_limits<uint64_t>::max());
                std::vector<Path> * getParentLocks(unsigned int partition_id);
                std::vector<std::pair<ConstraintKey, ConstraintKey>> * getRangeLocks(unsigned int partition_id);
                std::vector<std::pair<ConstraintKey, ConstraintType>> * getConstraintChecks(unsigned int partition_id);
                
                std::vector<uint64_t> * getConstraintCheckVids(unsigned int partition_id);
                

            private:
                TransactionV2 * txn_;
                std::vector<std::unique_ptr<std::vector<Path>>> parent_locks_;
                // first key has vid of maximum and the second key has vid of 0
                std::vector<std::unique_ptr<std::vector<std::pair<ConstraintKey, ConstraintKey>>>> range_locks_;
                // the vid part of ConstraintKey is set to maximum, so the closest path with the most recent 
                // vid can be fetched via tryGet(). 
                std::vector<std::unique_ptr<std::vector<std::pair<ConstraintKey, ConstraintType>>>> constraint_checks_;
                // This is messy, but for the moment I don't see a way around it. 
                // This is a separate data structure for keeping track of actual constraint check vid 
                std::vector<std::unique_ptr<std::vector<uint64_t>>> constraint_check_vids_;
        };

        class WriteSet {
            public:
                WriteSet(unsigned int num_partitions, ObjStore * obj_store, TransactionV2 * txn);
                ~WriteSet();
                rocksdb::WriteBatchWithIndex * getWriteBatch(unsigned int partition_id);
                boost::unordered_flat_map<Path, std::pair<size_t, size_t>>* getMergeSet(unsigned int partition_id);
                boost::unordered_flat_set<Path>* getVersionMapUpdates(unsigned int partition_id);

                std::vector<LFSkipList<ConstraintKey,ConstraintType>::Node*> * getConstraintUpdates(unsigned int partition_id);
                std::vector<ConstraintKey> * getConstraintUpdatesKeys(unsigned int partition_id);
                
                bool add(const Write& write);
                bool merge(const Write& write);
                bool update(const Write& write);
                bool remove(const Write & write);


                bool getInnerObj(const Path & path, mongo::BSONObj * bson_obj_val);
                bool contains(const Path & path, bool is_leaf);
                const std::string & getBuf();
                
                void addToLeafStore(const Path & path, const Path & primary_path);
                void addToLeafStore(const Path & path, std::string_view obj_val);
                void addToSnapshot(const Path & path, std::string_view obj_val);
                void addToSnapshot(const Path & path, std::string_view obj_val, SnapshotStore::Header header);
                void addToMerge(const Path & path, std::string_view merge_val);
                void addToDelta(std::string_view path_str, const mongo::BSONObj & obj_val);

            private:
                void updateConstraint(const Path & path, ConstraintType constraint_type);
                void updateVersionMap(const Path & path);  

                // RocksDB write batch that can be atomically applied during commit after validation
                std::vector<std::unique_ptr<rocksdb::WriteBatchWithIndex>> write_batch_;
                // merge operations that are evaluated during commit
                std::vector<std::unique_ptr<boost::unordered_flat_map<Path, std::pair<size_t, size_t>>>> merge_set_;
                // updates to the version map
                std::vector<std::unique_ptr<boost::unordered_flat_set<Path>>> version_map_updates_;
                // constraint updates
                std::vector<std::unique_ptr<std::vector<LFSkipList<ConstraintKey,ConstraintType>::Node*>>> constraint_updates_;
                // constraint update keys for garbage collection 
                std::vector<std::unique_ptr<std::vector<ConstraintKey>>> constraint_updates_keys_;
                
                // buffer for holding newly constructed objects, including new delta op as a result of merging 2 delta op 
                std::string buf_;
                ObjStore * obj_store_;
                TransactionV2 * txn_;

        };

        TransactionV2(unsigned int num_partitions, ObjStore * obj_store, uint64_t read_vid);
        ~TransactionV2();
        uint64_t readVid() override;
        Transaction::ReadSet * readSet() override;
        void addQueryRequest(ExecuteQueryRequest * query_request) override;
        WriteSet * writeSet();
        void clear();

        std::unique_ptr<ReadSet> read_set_;
        std::unique_ptr<WriteSet> write_set_;
        uint64_t txn_id_;
        uint64_t old_read_vid_;
        uint64_t new_read_vid_;
        uint64_t commit_vid_;
        std::atomic<unsigned int> votes_;
        std::mutex mtx_;
        std::condition_variable cond_;
        bool processed_;
        std::atomic<bool> abort_;
        // slot the txn belongs to 
        size_t txn_slot_;

    private:
        

};

#endif