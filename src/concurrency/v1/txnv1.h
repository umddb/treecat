// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef TXN_V1_H
#define TXN_V1_H

#include <condition_variable>
#include <unordered_map>
#include <vector>


#include "rocksdb/utilities/write_batch_with_index.h"

#include "concurrency/txn.h"
#include "common/stringhash.h"
#include "common/lfskiplist.h"
#include "storage/objstore.h"
#include "grpc/grpccatalog.pb.h"

#include "boost/unordered/unordered_flat_map.hpp"
#include "boost/unordered/unordered_flat_set.hpp"
#include <boost/functional/hash.hpp>




class TransactionV1 : public Transaction {

    friend class TransactionManagerV1;

    public:
        enum class ConstraintType {
            EXIST,
            NOT_EXIST,
            // this field is not used for any entry that is actually persisted. 
            NOT_MODIFIED
        };

        struct Constraint {
            ConstraintType type_;
            uint64_t vid_;

            Constraint(ConstraintType type) : type_(type), vid_(0) { }
            Constraint(ConstraintType type, uint64_t vid) : type_(type), vid_(vid) { }
        };

        struct LogIdxKey {
            uint64_t vid_;
            Path path_;

            LogIdxKey(uint64_t vid, const Path & path) : vid_(vid), path_(path) { }
            
            LogIdxKey(const LogIdxKey & other) : vid_(other.vid_), path_(other.path_){ }

            LogIdxKey& operator=(const LogIdxKey & other) {
                if (this != &other) {
                    vid_ = other.vid_;
                    path_ = other.path_;
                }
                
                return *this;
            }

            LogIdxKey(LogIdxKey && other) : vid_(other.vid_), path_(std::move(other.path_)){ }

            LogIdxKey& operator=(LogIdxKey&& other) {
                if (this != &other) {
                    vid_ = other.vid_;
                    path_ = std::move(other.path_);
                }
                
                return *this;
            }

            bool operator==(const LogIdxKey& other) const {
                return compare(other) == 0;
            }

            // Inequality operator (!=)
            bool operator!=(const LogIdxKey& other) const {
                return compare(other) != 0; 
            }

            // Less than operator (<)
            bool operator<(const LogIdxKey& other) const {
                return compare(other) < 0;
            }

            // Less than or equal to operator (<=)
            bool operator<=(const LogIdxKey& other) const {
                return compare(other) <= 0;
            }

            // Greater than operator (>)
            bool operator>(const LogIdxKey& other) const {
                return compare(other) > 0;
            }

            // Greater than or equal to operator (>=)
            bool operator>=(const LogIdxKey& other) const {
                return compare(other) >= 0;
            }

            int compare(const LogIdxKey & other) const {
                // first with path depth
                uint32_t this_depth, other_depth;
                memcpy(&this_depth, this->path_.data_, sizeof(uint32_t));
                memcpy(&other_depth, other.path_.data_, sizeof(uint32_t));
                this_depth = le32toh(this_depth);
                other_depth = le32toh(other_depth);

                if (this_depth < other_depth) {
                    return -1;
                }
                else if (this_depth > other_depth) {
                    return 1;
                }

                // compare by parent path second
                size_t this_parent_path_size = std::string_view(this->path_.data_, this->path_.size_).find_last_of('/');
                size_t other_parent_path_size = std::string_view(other.path_.data_, other.path_.size_).find_last_of('/');

                const size_t min_parent_path_size = std::min(this_parent_path_size, other_parent_path_size);
                int r = memcmp(this->path_.data_, other.path_.data_, min_parent_path_size);
                if (r != 0) {
                    return r;
                }                    
                    
                if (this_parent_path_size < other_parent_path_size) {
                    return -1;
                }    
                else if (this_parent_path_size > other_parent_path_size) {
                    return 1;
                }

                //same parent path, so compare by vid
                if (this->vid_ < other.vid_) {
                    return -1;
                    
                }
                else if (this->vid_ > other.vid_) {
                    return 1;
                    
                }

                // Lastly, compare the last oid of each path
                size_t this_oid_size = this->path_.size_ - this_parent_path_size - 1;
                size_t other_oid_size = other.path_.size_ - other_parent_path_size - 1;
                const size_t min_oid_size = std::min(this_oid_size, other_oid_size);
                r = memcmp(&this->path_.data_[this_parent_path_size + 1], 
                    &other.path_.data_[other_parent_path_size + 1], min_oid_size);
                
                if (r == 0 ) {
                    if (this_oid_size < other_oid_size) {
                        r = -1;
                    }    
                    else if (this_oid_size > other_oid_size) {
                        r = 1;
                    }
                }
                
                return r;

            }
            
            
        };

        struct LogIdxValue {
            std::string * buf_; 
            size_t before_;
            size_t after_;
            LogIdxValue() : buf_(nullptr), before_(0), after_(0) { }
            LogIdxValue(std::string * buf, size_t before, size_t after) : buf_(buf), before_(before), after_(after) { }
            ~LogIdxValue() { }
        };

        struct LiveTxnKey {
            uint64_t vid_;
            TransactionV1 * txn_;

            LiveTxnKey() : vid_(0), txn_(nullptr) { }
            LiveTxnKey(uint64_t vid, TransactionV1 * txn) : vid_(vid), txn_(txn) { }

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
                ReadSet(unsigned int num_partitions);
                ~ReadSet();
                void addScan(const Path & path, const Predicate& predicate);
                void addConstraintCheck(const Path & path, Constraint constraint);
                std::vector<std::pair<Path, const Predicate&>> * getScanSet(unsigned int partition_id);
                std::vector<std::pair<Path, Constraint>> * getConstraintChecks(unsigned int partition_id);

            private:
                std::vector<std::unique_ptr<std::vector<std::pair<Path, const Predicate&>>>> scan_set_;
                //TODO might change this to hash map
                std::vector<std::unique_ptr<std::vector<std::pair<Path, Constraint>>>> constraint_checks_;
        };

        class WriteSet {
            public:
                WriteSet(unsigned int num_partitions, ObjStore * obj_store, TransactionV1 * txn);
                ~WriteSet();
                rocksdb::WriteBatchWithIndex * getWriteBatch(unsigned int partition_id);
                boost::unordered_flat_map<Path, std::pair<size_t, size_t>>* getMergeSet(unsigned int partition_id);
                boost::unordered_flat_set<Path>* getVersionMapUpdates(unsigned int partition_id);
                boost::unordered_flat_map<Path, LFSkipList<LogIdxKey,LogIdxValue>::Node*>* getLogIdxUpdates(unsigned int partition_id);
                std::vector<std::pair<Path, Constraint>> * getConstraintUpdates(unsigned int partition_id);
                
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
                // public because of clone operation...
                void updateConstraint(const Path & path, Constraint constraint);
                void updateVersionMap(const Path & path);
                void updateLogIdx(const Path & path, const mongo::BSONObj & before_image, const mongo::BSONObj & after_image);
                void updateLogIdx(const Path & path, const mongo::BSONObj & before_image);   

            private:
                static const Predicate & getWildCard();
                

                // RocksDB write batch that can be atomically applied during commit after validation
                std::vector<std::unique_ptr<rocksdb::WriteBatchWithIndex>> write_batch_;
                // merge operations that are evaluated during commit
                std::vector<std::unique_ptr<boost::unordered_flat_map<Path, std::pair<size_t, size_t>>>> merge_set_;
                // updates to the version map
                std::vector<std::unique_ptr<boost::unordered_flat_set<Path>>> version_map_updates_;
                // log index entries to be inserted
                std::vector<std::unique_ptr<boost::unordered_flat_map<Path, LFSkipList<LogIdxKey,LogIdxValue>::Node*>>> log_idx_updates_;
                // constraint updates
                std::vector<std::unique_ptr<std::vector<std::pair<Path, Constraint>>>> constraint_updates_;
                // buffer for holding newly constructed objects, including 
                // 1) new delta op as a result of merging 2 delta op 
                // 2) log entries ((before, after) image pair or delta)
                // TODO figure out a way to move log entries to WriteBatch

                
                std::string buf_;
                ObjStore * obj_store_;
                TransactionV1 * txn_;

        };

        TransactionV1(unsigned int num_partitions, ObjStore * obj_store, uint64_t read_vid);
        ~TransactionV1();
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
        std::vector<ExecuteQueryRequest*> query_requests_;
        // slot the txn belongs to 
        size_t txn_slot_;

    private:
        

};

inline std::size_t hash_value(const TransactionV1::LogIdxKey  &log_idx_key)
{
    size_t hash_val = std::hash<uint64_t>()(log_idx_key.vid_);
    boost::hash_combine(hash_val, log_idx_key.path_);
    return hash_val;
}

#endif