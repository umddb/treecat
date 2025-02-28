// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef LEAF_OBJ_STORE_H
#define LEAF_OBJ_STORE_H

#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"

#include "common/bson/bsonobj.h"
#include "common/bson/document.h"
#include "common/path.h"

class LeafObjStore {
    public:
        // lower bound is inclusive, upper_bound is exclusive
        struct ReadOptions {
            std::string lower_bound_;
            std::string upper_bound_;
        };

        struct Header {
            // vid at which the object was created 
            uint64_t create_vid_;
            // the vid at which the object was removed
            uint64_t tombstone_vid_; 
            // whether this is the primary path default value is true
            char is_primary_;

            Header() { create_vid_ = 0;
                       tombstone_vid_ = 0;
                       is_primary_ = 1; }
            ~Header() {  }
            Header(const char * data) {
                memcpy(&create_vid_, data, sizeof(uint64_t));
                create_vid_ = le64toh(create_vid_);
                memcpy(&tombstone_vid_, &data[sizeof(uint64_t)], sizeof(uint64_t));
                tombstone_vid_ = le64toh(tombstone_vid_);
                memcpy(&is_primary_, &data[2*sizeof(uint64_t)], sizeof(char));
            }
        };

        class Iterator {
                
                public:
                    Iterator(const Path &path, uint64_t vid, rocksdb::DB *db, rocksdb::ColumnFamilyHandle * leaf_store, const ReadOptions & read_options);
                    ~Iterator();
                    void reset(const Path &path, uint64_t vid, const ReadOptions & read_options);
                    void next();
                    rocksdb::Slice key();
                    // The BSON object will contain 2 BSON objects with empty field names: 
                    // 1) list of <path:last_version>, which are backward pointers to parents
                    // 2) the actual leaf object attributes
                    mongo::BSONObj* value();
                    uint64_t vid();
                    bool valid();
                
                private:
                    uint64_t vid_;
                    bool valid_;
                    std::string path_str_;
                    rocksdb::Slice path_;
                    rocksdb::DB *db_;
                    rocksdb::ColumnFamilyHandle *leaf_store_;
                    rocksdb::ReadOptions read_options_;
                    std::unique_ptr<rocksdb::Iterator> leaf_iter_;
                    rocksdb::PinnableSlice real_value_;
                    mongo::BSONObj value_;
                    std::string upper_bound_str_;
                    std::unique_ptr<rocksdb::Slice> upper_bound_;

                    bool prefixMatch();

        };

        class Comparator : public rocksdb::Comparator {
            public:
                int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
                    return Path::compare(a.data(), b.data(), a.size(), b.size());            
                }

                const char* Name() const override {
                    return "LeafStoreComparator";
                }

                void FindShortestSeparator(std::string*, const rocksdb::Slice&) const override {  }
                void FindShortSuccessor(std::string*) const override {  }

        };

        // merge operator for merging tombstone_vid
        class MergeOperator : public rocksdb::MergeOperator {          // not associative
            public:
                virtual bool FullMerge(const rocksdb::Slice& key,
                                    const rocksdb::Slice* existing_value,
                                    const std::deque<std::string>& operand_list,
                                    std::string* new_value,
                                    rocksdb::Logger* logger) const override {
                    
                    if (existing_value->size() < sizeof(Header)) {
                        return false;
                    }
                    
                    // the initial tombstone_vid
                    uint64_t final_vid;
                    memcpy(&final_vid, &existing_value->data()[sizeof(uint64_t)], sizeof(uint64_t));
                    final_vid = le64toh(final_vid);
                    
                    //iterate over operands to compute the max vid
                    uint64_t temp_vid;
                    for (const auto & operand : operand_list) {
                        if (operand.size() >= sizeof(uint64_t)) {
                            memcpy(&temp_vid, operand.data(), sizeof(uint64_t));
                            temp_vid = le64toh(temp_vid);
                            final_vid = final_vid >= temp_vid ? final_vid : temp_vid;
                        }
                    }

                    // copy the final tombstone_vid into the new value
                    final_vid = htole64(final_vid);
                    new_value->clear();
                    new_value->append(existing_value->data(), existing_value->size());
                    memcpy(&new_value->data()[sizeof(uint64_t)], &final_vid, sizeof(uint64_t));
                    return true;
            
                }


                // Partial-merge two operands
                virtual bool PartialMerge(const rocksdb::Slice& key,
                                        const rocksdb::Slice& left_operand,
                                        const rocksdb::Slice& right_operand,
                                        std::string* new_value,
                                        rocksdb::Logger* logger) const override {
                    
                    uint64_t left_vid;
                    uint64_t right_vid;
                    uint64_t final_vid;

                    if (left_operand.size() < sizeof(uint64_t) || right_operand.size() < sizeof(uint64_t)) {
                        return false;
                    }

                    memcpy(&left_vid, left_operand.data(), sizeof(uint64_t));
                    memcpy(&right_vid, right_operand.data(), sizeof(uint64_t));
                    left_vid = le64toh(left_vid);
                    right_vid = le64toh(right_vid);
                    final_vid = left_vid >= right_vid ? left_vid : right_vid;
                    final_vid = htole64(final_vid);
                    new_value->clear();
                    new_value->append(reinterpret_cast<char*>(&final_vid), sizeof(uint64_t));
                    return true;
                }

                virtual const char* Name() const override {
                    return "LeafStoreMergeOperator";
                }
        };   

        LeafObjStore(rocksdb::DB * db, rocksdb::ColumnFamilyHandle * leaf_store);
        ~LeafObjStore();
        bool get(const Path &path, uint64_t vid, mongo::BSONObj * value);
        Path getPrimaryPath(const Path &path, uint64_t vid);
        bool put(const Path &path);
        bool contains(const Path &path, uint64_t vid);
        bool put(const Path &path, const mongo::BSONObj & val, uint64_t vid);
        bool remove(const Path &path, uint64_t vid, bool is_delta);
        //TODO pass in predicate
        Iterator * newIterator(const Path &path, uint64_t vid, const ReadOptions & read_options);

    private:
        rocksdb::DB *db_;
        rocksdb::ColumnFamilyHandle *leaf_store_;
        rocksdb::ReadOptions read_options_;
        rocksdb::WriteOptions write_options_;

        static bool visible(uint64_t vid, Header header);

};

#endif