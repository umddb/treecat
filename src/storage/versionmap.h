// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef VERSION_MAP_H
#define VERSION_MAP_H

#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "common/path.h"

class VersionMap {
    public:

        // friend class TransactionManagerV1::Validator;

        class Comparator : public rocksdb::Comparator {
            public:
                int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
                    return Path::compare(a.data(), b.data(), a.size(), b.size());            
                }

                const char* Name() const override {
                    return "VersionMapComparator";
                }

                void FindShortestSeparator(std::string*, const rocksdb::Slice&) const override {  }
                void FindShortSuccessor(std::string*) const override {  }

        };

        struct Header {
            // last time the object was modified or deleted
            uint64_t delta_vid_;
            // last time children were modified
            uint64_t children_vid_; 
            // last time object was deleted
            // uint64_t tombstone_vid_;

            Header() { delta_vid_ = 0;
                       children_vid_ = 0; }
            ~Header() {  }
            Header(const char * data) {
                memcpy(&delta_vid_, data, sizeof(uint64_t));
                delta_vid_ = le64toh(delta_vid_);
                memcpy(&children_vid_, &data[sizeof(uint64_t)], sizeof(uint64_t));
                children_vid_ = le64toh(children_vid_);
                // memcpy(&tombstone_vid_, &data[2*sizeof(uint64_t)], sizeof(uint64_t));
                // tombstone_vid_ = le64toh(tombstone_vid_);
            }
            
        };

        class Iterator {
                
            public:
                Iterator(rocksdb::DB *db, rocksdb::ColumnFamilyHandle * version_map_store, const Path & lower_bound, const Path & upper_bound);
                ~Iterator();
                void reset(const Path & lower_bound, const Path & upper_bound);
                void next();
                rocksdb::Slice key();
                Header value();
                bool valid();
            
            private:
                bool valid_;
                rocksdb::DB *db_;
                rocksdb::ColumnFamilyHandle * version_map_store_;
                rocksdb::ReadOptions read_options_;
                std::unique_ptr<rocksdb::Iterator> version_map_iter_;
                std::string upper_bound_str_;
                std::unique_ptr<rocksdb::Slice> upper_bound_;
        };

        VersionMap(rocksdb::DB * db, rocksdb::ColumnFamilyHandle * version_map_store);
        ~VersionMap();
        bool get(const Path &path, Header * value);
        bool contains(const Path &path);
        bool put(const Path &path, const Header & val);
        bool remove(const Path &path);
        // both lower bound and upper bound have to be alive while the iterator is alive 
        Iterator * newIterator(const Path & lower_bound, const Path & upper_bound);

    private:
        rocksdb::DB *db_;
        rocksdb::ColumnFamilyHandle *version_map_store_;
        rocksdb::ReadOptions read_options_;
        rocksdb::WriteOptions write_options_;

};

#endif