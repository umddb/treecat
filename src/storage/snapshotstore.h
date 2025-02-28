// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef SNAPSHOT_STORE_H
#define SNAPSHOT_STORE_H

#include <endian.h>

#include "rocksdb/slice.h"
#include "rocksdb/comparator.h"

#include "common/path.h"

namespace SnapshotStore {
    struct Header {
        // last time the object was modified 
        uint64_t delta_vid_;
        // most recent/current version
        uint64_t cur_vid_; 
        // last time object was deleted
        // uint64_t tombstone_vid_;

        Header() { delta_vid_ = 0; 
                   cur_vid_ = 0; }
        ~Header() {  }
        Header(const char * data) {
            memcpy(&delta_vid_, data, sizeof(uint64_t));
            delta_vid_ = le64toh(delta_vid_);
            memcpy(&cur_vid_, &data[sizeof(uint64_t)], sizeof(uint64_t));
            cur_vid_ = le64toh(cur_vid_);
            // memcpy(&tombstone_vid_, &data[2*sizeof(uint64_t)], sizeof(uint64_t));
            // tombstone_vid_ = le64toh(tombstone_vid_);
        }
    };

    class Comparator : public rocksdb::Comparator {
        public:
            int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
                return Path::compare(a.data(), b.data(), a.size(), b.size());            
            }

            const char* Name() const override {
                return "SnapshotStoreComparator";
            }

            void FindShortestSeparator(std::string*, const rocksdb::Slice&) const override {  }
            void FindShortSuccessor(std::string*) const override {  }

    };

    bool visible(uint64_t vid, const rocksdb::Slice & snapshot);
    bool removed(uint64_t vid, const rocksdb::Slice & snapshot);
    bool hasDelta(const rocksdb::Slice & snapshot);
    
}

#endif //SNAPSHOT_STORE_H