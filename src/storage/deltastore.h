// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef DELTA_STORE_H
#define DELTA_STORE_H

#include <string>
#include <endian.h>

#include "rocksdb/slice.h"
#include "rocksdb/comparator.h"

#include "common/path.h"
#include "common/bson/document.h"

namespace mmb = mongo::mutablebson;

namespace DeltaStore {

/*
    // Types of Delta Operations
    enum DeltaOp {
        // add the given field as a child 
        AddField = 48,
        // add the list of fields in the object as children
        AddFields = 49,
        // remove the field
        Remove = 50,
        // replace the value
        Replace = 51,
        // increment the value by delta amount
        Increment = 52,
        // decrement the value by delta amount
        Decrement = 53
    };
*/

    struct Header {
        // vid of the delta before
        uint64_t start_vid_;
        // vid of this delta
        uint64_t end_vid_; 

        Header() {  start_vid_ = 0;
                    end_vid_ = 0; }
        Header(uint64_t start_vid, uint64_t end_vid) : start_vid_(start_vid), end_vid_(end_vid) { } 
        ~Header() {  }
        Header(const char * data) {
            memcpy(&start_vid_, data, sizeof(uint64_t));
            start_vid_ = le64toh(start_vid_);
            memcpy(&end_vid_, &data[sizeof(uint64_t)], sizeof(uint64_t));
            end_vid_ = le64toh(end_vid_);
        }
    };

/*
    class Snapshot {
        public:
            Snapshot();
            Snapshot(const rocksdb::Slice & snapshot);
            ~Snapshot();

            // disable copy constructor
            Snapshot(const Snapshot&) = delete;
            
            // disable assign constructor
            Snapshot& operator=(const Snapshot&) = delete;

            void reset(const rocksdb::Slice & snapshot);
            //returns true if rollback was successful
            bool rollBack(const rocksdb::Slice & delta_key, const rocksdb::Slice & delta_value);

            uint64_t lastVid();

            bool removed();

            const std::string& value();
        private:
            uint64_t last_vid_;
            bool removed_;
            rocksdb::Slice obj_data_;

    
            mmb::Document doc_;

            void rollBackImpl(mmb::Element snapshot_elm, mmb::Element delta_elm);
            void deltaAddField(mmb::Element elm, mmb::Element delta_val);
            void deltaAddFields(mmb::Element elm, mmb::Element delta_val);
            void deltaReplace(mmb::Element elm, mmb::Element delta_val);
            void deltaIncrement(mmb::Element elm, mmb::Element delta_val);
            void deltaDecrement(mmb::Element elm, mmb::Element delta_val);

        

    };
*/
    class Comparator : public rocksdb::Comparator {
        public:
            virtual int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
                int r;
                r = Path::compare(&a.data()[sizeof(DeltaStore::Header)], &b.data()[sizeof(DeltaStore::Header)] , 
                                  a.size() - sizeof(DeltaStore::Header), b.size() - sizeof(DeltaStore::Header));

                if (r == 0) {
                    uint64_t a_timestamp, b_timestamp;
                    memcpy(&a_timestamp, a.data(), sizeof(uint64_t));
                    memcpy(&b_timestamp, b.data(), sizeof(uint64_t));
                    a_timestamp = le64toh(a_timestamp);
                    b_timestamp = le64toh(b_timestamp);
                    // most recent first
                    if (a_timestamp > b_timestamp) {
                        r = -1;
                    }
                    else if (a_timestamp < b_timestamp) {
                        r = 1;
                    }

                }

                return r;
            
            }

            virtual const char* Name() const override {
                return "DeltaStoreComparator";
            }

            virtual void FindShortestSeparator(std::string*, const rocksdb::Slice&) const override {  }
            virtual void FindShortSuccessor(std::string*) const override {  }
    
    };

    rocksdb::Slice lastDeltaKey(const rocksdb::Slice & path, const rocksdb::Slice & snapshot);
    bool validPath(const rocksdb::Slice & delta_key, const rocksdb::Slice & path);
    // uint64_t deltaVid(const rocksdb::Slice & delta_key);

}

#endif //DELTA_STORE_H