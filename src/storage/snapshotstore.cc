// Author: Keonwoo Oh (koh3@umd.edu)

#include <endian.h>

#include "common/bson/bsonobj.h"
#include "storage/snapshotstore.h"

namespace SnapshotStore {

    bool visible(uint64_t vid, const rocksdb::Slice & snapshot) {
        Header header(snapshot.data());
        return (header.cur_vid_ <= vid) && (snapshot.size() > sizeof(Header) + mongo::BSONObj::kMinBSONLength); 
    }

    bool removed(uint64_t vid, const rocksdb::Slice & snapshot) {
        Header header(snapshot.data());
        return (header.cur_vid_ <= vid) && (snapshot.size() <= sizeof(Header) + mongo::BSONObj::kMinBSONLength);

    }

    // If delta_vid_ is 0, there is no preceding delta
    bool hasDelta(const rocksdb::Slice & snapshot) {
        uint64_t delta_vid;
        memcpy(&delta_vid, snapshot.data(), sizeof(uint64_t));
        
        return (delta_vid != 0);
    }
}