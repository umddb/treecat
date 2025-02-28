// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef OBJ_STORE_H
#define OBJ_STORE_H

#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include "common/config.h"

#include "storage/innerobjstore.h"
#include "storage/leafobjstore.h"
#include "storage/versionmap.h"
#include "storage/snapshotmap.h"

class ObjStore {
    friend class TransactionV1;
    friend class TransactionManagerV1;
    friend class TransactionV2;
    friend class TransactionManagerV2;
    friend class TransactionV3;
    friend class TransactionManagerV3;

    public:
        ObjStore(const std::string & db_path, const Config * config, bool create);
        ~ObjStore();
        LeafObjStore * leafObjStore();
        InnerObjStore * innerObjStore();
        VersionMap * versionMap();
        SnapshotMap * snapshotMap();

    private:    
        //rocksdb related data structures
        rocksdb::DB * kv_store_;
        rocksdb::Options kv_store_options_;
        rocksdb::ColumnFamilyOptions delta_store_options_;
        rocksdb::ColumnFamilyOptions snapshot_store_options_;
        rocksdb::ColumnFamilyOptions leaf_store_options_;
        rocksdb::ColumnFamilyOptions version_map_options_;
        rocksdb::ColumnFamilyOptions snapshot_map_options_;   
        std::unique_ptr<DeltaStore::Comparator> delta_store_comparator_;
        std::unique_ptr<SnapshotStore::Comparator> snapshot_store_comparator_;
        std::unique_ptr<LeafObjStore::Comparator> leaf_store_comparator_;
        std::unique_ptr<VersionMap::Comparator> version_map_comparator_;
        rocksdb::ColumnFamilyHandle * delta_store_;
        rocksdb::ColumnFamilyHandle * snapshot_store_;
        rocksdb::ColumnFamilyHandle * leaf_store_;
        rocksdb::ColumnFamilyHandle * version_map_store_;
        rocksdb::ColumnFamilyHandle * snapshot_map_store_;
        
        //storage subsystems
        LeafObjStore * leaf_obj_store_;
        InnerObjStore * inner_obj_store_;
        VersionMap * version_map_;
        SnapshotMap * snapshot_map_;

};



#endif