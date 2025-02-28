// Author: Keonwoo Oh (koh3@umd.edu)

#include "storage/objstore.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"

ObjStore::ObjStore(const std::string & db_path, const Config * config, bool create) {
    kv_store_options_.IncreaseParallelism();
    kv_store_options_.OptimizeLevelStyleCompaction();
    kv_store_options_.create_if_missing = true;
    kv_store_options_.max_background_jobs = 6;

    //set table options
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    // table_options.index_type = rocksdb::BlockBasedTableOptions::kBinarySearchWithFirstKey;
    table_options.block_size = 16 * 1024;
    std::shared_ptr<rocksdb::TableFactory> table_factory(rocksdb::NewBlockBasedTableFactory(table_options));
    delta_store_options_.table_factory = table_factory;
    snapshot_store_options_.table_factory = table_factory;
    leaf_store_options_.table_factory = table_factory;
    version_map_options_.table_factory = table_factory;
    snapshot_map_options_.table_factory = table_factory;

    //set comparators
    delta_store_comparator_.reset(new DeltaStore::Comparator());
    snapshot_store_comparator_.reset(new SnapshotStore::Comparator());
    leaf_store_comparator_.reset(new LeafObjStore::Comparator());
    version_map_comparator_.reset(new VersionMap::Comparator());
    delta_store_options_.comparator = delta_store_comparator_.get();
    snapshot_store_options_.comparator = snapshot_store_comparator_.get();
    leaf_store_options_.comparator = leaf_store_comparator_.get();
    version_map_options_.comparator = version_map_comparator_.get();
    
    //set merge operator for leaf store
    leaf_store_options_.merge_operator.reset(new LeafObjStore::MergeOperator());

    //set up compression options
    std::vector<rocksdb::CompressionType> compression_per_level = { rocksdb::kNoCompression, rocksdb::kNoCompression, 
                rocksdb::kLZ4Compression, rocksdb::kLZ4Compression, rocksdb::kLZ4Compression, rocksdb::kLZ4Compression };
    delta_store_options_.compression_per_level = compression_per_level;
    snapshot_store_options_.compression_per_level = compression_per_level;
    leaf_store_options_.compression_per_level = compression_per_level;
    version_map_options_.compression_per_level = compression_per_level;
    snapshot_map_options_.compression_per_level = compression_per_level;

    if (create) {
        rocksdb::Status status = rocksdb::DB::Open(kv_store_options_, db_path, &kv_store_);
        assert(status.ok());
        status = kv_store_->CreateColumnFamily(delta_store_options_, "delta_store", &delta_store_);
        assert(status.ok());
        status = kv_store_->CreateColumnFamily(snapshot_store_options_, "snapshot_store", &snapshot_store_);
        assert(status.ok());
        status = kv_store_->CreateColumnFamily(leaf_store_options_, "leaf_store", &leaf_store_);
        assert(status.ok());
        status = kv_store_->CreateColumnFamily(version_map_options_, "version_map", &version_map_store_);
        assert(status.ok());
        status = kv_store_->CreateColumnFamily(snapshot_map_options_, "snapshot_map", &snapshot_map_store_);
        assert(status.ok());
    }
    else {
        std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        column_families.push_back(rocksdb::ColumnFamilyDescriptor());
        column_families.push_back(rocksdb::ColumnFamilyDescriptor("delta_store", delta_store_options_));
        column_families.push_back(rocksdb::ColumnFamilyDescriptor("snapshot_store", snapshot_store_options_));
        column_families.push_back(rocksdb::ColumnFamilyDescriptor("leaf_store", leaf_store_options_));
        column_families.push_back(rocksdb::ColumnFamilyDescriptor("version_map", version_map_options_));
        column_families.push_back(rocksdb::ColumnFamilyDescriptor("snapshot_map", snapshot_map_options_));
        rocksdb::Status status = rocksdb::DB::Open(kv_store_options_, db_path, column_families, &handles, &kv_store_);
        assert(status.ok());
        // destroy default column family handle
        kv_store_->DestroyColumnFamilyHandle(handles[0]);
        delta_store_ = handles[1];
        snapshot_store_ = handles[2];
        leaf_store_ = handles[3];
        version_map_store_ = handles[4];
        snapshot_map_store_ = handles[5];

    }

    inner_obj_store_ = new InnerObjStore(kv_store_, delta_store_, snapshot_store_);
    leaf_obj_store_ = new LeafObjStore(kv_store_, leaf_store_); 
    version_map_ = new VersionMap(kv_store_, version_map_store_);
    snapshot_map_ = new SnapshotMap(kv_store_, snapshot_map_store_);

}

ObjStore::~ObjStore() {
    delete leaf_obj_store_;
    delete inner_obj_store_;
    delete version_map_;
    delete snapshot_map_;
    kv_store_->DestroyColumnFamilyHandle(leaf_store_);
    kv_store_->DestroyColumnFamilyHandle(snapshot_store_);
    kv_store_->DestroyColumnFamilyHandle(delta_store_);
    kv_store_->DestroyColumnFamilyHandle(version_map_store_);
    kv_store_->DestroyColumnFamilyHandle(snapshot_map_store_);
    delete kv_store_;
}


LeafObjStore * ObjStore::leafObjStore() {
    return leaf_obj_store_;
}

InnerObjStore * ObjStore::innerObjStore() {
    return inner_obj_store_;
}

VersionMap * ObjStore::versionMap() {
    return version_map_;
}

SnapshotMap * ObjStore::snapshotMap() {
    return snapshot_map_;
}
