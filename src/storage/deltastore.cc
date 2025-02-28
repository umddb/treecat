// Author: Keonwoo Oh (koh3@umd.edu)

#include <cstdlib>
#include <cstring>

#include <endian.h>

#include "common/bson/bsonobj.h"
#include "common/bson/decimal128.h"
#include "common/bson/document.h"
#include "common/bson/element.h"

#include "storage/deltastore.h"
#include "storage/snapshotstore.h"

namespace mmb = mongo::mutablebson;

namespace DeltaStore {

    rocksdb::Slice lastDeltaKey(const rocksdb::Slice & path, const rocksdb::Slice & snapshot) {
        size_t size = path.size() + sizeof(DeltaStore::Header);
        char * data = static_cast<char*>(malloc(size));
        memcpy(data, snapshot.data(), sizeof(DeltaStore::Header));
        memcpy(&data[sizeof(DeltaStore::Header)], path.data(), path.size());
        
        return rocksdb::Slice(data, size);
    }

    bool validPath(const rocksdb::Slice & delta_key, const rocksdb::Slice & path) {
        if (path.size() != delta_key.size() - sizeof(DeltaStore::Header)) {
            return false;
        }

        return !memcmp(&delta_key.data()[sizeof(DeltaStore::Header)], path.data(), path.size());
    }

    // uint64_t deltaVid(const rocksdb::Slice & delta_key) {
    //     uint64_t delta_vid;
    //     memcpy(&delta_vid, delta_key.data(), sizeof(uint64_t));
    //     return le64toh(delta_vid);
    // }

/*
    Snapshot::Snapshot() : obj_data_(nullptr, 0) { }

    Snapshot::Snapshot(const rocksdb::Slice & snapshot) : obj_data_(nullptr, 0) {
        reset(snapshot);
    }

    Snapshot::~Snapshot() { }

    void Snapshot::reset(const rocksdb::Slice & snapshot) {
        SnapshotStore::Header header(snapshot.data());
        last_vid_ = header.cur_vid_;
        removed_ = (snapshot.size() <= sizeof(SnapshotStore::Header) + mongo::BSONObj::kMinBSONLength);
        if (!removed_) {
            obj_data_.size_ = snapshot.size() - sizeof(SnapshotStore::Header);
            obj_data_.data_ = &snapshot.data()[sizeof(SnapshotStore::Header)];
        }
    }

    bool Snapshot::rollBack(const rocksdb::Slice & delta_key, const rocksdb::Slice & delta_value) {
        uint64_t delta_vid = DeltaStore::deltaVid(delta_key);
        if (delta_vid <= last_vid_) {
            obj_data_.size_ = delta_value.size();
            obj_data_.data_ = delta_value.data();
            last_vid_ = delta_vid - 1;
            removed_ = (delta_value.size() <= mongo::BSONObj::kMinBSONLength);
            return true;
        }
        return false;
    }
    
    uint64_t Snapshot::lastVid() {
        return last_vid_;
    }

    bool Snapshot::removed() {
        return removed_;
    }

    rocksdb::Slice Snapshot::value() {
        return obj_data_;
    }
*/
/*
    void Snapshot::reset(const rocksdb::Slice & snapshot) {
        SnapshotStore::Header header(snapshot.data());
        last_vid_ = header.cur_vid_;
        removed_ = (header.tombstone_vid_ == header.cur_vid_);
        if (!removed_) {
            obj_data_.size_ = snapshot.size() - sizeof(SnapshotStore::Header);
            obj_data_.data_ = &snapshot.data()[sizeof(SnapshotStore::Header)];
        }
        doc_.reset(mongo::BSONObj(obj_data_.data()));
    }

    void Snapshot::rollBack(const rocksdb::Slice & delta_key, const rocksdb::Slice & delta_value) {
        uint64_t delta_vid = DeltaStore::deltaVid(delta_key);
        if (delta_vid <= last_vid_) {
            mmb::Document delta_doc(mongo::BSONObj(delta_value.data()));
            rollBackImpl(doc_.root().rightChild(), delta_doc.root());
            last_vid_ = delta_vid - 1;
        }
    }

    void Snapshot::rollBackImpl(mmb::Element snapshot_elm, mmb::Element delta_elm) {
        mmb::Element delta_elm_child = delta_elm.leftChild(); 
        // check if first fieldname is "op", which should store Delta OP codes
        if (delta_elm_child.getFieldName() == "op" && delta_elm_child.getType() == mongo::BSONType::String) {
            mmb::Element delta_elm_value = delta_elm_child.rightSibling();
            // delta "value" exists
            bool has_val = (delta_elm_value.getIdx() != mmb::Element::kInvalidRepIdx);
            switch(static_cast<DeltaOp>(delta_elm_child.getValueString()[0])) {
                case AddField:
                    if (has_val) {
                        deltaAddField(snapshot_elm, delta_elm_value);
                    }
                    break;
                case AddFields:
                    if (has_val) {
                        deltaAddFields(snapshot_elm, delta_elm_value);
                    }
                    break;
                case Remove:
                    // if root, reset the document
                    if (snapshot_elm.parent().getIdx() == mmb::Element::kInvalidRepIdx) {
                        snapshot_elm.getDocument().reset();  
                    }
                    // else remove the element
                    else {
                        snapshot_elm.remove().isOK();
                    }
                    
                    break;
                case Replace:
                    if (has_val) {
                        deltaReplace(snapshot_elm, delta_elm_value);
                    }
                    break;
                case Increment:
                    if (has_val) {
                        deltaIncrement(snapshot_elm, delta_elm_value);
                    }
                    break;
                case Decrement:
                    if (has_val) {
                        deltaDecrement(snapshot_elm, delta_elm_value);
                    }
                    break;
                default:
                    break;

            }
        }
        else {
            // apply delta in a dfs manner
            mmb::Element snapshot_elm_child = snapshot_elm.findFirstChildNamed(delta_elm_child.getFieldName());
            if (snapshot_elm_child.getIdx() != mmb::Element::kInvalidRepIdx) {
                rollBackImpl(snapshot_elm_child, delta_elm_child);
            }
            delta_elm_child = delta_elm_child.rightSibling();
            while(delta_elm_child.getIdx() != mmb::Element::kInvalidRepIdx) {
                snapshot_elm_child = snapshot_elm.findFirstChildNamed(delta_elm_child.getFieldName());
                if (snapshot_elm_child.getIdx() != mmb::Element::kInvalidRepIdx) {
                    rollBackImpl(snapshot_elm_child, delta_elm_child);
                }
                delta_elm_child = delta_elm_child.rightSibling();
            }
        }

    }

    void Snapshot::deltaAddField(mmb::Element elm, mmb::Element delta_val) {
        mongo::BSONType elm_type = elm.getType();
        mongo::BSONType delta_val_type = delta_val.getType();

        switch (elm_type) {
            case mongo::Object:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.appendDouble(delta_val.getFieldName(), delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::String:
                        elm.appendString(delta_val.getFieldName(), delta_val.getValueString()).isOK();
                        break;
                    case mongo::Object:
                        elm.appendObject(delta_val.getFieldName(), delta_val.getValueObject()).isOK();
                        break;
                    case mongo::Array:
                        elm.appendArray(delta_val.getFieldName(), delta_val.getValueArray()).isOK();
                        break;
                    case mongo::Bool:
                        elm.appendBool(delta_val.getFieldName(), delta_val.getValueBool()).isOK();
                        break;
                    case mongo::Date:
                        elm.appendDate(delta_val.getFieldName(), delta_val.getValueDate()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.appendInt(delta_val.getFieldName(), delta_val.getValueInt()).isOK();
                        break;
                    case mongo::bsonTimestamp:
                        elm.appendTimestamp(delta_val.getFieldName(), delta_val.getValueTimestamp()).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.appendLong(delta_val.getFieldName(), delta_val.getValueLong()).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.appendDecimal(delta_val.getFieldName(), delta_val.getValueDecimal()).isOK();
                        break;
                    default:
                        break;  
                    }
                break;

            case mongo::Array:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.appendDouble(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::String:
                        elm.appendString(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueString()).isOK();
                        break;
                    case mongo::Object:
                        elm.appendObject(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueObject()).isOK();
                        break;
                    case mongo::Array:
                        elm.appendArray(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueArray()).isOK();
                        break;
                    case mongo::Bool:
                        elm.appendBool(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueBool()).isOK();
                        break;
                    case mongo::Date:
                        elm.appendDate(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueDate()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.appendInt(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueInt()).isOK();
                        break;
                    case mongo::bsonTimestamp:
                        elm.appendTimestamp(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueTimestamp()).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.appendLong(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueLong()).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.appendDecimal(std::to_string(std::stoi(elm.rightChild().getFieldName().toString())+1), delta_val.getValueDecimal()).isOK();
                        break;
                    default:
                        break;  
                    }
                break;

            default:
                break;
        }
        
    }

    void Snapshot::deltaAddFields(mmb::Element elm, mmb::Element delta_val) {
        switch (delta_val.getType()) {
            case mongo::Object: {
                mmb::Element delta_val_child = delta_val.leftChild(); 
                while (delta_val_child.getIdx() != mmb::Element::kInvalidRepIdx) {
                    deltaAddField(elm, delta_val_child);
                    delta_val_child = delta_val_child.rightSibling();
                }
            } break;

            default:
                break;
        }
        
    }

    void Snapshot::deltaReplace(mmb::Element elm, mmb::Element delta_val) {
        mongo::BSONType delta_val_type = delta_val.getType();
        switch (delta_val_type) {
            case mongo::NumberDouble:
                elm.setValueDouble(delta_val.getValueDouble()).isOK();
                break;
            case mongo::String:
                elm.setValueString(delta_val.getValueString()).isOK();
                break;
            case mongo::Object:
                elm.setValueObject(delta_val.getValueObject()).isOK();
                break;
            case mongo::Array:
                elm.setValueArray(delta_val.getValueArray()).isOK();
                break;
            case mongo::Bool:
                elm.setValueBool(delta_val.getValueBool()).isOK();
                break;
            case mongo::Date:
                elm.setValueDate(delta_val.getValueDate()).isOK();
                break;
            case mongo::NumberInt:
                elm.setValueInt(delta_val.getValueInt()).isOK();
                break;
            case mongo::bsonTimestamp:
                elm.setValueTimestamp(delta_val.getValueTimestamp()).isOK();
                break;
            case mongo::NumberLong:
                elm.setValueLong(delta_val.getValueLong()).isOK();
                break;
            case mongo::NumberDecimal:
                elm.setValueDecimal(delta_val.getValueDecimal()).isOK();
                break;
            default:
                break;
        }
    }

    void Snapshot::deltaIncrement(mmb::Element elm, mmb::Element delta_val) {
        mongo::BSONType elm_type = elm.getType();
        mongo::BSONType delta_val_type = delta_val.getType();
        switch (elm_type) {
            case mongo::NumberDouble:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.setValueDouble(elm.getValueDouble() + delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.setValueDouble(elm.getValueDouble() + delta_val.getValueInt()).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.setValueDouble(elm.getValueDouble() + delta_val.getValueLong()).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.setValueDouble(elm.getValueDouble() + delta_val.getValueDecimal().toDouble()).isOK();
                        break;
                    default:
                        break;
                }
                break;
            case mongo::NumberInt:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.setValueDouble(elm.getValueInt() + delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.setValueInt(elm.getValueInt() + delta_val.getValueInt()).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.setValueLong(elm.getValueInt() + delta_val.getValueLong()).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.setValueDecimal(mongo::Decimal128(elm.getValueInt()).add(delta_val.getValueDecimal())).isOK();
                        break;
                    default:
                        break;
                }
                break;
            case mongo::NumberLong:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.setValueDouble(elm.getValueLong() + delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.setValueLong(elm.getValueLong() + delta_val.getValueInt()).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.setValueLong(elm.getValueLong() + delta_val.getValueLong()).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.setValueDecimal(mongo::Decimal128(elm.getValueLong()).add(delta_val.getValueDecimal())).isOK();
                        break;
                    default:
                        break;
                }
                break;
            case mongo::NumberDecimal:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.setValueDouble(elm.getValueDecimal().toDouble() + delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.setValueDecimal(elm.getValueDecimal().add(mongo::Decimal128(delta_val.getValueInt()))).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.setValueDecimal(elm.getValueDecimal().add(mongo::Decimal128(delta_val.getValueLong()))).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.setValueDecimal(elm.getValueDecimal().add(delta_val.getValueDecimal())).isOK();
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
    }

    void Snapshot::deltaDecrement(mmb::Element elm, mmb::Element delta_val) {
        mongo::BSONType elm_type = elm.getType();
        mongo::BSONType delta_val_type = delta_val.getType();
        switch (elm_type) {
            case mongo::NumberDouble:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.setValueDouble(-delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.setValueDouble(-delta_val.getValueInt()).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.setValueDouble(-delta_val.getValueLong()).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.setValueDouble(-delta_val.getValueDecimal().toDouble()).isOK();
                        break;
                    default:
                        break;
                }
                break;
            case mongo::NumberInt:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.setValueDouble(elm.getValueInt() - delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.setValueInt(elm.getValueInt() - delta_val.getValueInt()).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.setValueLong(elm.getValueInt() - delta_val.getValueLong()).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.setValueDecimal(mongo::Decimal128(elm.getValueInt()).subtract(delta_val.getValueDecimal())).isOK();
                        break;
                    default:
                        break;
                }
                break;
            case mongo::NumberLong:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.setValueDouble(elm.getValueLong() - delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.setValueLong(elm.getValueLong() - delta_val.getValueInt()).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.setValueLong(elm.getValueLong() - delta_val.getValueLong()).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.setValueDecimal(mongo::Decimal128(elm.getValueLong()).subtract(delta_val.getValueDecimal())).isOK();
                        break;
                    default:
                        break;
                }
                break;
            case mongo::NumberDecimal:
                switch (delta_val_type) {
                    case mongo::NumberDouble:
                        elm.setValueDouble(elm.getValueDecimal().toDouble() - delta_val.getValueDouble()).isOK();
                        break;
                    case mongo::NumberInt:
                        elm.setValueDecimal(elm.getValueDecimal().subtract(mongo::Decimal128(delta_val.getValueInt()))).isOK();
                        break;
                    case mongo::NumberLong:
                        elm.setValueDecimal(elm.getValueDecimal().subtract(mongo::Decimal128(delta_val.getValueLong()))).isOK();
                        break;
                    case mongo::NumberDecimal:
                        elm.setValueDecimal(elm.getValueDecimal().subtract(delta_val.getValueDecimal())).isOK();
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
    }

    uint64_t Snapshot::lastVid() {
        return last_vid_;
    }

    bool Snapshot::removed() {
        return removed_;
    }

    mmb::Document * Snapshot::value() {
        return &doc_;
    }

*/
    

}
