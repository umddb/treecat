// Author: Keonwoo Oh (koh3@umd.edu)

#include <chrono>

#include "common/bson/bsonobj.h"
#include "common/bson/status.h"
#include "common/bson/document.h"
#include "common/path.h"
#include "common/objkind.h"

#include "exec/execbuffer.h"
#include "grpc/grpccatalog.pb.h"
#include "storage/objstore.h"

#include "exec/execnodev1.h"
#include "exec/evalexpr.h"

namespace mmb = mongo::mutablebson;


ExecNodeV1::ExecNodeV1(ObjStore * obj_store, ExecNode* child_node, const Predicate & pred, bool is_last, bool base_only, 
unsigned int buf_size, uint64_t vid, uint32_t level, Type type, Transaction::ReadSet * read_set): obj_store_(obj_store), 
        child_node_(child_node), pred_(pred), is_last_(is_last), base_only_(base_only), 
        buf_size_(buf_size), vid_(vid), level_(level), valid_(true), init_(false), input_buffer_(nullptr), 
        leaf_obj_iter_(nullptr), inner_obj_iter_(nullptr), type_(type), read_set_(static_cast<TransactionV1::ReadSet*>(read_set)) {

    is_leaf_ = (((type_ & Type::LEAF) == Type::LEAF) && is_last_);

    eval_info_= std::make_unique<EvalExpr::EvalInfo>();

}

ExecNodeV1::~ExecNodeV1() {
    if (child_node_){
        delete child_node_;
    } 
}

bool ExecNodeV1::next() {
    if (!valid_) {
        return false;
    }
    
    if (level_ == 0) {
        init_ = true;
        valid_ = false;
        Path root_path("/");
        output_buffer_.init(sizeof(uint32_t) + root_path.size_);
        output_buffer_.finalAppend(root_path.data_, root_path.size_);
        return true;
    }
    
    //if evaluating a predicate expression, rather than finding single matching oid
    if (tight_bound_.empty()) {
        if(!init_) {
            init_ = true;
            output_buffer_.init(buf_size_);
            if (child_node_->next()) {
                input_buffer_ = child_node_->getOutputBuffer();
                resetInputBufferIter();
                if (input_buffer_iter_.valid()) {
                    Path parent_path(input_buffer_iter_.data(), input_buffer_iter_.size(), false);
                    resetObjIter(parent_path);
                    valid_ = true; 
                }
                else {
                    valid_ = false;
                    //TODO deallocate buffer, rather than deleting the entire node
                    delete child_node_;
                    child_node_ = nullptr;  
                    return false;
                }
                
            }
            else {
                valid_ = false;
                //TODO deallocate buffer, rather than deleting the entire node
                delete child_node_;
                child_node_ = nullptr;
                return false;

            }

        }

        output_buffer_.clear();
        while (!output_buffer_.isFull()) {
            bool obj_iter_valid;
            if (is_leaf_) {
                obj_iter_valid = leaf_obj_iter_->valid();
            }
            else {
                obj_iter_valid = inner_obj_iter_->valid();
            }


            if (obj_iter_valid) {
                // Either ingest input and call next OR set the output buffer to full
                processInput();
            }
            else {
                if (type_ == Type::INNER_LEAF && !is_leaf_) {
                    Path parent_path(input_buffer_iter_.data(), input_buffer_iter_.size(), false);
                    resetObjIter(parent_path);
                }
                else {
                    if (input_buffer_iter_.next()) {
                        Path parent_path(input_buffer_iter_.data(), input_buffer_iter_.size(), false);
                        resetObjIter(parent_path);
                    }
                    else {
                        // This should have reset the input buffer unless it's the end
                        if (child_node_->next()) {
                            resetInputBufferIter();
                            Path parent_path(input_buffer_iter_.data(), input_buffer_iter_.size(), false);
                            resetObjIter(parent_path); 
                        }
                        else {
                            valid_ = false;
                            deleteObjIter();
                            delete child_node_;
                            child_node_ = nullptr;
                            break;

                        }
                    }
                }

            }

        }
    }
    //tight bound
    else {
        if(!init_) {
            init_ = true;
            output_buffer_.init(buf_size_);
            if (child_node_->next()) {
                input_buffer_ = child_node_->getOutputBuffer();
                resetInputBufferIter();
                if (input_buffer_iter_.valid()) {
                    valid_ = true;
                }
                else {
                    valid_ = false;
                    //TODO deallocate buffer, rather than deleting the entire node
                    delete child_node_;
                    child_node_ = nullptr;  
                    return false;
                }
                
            }
            else {
                valid_ = false;
                //TODO deallocate buffer, rather than deleting the entire node
                delete child_node_;
                child_node_ = nullptr;
                return false;

            }

        }

        output_buffer_.clear();
        while (!output_buffer_.isFull()) {
            // Either ingest input OR set the output buffer to full
            processSingleInput();
            if (!output_buffer_.isFull()) {
                if (!input_buffer_iter_.next()) {
                    // This should have reset the input buffer unless it's the end
                    if (child_node_->next()) {
                        resetInputBufferIter();
                    }
                    else {
                        valid_ = false;
                        delete child_node_;
                        child_node_ = nullptr;
                        break;

                    }
                }
            }
            
        }

    }

    if (!output_buffer_.empty()) {
        return true;
    }
    else {
        return false;
    }

}

ExecBuffer * ExecNodeV1::getOutputBuffer() {
    return &output_buffer_;
}

const Predicate & ExecNodeV1::getPred() {
    return pred_;
}

ExecNode * ExecNodeV1::getChild() {
    return child_node_;
}

void ExecNodeV1::setLowerBound(const std::string & lower_bound) {
    leaf_obj_ro_.lower_bound_ = lower_bound;
    inner_obj_ro_.lower_bound_ = lower_bound;
}


void ExecNodeV1::setUpperBound(const std::string & upper_bound) {
    leaf_obj_ro_.upper_bound_ = upper_bound;
    inner_obj_ro_.upper_bound_ = upper_bound;
}

void ExecNodeV1::setTightBound(const std::string & tight_bound) {
    //properties have to be checked
    tight_bound_ = tight_bound;
}


void ExecNodeV1::processSingleInput() {
    //construct the full path
    std::string path_str;
    path_str.reserve(input_buffer_iter_.size() + tight_bound_.size() + 1);
    path_str.append(input_buffer_iter_.data(), input_buffer_iter_.size());
    if (path_str.back() != '/') {
        path_str.append("/");
        // change the depth
        uint32_t depth;
        memcpy(&depth, path_str.data(), sizeof(uint32_t));
        depth = le32toh(depth);
        depth++;
        depth = htole32(depth);
        memcpy(path_str.data(), &depth, sizeof(uint32_t));
    }
    
    if (read_set_ != nullptr) {
        read_set_->addScan(Path(path_str.data(), path_str.size(), false), pred_);
    }
    
    path_str.append(tight_bound_);
    
    // the actual path
    Path path(path_str.data(),path_str.size(), false);

    mongo::BSONObj value;
    bool satisfy_pred;
    if (is_leaf_) {
        // check for existence
        satisfy_pred = obj_store_->leafObjStore()->get(path, vid_, &value);
        // evaluate the rest of predicate
        if (satisfy_pred) {
            eval_info_->path_ = rocksdb::Slice(path.data_, path.size_);
            satisfy_pred = EvalExpr::evalPred<mongo::BSONObj>(&value, pred_, eval_info_.get());
        }

        if (satisfy_pred) {
            int obj_size;
            if (base_only_) {
                // Don't include any metadata of the leaf object
                mongo::BSONObj return_value = value.secondElement().Obj();
                obj_size = return_value.objsize();
                if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= static_cast<uint32_t>(obj_size)) {
                    output_buffer_.finalAppend(return_value.objdata(), obj_size);
                }
                else {
                    output_buffer_.setFull(true);
                }
            }
            else {
                obj_size = value.objsize();
                if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= 
                        static_cast<uint32_t>(obj_size) + path.size_ ) {
                    // take out the path depth, which is uint32_t but add null terminator
                    output_buffer_.finalAppend(&path.data_[sizeof(uint32_t)], path.size_ - sizeof(uint32_t));
                    output_buffer_.finalAppend(value.objdata(), obj_size);
                }
                else {
                    output_buffer_.setFull(true);
                }
            }
        }
    }
    if ((type_ & Type::INNER) == Type::INNER) {
        // check for existence
        satisfy_pred = obj_store_->innerObjStore()->get(path, vid_, &value);
        // evaluate the rest of predicate
        if (satisfy_pred) {
            eval_info_->path_ = rocksdb::Slice(path.data_, path.size_);
            satisfy_pred = EvalExpr::evalPred<mongo::BSONObj>(&value, pred_, eval_info_.get());
        }

        if (satisfy_pred) {
            // output the object 
            if (is_last_) {
                //Don't include the metadata
                if (base_only_) {
                    // Don't include any metadata of the inner object
                    mongo::BSONObj return_value = value.secondElement().Obj();
                    if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= static_cast<uint32_t>(return_value.objsize())) {
                        output_buffer_.finalAppend(return_value.objdata(), return_value.objsize());
                    }
                    else {
                        output_buffer_.setFull(true);
                    }    
                }
                else {
                    if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= 
                        static_cast<uint32_t>(value.objsize()) + path.size_) {
                        // take out the path depth, which is uint32_t, but add null terminator
                        output_buffer_.finalAppend(&path.data_[sizeof(uint32_t)], path.size_ - sizeof(uint32_t));
                        output_buffer_.finalAppend(value.objdata(), value.objsize());
                    }
                    else {
                        output_buffer_.setFull(true);
                    }

                }

            }
            // output the path
            else {
                if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= path.size_) {
                    output_buffer_.finalAppend(path.data_, path.size_);
                }
                else {
                    output_buffer_.setFull(true);
                }
            }
        
        }
    }
}

void ExecNodeV1::processInput() {
    bool satisfy_pred;
    rocksdb::Slice path;
    // to distinguish path string and the actual object, depth of the path string is omitted

    if (is_leaf_) {
        path = leaf_obj_iter_->key();
        mongo::BSONObj* value = leaf_obj_iter_->value();
        eval_info_->path_ = path;
        satisfy_pred = EvalExpr::evalPred<mongo::BSONObj>(value, pred_, eval_info_.get());
        // if satisfies predicate and is the last exec node, write to the output buffer
        if (satisfy_pred) {
            if (base_only_) {
                // Don't include any metadata of the leaf object
                mongo::BSONObj return_value = value->secondElement().Obj();
                if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= static_cast<uint32_t>(return_value.objsize())) {
                    output_buffer_.finalAppend(return_value.objdata(), return_value.objsize());
                    leaf_obj_iter_->next(); 
                }
                else {
                    output_buffer_.setFull(true);
                }
            }
            else {
                if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= 
                        static_cast<uint32_t>(value->objsize()) + path.size()) {
                    // take out the path depth, which is uint32_t but add null terminator
                    output_buffer_.finalAppend(&path.data()[sizeof(uint32_t)], path.size() - sizeof(uint32_t));
                    output_buffer_.finalAppend(value->objdata(), value->objsize());
                    leaf_obj_iter_->next(); 
                }
                else {
                    output_buffer_.setFull(true);
                }
            }

        }
        // skip
        else {
            leaf_obj_iter_->next();
        }

    }
    else {
        path = inner_obj_iter_->key();
        mongo::BSONObj value = inner_obj_iter_->value();
        eval_info_->path_ = path;
        satisfy_pred = EvalExpr::evalPred<mongo::BSONObj>(&value, pred_, eval_info_.get());
        if (satisfy_pred) {
            // output the object 
            if (is_last_) {
                //Don't include the metadata
                if (base_only_) {
                    // Don't include any metadata of the inner object
                    mongo::BSONObj return_value = value.secondElement().Obj();
                    if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= static_cast<uint32_t>(return_value.objsize())) {
                        output_buffer_.finalAppend(return_value.objdata(), return_value.objsize());
                        inner_obj_iter_->next(); 
                    }
                    else {
                        output_buffer_.setFull(true);
                    }    
                }
                else {
                    if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= 
                        static_cast<uint32_t>(value.objsize()) + path.size()) {
                        // take out the path depth, which is uint32_t, but add null terminator
                        output_buffer_.finalAppend(&path.data()[sizeof(uint32_t)], path.size() - sizeof(uint32_t));
                        output_buffer_.finalAppend(value.objdata(), value.objsize());
                        inner_obj_iter_->next(); 
                    }
                    else {
                        output_buffer_.setFull(true);
                    }

                }

            }
            // output the path
            else {
                if ((!output_buffer_.isFull()) && (output_buffer_.capacity() - output_buffer_.size()) >= path.size()) {
                    output_buffer_.finalAppend(path.data(), path.size());
                    inner_obj_iter_->next();
                }
                else {
                    output_buffer_.setFull(true);
                }
            }
        
        }
        else {
            // skip
            inner_obj_iter_->next();
        }

    }

}


void ExecNodeV1::resetInputBufferIter() {
    input_buffer_iter_.reset(input_buffer_);
}


void ExecNodeV1::resetObjIter(const Path & parent_path) {
    if (read_set_ != nullptr && (!(type_ == Type::INNER_LEAF) || is_leaf_)) {
        std::string path_str;
        path_str.reserve(parent_path.size_ + 1);
        path_str.append(parent_path.data_, parent_path.size_);
        if (path_str.back() != '/') {
            path_str.append(1, '/');

            //update depth
            uint32_t depth;
            memcpy(&depth, parent_path.data_, sizeof(uint32_t));
            depth = le32toh(depth);
            depth++;
            depth = htole32(depth);
            memcpy(const_cast<char*>(path_str.data()), &depth, sizeof(uint32_t));
        }
        
        read_set_->addScan(Path(path_str.data(), path_str.size(), false), pred_);
    }

    switch (type_) {
        case Type::INNER_LEAF:
            is_leaf_ = !is_leaf_;
            if (is_leaf_) {
                if (leaf_obj_iter_ == nullptr) {
                    leaf_obj_iter_ = obj_store_->leafObjStore()->newIterator(parent_path, vid_, leaf_obj_ro_);
                }
                else {
                    leaf_obj_iter_->reset(parent_path, vid_, leaf_obj_ro_);
                }
            }
            else {
                if (inner_obj_iter_ == nullptr) {
                    inner_obj_iter_ = obj_store_->innerObjStore()->newIterator(parent_path, vid_, inner_obj_ro_);
                }
                else {
                    inner_obj_iter_->reset(parent_path, vid_, inner_obj_ro_);
                }

            }
            break;
        case Type::INNER:
            if (inner_obj_iter_ == nullptr) {
                inner_obj_iter_ = obj_store_->innerObjStore()->newIterator(parent_path, vid_, inner_obj_ro_);
            }
            else {
                inner_obj_iter_->reset(parent_path, vid_, inner_obj_ro_);
            }
            
            break;
        case Type::LEAF:
            if (leaf_obj_iter_ == nullptr) {
                leaf_obj_iter_ = obj_store_->leafObjStore()->newIterator(parent_path, vid_, leaf_obj_ro_);
            }
            else {
                leaf_obj_iter_->reset(parent_path, vid_, leaf_obj_ro_);
            }
            break;
    }
}

void ExecNodeV1::deleteObjIter() {
    if (leaf_obj_iter_ != nullptr) {
        delete leaf_obj_iter_;
        leaf_obj_iter_ = nullptr;  
    }
    if (inner_obj_iter_ != nullptr) {
        delete inner_obj_iter_;
        inner_obj_iter_ = nullptr;
    }  

}



