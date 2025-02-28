// Author: Keonwoo Oh (koh3@umd.edu)

#include <stack>

#include "exec/clone.h"
#include "storage/objstore.h"
#include "concurrency/v1/txnv1.h"
#include "concurrency/v1/txnmgrv1.h"
// #include "concurrency/v2/txnmgrv2.h"
// #include "concurrency/v2/txnmgrv2.h"
// #include "concurrency/v3/txnmgrv3.h"
// #include "concurrency/v3/txnmgrv3.h"

#include "common/bson/document.h"

namespace mmb = mongo::mutablebson;


static void cloneV1(CloneRequest * clone_request, CloneResponse * clone_response, 
        ObjStore * obj_store, TransactionManager * txn_mgr);


/*
static void cloneV2(CloneRequest * clone_request, CloneResponse * clone_response, 
        ObjStore * obj_store, TransactionManager * txn_mgr);

static void cloneV3(CloneRequest * clone_request, CloneResponse * clone_response, 
        ObjStore * obj_store, TransactionManagerV3 * txn_mgr);
*/
    
static Path switchPrefix(const Path& base_path, const Path& src_path, const Path& dest_path);


void clone(CloneRequest * clone_request, CloneResponse * clone_response, 
        ObjStore * obj_store, TransactionManager * txn_mgr, int txn_mgr_version) {
    switch(txn_mgr_version) {
        case 1:
            cloneV1(clone_request, clone_response, obj_store, txn_mgr);
            break;
        case 2:
            clone_response->set_success(false);
            break;
        case 3:
            clone_response->set_success(false);
            break;

        default:
            cloneV1(clone_request, clone_response, obj_store, txn_mgr);
            break;    
    }
}

static Path switchPrefix(const Path& base_path, const Path& src_path, const Path& dest_path) {
    std::string final_path_str;
    uint32_t final_path_depth = base_path.depth() - src_path.depth() +  dest_path.depth();
    final_path_depth = htole32(final_path_depth);
    final_path_str.append(reinterpret_cast<char*>(&final_path_depth), sizeof(uint32_t));
    final_path_str.append(&(dest_path.data_[sizeof(uint32_t)]), dest_path.size_ - sizeof(uint32_t));
    final_path_str.append(&base_path.data_[src_path.size_], base_path.size_ - src_path.size_);
    return Path(final_path_str.data(), final_path_str.size(), true);

}

static void cloneV1(CloneRequest * clone_request, CloneResponse * clone_response, 
        ObjStore * obj_store, TransactionManager * txn_mgr) {
    TransactionV1* txn = static_cast<TransactionV1*>(txn_mgr->startTransaction());
    TransactionV1::ReadSet * read_set = txn->read_set_.get();
    TransactionV1::WriteSet * write_set = txn->write_set_.get();
    
    Path src_path(clone_request->src_path());
    Path dest_path(clone_request->dest_path());
    // check if vid is set and is lower than read_vid
    if (clone_request->has_vid()) {
        if (clone_request->vid() <= txn_mgr->getReadVid()) {
            txn->new_read_vid_ = clone_request->vid();
        }
        else {
            txn->abort_.store(true);
            clone_response->set_success(false);
            return;
        }
    }

    uint64_t read_vid = txn->new_read_vid_;
    InnerObjStore * inner_obj_store = obj_store->innerObjStore();
    LeafObjStore * leaf_obj_store = obj_store->leafObjStore();
    //check if destination already exists or the parent exists
    if (inner_obj_store->contains(dest_path, read_vid) || leaf_obj_store->contains(dest_path, read_vid) ||
            (dest_path.depth() > 1 && !inner_obj_store->contains(dest_path.parent(), read_vid))) {
        txn->abort_.store(true);
        clone_response->set_success(false);
        return;
    }

    read_set->addConstraintCheck(dest_path.parent(), TransactionV1::Constraint(TransactionV1::ConstraintType::EXIST));
    read_set->addConstraintCheck(dest_path, TransactionV1::Constraint(TransactionV1::ConstraintType::NOT_EXIST));

    mongo::BSONObj root_obj;
    // if the object is leaf object, simply add the object and be done
    if (leaf_obj_store->get(src_path, read_vid, &root_obj)) {
        Path primary_path = leaf_obj_store->getPrimaryPath(src_path, read_vid);
        write_set->addToLeafStore(dest_path, primary_path);
        write_set->updateConstraint(dest_path, TransactionV1::Constraint(TransactionV1::ConstraintType::EXIST));
        write_set->updateVersionMap(dest_path);
        write_set->updateLogIdx(dest_path, mongo::BSONObj(), root_obj);
    }
    // if the object is non-leaf, recursively clone all descendants, using DFS. 
    else if (inner_obj_store->get(src_path, read_vid, &root_obj)) {
        write_set->addToSnapshot(dest_path, std::string_view(root_obj.objdata(), root_obj.objsize()));
        write_set->updateConstraint(dest_path, TransactionV1::Constraint(TransactionV1::ConstraintType::EXIST));
        // Its prefix, not the full path, is added to the Version map!
        write_set->updateVersionMap(dest_path);
        write_set->updateLogIdx(dest_path, mongo::BSONObj(), root_obj);
        // stack used for DFS
        std::stack<Path> path_queue;
        path_queue.push(Path(src_path.data_, src_path.size_, true));
        while (!path_queue.empty()) {
            Path cur_path(path_queue.top().data_, path_queue.top().size_, true);
            path_queue.pop();
            // traverse all non-leaf children
            InnerObjStore::Iterator * inner_obj_iter = inner_obj_store->newIterator(cur_path, read_vid, InnerObjStore::ReadOptions());
            while(inner_obj_iter->valid()) {
                Path child_path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true);
                Path new_path = switchPrefix(child_path, src_path, dest_path);
                mongo::BSONObj obj_val = inner_obj_iter->value();
                mmb::Document base_doc = mmb::Document(obj_val);
                auto meta = base_doc.root().findFirstChildNamed("meta");
                auto paths = meta.findFirstChildNamed("paths");
                paths.leftChild().remove().isOK();
                paths.appendString("0", new_path.toString()).isOK();
                meta.appendString("src_path", child_path.toString()).isOK();
                meta.appendInt("src_vid", read_vid).isOK();
                mongo::BSONObj final_val = base_doc.getObject().getOwned();
                write_set->addToSnapshot(new_path, std::string_view(final_val.objdata(), final_val.objsize()));
                write_set->updateConstraint(new_path, TransactionV1::Constraint(TransactionV1::ConstraintType::EXIST));
                // Its prefix, not the full path, is added to the Version map!
                write_set->updateVersionMap(new_path);
                write_set->updateLogIdx(new_path, mongo::BSONObj(), final_val);
                path_queue.push(Path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true));
                inner_obj_iter->next();
            }
            delete inner_obj_iter;
            // traverse all leaf children
            LeafObjStore::Iterator * leaf_obj_iter = leaf_obj_store->newIterator(cur_path, read_vid, LeafObjStore::ReadOptions());
            while(leaf_obj_iter->valid()) {
                Path new_path = switchPrefix(Path(leaf_obj_iter->key().data(), leaf_obj_iter->key().size(), false), src_path, dest_path);
                Path primary_path = leaf_obj_store->getPrimaryPath(Path(leaf_obj_iter->key().data(), leaf_obj_iter->key().size(), false), read_vid);
                write_set->addToLeafStore(new_path, primary_path);
                write_set->updateConstraint(new_path, TransactionV1::Constraint(TransactionV1::ConstraintType::EXIST));
                write_set->updateVersionMap(new_path);
                write_set->updateLogIdx(new_path, mongo::BSONObj(), *leaf_obj_iter->value());
                path_queue.push(Path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true));
                leaf_obj_iter->next(); 
            }
            delete leaf_obj_iter;
            
        }


    }

    clone_response->set_success(txn_mgr->commit(txn));
    clone_response->set_vid(txn->commit_vid_);
    
}

/*
static void cloneV2(CloneRequest * clone_request, CloneResponse * clone_response, 
        ObjStore * obj_store, TransactionManager * txn_mgr) {
    TransactionV2* txn = static_cast<TransactionV2*>(txn_mgr->startTransaction());
    TransactionV2::ReadSet * read_set = txn->read_set_.get();
    TransactionV2::WriteSet * write_set = txn->write_set_.get();
    
    Path src_path(clone_request->src_path());
    Path dest_path(clone_request->dest_path());
    // check if vid is set and is lower than read_vid
    if (clone_request->has_vid()) {
        if (clone_request->vid() <= txn_mgr->getReadVid()) {
            txn->read_vid_ = clone_request->vid();
        }
        else {
            txn->abort_.store(true);
            clone_response->set_success(false);
            return;
        }
    }

    uint64_t read_vid = txn->read_vid_;
    InnerObjStore * inner_obj_store = obj_store->innerObjStore();
    LeafObjStore * leaf_obj_store = obj_store->leafObjStore();
    //check if destination already exists or the parent exists
    if (inner_obj_store->contains(dest_path, read_vid) || leaf_obj_store->contains(dest_path, read_vid) ||
            (dest_path.depth() > 1 && !inner_obj_store->contains(dest_path.parent(), read_vid))) {
        txn->abort_.store(true);
        clone_response->set_success(false);
        return;
    }
    read_set->add(dest_path, ReadValidOption::PROPERTIES);
    read_set->add(dest_path.parent(), ReadValidOption::PROPERTIES);

    mongo::BSONObj root_obj;
    // if the object is leaf object, simply add the object and be done
    if (leaf_obj_store->contains(src_path, read_vid)) {
        Path primary_path = leaf_obj_store->getPrimaryPath(src_path, read_vid);
        write_set->addToLeafStore(dest_path, primary_path);
    }
    // if the object is non-leaf, recursively clone all descendants, using DFS. 
    else if (inner_obj_store->get(src_path, read_vid, &root_obj)) {
        write_set->addToSnapshot(dest_path, std::string_view(root_obj.objdata(), root_obj.objsize()));
        // stack used for DFS
        std::stack<Path> path_queue;
        path_queue.push(Path(src_path.data_, src_path.size_, true));
        while (!path_queue.empty()) {
            Path cur_path(path_queue.top().data_, path_queue.top().size_, true);
            path_queue.pop();
            // traverse all non-leaf children
            InnerObjStore::Iterator * inner_obj_iter = inner_obj_store->newIterator(cur_path, read_vid, InnerObjStore::ReadOptions());
            while(inner_obj_iter->valid()) {
                Path child_path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true);
                Path new_path = switchPrefix(child_path, src_path, dest_path);
                mongo::BSONObj obj_val = inner_obj_iter->value();
                mmb::Document base_doc = mmb::Document(obj_val);
                auto meta = base_doc.root().findFirstChildNamed("meta");
                auto paths = meta.findFirstChildNamed("paths");
                paths.leftChild().remove().isOK();
                paths.appendString("0", new_path.toString()).isOK();
                meta.appendString("src_path", child_path.toString()).isOK();
                meta.appendInt("src_vid", read_vid).isOK();
                mongo::BSONObj final_val = base_doc.getObject().getOwned();
                write_set->addToSnapshot(new_path, std::string_view(final_val.objdata(), final_val.objsize()));
                path_queue.push(Path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true));
                inner_obj_iter->next();
            }
            delete inner_obj_iter;
            // traverse all leaf children
            LeafObjStore::Iterator * leaf_obj_iter = leaf_obj_store->newIterator(cur_path, read_vid, LeafObjStore::ReadOptions());
            while(leaf_obj_iter->valid()) {
                Path new_path = switchPrefix(Path(leaf_obj_iter->key().data(), leaf_obj_iter->key().size(), false), src_path, dest_path);
                Path primary_path = leaf_obj_store->getPrimaryPath(Path(leaf_obj_iter->key().data(), leaf_obj_iter->key().size(), false), read_vid);
                write_set->addToLeafStore(new_path, primary_path);
                leaf_obj_iter->next(); 
            }
            delete leaf_obj_iter;
            
        }


    }

    clone_response->set_success(txn_mgr->commit(txn));
    clone_response->set_vid(txn->commit_vid_);
    
}


static void cloneV3(CloneRequest * clone_request, CloneResponse * clone_response, 
        ObjStore * obj_store, TransactionManagerV3 * txn_mgr) {
    TransactionV3* txn = static_cast<TransactionV3*>(txn_mgr->startTransaction());
    TransactionV3::ReadSet * read_set = txn->read_set_.get();
    TransactionV3::WriteSet * write_set = txn->write_set_.get();
    
    Path src_path(clone_request->src_path());
    Path dest_path(clone_request->dest_path());
    txn->read_vid_ = txn_mgr->getReadVid();
    // check if vid is set and is lower than read_vid
    if (clone_request->has_vid()) {
        if (clone_request->vid() <= txn_mgr->getReadVid()) {
            txn->read_vid_ = clone_request->vid();
        }
        else {
            txn->abort_.store(true);
            clone_response->set_success(false);
            return;
        }
    }

    uint64_t read_vid = txn->read_vid_;
    InnerObjStore * inner_obj_store = obj_store->innerObjStore();
    LeafObjStore * leaf_obj_store = obj_store->leafObjStore();

    bool locked = read_set->add(dest_path, ReadValidOption::PROPERTIES);
    if (!locked) {
        txn_mgr->unlockAll(txn);
        clone_response->set_success(false);
        return;
    }
    locked = read_set->add(dest_path.parent(), ReadValidOption::PROPERTIES);
    if (!locked) {
        txn_mgr->unlockAll(txn);
        clone_response->set_success(false);
        return;
    }

    //check if destination already exists or the parent exists
    if (inner_obj_store->contains(dest_path, read_vid) || leaf_obj_store->contains(dest_path, read_vid) ||
            (dest_path.depth() > 1 && !inner_obj_store->contains(dest_path.parent(), read_vid))) {
        txn->abort_.store(true);
        clone_response->set_success(false);
        return;
    }
    

    mongo::BSONObj root_obj;
    // if the object is leaf object, simply add the object and be done
    if (leaf_obj_store->contains(src_path, read_vid)) {
        Path primary_path = leaf_obj_store->getPrimaryPath(src_path, read_vid);
        write_set->addToLeafStore(dest_path, primary_path);
    }
    // if the object is non-leaf, recursively clone all descendants, using DFS. 
    else if (inner_obj_store->get(src_path, read_vid, &root_obj)) {
        write_set->addToSnapshot(dest_path, std::string_view(root_obj.objdata(), root_obj.objsize()));
        // stack used for DFS
        std::stack<Path> path_queue;
        path_queue.push(Path(src_path.data_, src_path.size_, true));
        while (!path_queue.empty()) {
            Path cur_path(path_queue.top().data_, path_queue.top().size_, true);
            path_queue.pop();
            // traverse all non-leaf children
            InnerObjStore::Iterator * inner_obj_iter = inner_obj_store->newIterator(cur_path, read_vid, InnerObjStore::ReadOptions());
            while(inner_obj_iter->valid()) {
                Path child_path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true);
                Path new_path = switchPrefix(child_path, src_path, dest_path);
                mongo::BSONObj obj_val = inner_obj_iter->value();
                mmb::Document base_doc = mmb::Document(obj_val);
                auto meta = base_doc.root().findFirstChildNamed("meta");
                auto paths = meta.findFirstChildNamed("paths");
                paths.leftChild().remove().isOK();
                paths.appendString("0", new_path.toString()).isOK();
                meta.appendString("src_path", child_path.toString()).isOK();
                meta.appendInt("src_vid", read_vid).isOK();
                mongo::BSONObj final_val = base_doc.getObject().getOwned();
                write_set->addToSnapshot(new_path, std::string_view(final_val.objdata(), final_val.objsize()));
                path_queue.push(Path(inner_obj_iter->key().data(), inner_obj_iter->key().size(), true));
                inner_obj_iter->next();
            }
            delete inner_obj_iter;
            // traverse all leaf children
            LeafObjStore::Iterator * leaf_obj_iter = leaf_obj_store->newIterator(cur_path, read_vid, LeafObjStore::ReadOptions());
            while(leaf_obj_iter->valid()) {
                Path new_path = switchPrefix(Path(leaf_obj_iter->key().data(), leaf_obj_iter->key().size(), false), src_path, dest_path);
                Path primary_path = leaf_obj_store->getPrimaryPath(Path(leaf_obj_iter->key().data(), leaf_obj_iter->key().size(), false), read_vid);
                write_set->addToLeafStore(new_path, primary_path);
                leaf_obj_iter->next(); 
            }
            delete leaf_obj_iter;
            
        }


    }

    clone_response->set_success(txn_mgr->commit(txn));
    clone_response->set_vid(txn->commit_vid_);
    
}
*/

