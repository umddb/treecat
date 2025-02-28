// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef CLONE_H
#define CLONE_H


#include "grpc/grpccatalog.pb.h"

#include "concurrency/txn.h"
#include "concurrency/txnmgr.h"
#include "storage/objstore.h"


void clone(CloneRequest * clone_request, CloneResponse * clone_response, 
        ObjStore * obj_store, TransactionManager * txn_mgr, int txn_mgr_version);

#endif