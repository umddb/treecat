// Author: Keonwoo Oh (koh3@umd.edu)

// Could later separate this out to "optimizer", but for now, we keep it under "executor"
#ifndef PLANNER_H
#define PLANNER_H

// namespace Planner {

#include "exec/execnode.h"
#include "concurrency/txn.h"
#include "concurrency/txnmgr.h"
#include "grpc/grpccatalog.pb.h"

ExecNode * constructPlan(ObjStore* obj_store, unsigned int buf_size, ExecuteQueryRequest* request, Transaction * txn, TransactionManager * txn_mgr, int txn_mgr_version);

ExecNode * constructPlanImpl(ObjStore* obj_store, unsigned int buf_size, const ::PathExpr& path_expr, bool base_only, uint64_t vid, ExecNode::Type exec_node_type, Transaction * txn, TransactionManager * txn_mgr, int txn_mgr_version);

void applyObjIdFilters(ExecNode * root_node, int path_length);

void applyObjIdFiltersImpl(ExecNode * exec_node);

void collectObjIdFilters(std::vector<const ExprOp*> & obj_id_filters, const ExprNode & expr_node);

// }

#endif