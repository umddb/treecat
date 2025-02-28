// Author: Keonwoo Oh (koh3@umd.edu)

#include "exec/execnodev1.h"
#include "exec/execnodev2.h"
#include "exec/execnodev3.h"
#include "exec/planner.h"
#include "storage/objstore.h"

#include "grpc/grpccatalog.pb.h"

// namespace Planner {
ExecNode * constructPlan(ObjStore* obj_store, unsigned int buf_size, ExecuteQueryRequest* request, Transaction * txn, TransactionManager * txn_mgr, int txn_mgr_version) {
    ExecuteQueryRequest::QueryCase query_case = request->query_case();
    switch (query_case) {
        case ExecuteQueryRequest::QueryCase::kParseTree:
            if (request->parse_tree().preds_size() == 0) {
                std::cout << "No predicates!" << std::endl;
                return nullptr;
            }
            else {
                ExecNode::Type exec_node_type;
                if (request->has_return_type()) {
                    exec_node_type = static_cast<ExecNode::Type>(request->return_type());
                }
                else {
                    exec_node_type = ExecNode::Type::INNER_LEAF;
                }
                // Some sanity check for the lock modes, including:
                // 1) number of lock modes = path length + 1
                // 2) lock modes have to be descendable
                if (txn_mgr_version == 3 && txn != nullptr) {
                    const PathExpr & path_expr = request->parse_tree();
                    if (path_expr.lock_modes_size() != (path_expr.preds_size() + 1)) {
                        std::cout << "Invalid number of lock modes! Expected " << (path_expr.preds_size() + 1) 
                        << "but got " << path_expr.lock_modes_size() << std::endl;
                        return nullptr;
                    }
                    for (int i = 0 ; i < path_expr.lock_modes_size() - 1 ; i++) {
                        if (!LockManager::descendable(path_expr.lock_modes(i), path_expr.lock_modes(i + 1))) {
                            std::cout << "Invalid lock modes!" << std::endl;
                            return nullptr;
                        }
                    }
                }

                return constructPlanImpl(obj_store, buf_size, request->parse_tree(),  request->base_only(), request->vid(), exec_node_type, txn, txn_mgr, txn_mgr_version);
            }
            break;
        case ExecuteQueryRequest::QueryCase::kQueryStr:
            //TODO implement parser
            return nullptr;
            break;
        default:
            return nullptr;
            break;
    }
}

ExecNode * constructPlanImpl(ObjStore* obj_store, unsigned int buf_size, const ::PathExpr& path_expr, bool base_only, uint64_t vid, ExecNode::Type exec_node_type, Transaction * txn, TransactionManager * txn_mgr, int txn_mgr_version) {
    int path_length = path_expr.preds_size();
    bool is_last;
    Transaction::ReadSet * read_set = nullptr;
    if (txn != nullptr) {
        read_set = txn->readSet();
    }

    ExecNode* child_node, * cur_node;
    switch (txn_mgr_version) {
        case 1:
            cur_node  = new ExecNodeV1(obj_store, nullptr, path_expr.preds(0), false, base_only, buf_size,
                vid, 0, ExecNode::Type::INNER, read_set);
            break;
        case 2:
            cur_node  = new ExecNodeV2(obj_store, nullptr, path_expr.preds(0), false, base_only, buf_size,
                vid, 0, ExecNode::Type::INNER, read_set);
            break;
        case 3:
            if (txn != nullptr) {
                cur_node  = new ExecNodeV3(obj_store, nullptr, path_expr.preds(0), false, base_only, buf_size,
                    vid, 0, ExecNode::Type::INNER, txn_mgr, txn, path_expr.lock_modes(0));
            }
            else {
                cur_node  = new ExecNodeV3(obj_store, nullptr, path_expr.preds(0), false, base_only, buf_size,
                    vid, 0, ExecNode::Type::INNER, txn_mgr, txn, LOCK_MODE_NL);
            }
            
            break;
        default:
            cur_node  = new ExecNodeV1(obj_store, nullptr, path_expr.preds(0), false, base_only, buf_size,
                vid, 0, ExecNode::Type::INNER, read_set);
            break;
    }
    for (int i = 0; i < path_length ; i++) {
        child_node = cur_node;
        is_last = (i == (path_length -1));
        
        //the last buf size is 10 times as the actual object is output 
        if (is_last){
            switch (txn_mgr_version) {
                case 1:
                    cur_node = new ExecNodeV1(obj_store, child_node, path_expr.preds(i), is_last, base_only, 10 * buf_size,
                        vid, i + 1, exec_node_type, read_set); 
                    break;
                case 2:
                    cur_node = new ExecNodeV2(obj_store, child_node, path_expr.preds(i), is_last, base_only, 10 * buf_size,
                        vid, i + 1, exec_node_type, read_set); 
                    break;
                case 3:
                    if (txn != nullptr) {
                        cur_node = new ExecNodeV3(obj_store, child_node, path_expr.preds(i), is_last, base_only, 10 * buf_size,
                            vid, i + 1, exec_node_type, txn_mgr, txn, path_expr.lock_modes(i + 1));
                    }
                    else {
                        cur_node = new ExecNodeV3(obj_store, child_node, path_expr.preds(i), is_last, base_only, 10 * buf_size,
                            vid, i + 1, exec_node_type, txn_mgr, txn, LOCK_MODE_NL);
                    }
                    
                    break;
                default:
                    cur_node = new ExecNodeV1(obj_store, child_node, path_expr.preds(i), is_last, base_only, 10 * buf_size,
                        vid, i + 1, exec_node_type, read_set); 
                    break;
            }
        }
        else {
            switch (txn_mgr_version) {
                case 1:
                    cur_node = new ExecNodeV1(obj_store, child_node, path_expr.preds(i), is_last, base_only, buf_size,
                        vid, i + 1, ExecNode::Type::INNER, read_set); 
                    break;
                case 2:
                    cur_node = new ExecNodeV2(obj_store, child_node, path_expr.preds(i), is_last, base_only, buf_size, 
                        vid, i + 1, ExecNode::Type::INNER, read_set); 
                    break;
                case 3:
                    if (txn != nullptr) {
                        cur_node = new ExecNodeV3(obj_store, child_node, path_expr.preds(i), is_last, base_only, buf_size,
                            vid, i + 1, ExecNode::Type::INNER, txn_mgr, txn, path_expr.lock_modes(i + 1));
                    }
                    else {
                        cur_node = new ExecNodeV3(obj_store, child_node, path_expr.preds(i), is_last, base_only, buf_size,
                            vid, i + 1, ExecNode::Type::INNER, txn_mgr, txn, LOCK_MODE_NL);
                    }
                     
                    break;
                default:
                    cur_node = new ExecNodeV1(obj_store, child_node, path_expr.preds(i), is_last, base_only, buf_size,
                        vid, i + 1, ExecNode::Type::INNER, read_set); 
                    break;
            }
        }
    }
    // rule which sets the range of scan (lower bound and upper bound) based on the 
    // oid filters
    applyObjIdFilters(cur_node, path_length);

    return cur_node;

}

void applyObjIdFilters(ExecNode * root_node, int path_length) {
    ExecNode * cur_node = root_node;
    for (int i = 0; i < path_length; i++) {
        applyObjIdFiltersImpl(cur_node);
        cur_node = cur_node->getChild();
    }

}

void applyObjIdFiltersImpl(ExecNode * exec_node) {
    std::vector<const ExprOp*> obj_id_filters;
    // collect obj filters that are in conjunction
    collectObjIdFilters(obj_id_filters, exec_node->getPred().expr_node());
    
    // evaluate lower_bound and upper_bound and set the appropriate bounds 
    // on the scan range of the ExecNode
    std::string lower_bound;
    std::string upper_bound;
    std::string tight_bound;
    bool lb_set = false;
    bool ub_set = false;
    bool tb_set = false;
    for (const auto & filter : obj_id_filters){
        const ExprNode & left = filter->left();
        const ExprNode & right = filter->right();
        bool const_on_right = (right.node_case() == ExprNode::NodeCase::kExprConst);

        switch(filter->op_type()) {
            case EXPR_OP_TYPE_LESS: {
                if (const_on_right) {
                    if (!ub_set) {
                        upper_bound = right.expr_const().string_val();
                        ub_set = true;
                    }
                    else {
                        upper_bound = std::min(upper_bound, right.expr_const().string_val());
                    }
                }
                else {
                    if (!lb_set) {
                        lower_bound = left.expr_const().string_val() + std::string(1,'\0');
                        lb_set = true;
                    }
                    else {
                        lower_bound = std::max(lower_bound, left.expr_const().string_val() + std::string(1,'\0'));
                    }

                }
            }
                break;
            case EXPR_OP_TYPE_GREATER: {
                if (const_on_right) {
                    if (!lb_set) {
                        lower_bound = right.expr_const().string_val() + std::string(1,'\0');
                        lb_set = true;
                    }
                    else {
                        lower_bound = std::max(lower_bound, right.expr_const().string_val() + std::string(1,'\0'));
                    }
                    
                }
                else {
                    if (!ub_set) {
                        upper_bound = left.expr_const().string_val();
                        ub_set = true;
                    }
                    else {
                        upper_bound = std::min(upper_bound, left.expr_const().string_val());
                    }

                }
            }
                break;
            case EXPR_OP_TYPE_EQUALS: {
                if (const_on_right) {
                    tight_bound = right.expr_const().string_val();
                }
                else {
                    tight_bound = left.expr_const().string_val();
                }
                tb_set = true;
            }
                break;
            case EXPR_OP_TYPE_LESS_EQUALS:{
                if (const_on_right) {
                    if (!ub_set) {
                        upper_bound = right.expr_const().string_val() + std::string(1,'\0');
                        ub_set = true;
                    }
                    else {
                        upper_bound = std::min(upper_bound, right.expr_const().string_val() + std::string(1,'\0'));
                    }
                }
                else {
                    if (!lb_set) {
                        lower_bound = left.expr_const().string_val();
                        lb_set = true;
                    }
                    else {
                        lower_bound = std::max(lower_bound, left.expr_const().string_val());
                    }

                }
            }
                break;
            case EXPR_OP_TYPE_GREATER_EQUALS: {
                if (const_on_right) {
                    if (!lb_set) {
                        lower_bound = right.expr_const().string_val();
                        lb_set = true;
                    }
                    else {
                        lower_bound = std::max(lower_bound, right.expr_const().string_val());
                    }
                    
                }
                else {
                    if (!ub_set) {
                        upper_bound = left.expr_const().string_val() + std::string(1,'\0');
                        ub_set = true;
                    }
                    else {
                        upper_bound = std::min(upper_bound, left.expr_const().string_val() + std::string(1,'\0'));
                    }

                }
            }
                break;
            default:
                break;
        }
        
    }
    if (tb_set) {
        exec_node->setTightBound(tight_bound);
    }
    else {
        if (lb_set) {
            exec_node->setLowerBound(lower_bound);
        }
        if (ub_set) {
            exec_node->setUpperBound(upper_bound);
        }
    }

}

void collectObjIdFilters(std::vector<const ExprOp*> & obj_id_filters, const ExprNode & expr_node) {
    switch(expr_node.node_case()) {
        case ExprNode::NodeCase::kExprOp: {
            const ExprOp & expr_op = expr_node.expr_op();
            switch(expr_op.op_type()) {
                case EXPR_OP_TYPE_LESS:
                case EXPR_OP_TYPE_GREATER:
                case EXPR_OP_TYPE_EQUALS:
                case EXPR_OP_TYPE_LESS_EQUALS:
                case EXPR_OP_TYPE_GREATER_EQUALS: {
                    const ExprNode & left = expr_op.left();
                    const ExprNode & right = expr_op.right();
                    //obj_id can only be compared against strings
                    if ((left.node_case() == ExprNode::NodeCase::kExprConst && 
                            left.expr_const().const_type() == EXPR_CONST_TYPE_STRING && 
                            right.node_case() == ExprNode::NodeCase::kExprFieldRef && 
                            right.expr_field_ref().field_refs(0) == "obj_id") || 
                        (right.node_case() == ExprNode::NodeCase::kExprConst && 
                            right.expr_const().const_type() == EXPR_CONST_TYPE_STRING && 
                            left.node_case() == ExprNode::NodeCase::kExprFieldRef && 
                            left.expr_field_ref().field_refs(0) == "obj_id")) {
                        obj_id_filters.push_back(&expr_op);
                    }
                }
                    break;
                default:
                    break;

            } 

        }
            break;  
        case ExprNode::NodeCase::kExprBool: {
            const ExprBool & expr_bool = expr_node.expr_bool();
            if (expr_bool.op_type() == EXPR_BOOL_TYPE_AND) {
                for (int i = 0; i <  expr_bool.args_size(); i++) {
                    collectObjIdFilters(obj_id_filters, expr_bool.args(i));
                }
            }
        }     
            break;
        default:
            break;
    }

}

// }