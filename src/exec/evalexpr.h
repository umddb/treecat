
// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef EVAL_EXPR_H
#define EVAL_EXPR_H

#include "rocksdb/slice.h"

#include "common/bson/document.h"
#include "common/bson/bsonobj.h"
#include "grpc/grpccatalog.pb.h"
#include "concurrency/txn.h"
#include "storage/objstore.h"

namespace mmb = mongo::mutablebson;

namespace EvalExpr {
    struct EvalResult {
        EvalResult() {
            const_type_ = ExprConstType::EXPR_CONST_TYPE_NULL;
            data_.str_ = nullptr;
        }

        union Data {
            int32_t int_;
            int64_t long_;
            double double_;
            bool bool_;
            char * str_;
        };
        
        ExprConstType const_type_;
        Data data_;
        size_t size_;


    };

    struct EvalInfo {
        // ObjStore * obj_store_;
        // Transaction::ReadSet * read_set_;
        // uint64_t vid_;
        rocksdb::Slice path_;

        EvalInfo() { }
        ~EvalInfo() { }
    };

    bool cmpOid(const rocksdb::Slice & path, const std::string& oid, uint32_t level);
    EvalResult evalExprConst(const ExprConst& expr_const);
    EvalResult evalExprFieldRef(mongo::BSONObj * obj, const ExprFieldRef & expr_field_ref, EvalInfo * eval_info);
    EvalResult evalExprFieldRef(mmb::Document * obj, const ExprFieldRef & expr_field_ref, EvalInfo * eval_info);
    

    template <typename BSONFormat>
    bool evalPred(BSONFormat * obj, const Predicate & pred, EvalInfo * eval_info) {
        auto pred_case = pred.pred_case();
        switch(pred_case) {
            case Predicate::PredCase::kExprNode:
                return evalExpr<BSONFormat>(obj, pred.expr_node(), eval_info);
            case Predicate::PredCase::kWildcard:
                return true; 
            default:
                return false;
        }
    }

    

    template <typename BSONFormat>
    bool evalExpr(BSONFormat * obj, const ExprNode& expr, EvalInfo * eval_info) {
        EvalResult final_result = evalExprImpl<BSONFormat>(obj, expr, eval_info);
        if (final_result.const_type_ == EXPR_CONST_TYPE_BOOLEAN) {
            return final_result.data_.bool_;
        }    
        
        return false;
    
    }

    template <typename BSONFormat>
    EvalResult evalExprImpl(BSONFormat * obj, const ExprNode& expr, EvalInfo * eval_info) {
        switch(expr.node_case()) {
            case ExprNode::NodeCase::kExprOp:
                return evalExprOp<BSONFormat>(obj, expr.expr_op(), eval_info);
            case ExprNode::NodeCase::kExprBool:
                return evalExprBool<BSONFormat>(obj, expr.expr_bool(), eval_info);
            case ExprNode::NodeCase::kExprConst:
                return evalExprConst(expr.expr_const());
            case ExprNode::NodeCase::kExprFieldRef:
                return evalExprFieldRef(obj, expr.expr_field_ref(), eval_info);
            default:
                return EvalResult();
        }
    }

    template <typename BSONFormat>
    EvalResult evalExprOp(BSONFormat * obj, const ExprOp& expr_op, EvalInfo * eval_info) {
        EvalResult final_result, left, right;
        bool has_left = expr_op.has_left();
        // NULL is the default as expression may be invalid
        final_result.const_type_ = EXPR_CONST_TYPE_NULL;

        if (has_left) {
            left = evalExprImpl<BSONFormat>(obj, expr_op.left(), eval_info);
        }
        
        right = evalExprImpl<BSONFormat>(obj, expr_op.right(), eval_info);

        switch(expr_op.op_type()) {
            case EXPR_OP_TYPE_LESS:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = false;
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_STRING:
                        if (right.const_type_ == EXPR_CONST_TYPE_STRING) {
                            int cmp_int = memcmp(left.data_.str_, right.data_.str_, std::min(left.size_, right.size_));
                            final_result.data_.bool_ = (cmp_int < 0) || (cmp_int == 0 && left.size_ < right.size_);
                        }
                        break;
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.int_ < right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.int_ < right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.int_ < right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.long_ < right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.long_ < right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.long_ < right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.double_ < right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.double_ < right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.double_ < right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DATE:
                        if (right.const_type_ == EXPR_CONST_TYPE_DATE) {
                            final_result.data_.bool_ = (left.data_.long_ < right.data_.long_);
                        }
                        break;
                    default:
                        break;
                }
                break;
            case EXPR_OP_TYPE_GREATER:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = false;
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_STRING:
                        if (right.const_type_ == EXPR_CONST_TYPE_STRING) {
                            int cmp_int = memcmp(left.data_.str_, right.data_.str_, std::min(left.size_, right.size_));
                            final_result.data_.bool_ = (cmp_int > 0) || (cmp_int == 0 && left.size_ > right.size_);
                        }
                        break;
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.int_ > right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.int_ > right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.int_ > right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.long_ > right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.long_ > right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.long_ > right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.double_ > right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.double_ > right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.double_ > right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DATE:
                        if (right.const_type_ == EXPR_CONST_TYPE_DATE) {
                            final_result.data_.bool_ = (left.data_.long_ > right.data_.long_);
                        }
                        break;
                    default:
                        break;
                }
                break; 
            case EXPR_OP_TYPE_EQUALS:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = false;
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_STRING:
                        if (right.const_type_ == EXPR_CONST_TYPE_STRING) {
                            final_result.data_.bool_ = (left.size_ == right.size_) && (memcmp(left.data_.str_, right.data_.str_, left.size_) == 0);
                        }
                        break;
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.int_ == right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.int_ == right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.int_ == right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.long_ == right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.long_ == right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.long_ == right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.double_ == right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.double_ == right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.double_ == right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DATE:
                        if (right.const_type_ == EXPR_CONST_TYPE_DATE) {
                            final_result.data_.bool_ = (left.data_.long_ == right.data_.long_);
                        }
                        break;
                    case EXPR_CONST_TYPE_NULL:
                        final_result.data_.bool_ = (right.const_type_ == EXPR_CONST_TYPE_NULL);
                        break;
                    default:
                        break;
                }
                break;
            case EXPR_OP_TYPE_LESS_EQUALS:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = false;
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_STRING:
                        if (right.const_type_ == EXPR_CONST_TYPE_STRING) {
                            int cmp_int = memcmp(left.data_.str_, right.data_.str_, std::min(left.size_, right.size_));
                            final_result.data_.bool_ = (cmp_int < 0) || (cmp_int == 0 && left.size_ <= right.size_);
                        }
                        break;
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.int_ <= right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.int_ <= right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.int_ <= right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.long_ <= right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.long_ <= right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.long_ <= right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.double_ <= right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.double_ <= right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.double_ <= right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DATE:
                        if (right.const_type_ == EXPR_CONST_TYPE_DATE) {
                            final_result.data_.bool_ = (left.data_.long_ <= right.data_.long_);
                        }
                        break;
                    default:
                        break;
                }
                break; 
            case EXPR_OP_TYPE_GREATER_EQUALS:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = false;
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_STRING:
                        if (right.const_type_ == EXPR_CONST_TYPE_STRING) {
                            int cmp_int = memcmp(left.data_.str_, right.data_.str_, std::min(left.size_, right.size_));
                            final_result.data_.bool_ = (cmp_int > 0) || (cmp_int == 0 && left.size_ >= right.size_);
                        }
                        break;
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.int_ >= right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.int_ >= right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.int_ >= right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.long_ >= right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.long_ >= right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.long_ >= right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.double_ >= right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.double_ >= right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.double_ >= right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DATE:
                        if (right.const_type_ == EXPR_CONST_TYPE_DATE) {
                            final_result.data_.bool_ = (left.data_.long_ >= right.data_.long_);
                        }
                        break;
                    default:
                        break;
                }
                break; 
            case EXPR_OP_TYPE_NOT_EQUALS:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = true;
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_STRING:
                        if (right.const_type_ == EXPR_CONST_TYPE_STRING) {
                            final_result.data_.bool_ = (left.size_ != right.size_) || (memcmp(left.data_.str_, right.data_.str_, left.size_) != 0);
                        }
                        break;
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.int_ != right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.int_ != right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.int_ != right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.long_ != right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.long_ != right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.long_ != right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.data_.bool_ = (left.data_.double_ != right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.data_.bool_ = (left.data_.double_ != right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.data_.bool_ = (left.data_.double_ != right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DATE:
                        if (right.const_type_ == EXPR_CONST_TYPE_DATE) {
                            final_result.data_.bool_ = (left.data_.long_ != right.data_.long_);
                        }
                        break;
                    case EXPR_CONST_TYPE_NULL:
                        final_result.data_.bool_ = (right.const_type_ != EXPR_CONST_TYPE_NULL);
                        break;
                    default:
                        break;
                }
                break;
            case EXPR_OP_TYPE_PLUS:
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_STRING:
                        if (right.const_type_ == EXPR_CONST_TYPE_STRING) {
                            final_result.const_type_ = EXPR_CONST_TYPE_STRING;
                            final_result.data_.str_ = static_cast<char*>(malloc(left.size_ + right.size_));
                            memcpy(final_result.data_.str_, left.data_.str_, left.size_);
                            memcpy(&final_result.data_.str_[left.size_], right.data_.str_, right.size_);
                            final_result.size_ = left.size_ + right.size_;
                        }
                        break;
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_INT;
                                final_result.data_.int_ = (left.data_.int_ + right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = (left.data_.int_ + right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.int_ + right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = (left.data_.long_ + right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = (left.data_.long_ + right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.long_ + right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.double_ + right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.double_ + right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.double_ + right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DATE:
                        if (right.const_type_ == EXPR_CONST_TYPE_DATE) {
                            final_result.const_type_ = EXPR_CONST_TYPE_DATE;
                            final_result.data_.long_ = (left.data_.long_ + right.data_.long_);
                        }
                        break;
                    case EXPR_CONST_TYPE_NULL:
                        if (!has_left) {
                            switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_INT;
                                final_result.data_.int_ = right.data_.int_;
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = right.data_.long_;
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = right.data_.double_;
                                break;
                            default:
                                break;
                            }   
                        }
                        break;
                    default:
                        break;
                }
                break;
            case EXPR_OP_TYPE_MINUS:
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_INT;
                                final_result.data_.int_ = (left.data_.int_ - right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = (left.data_.int_ - right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.int_ - right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = (left.data_.long_ - right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = (left.data_.long_ - right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.long_ - right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.double_ - right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.double_ - right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.double_ - right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DATE:
                        if (right.const_type_ == EXPR_CONST_TYPE_DATE) {
                            final_result.const_type_ = EXPR_CONST_TYPE_DATE;
                            final_result.data_.long_ = (left.data_.long_ - right.data_.long_);
                        }
                        break;
                    case EXPR_CONST_TYPE_NULL:
                        if (!has_left) {
                            switch(right.const_type_) {
                                case EXPR_CONST_TYPE_INT:
                                    final_result.const_type_ = EXPR_CONST_TYPE_INT;
                                    final_result.data_.int_ = -right.data_.int_;
                                    break;
                                case EXPR_CONST_TYPE_LONG:
                                    final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                    final_result.data_.long_ = -right.data_.long_;
                                    break;
                                case EXPR_CONST_TYPE_DOUBLE:
                                    final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                    final_result.data_.double_ = -right.data_.double_;
                                    break;
                                default:
                                    break;
                            }
                        }
                        break;
                    default:
                        break;
                }
                break;
            case EXPR_OP_TYPE_MULT:
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_INT;
                                final_result.data_.int_ = (left.data_.int_ * right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = (left.data_.int_ * right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.int_ * right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = (left.data_.long_ * right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                final_result.data_.long_ = (left.data_.long_ * right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.long_ * right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.double_ * right.data_.int_);
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.double_ * right.data_.long_);
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                final_result.data_.double_ = (left.data_.double_ * right.data_.double_);
                                break;
                            default:
                                break;
                        }
                        break;
                    default:
                        break; 
                }
                break;
            case EXPR_OP_TYPE_DIV:
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_INT:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                if (right.data_.int_ != 0) {
                                    final_result.const_type_ = EXPR_CONST_TYPE_INT;
                                    final_result.data_.int_ = (left.data_.int_ / right.data_.int_);
                                }
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                if (right.data_.long_ != 0) {
                                    final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                    final_result.data_.long_ = (left.data_.int_ / right.data_.long_);
                                }
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                if (right.data_.double_ != 0) {
                                    final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                    final_result.data_.double_ = (left.data_.int_ / right.data_.double_);
                                    
                                }
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_LONG:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                if (right.data_.int_ != 0) {
                                    final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                    final_result.data_.long_ = (left.data_.long_ / right.data_.int_);
                                }
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                if (right.data_.long_ != 0) {
                                    final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                                    final_result.data_.long_ = (left.data_.long_ / right.data_.long_);
                                }
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                if (right.data_.double_ != 0) {
                                    final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                    final_result.data_.double_ = (left.data_.long_ / right.data_.double_);
                                } 
                                break;
                            default:
                                break;
                        }
                        break;
                    case EXPR_CONST_TYPE_DOUBLE:
                        switch(right.const_type_) {
                            case EXPR_CONST_TYPE_INT:
                                if (right.data_.int_ != 0) {
                                    final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                    final_result.data_.double_ = (left.data_.double_ / right.data_.int_);
                                }
                                break;
                            case EXPR_CONST_TYPE_LONG:
                                if (right.data_.long_ != 0) {
                                    final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                    final_result.data_.double_ = (left.data_.double_ / right.data_.long_);
                                }
                                break;
                            case EXPR_CONST_TYPE_DOUBLE:
                                if (right.data_.double_ != 0) {
                                    final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                                    final_result.data_.double_ = (left.data_.double_ / right.data_.double_);
                                }
                                break;
                            default:
                                break;
                        }
                        break;
                    default:
                        break;
                }
                break;
            case EXPR_OP_TYPE_ENDSWITH:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = false;
                switch(left.const_type_) {
                    case EXPR_CONST_TYPE_STRING:
                        if (right.const_type_ == EXPR_CONST_TYPE_STRING) {
                            final_result.data_.bool_ = (left.size_ >= right.size_) && (memcmp(&left.data_.str_[left.size_ - right.size_], right.data_.str_, right.size_) == 0);
                        }
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }

        if (has_left && left.const_type_ == EXPR_CONST_TYPE_STRING) {
            free(left.data_.str_);
        }

        if (right.const_type_ == EXPR_CONST_TYPE_STRING) {
            free(right.data_.str_);
        }

        return final_result;
    }

    template<typename BSONFormat>
    EvalResult evalExprBool(BSONFormat * obj, const ExprBool& expr_bool, EvalInfo * eval_info) {
        EvalResult final_result;

        switch(expr_bool.op_type()) {
            case EXPR_BOOL_TYPE_OR:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = false;
                for (int i = 0; i <  expr_bool.args_size() && !final_result.data_.bool_; i++) {
                    EvalResult arg_result = evalExprImpl<BSONFormat>(obj, expr_bool.args(i), eval_info);
                    if (arg_result.const_type_ == EXPR_CONST_TYPE_BOOLEAN) {
                        final_result.data_.bool_ = arg_result.data_.bool_;
                    }
                    else if (arg_result.const_type_ == EXPR_CONST_TYPE_STRING) {
                        free(arg_result.data_.str_);
                    }
                }
                break;
            case EXPR_BOOL_TYPE_AND:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = true;
                for (int i = 0; i <  expr_bool.args_size() && final_result.data_.bool_; i++) {
                    EvalResult arg_result = evalExprImpl<BSONFormat>(obj, expr_bool.args(i), eval_info);
                    if (arg_result.const_type_ == EXPR_CONST_TYPE_BOOLEAN) {
                        final_result.data_.bool_ = arg_result.data_.bool_;
                    }
                    else {
                        final_result.data_.bool_ = false;
                        if (arg_result.const_type_ == EXPR_CONST_TYPE_STRING) {
                            free(arg_result.data_.str_);
                        }
                    }
                    
                }
                break;
            case EXPR_BOOL_TYPE_NOT: {
                EvalResult arg_result = evalExprImpl<BSONFormat>(obj, expr_bool.args(0), eval_info);
                if (arg_result.const_type_ == EXPR_CONST_TYPE_BOOLEAN) {
                    final_result.data_.bool_ = !arg_result.data_.bool_;
                }
                else {
                    final_result.const_type_ = EXPR_CONST_TYPE_NULL;
                    if (arg_result.const_type_ == EXPR_CONST_TYPE_STRING) {
                        free(arg_result.data_.str_);
                    }
                }
                }
                break;
            default:
                final_result.const_type_ = EXPR_CONST_TYPE_NULL;
                break;
        }

        return final_result;
    }

}

#endif