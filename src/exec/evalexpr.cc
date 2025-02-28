// Author: Keonwoo Oh (koh3@umd.edu)

#include "exec/evalexpr.h"

namespace mmb = mongo::mutablebson;

namespace EvalExpr {

bool cmpOid(const rocksdb::Slice & path, const std::string & oid, uint32_t level) {
    uint32_t path_level;
    memcpy(&path_level, path.data(), sizeof(uint32_t));
    path_level = le32toh(path_level);
    if (level == 0 || level > path_level) {
        return false;
    }
    
    uint32_t occur = 0;
    uint32_t pre_idx = sizeof(uint32_t);
    uint32_t post_idx = sizeof(uint32_t);
    for (uint32_t i = sizeof(uint32_t); i < path.size(); i++) {
        if (path[i] == '/') {
            occur++;
            if (occur == level) {
                pre_idx = i;
            }
            if (occur == level + 1) {
                post_idx = i;
                break;
            }
        }
                    
    }
    // the level is in the middle of the path
    if (post_idx > pre_idx && (post_idx - pre_idx - 1) == oid.size()) {
        return !memcmp(&(path.data()[pre_idx + 1]), oid.data(), oid.size());
    }
    // the level is at the end of the path
    else if (occur >= level && level == path_level && (path.size() - pre_idx - 1) == oid.size()){
        return !memcmp(&(path.data()[pre_idx + 1]), oid.data(), oid.size());
    }

    return false;

}

EvalResult evalExprConst(const ExprConst& expr_const) {
    EvalResult final_result;
    switch(expr_const.const_type()) {
        case EXPR_CONST_TYPE_STRING:{
            final_result.const_type_ = EXPR_CONST_TYPE_STRING;
            const std::string & string_val = expr_const.string_val();
            final_result.data_.str_ = static_cast<char*>(malloc(string_val.size()));
            final_result.size_ = string_val.size();
            memcpy(final_result.data_.str_, string_val.data(), string_val.size());
            }
            break;
        case EXPR_CONST_TYPE_INT:
            final_result.const_type_ = EXPR_CONST_TYPE_INT;
            final_result.data_.int_ = expr_const.int32_val();
            break;
        case EXPR_CONST_TYPE_LONG:
            final_result.const_type_ = EXPR_CONST_TYPE_LONG;
            final_result.data_.long_ = expr_const.int64_val();
            break;
        case EXPR_CONST_TYPE_DOUBLE:
            final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
            final_result.data_.double_ = expr_const.double_val();
            break;
        case EXPR_CONST_TYPE_BOOLEAN:
            final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
            final_result.data_.bool_ = expr_const.bool_val();
            break;
        case EXPR_CONST_TYPE_DATE:
            final_result.const_type_ = EXPR_CONST_TYPE_DATE;
            final_result.data_.long_ = expr_const.int64_val();
            break;
        case EXPR_CONST_TYPE_NULL:
            final_result.const_type_ = EXPR_CONST_TYPE_NULL;
            break;
        default:
            break;
    }

    return final_result;

}


EvalResult evalExprFieldRef(mongo::BSONObj * obj, const ExprFieldRef & expr_field_ref, EvalInfo * eval_info) {
    int i;
    EvalResult final_result;
    mongo::BSONObj ref_obj;
    mongo::BSONObj sub_obj;

    final_result.const_type_ = EXPR_CONST_TYPE_NULL;
    // check if it is obj_id and evaluate first
    if (expr_field_ref.field_refs(0) == "obj_id") {
        std::string_view path_str(eval_info->path_.data(), eval_info->path_.size());
        size_t last_oid = std::string_view::npos;
        for (size_t j = path_str.size(); j > sizeof(uint32_t); j--) {
            if (path_str[j - 1] == '/') {
                last_oid = j;
                break;
            }
        }

        if (last_oid != std::string_view::npos) {
            size_t oid_size = path_str.size() - last_oid;
            final_result.const_type_ = EXPR_CONST_TYPE_STRING;
            final_result.data_.str_ = static_cast<char*>(malloc(oid_size));
            final_result.size_ = oid_size;
            memcpy(final_result.data_.str_, &path_str.data()[last_oid], oid_size);
        }

        return final_result;

    }
    // else if (expr_field_ref.field_refs(0)[0] == '/') {
    //     Path path(eval_info->path_.data(), eval_info->path_.size(), false);
    //     path.concat(expr_field_ref.field_refs(0));
    //     if (eval_info->obj_store_->innerObjStore()->get(path, eval_info->vid_, &sub_obj)) {
    //         ref_obj = sub_obj.secondElement().Obj();
    //     }
    //     else {
    //         return final_result;
    //     }
    //     if (read_set_ != nullptr) {
    //         eval_info->read_set_->add(path, ReadValidOption::PROPERTIES);
    //     }
        
    //     i = 1;
    // }
    else if (!obj->isEmpty()){
        ref_obj = obj->secondElement().Obj();
        i = 0;

        for (; i < expr_field_ref.field_refs_size() - 1; i++) {
            mongo::BSONElement elem = ref_obj.getField(expr_field_ref.field_refs(i));
            if (elem.ok() && elem.type() == mongo::BSONType::Object) {
                ref_obj = elem.Obj();
            }
            else {
                return final_result;
            }
        }

        mongo::BSONElement last_elem = ref_obj.getField(expr_field_ref.field_refs(expr_field_ref.field_refs_size() - 1));

        if (last_elem.ok()) {
            switch(last_elem.type()) {
                case mongo::BSONType::NumberDouble:
                    final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                    final_result.data_.double_ = last_elem.Double();
                    break;
                case mongo::BSONType::String: {
                    final_result.const_type_ = EXPR_CONST_TYPE_STRING;
                    mongo::StringData str_data = last_elem.checkAndGetStringData();
                    final_result.data_.str_ = static_cast<char*>(malloc(str_data.size()));
                    final_result.size_ =  str_data.size();
                    memcpy(final_result.data_.str_, str_data.rawData(), str_data.size());
                    } 
                    break;
                case mongo::BSONType::Bool:
                    final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                    final_result.data_.bool_ = last_elem.Bool();
                    break;
                case mongo::BSONType::Date:
                    final_result.const_type_ = EXPR_CONST_TYPE_DATE;
                    final_result.data_.long_ = last_elem.Date().toMillisSinceEpoch();
                    break;
                case mongo::BSONType::NumberInt:
                    final_result.const_type_ = EXPR_CONST_TYPE_INT;
                    final_result.data_.int_ = last_elem.Int();
                    break;
                case mongo::BSONType::NumberLong:
                    final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                    final_result.data_.long_ = last_elem.Long();
                    break;
                default:
                    break;
            }
        }
    }

    return final_result;
    
}

EvalResult evalExprFieldRef(mmb::Document * obj, const ExprFieldRef & expr_field_ref, EvalInfo * eval_info) {
    EvalResult final_result;
    mmb::Element ref_obj = obj->root().rightChild();

    final_result.const_type_ = EXPR_CONST_TYPE_NULL;
    
    for (int i = 0; i < expr_field_ref.field_refs_size() - 1; i++) {
        ref_obj = ref_obj.findFirstChildNamed(expr_field_ref.field_refs(i));
        if (!ref_obj.ok() || !ref_obj.isType(mongo::BSONType::Object)) {
            return final_result;
        }
    }

    mmb::Element last_elem = ref_obj.findFirstChildNamed(expr_field_ref.field_refs(expr_field_ref.field_refs_size() - 1));

    if (last_elem.ok()) {
        switch(last_elem.getType()) {
            case mongo::BSONType::NumberDouble:
                final_result.const_type_ = EXPR_CONST_TYPE_DOUBLE;
                final_result.data_.double_ = last_elem.getValueDouble();
                break;
            case mongo::BSONType::String: {
                final_result.const_type_ = EXPR_CONST_TYPE_STRING;
                mongo::StringData str_data = last_elem.getValueString();
                final_result.data_.str_ = static_cast<char*>(malloc(str_data.size()));
                final_result.size_ =  str_data.size();
                memcpy(final_result.data_.str_, str_data.rawData(), str_data.size());
                } 
                break;
            case mongo::BSONType::Bool:
                final_result.const_type_ = EXPR_CONST_TYPE_BOOLEAN;
                final_result.data_.bool_ = last_elem.getValueBool();
                break;
            case mongo::BSONType::Date:
                final_result.const_type_ = EXPR_CONST_TYPE_DATE;
                final_result.data_.long_ = last_elem.getValueDate().toMillisSinceEpoch();
                break;
            case mongo::BSONType::NumberInt:
                final_result.const_type_ = EXPR_CONST_TYPE_INT;
                final_result.data_.int_ = last_elem.getValueInt();
                break;
            case mongo::BSONType::NumberLong:
                final_result.const_type_ = EXPR_CONST_TYPE_LONG;
                final_result.data_.long_ = last_elem.getValueLong();
                break;
            default:
                break;
        }
    }

    return final_result;
}

}

