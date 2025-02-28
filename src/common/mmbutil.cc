// Author: Keonwoo Oh (koh3@umd.edu)

#include <cstdlib>
#include <cstring>

#include <endian.h>

#include "common/mmbutil.h"

#include "common/bson/bsonobj.h"
#include "common/bson/decimal128.h"
#include "common/bson/document.h"
#include "common/bson/element.h"

namespace mmb = mongo::mutablebson;

namespace MmbUtil {
    // merge delta to base
    static bool mergeDeltaToBaseImpl(mmb::Element base, const mongo::BSONObj & delta, bool appended);
    static bool mergeDeltaOp(mmb::Element base, const mongo::BSONObj & delta, bool appended);
    static bool mergeIncrementField(mmb::Element base, const mongo::BSONElement & delta_val, bool appended);
    static bool mergeDecrementField(mmb::Element base, const mongo::BSONElement & delta_val, bool appended);
    static bool mergeMinField(mmb::Element base, const mongo::BSONElement & delta_val, bool appended);
    static bool mergeMaxField(mmb::Element base, const mongo::BSONElement & delta_val, bool appended);

    // merge 2 deltas
    static void mergeDeltasImpl(mmb::Element delta1, const mongo::BSONObj & delta2);
    static void mergeDeltaOps(mmb::Element delta1, const mongo::BSONObj & delta2);
    static void mergeDeltaOpToAddField(mmb::Element delta1, const mongo::BSONObj & delta2);
    static void mergeDeltaOpToRemove(mmb::Element delta1, const mongo::BSONObj & delta2);
    static void mergeDeltaOpToUpsert(mmb::Element delta1, const mongo::BSONObj & delta2);
    static void mergeDeltaOpToReplace(mmb::Element delta1, const mongo::BSONObj & delta2);
    static void mergeDeltaOpToIncrement(mmb::Element delta1, const mongo::BSONObj & delta2);
    static void mergeDeltaOpToDecrement(mmb::Element delta1, const mongo::BSONObj & delta2);
    static void mergeDeltaOpToMin(mmb::Element delta1, const mongo::BSONObj & delta2);
    static void mergeDeltaOpToMax(mmb::Element delta1, const mongo::BSONObj & delta2);
    
    // merge delta to the right child ("val") of the object 
    mongo::BSONObj mergeDeltaToBase(const mongo::BSONObj & base, const mongo::BSONObj & delta) {
        mmb::Document base_doc = mmb::Document(base);
        mergeDeltaToBaseImpl(base_doc.root().rightChild(), delta, false);
        return base_doc.getObject().getOwned();
    }

    mongo::BSONObj mergeDeltas(const mongo::BSONObj & delta1, const mongo::BSONObj & delta2) {
        mmb::Document delta1_doc = mmb::Document(delta1);
        mergeDeltasImpl(delta1_doc.root(), delta2);
        return delta1_doc.getObject().getOwned();
    }

    static void mergeDeltasImpl(mmb::Element delta1, const mongo::BSONObj & delta2) {
        if (delta1.leftChild().getFieldName() == "op") {
            // merge the 2 delta ops in the leaves 
            if (delta2.hasField("op")) {
                mergeDeltaOps(delta1, delta2);
            }
            // delta2 has finer changes that should be merged to whatever base value in delta1
            else {
                switch (static_cast<DeltaOp>(delta1.leftChild().getValueString()[0])) {
                    //add value, and recurse down. If valid op, upsert. Otherwise, delta2 is ignored
                    case DeltaOp::Remove:
                        delta1.leftChild().setValueString(std::string(1, static_cast<char>(DeltaOp::Upsert))).isOK();
                        delta1.appendObject("", mongo::BSONObj()).isOK();
                        //if not valid op, remove child and revert back to remove
                        if (!mergeDeltaToBaseImpl(delta1.rightChild(), delta2, true)) {
                            delta1.leftChild().setValueString(std::string(1, static_cast<char>(DeltaOp::Remove))).isOK();
                            delta1.rightChild().remove().isOK();
                        }
                        break;
                    case DeltaOp::AddField:
                    case DeltaOp::Replace:
                    case DeltaOp::Upsert:
                        mergeDeltaToBaseImpl(delta1.rightChild(), delta2, false);
                        break;
                    default:
                        break;
                }
            }
        }
        else {
            //delta 2 is object level change, so overwrite any finer change in delta1
            if (delta2.hasField("op")) {
                delta1.setValueObject(delta2).isOK();
            }
            // check children and see if there is a match
            else {
                mongo::BSONObjIterator delta2_iter(delta2);
                while (delta2_iter.more()) {
                    mongo::BSONElement delta2_child = *delta2_iter;
                    mmb::Element delta1_child = delta1.findFirstChildNamed(delta2_child.fieldName());
                    // recurse down if there is a match
                    if (delta1_child.ok()) {
                        mergeDeltasImpl(delta1_child, delta2_child.Obj());
                    }
                    // Otherwise append the child delta from delta2
                    else {
                        delta1.appendElement(delta2_child).isOK();
                    }
                    delta2_iter.next();
                }
            }

        }
    }


    static void mergeDeltaOps(mmb::Element delta1, const mongo::BSONObj & delta2) {
        switch (static_cast<DeltaOp>(delta1.leftChild().getValueString()[0])) {
            case DeltaOp::AddField:
                mergeDeltaOpToAddField(delta1, delta2);
                break;
            case DeltaOp::Remove:
                mergeDeltaOpToRemove(delta1, delta2);
                break;
            case DeltaOp::Upsert:
                mergeDeltaOpToUpsert(delta1, delta2);
                break;
            case DeltaOp::Replace:
                mergeDeltaOpToReplace(delta1, delta2);
                break;
            case DeltaOp::Increment:
                mergeDeltaOpToIncrement(delta1, delta2);
                break;
            case DeltaOp::Decrement:
                mergeDeltaOpToDecrement(delta1, delta2);
                break;
            case DeltaOp::Min:
                mergeDeltaOpToMin(delta1, delta2);
                break;
            case DeltaOp::Max:
                mergeDeltaOpToMax(delta1, delta2);
                break;
            default:
                break;
        }

    }

    static void mergeDeltaOpToAddField(mmb::Element delta1, const mongo::BSONObj & delta2) {
        switch (static_cast<DeltaOp>(delta2.firstElement().String()[0])) {
            case DeltaOp::Remove:
            case DeltaOp::Upsert:
                delta1.setValueObject(delta2).isOK();
                break;
            case DeltaOp::Replace:
                delta1.leftChild().setValueString(std::string(1, static_cast<char>(DeltaOp::Upsert))).isOK();
                delta1.rightChild().setValueBSONElement(delta2.secondElement()).isOK();
                break;
            case DeltaOp::Increment:
                mergeIncrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Decrement:
                mergeDecrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Min:
                mergeMinField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Max:
                mergeMaxField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::AddField:
            default:
                break;
        }
    }

    static void mergeDeltaOpToRemove(mmb::Element delta1, const mongo::BSONObj & delta2) {
        switch (static_cast<DeltaOp>(delta2.firstElement().String()[0])) {
            case DeltaOp::Increment:
            case DeltaOp::Min:
            case DeltaOp::Max:
            case DeltaOp::AddField:
                delta1.leftChild().setValueString(std::string(1, static_cast<char>(DeltaOp::Upsert))).isOK();
                delta1.appendElement(delta2.secondElement()).isOK();
                break;
            case DeltaOp::Decrement:
                delta1.leftChild().setValueString(std::string(1, static_cast<char>(DeltaOp::Upsert))).isOK();
                switch(delta2.secondElement().type()) {
                    case mongo::NumberDouble: 
                        delta1.appendDouble("", -delta2.secondElement().Double()).isOK();
                        break;
                    case mongo::NumberInt:
                        delta1.appendInt("", -delta2.secondElement().Int()).isOK();
                        break;
                    case mongo::NumberLong:
                        delta1.appendLong("", -delta2.secondElement().Long()).isOK();
                        break;
                    case mongo::NumberDecimal:
                        delta1.appendDecimal("", mongo::Decimal128(0).subtract(delta2.secondElement().Decimal())).isOK();
                        break;
                    default:
                        break;
                }
                break;
            case DeltaOp::Upsert:
                delta1.setValueObject(delta2).isOK();
                break;
            case DeltaOp::Replace:
            case DeltaOp::Remove:       
            default:
                break;
        }
    }

    static void mergeDeltaOpToUpsert(mmb::Element delta1, const mongo::BSONObj & delta2) {
        switch (static_cast<DeltaOp>(delta2.firstElement().String()[0])) {
            case DeltaOp::Increment:
                mergeIncrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Decrement:
                mergeDecrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Min:
                mergeMinField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Max:
                mergeMaxField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Replace:
                delta1.rightChild().setValueBSONElement(delta2.secondElement()).isOK();
                break;
            case DeltaOp::Upsert:
            case DeltaOp::Remove:
                delta1.setValueObject(delta2).isOK(); 
                break;
            case DeltaOp::AddField:   
            default:
                break;
        }
    }

    static void mergeDeltaOpToReplace(mmb::Element delta1, const mongo::BSONObj & delta2) {
        switch (static_cast<DeltaOp>(delta2.firstElement().String()[0])) {
            case DeltaOp::Increment:
                mergeIncrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Decrement:
                mergeDecrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Min:
                mergeMinField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Max:
                mergeMaxField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Replace:
            case DeltaOp::Upsert:
            case DeltaOp::Remove:
                delta1.setValueObject(delta2).isOK(); 
                break;
            // undefined and should never happen
            case DeltaOp::AddField:   
            default:
                break;
        }
    }

    static void mergeDeltaOpToIncrement(mmb::Element delta1, const mongo::BSONObj & delta2) {
        switch (static_cast<DeltaOp>(delta2.firstElement().String()[0])) {
            case DeltaOp::Increment:
                mergeIncrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Decrement:
                mergeDecrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Replace:
            case DeltaOp::Upsert:
            case DeltaOp::Remove:
                delta1.setValueObject(delta2).isOK(); 
                break;
            // undefined and should never happen
            case DeltaOp::AddField:
            case DeltaOp::Min:
            case DeltaOp::Max:  
            default:
                break;
        }
    }

    static void mergeDeltaOpToDecrement(mmb::Element delta1, const mongo::BSONObj & delta2) {
        switch (static_cast<DeltaOp>(delta2.firstElement().String()[0])) {
            // opposite as this is decrement operation
            case DeltaOp::Increment:
                mergeDecrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Decrement:
                mergeIncrementField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Replace:
            case DeltaOp::Upsert:
            case DeltaOp::Remove:
                delta1.setValueObject(delta2).isOK(); 
                break;
            // undefined and should never happen
            case DeltaOp::AddField:
            case DeltaOp::Min:
            case DeltaOp::Max:  
            default:
                break;
        }
    }

    static void mergeDeltaOpToMin(mmb::Element delta1, const mongo::BSONObj & delta2) {
        switch (static_cast<DeltaOp>(delta2.firstElement().String()[0])) {
            case DeltaOp::Min:
                mergeMinField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Replace:
            case DeltaOp::Upsert:
            case DeltaOp::Remove:
                delta1.setValueObject(delta2).isOK(); 
                break;
            // undefined and should never happen
            case DeltaOp::AddField:
            case DeltaOp::Increment:
            case DeltaOp::Decrement:
            case DeltaOp::Max:  
            default:
                break;
        }
    }
    
    static void mergeDeltaOpToMax(mmb::Element delta1, const mongo::BSONObj & delta2) {
        switch (static_cast<DeltaOp>(delta2.firstElement().String()[0])) {
            case DeltaOp::Max:
                mergeMaxField(delta1.rightChild(), delta2.secondElement(), false);
                break;
            case DeltaOp::Replace:
            case DeltaOp::Upsert:
            case DeltaOp::Remove:
                delta1.setValueObject(delta2).isOK(); 
                break;
            // undefined and should never happen
            case DeltaOp::AddField:
            case DeltaOp::Increment:
            case DeltaOp::Decrement:
            case DeltaOp::Min:  
            default:
                break;
        }
    }

    static bool mergeDeltaToBaseImpl(mmb::Element base, const mongo::BSONObj & delta, bool appended) {
        // base case where delta has "op" field
        bool has_op = false;
        if (delta.hasField("op")) {
            has_op = mergeDeltaOp(base, delta, appended);
        }
        else {
            mongo::BSONObjIterator delta_iter(delta);
            while (delta_iter.more()) {
                mongo::BSONElement delta_child = *delta_iter;
                mmb::Element base_child = base.findFirstChildNamed(delta_child.fieldName());
                // recurse down if there is a match
                bool child_has_op;
                if (base_child.ok()) {
                    child_has_op = mergeDeltaToBaseImpl(base_child, delta_child.Obj(), appended);
                }
                else if ((base.getType() == mongo::Object) || (base.getType() == mongo::Array)) {
                    base.appendObject(delta_child.fieldName(), mongo::BSONObj()).isOK();
                    child_has_op = mergeDeltaToBaseImpl(base.rightChild(), delta_child.Obj(), true);

                    if (!child_has_op) {
                        //if there has not been any valid op, remove base child that was added
                        base.rightChild().remove().isOK();        
                    }
                }
                else {
                    child_has_op = false;
                }
                has_op = has_op || child_has_op;
                delta_iter.next();
            }
            

        }

        return has_op;

    }

    static bool mergeDeltaOp(mmb::Element base, const mongo::BSONObj & delta, bool appended) {
        bool has_op = false;
        mongo::BSONElement delta_op = delta.firstElement();
        mongo::BSONElement delta_val = delta.secondElement();

        switch(static_cast<DeltaOp>(delta_op.String()[0])) {
            case DeltaOp::AddField:
                has_op = appended;
                if (has_op) {
                    base.setValueBSONElement(delta_val).isOK();
                }
                break;
            case DeltaOp::Remove:
                has_op = !appended;
                if (has_op) {
                    base.remove().isOK();
                }
                break;
            case DeltaOp::Upsert:
                base.setValueBSONElement(delta_val).isOK();
                has_op = true;
                break;
            case DeltaOp::Replace:
                has_op = !appended;
                if (has_op) {
                    base.setValueBSONElement(delta_val).isOK();
                }
                break;
            // the following operations are only valid if base has the operand
            case DeltaOp::Increment:
                has_op = mergeIncrementField(base, delta_val, appended);
                break;
            case DeltaOp::Decrement:
                has_op = mergeDecrementField(base, delta_val, appended);
                break;
            case DeltaOp::Min:
                has_op = mergeMinField(base, delta_val, appended);
                break;
            case DeltaOp::Max:
                has_op = mergeMaxField(base, delta_val, appended);
                break;
            default:
                break;
        }

        return has_op;

    }

    static bool mergeIncrementField(mmb::Element base, const mongo::BSONElement & delta_val, bool appended) {
        bool has_op = true;
        mongo::BSONType base_type = base.getType();
        mongo::BSONType delta_val_type = delta_val.type();

        // base operand does not exist, so operation is invalid
        if (appended) {
            has_op = false;
        }
        else {
            switch (base_type) {
                case mongo::NumberDouble:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(base.getValueDouble() + delta_val.Double()).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueDouble(base.getValueDouble() + delta_val.Int()).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueDouble(base.getValueDouble() + delta_val.Long()).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDouble(base.getValueDouble() + delta_val.Decimal().toDouble()).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberInt:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(base.getValueInt() + delta_val.Double()).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueInt(base.getValueInt() + delta_val.Int()).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueLong(base.getValueInt() + delta_val.Long()).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(mongo::Decimal128(base.getValueInt()).add(delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberLong:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(base.getValueLong() + delta_val.Double()).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueLong(base.getValueLong() + delta_val.Int()).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueLong(base.getValueLong() + delta_val.Long()).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(mongo::Decimal128(base.getValueLong()).add(delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberDecimal:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(base.getValueDecimal().toDouble() + delta_val.Double()).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueDecimal(base.getValueDecimal().add(mongo::Decimal128(delta_val.Int()))).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueDecimal(base.getValueDecimal().add(mongo::Decimal128(delta_val.Long()))).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(base.getValueDecimal().add(delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                default:
                    has_op = false;
                    break;
            }
        }

        return has_op;

    }


    static bool mergeDecrementField(mmb::Element base, const mongo::BSONElement & delta_val, bool appended) {
        bool has_op = true;
        mongo::BSONType base_type = base.getType();
        mongo::BSONType delta_val_type = delta_val.type();

        // base operand does not exist, so operation is invalid
        if (appended) {
            has_op = false;
        }
        else {
            switch (base_type) {
                case mongo::NumberDouble:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(base.getValueDouble() - delta_val.Double()).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueDouble(base.getValueDouble() - delta_val.Int()).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueDouble(base.getValueDouble() - delta_val.Long()).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDouble(base.getValueDouble() - delta_val.Decimal().toDouble()).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberInt:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(base.getValueInt() - delta_val.Double()).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueInt(base.getValueInt() - delta_val.Int()).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueLong(base.getValueInt() - delta_val.Long()).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(mongo::Decimal128(base.getValueInt()).subtract(delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberLong:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(base.getValueLong() - delta_val.Double()).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueLong(base.getValueLong() - delta_val.Int()).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueLong(base.getValueLong() - delta_val.Long()).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(mongo::Decimal128(base.getValueLong()).subtract(delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberDecimal:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(base.getValueDecimal().toDouble() - delta_val.Double()).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueDecimal(base.getValueDecimal().subtract(mongo::Decimal128(delta_val.Int()))).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueDecimal(base.getValueDecimal().subtract(mongo::Decimal128(delta_val.Long()))).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(base.getValueDecimal().subtract(delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                default:
                    has_op = false;
                    break;
            }
        }

        return has_op;

    }


    static bool mergeMinField(mmb::Element base, const mongo::BSONElement & delta_val, bool appended) {
        bool has_op = true;
        mongo::BSONType base_type = base.getType();
        mongo::BSONType delta_val_type = delta_val.type();

        // base operand does not exist, so operation is invalid
        if (appended) {
            has_op = false;
        }
        else {
            switch (base_type) {
                case mongo::NumberDouble:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(std::min(base.getValueDouble(), delta_val.Double())).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueDouble(std::min(base.getValueDouble(), static_cast<double>(delta_val.Int()))).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueDouble(std::min(base.getValueDouble(), static_cast<double>(delta_val.Long()))).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDouble(std::min(base.getValueDouble(), delta_val.Decimal().toDouble())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberInt:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(std::min(static_cast<double>(base.getValueInt()), delta_val.Double())).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueInt(std::min(base.getValueInt(), delta_val.Int())).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueLong(std::min(static_cast<long long>(base.getValueInt()), delta_val.Long())).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(std::min(mongo::Decimal128(base.getValueInt()), delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberLong:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(std::min(static_cast<double>(base.getValueLong()), delta_val.Double())).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueLong(std::min(base.getValueLong(), static_cast<int64_t>(delta_val.Int()))).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueLong(std::min(base.getValueLong(), static_cast<int64_t>(delta_val.Long()))).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(std::min(mongo::Decimal128(base.getValueLong()), delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberDecimal:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(std::min(base.getValueDecimal().toDouble(), delta_val.Double())).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueDecimal(std::min(base.getValueDecimal(), mongo::Decimal128(delta_val.Int()))).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueDecimal(std::min(base.getValueDecimal(), mongo::Decimal128(delta_val.Long()))).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(std::min(base.getValueDecimal(), delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                default:
                    has_op = false;
                    break;
            }
        }

        return has_op;

    }

    static bool mergeMaxField(mmb::Element base, const mongo::BSONElement & delta_val, bool appended) {
        bool has_op = true;
        mongo::BSONType base_type = base.getType();
        mongo::BSONType delta_val_type = delta_val.type();

        // base operand does not exist, so operation is invalid
        if (appended) {
            has_op = false;
        }
        else {
            switch (base_type) {
                case mongo::NumberDouble:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(std::max(base.getValueDouble(), delta_val.Double())).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueDouble(std::max(base.getValueDouble(), static_cast<double>(delta_val.Int()))).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueDouble(std::max(base.getValueDouble(), static_cast<double>(delta_val.Long()))).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDouble(std::max(base.getValueDouble(), delta_val.Decimal().toDouble())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberInt:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(std::max(static_cast<double>(base.getValueInt()), delta_val.Double())).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueInt(std::max(base.getValueInt(), delta_val.Int())).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueLong(std::max(static_cast<long long>(base.getValueInt()), delta_val.Long())).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(std::max(mongo::Decimal128(base.getValueInt()), delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberLong:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(std::max(static_cast<double>(base.getValueLong()), delta_val.Double())).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueLong(std::max(base.getValueLong(), static_cast<int64_t>(delta_val.Int()))).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueLong(std::max(base.getValueLong(), static_cast<int64_t>(delta_val.Long()))).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(std::max(mongo::Decimal128(base.getValueLong()), delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                case mongo::NumberDecimal:
                    switch (delta_val_type) {
                        case mongo::NumberDouble:
                            base.setValueDouble(std::max(base.getValueDecimal().toDouble(), delta_val.Double())).isOK();
                            break;
                        case mongo::NumberInt:
                            base.setValueDecimal(std::max(base.getValueDecimal(), mongo::Decimal128(delta_val.Int()))).isOK();
                            break;
                        case mongo::NumberLong:
                            base.setValueDecimal(std::max(base.getValueDecimal(), mongo::Decimal128(delta_val.Long()))).isOK();
                            break;
                        case mongo::NumberDecimal:
                            base.setValueDecimal(std::max(base.getValueDecimal(), delta_val.Decimal())).isOK();
                            break;
                        default:
                            has_op = false;
                            break;
                    }
                    break;
                default:
                    has_op = false;
                    break;
            }
        }

        return has_op;

    }



    /*
    mongo::BSONObj computeDelta(const mongo::BSONObj & src, const mongo::BSONObj & dst) {
        mmb::Document delta_doc;
        computeDeltaImpl(src, dst, delta_doc.root());
        return delta_doc.getObject().getOwned();
    }

    bool computeDeltaImpl(const mongo::BSONObj & src, const mongo::BSONObj & dst, mmb::Element delta) {
        mongo::BSONObjIteratorSorted src_iter(src);
        mongo::BSONObjIteratorSorted dst_iter(dst);
        bool changed = false;

        while(src_iter.more() && dst_iter.more()) {
            mongo::BSONElement src_child = *src_iter;
            mongo::BSONElement dst_child = *dst_iter;
            int cmp = src_child.fieldNameStringData().compare(dst_child.fieldNameStringData());
            if (cmp == 0) {
                if (src_child.type() == dst_child.type()) {
                    bool child_changed = false;
                    switch(dst_child.type()) {
                        // cases where we have to go further down to get finer delta
                        case mongo::Object:
                            delta.appendObject(dst_child.fieldName(), mongo::BSONObj());
                            child_changed = computeDeltaImpl(src_child.Obj(), dst_child.Obj(), delta.rightChild());
                            // if the child has not changed at all, remove it.
                            if (!child_changed) {
                                delta.rightChild().remove();
                            }   
                            break;
                        case mongo::Array:
                            delta.appendArray(dst_child.fieldName(), mongo::BSONArray());
                            child_changed = computeDeltaImpl(src_child.Obj(), dst_child.Obj(), delta.rightChild());
                            // if the child has not changed at all, remove it.
                            if (!child_changed) {
                                delta.rightChild().remove();
                            }
                            break;

                        default:
                            //same name, same primitive type, compare values to see if it has to be replaced
                            if (src_child.woCompare(dst_child, 0, nullptr) != 0) {
                                delta.appendObject(dst_child.fieldName(), mongo::BSONObj());
                                delta.rightChild().appendString("op", "2");
                                appendFieldNamed(delta.rightChild(), dst_child, "");
                                changed = true;
                            }
                            break;
                    }
                    changed = changed || child_changed;

                }
                else {
                    // replace if names are the same, but types don't match
                    delta.appendObject(dst_child.fieldNameStringData(), mongo::BSONObj());
                    delta.rightChild().appendString("op", "2");
                    appendFieldNamed(delta.rightChild(), dst_child, "");
                    changed = true;
                }

                src_iter.next();
                dst_iter.next();
            }
            else if (cmp < 0) {
                // not in the dst image, so remove
                delta.appendObject(src_child.fieldNameStringData(), mongo::BSONObj());
                delta.rightChild().appendString("op", "1");
                changed = true;
                src_iter.next();
            }
            else {
                // only in the dst image, so add
                delta.appendObject(dst_child.fieldNameStringData(), mongo::BSONObj());
                delta.rightChild().appendString("op", "0");
                appendFieldNamed(delta.rightChild(), dst_child, "");
                changed = true;
                dst_iter.next();

            }
        }

        // remove extra fields in src, not present in the dst image
        while(src_iter.more()) {
            // not in the dst image, so remove
            delta.appendObject(src_child.fieldNameStringData(), mongo::BSONObj());
            delta.rightChild().appendString("op", "1");
            changed = true;
            src_iter.next();
        }

        // add extra fields in dst, not originally in the src image
        while(dst_iter.more()) {
            // not in the src image, so add
            delta.appendObject(dst_iter.fieldNameStringData(), mongo::BSONObj());
            delta.rightChild().appendString("op", "0");
            appendFieldNamed(delta.rightChild(), dst_child, "");
            changed = true;
            dst_iter.next();
        }

        return changed;
    }

    mongo::BSONObj mergeDeltaToBase(const mongo::BSONObj & base, const mongo::BSONObj & delta, mongo::BSONObj * reverse_delta) {
        mmb::Document base_doc = mmb::Document(base);
        mmb::Document reverse_delta_doc;
        
        if (reverse_delta != nullptr) {    
            mergeDeltaToBaseImpl(base_doc.root(), delta, reverse_delta_doc.root());
            *reverse_delta = reverse_delta_doc.getObject().getOwned();
        }
        else {
            // the user did not ask for reverse delta
            mergeDeltaToBaseImpl(base_doc.root(), delta, mmb::Element(&reverse_delta_doc, mmb::Element::kInvalidRepIdx));
        }
        
        return base_doc.getObject().getOwned();
    }

    bool mergeDeltaToBaseImpl(mmb::Element base, const mongo::BSONObj & delta, mmb::Element reverse_delta) {
        // base case where delta has "op" field
        bool has_op = false;
        if (delta.hasField("op")) {
            has_op = mergeDeltaOp(base, delta, reverse_delta);
        }
        else {
            mongo::BSONObjIterator delta_iter(delta);
            while (delta_iter.more()) {
                mongo::BSONElement delta_child = *delta_iter;
                mmb::Element base_child = base.findFirstChildNamed(delta_child.fieldName());
                // recurse down if there is a match
                bool child_has_op;
                if (base_child.ok()) {
                    // if reverse delta is valid, make child and recurse down
                    if (reverse_delta.ok()) {
                        reverse_delta.appendObject(delta_child.fieldName(), mongo::BSONObj());
                        child_has_op = mergeDeltaToBaseImpl(base_child, delta_child.Obj(), reverse_delta.rightChild());
                        if (!child_has_op) {
                            //if reverse delta's child does not have any valid op, remove it
                            reverse_delta.rightChild().remove();
                        }
                    }
                    else {
                        child_has_op = mergeDeltaToBaseImpl(base_child, delta_child.Obj(), reverse_delta);
                    }
                }
                else {
                    base.appendObject(delta_child.fieldName(), mongo::BSONObj());
                    if (reverse_delta.ok()) {
                        child_has_op = mergeDeltaToBaseImpl(base.rightChild(), delta_child.Obj(), mmb::Element(*reverse_delta.getDocument(), mmb::Element::kInvalidRepIdx));
                        // The merge operation added a child to the base, which should be removed in reverse action
                        if (child_has_op) {
                            reverse_delta.addObject(delta_child.fieldName(), mongo::BSONObj());
                            reverse_delta.rightChild().appendString("op", "1");
                        }
                    }
                    else {
                        //In this case, removal op has already been made for some ancestor
                        child_has_op = mergeDeltaToBaseImpl(base.rightChild(), delta_child.Obj(), reverse_delta);
                    }

                    if (!child_has_op) {
                        //if there has not been any valid op, remove base child that was added
                        base.rightChild().remove();        
                    }
                }
                has_op = has_op || child_has_op;

            }
            delta_iter.next();

        }

        return has_op;

    }

    // delta must be well formed
    bool mergeDeltaOp(mmb::Element base, const mongo::BSONObj & delta, mmb::Element reverse_delta) {
        bool has_op = false;
        mongo::BSONElement delta_op = delta.firstElement();
        switch(static_cast<DeltaOp>(delta_op.String()[0])) {
            case AddField:
                has_op = mergeAddField(base, delta.secondElement(), reverse_delta);
                break;
            //the base field is not dynamically added when valid reverse_delta has been passed
            case Remove:
                has_op = mergeRemoveField(base, reverse_delta);
                break;
            case Replace:
                has_op = mergeReplaceField(base, delta.secondElement(), reverse_delta);
                break;
            case Increment:
                has_op = mergeIncrementField(base, delta.secondElement(), reverse_delta);
                break;
            case Decrement:
                has_op = mergeDecrementField(base, delta.secondElement(), reverse_delta);
                break;
            default:
                break;
        }

        return has_op;

    }

    bool mergeAddField(mmb::Element base, const mongo::BSONElement & delta_val, mmb::Element reverse_delta) {
        if (base.hasChildren()) {
            return false;
        }
        else {
            base.setValueBSONElement(delta_val);
            if (reverse_delta.ok()) {
                reverse_delta.appendString("op", "1");
            }
            return true;
        }
    }

    bool mergeRemoveField(mmb::Element base, mmb::Element reverse_delta) {
        if (reverse_delta.ok()) {
            reverse_delta.appendString("op", "0");
            if (base.hasValue()) {
                reverse_delta.appendElement(base.getValue());
            }
            else {
                reverse_delta.appendObject("", mongo::BSONObj());
            }
            
        }
        base.remove();

        return true;
    }

    bool mergeReplaceField(mmb::Element base, const mongo::BSONElement & delta_val, mmb::Element reverse_delta) {
        if (reverse_delta.ok()) {
            reverse_delta.appendString("op", "2");
            // this should always succeed, but sanity check
            if (base.hasValue()) {
                reverse_delta.appendElement(base.getValue());
            }
        }
        base.setValueElement(delta_val);

        return true;
    }

    bool mergeIncrementField(mmb::Element base, const mongo::BSONElement & delta_val, mmb::Element reverse_delta) {
    
    }

    */

    // mongo::BSONObj computeDelta(const mongo::BSONObj & src, const mongo::BSONObj & dst) {
    //     mmb::Document src_doc = mmb::Document(src);
    //     mmb::Document dst_doc = mmb::Document(dst);
    //     mmb::Document delta_doc;
    //     computeDeltaImpl(src_doc.root(), dst_doc.root(), delta_doc.root());

    //     return delta_doc.getObject().getOwned();
    // }

    // bool computeDeltaImpl(mmb::Element src_elem, mmb::Element dst_elem, mmb::Element delta_elem) {
    //     bool changed = false;
    //     //iterate over dst
    //     mmb::Element dst_elem_child = dst_elem.leftChild();
    //     while(dst_elem_child.getIdx() != mmb::Element::kInvalidRepIdx) {
    //         mmb::Element src_elem_child = src_elem.findFirstChildNamed(dst_elem_child.getFieldName());
    //         if (src_elem_child.getIdx() != mmb::Element::kInvalidRepIdx) {
    //             mongo::BSONType dst_elem_child_type = dst_elem_child.getType();
    //             if (src_elem_child.getType() == dst_elem_child_type) {
    //                 bool child_changed = false;
    //                 switch(dst_elem_child_type) {
    //                     // cases where we have to go further down to get finer delta
    //                     case mongo::Object:
    //                         delta_elem.appendObject(dst_elem_child.getFieldName(), mongo::BSONObj());
    //                         child_changed = computeDeltaImpl(src_elem_child, dst_elem_child, delta_elem.rightChild());
    //                         // if the child has not changed at all, remove it.
    //                         if (!child_changed) {
    //                             delta_elem.rightChild().remove();
    //                         }   
    //                         break;
    //                     case mongo::Array:
    //                         delta_elem.appendArray(dst_elem_child.getFieldName(), mongo::BSONArray());
    //                         child_changed = computeDeltaImpl(src_elem_child, dst_elem_child, delta_elem.rightChild());
    //                         // if the child has not changed at all, remove it.
    //                         if (!child_changed) {
    //                             delta_elem.rightChild().remove();
    //                         }
    //                         break;

    //                     default:
    //                         //same name, same primitive type, compare values to see if it has to be replaced
    //                         if (src_elem.compareWithElement(dst_elem, nullptr, false)) {
    //                             delta_elem.appendObject(dst_elem_child.getFieldName(), mongo::BSONObj());
    //                             delta_elem.rightChild().appendString("op", "2");
    //                             appendFieldNamed(delta_elem.rightChild(), dst_elem_child, "");
    //                             changed = true;
    //                         }
    //                         break;
    //                 }
                    
    //                 changed = changed || child_changed;
    //             }
    //             // same name, but different type, so replace it
    //             else {
    //                 delta_elem.appendObject(dst_elem_child.getFieldName(), mongo::BSONObj());
    //                 delta_elem.rightChild().appendString("op", "2");
    //                 appendFieldNamed(delta_elem.rightChild(), dst_elem_child, "");
    //                 changed = true;
    //             }
    //         }
    //         //field is not in src, so simply add it.
    //         else {
    //             delta_elem.appendObject(dst_elem_child.getFieldName(), mongo::BSONObj());
    //             delta_elem.rightChild().appendString("op", "0");
    //             appendFieldNamed(delta_elem.rightChild(), dst_elem_child, "");
    //             changed = true;
    //         }


    //         dst_elem_child = dst_elem_child.rightSibling();
    //     }

    //     //iterate over fields in the src
    //     mmb::Element src_elem_child = src_elem.leftChild();
    //     while(src_elem_child.getIdx() != mmb::Element::kInvalidRepIdx) {
    //         mmb::Element dst_elem_child = dst_elem.findFirstChildNamed(src_elem_child.getFieldName());
    //         //if field does not exist in the dst, remove
    //         if (dst_elem_child.getIdx() == mmb::Element::kInvalidRepIdx) {
    //             delta_elem.appendObject(src_elem_child.getFieldName(), mongo::BSONObj());
    //             delta_elem.rightChild().appendString("op", "1");
    //             changed = true;
    //         }

    //         src_elem_child = src_elem_child.rightSibling();
    //     }
        
    //     return changed;

    // }

/*
    void appendFieldNamed(mmb::Element elem, mongo::BSONElement field, mongo::StringData field_name) {
        if (elem.getType() == mongo::Object) {
            switch (field.type()) {
                case mongo::NumberDouble:
                    elem.appendDouble(field_name, field.Double()).isOK();
                    break;
                case mongo::String:
                    elem.appendString(field_name, field.String()).isOK();
                    break;
                case mongo::Object:
                    elem.appendObject(field_name, field.Obj()).isOK();
                    break;
                case mongo::Array:
                    elem.appendArray(field_name, field.Array()).isOK();
                    break;
                case mongo::Bool:
                    elem.appendBool(field_name, field.Bool()).isOK();
                    break;
                case mongo::Date:
                    elem.appendDate(field_name, field.Date()).isOK();
                    break;
                case mongo::NumberInt:
                    elem.appendInt(field_name, field.Int()).isOK();
                    break;
                case mongo::bsonTimestamp:
                    elem.appendTimestamp(field_name, field.timestamp()).isOK();
                    break;
                case mongo::NumberLong:
                    elem.appendLong(field_name, field.Long()).isOK();
                    break;
                case mongo::NumberDecimal:
                    elem.appendDecimal(field_name, field.Decimal()).isOK();
                    break;
                default:
                    break;  
            }
        }

    }
*/

}