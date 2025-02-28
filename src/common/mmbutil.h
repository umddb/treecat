// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef MMB_UTIL_H
#define MMB_UTIL_H

#include "common/bson/document.h"

namespace mmb = mongo::mutablebson;

namespace MmbUtil {

    // Types of Delta Operations
    enum class DeltaOp {
        // add the given field, only if it did not exist
        AddField = 48,
        // remove the field
        Remove = 49,
        //add the field / replace the field if it does not exist / exists. 
        Upsert = 50,
        // replace the value, only if it exists
        Replace = 51,
        // increment the value by delta amount
        Increment = 52,
        // decrement the value by delta amount
        Decrement = 53,
        // take min
        Min = 54,
        // take max
        Max = 55
    };

    // apply delta to the base
    mongo::BSONObj mergeDeltaToBase(const mongo::BSONObj & base, const mongo::BSONObj & delta);
    // merge/compose 2 deltas to produce a new delta
    mongo::BSONObj mergeDeltas(const mongo::BSONObj & delta1, const mongo::BSONObj & delta2);

    // bool computeDeltaImpl(const mongo::BSONObj & src, const mongo::BSONObj & dst, mmb::Element delta_elem);

    // mongo::BSONObj mergeToBase(const mongo::BSONObj & base, const mongo::BSONObj & delta, mongo::BSONObj * reverse_delta);

    // void mergeToBaseImpl(mmb::Element base, const mongo::BSONObj & delta, mmb::Element reverse_delta);

}




#endif 