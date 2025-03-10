/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

// Modified by Keonwoo Oh (koh3@umd.edu) from June 6th to 16th, 2023

#ifndef DAMAGE_VECTOR
#define DAMAGE_VECTOR

#include <cstdint>
#include <vector>

namespace mongo {
namespace mutablebson {

// A damage event represents a change of size 'targetSize' byte at starting at offset
// 'targetOffset' in some target buffer, with the replacement data being 'sourceSize' bytes of
// data from the 'sourceOffset'. The base addresses against which these offsets are to be
// applied are not captured here.
struct DamageEvent {
    typedef uint32_t OffsetSizeType;

    DamageEvent() = default;

    DamageEvent(OffsetSizeType srcOffset, size_t srcSize, OffsetSizeType tgtOffset, size_t tgtSize)
        : sourceOffset(srcOffset),
          sourceSize(srcSize),
          targetOffset(tgtOffset),
          targetSize(tgtSize) {}

    // Offset of source data (in some buffer held elsewhere).
    OffsetSizeType sourceOffset;

    // Size of the data from the source.
    // If 'sourceSize' is zero, no new data is inserted and 'targetSize' of bytes are deleted from
    // the target.
    size_t sourceSize;

    // Offset of target data (in some buffer held elsewhere).
    OffsetSizeType targetOffset;

    // Size of the data to be replaced in the target.
    // If 'targetSize' is zero, no bytes from the target are replaced and the new data is inserted.
    size_t targetSize;
};

typedef std::vector<DamageEvent> DamageVector;

}  // namespace mutablebson
}  // namespace mongo

#endif
