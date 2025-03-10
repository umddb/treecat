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

#ifndef COMPILER_H
#define COMPILER_H

/**
 * Include "mongo/platform/compiler.h" to get compiler-targeted macro definitions and utilities.
 *
 * The following macros are provided in all compiler environments:
 *
 *
 * MONGO_COMPILER_COLD_FUNCTION
 *
 *   Informs the compiler that the function is cold. This can have the following effects:
 *   - The function is optimized for size over speed.
 *   - The function may be placed in a special cold section of the binary, away from other code.
 *   - Code paths that call this function are considered implicitly unlikely.
 *
 *
 * MONGO_COMPILER_NORETURN
 *
 *   Instructs the compiler that the decorated function will not return through the normal return
 *   path. All noreturn functions are also implicitly cold since they are either run-once code
 *   executed at startup or shutdown or code that handles errors by throwing an exception.
 *
 *   Correct: MONGO_COMPILER_NORETURN void myAbortFunction();
 *
 *
 * MONGO_COMPILER_ALIGN_TYPE(ALIGNMENT)
 *
 *   Instructs the compiler to use the given minimum alignment for the decorated type.
 *
 *   Alignments should probably always be powers of two.  Also, note that most allocators will not
 *   be able to guarantee better than 16- or 32-byte alignment.
 *
 *   Correct:
 *     class MONGO_COMPILER_ALIGN_TYPE(16) MyClass {...};
 *
 *   Incorrect:
 *     MONGO_COMPILER_ALIGN_TYPE(16) class MyClass {...};
 *     class MyClass{...} MONGO_COMPILER_ALIGN_TYPE(16);
 *
 *
 * MONGO_COMPILER_ALIGN_VARIABLE(ALIGNMENT)
 *
 *   Instructs the compiler to use the given minimum alignment for the decorated variable.
 *
 *   Note that most allocators will not allow heap allocated alignments that are better than 16- or
 *   32-byte aligned.  Stack allocators may only guarantee up to the natural word length worth of
 *   alignment.
 *
 *   Correct:
 *     class MyClass {
 *         MONGO_COMPILER_ALIGN_VARIABLE(8) char a;
 *     };
 *
 *     MONGO_COMPILER_ALIGN_VARIABLE(8) class MyClass {...} singletonInstance;
 *
 *   Incorrect:
 *     int MONGO_COMPILER_ALIGN_VARIABLE(16) a, b;
 *
 *
 * MONGO_COMPILER_API_EXPORT
 *
 *   Instructs the compiler to label the given type, variable or function as part of the
 *   exported interface of the library object under construction.
 *
 *   Correct:
 *       MONGO_COMPILER_API_EXPORT int globalSwitch;
 *       class MONGO_COMPILER_API_EXPORT ExportedType { ... };
 *       MONGO_COMPILER_API_EXPORT SomeType exportedFunction(...);
 *
 *   NOTE: Rather than using this macro directly, one typically declares another macro named
 *   for the library, which is conditionally defined to either MONGO_COMIPLER_API_EXPORT or
 *   MONGO_COMPILER_API_IMPORT based on whether the compiler is currently building the library
 *   or building an object that depends on the library, respectively.  For example,
 *   MONGO_FOO_API might be defined to MONGO_COMPILER_API_EXPORT when building the MongoDB
 *   libfoo shared library, and to MONGO_COMPILER_API_IMPORT when building an application that
 *   links against that shared library.
 *
 *
 * MONGO_COMPILER_API_IMPORT
 *
 *   Instructs the compiler to label the given type, variable or function as imported
 *   from another library, and not part of the library object under construction.
 *
 *   Same correct/incorrect usage as for MONGO_COMPILER_API_EXPORT.
 *
 *
 * MONGO_COMPILER_API_CALLING_CONVENTION
 *
 *    Explicitly decorates a function declaration the api calling convention used for
 *    shared libraries.
 *
 *    Same correct/incorrect usage as for MONGO_COMPILER_API_EXPORT.
 *
 *
 * MONGO_COMPILER_ALWAYS_INLINE
 *
 *    Overrides compiler heuristics to force that a particular function should always
 *    be inlined.
 *
 *
 * MONGO_COMPILER_UNREACHABLE
 *
 *    Tells the compiler that it can assume that this line will never execute. Unlike with
 *    MONGO_UNREACHABLE, there is no runtime check and reaching this macro is completely undefined
 *    behavior. It should only be used where it is provably impossible to reach, even in the face of
 *    adversarial inputs, but for some reason the compiler cannot figure this out on its own, for
 *    example after a call to a function that never returns but cannot be labeled with
 *    MONGO_COMPILER_NORETURN. In almost all cases MONGO_UNREACHABLE is preferred.
 *
 *
 * MONGO_COMPILER_NOINLINE
 *
 *    Tells the compiler that it should not attempt to inline a function.  This option is not
 *    guaranteed to eliminate all optimizations, it only is used to prevent a function from being
 *    inlined.
 *
 *
 * MONGO_WARN_UNUSED_RESULT_CLASS
 *
 *    Tells the compiler that a class defines a type for which checking results is necessary.  Types
 *    thus defined turn functions returning them into "must check results" style functions.  Preview
 *    of the `[[nodiscard]]` C++17 attribute.
 *
 *
 * MONGO_WARN_UNUSED_RESULT_FUNCTION
 *
 *    Tells the compiler that a function returns a value for which consuming the result is
 *    necessary.  Functions thus defined are "must check results" style functions.  Preview of the
 *    `[[nodiscard]]` C++17 attribute.
 *
 *
 * MONGO_COMPILER_RETURNS_NONNULL
 *
 *    Tells the compiler that the function always returns a non-null value, potentially allowing
 *    additional optimizations at call sites.
 *
 *
 * MONGO_COMPILER_MALLOC
 *
 *    Tells the compiler that the function is "malloc like", in that the return value points
 *    to uninitialized memory which does not alias any other valid pointers.
 *
 *
 * MONGO_COMPILER_ALLOC_SIZE(varindex)
 *
 *    Tells the compiler that the parameter indexed by `varindex`
 *    provides the size of the allocated region that a "malloc like"
 *    function will return a pointer to, potentially allowing static
 *    analysis of use of the region when the argument to the
 *    allocation function is a constant expression.
 */


#if defined(_MSC_VER)
#include "common/bson/compiler_msvc.h"
#elif defined(__GNUC__)
#include "common/bson/compiler_gcc.h"
#else
#error "Unsupported compiler family"
#endif

#endif