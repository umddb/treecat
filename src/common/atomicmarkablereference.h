// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef ATOMIC_MARKABLE_REFERENCE_H
#define ATOMIC_MARKABLE_REFERENCE_H

#include <atomic>
#include <iostream>
#include <cstdint> // For intptr_t

// AtomicMarkableReference implementation for lockfree skiplist
template <typename T>
class AtomicMarkableReference {
public:
    AtomicMarkableReference() : ref_mark_(0) { }

    // Constructor initializes the reference and mark
    AtomicMarkableReference(T* ref, bool mark) {
        // Pack the pointer and mark together
        set(ref, mark);
    }

    // Get the current reference and mark (using atomic load)
    T* get(bool& mark) const {
        uintptr_t packed = ref_mark_.load();
        // Extract the pointer and mark
        mark = extractMark(packed);
        return extractRef(packed);
    }

    // Get the reference only
    T* getReference() const {
        uintptr_t packed = ref_mark_.load();
        // Extract the pointer and mark
        return extractRef(packed);
    }

    bool getMark() const {
        uintptr_t packed = ref_mark_.load();
        return extractMark(packed);
    }

    // Set the reference and mark atomically
    void set(T* new_ref, bool new_mark) {
        // Pack the pointer and mark together
        uintptr_t packed = pack(new_ref, new_mark);
        ref_mark_.store(packed);
    }

    // Compare and set the reference and mark atomically (CAS)
    bool compareAndSet(T* expected_ref, T* new_ref, bool expected_mark, bool new_mark) {
        uintptr_t expected = pack(expected_ref, expected_mark);
        uintptr_t desired = pack(new_ref, new_mark);
        return ref_mark_.compare_exchange_strong(expected, desired);
    }

private:
    // A single atomic variable to store both the reference and the mark
    std::atomic<uintptr_t> ref_mark_;

    // Helper function to pack a pointer and a mark into a single uintptr_t
    uintptr_t pack(T* ref, bool mark) const {
        uintptr_t ref_val = reinterpret_cast<uintptr_t>(ref);
        return (ref_val & pointerMask) | (mark ? 1 : 0);
    }

    // Helper function to extract the reference from the packed value
    T* extractRef(uintptr_t packed) const {
        return reinterpret_cast<T*>(packed & pointerMask);
    }

    // Helper function to extract the mark from the packed value
    bool extractMark(uintptr_t packed) const {
        return packed & 1;
    }

    static constexpr uintptr_t pointerMask = ~static_cast<uintptr_t>(1);
};

#endif