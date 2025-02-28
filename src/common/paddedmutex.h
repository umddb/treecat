// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef PADDED_MUTEX_H
#define PADDED_MUTEX_H

#include <mutex>

struct alignas(64) PaddedMutex {
    std::mutex mtx_;
    // Padding to ensure no other data shares this cache line
    char padding_[64 - sizeof(std::mutex)];
};

#endif