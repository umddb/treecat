// Author: Keonwoo Oh (koh3@umd.edu)

#include "common/randgen.h"

std::minstd_rand * RandGen::getTLSInstance() {
    static thread_local std::random_device rd;
    static thread_local std::minstd_rand rand_gen(rd());
    return &rand_gen;
}

