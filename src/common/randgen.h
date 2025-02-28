// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef RAND_GEN_H
#define RAND_GEN_H

#include <random>

class RandGen {
    public:
        static std::minstd_rand * getTLSInstance();
};

#endif