// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef SPINLOCK_H
#define SPINLOCK_H

#include <atomic>
#include <thread>
#include <chrono>

#include <immintrin.h>

// A variation of simple test and test and set lock with some backoff mechanism
// A thread alternates between spinning and backing off. 
class alignas(64) Spinlock {
    public:
        Spinlock() {
            locked_.store(false);
        }
  
    void lock() {
        // Keep trying
        while (true) {
            if (!locked_.exchange(true)) {
                return;
            }

            // simple adhoc backoff mechanism
            int counter = 0;
            while (locked_.load()) {
                if (counter < 100) {
                    _mm_pause();
                }
                else {
                    // alternate between busy waiting and sleeping
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
                    counter = 0;
                }
                counter++;
            }    
        }
    }

    // unlock
    void unlock() { 
        locked_.store(false); 
    }
    
    private:
        std::atomic<bool> locked_;
        char padding_[64 - sizeof(std::atomic<bool>)];

};

#endif