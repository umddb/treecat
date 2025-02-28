// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef EXEC_BUFFER_H 
#define EXEC_BUFFER_H

#include <cstddef>
#include <cstdint>

class ExecBuffer {
    public:

        class Iterator {
            public:
                Iterator();
                ~Iterator();
                void reset(ExecBuffer * buffer);
                bool next();
                uint32_t size();
                char * data();
                bool valid();


            private:
                ExecBuffer* buffer_;
                uint32_t elem_size_;
                uint32_t data_idx_;
                uint32_t next_;
                bool valid_;
        };


        ExecBuffer();
        ExecBuffer(char * data, uint32_t size, uint32_t capacity);
        ExecBuffer(const char * data, uint32_t size, uint32_t capacity);

        ~ExecBuffer();
        bool init(size_t capacity);
        bool realloc(size_t capacity);
        bool isFull();
        bool empty();
        void setFull(bool is_full);
        void clear();
        char* data();
        size_t size();
        size_t capacity();
        // precondition: check the free space!
        void append(const char * data, uint32_t size);
        // precondition: check the free space!
        void finalAppend(const char * data, uint32_t size);


    private:
        char * data_;
        char * size_pos_;
        char * data_pos_;
        // always accounts for the last size element, so it can actually be bigger than capacity
        size_t size_;
        size_t capacity_;
        bool is_full_;
        bool ctrl_;
        
};

#endif // EXEC_BUFFER_H