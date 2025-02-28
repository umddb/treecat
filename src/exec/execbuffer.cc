// Author: Keonwoo Oh (koh3@umd.edu)

#include <endian.h>

#include <cstring>
#include <cstdlib>
#include <cstdint>

#include "exec/execbuffer.h"

ExecBuffer::ExecBuffer() : data_(nullptr), size_pos_(nullptr), data_pos_(nullptr), size_(0), capacity_(0), is_full_(true), ctrl_(false) {    }

ExecBuffer::ExecBuffer(char * data, uint32_t size, uint32_t capacity) : data_(data), size_(size), capacity_(capacity), ctrl_(false)  {
    size_pos_ =  &data_[size_ - sizeof(uint32_t)];
    data_pos_ = &data_[size_];
    if (size_ >= capacity) {
        is_full_ = true;
    }
    else {
        is_full_ = false;
    }
}

ExecBuffer::ExecBuffer(const char * data, uint32_t size, uint32_t capacity) : data_(const_cast<char*>(data)), size_(size), capacity_(capacity), ctrl_(false)  {
    size_pos_ =  &data_[size_ - sizeof(uint32_t)];
    data_pos_ = &data_[size_];
    if (size_ >= capacity) {
        is_full_ = true;
    }
    else {
        is_full_ = false;
    }
}

ExecBuffer::~ExecBuffer() { if (ctrl_) { free(data_); } }

bool ExecBuffer::init(size_t capacity) {
    ctrl_ = true;
    data_ = static_cast<char*>(malloc(capacity));
    if (data_ != nullptr) {
        size_pos_ = data_;
        memset(size_pos_, 0, sizeof(uint32_t));
        data_pos_ = &data_[sizeof(uint32_t)];
        size_ = sizeof(uint32_t);
        capacity_ = capacity;

        if (size_ >= capacity) {
            is_full_ = true;
        }
        else {
            is_full_ = false;
        }
        return true;
    }

    return false;
    
}

bool ExecBuffer::realloc(size_t capacity) {
    if (size_ > capacity || data_ == nullptr) {
        return false;
    }

    char* temp;
    temp = static_cast<char*>(::realloc(data_, capacity));
    if (temp != nullptr) {
        size_pos_ = &temp[(size_pos_- data_)];
        data_pos_ = &temp[(data_pos_- data_)];
        data_ = temp;
        capacity_ = capacity;
        return true;
    }
    
    if (size_ >= capacity) {
        is_full_ = true;
    }
    
    return false;

}

void ExecBuffer::setFull(bool is_full) {
    is_full_ = is_full;
}

char* ExecBuffer::data() {
    return data_;
}

size_t ExecBuffer::capacity() {
    return capacity_;
}

// size always includes the 4 bytes from the next element size 
size_t ExecBuffer::size() {
    return size_;
}

bool ExecBuffer::isFull() {
    return is_full_;
}

bool ExecBuffer::empty() {
    return (size_ <= sizeof(uint32_t));
}

void ExecBuffer::clear() {
    size_pos_ = data_;
    memset(size_pos_, 0, sizeof(uint32_t));
    data_pos_ = &data_[sizeof(uint32_t)];
    size_ = sizeof(uint32_t);

    if (size_ >= capacity_) {
        is_full_ = true;
    }
    else {
        is_full_ = false;
    }
    
}

void ExecBuffer::append(const char * data, uint32_t size) {
    uint32_t elem_size;
    // append the given data
    memcpy(data_pos_, data, size);
    // update the element size
    memcpy(&elem_size, size_pos_, sizeof(uint32_t));
    elem_size = le32toh(elem_size);
    elem_size += size;
    elem_size = htole32(elem_size);
    memcpy(size_pos_, &elem_size, sizeof(uint32_t));
    // move data position
    data_pos_ = &data_pos_[size];
    size_ += size;
    if (size_ >= capacity_) {
        is_full_ = true;
    }

}

void ExecBuffer::finalAppend(const char * data, uint32_t size) {
    uint32_t elem_size;
    // append the given data
    memcpy(data_pos_, data, size);
    // update the element size
    memcpy(&elem_size, size_pos_, sizeof(uint32_t));
    elem_size = le32toh(elem_size);
    elem_size += size;
    elem_size = htole32(elem_size);
    memcpy(size_pos_, &elem_size, sizeof(uint32_t));

    // move size position to next element
    size_pos_ = &data_pos_[size];
    // move data position
    data_pos_ = &data_pos_[size + sizeof(uint32_t)];
    size_ += (size + sizeof(uint32_t));

    if (size_ >= capacity_) {
        is_full_ = true;
    }
    else {
        memset(size_pos_, 0, sizeof(uint32_t));
    }

}

ExecBuffer::Iterator::Iterator(): buffer_(nullptr), elem_size_(0), data_idx_(0), next_(0), valid_(false) { }

ExecBuffer::Iterator::~Iterator() { }

void ExecBuffer::Iterator::reset(ExecBuffer * buffer) {
    buffer_ = buffer;
    memcpy(&elem_size_, buffer_->data_, sizeof(uint32_t));
    elem_size_ = le32toh(elem_size_);
    data_idx_ = sizeof(uint32_t);
    next_ = sizeof(uint32_t) + elem_size_;

    if (next_ > buffer_->size_) {
        valid_ = false;
    }
    else {
        valid_ = true;
    }

}

bool ExecBuffer::Iterator::next() {
    if (!valid_) {
        return false;
    }

    if (next_ + sizeof(uint32_t) >= buffer_->size_) {
        valid_ = false;
        return false;
    }

    memcpy(&elem_size_, &(buffer_->data_[next_]), sizeof(uint32_t));
    elem_size_ = le32toh(elem_size_);
    data_idx_ = next_ + sizeof(uint32_t);
    next_ = data_idx_ + elem_size_;

    if (next_ > buffer_->size_) {
        valid_ = false;
        return false;
    }
    else {
        valid_ = true;
        return true;
    }

}


char * ExecBuffer::Iterator::data() {
    return &(buffer_->data_[data_idx_]);
}


uint32_t ExecBuffer::Iterator::size() {
    return elem_size_;
}

bool ExecBuffer::Iterator::valid() {
    return valid_;
}





