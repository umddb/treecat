// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef PATH_H
#define PATH_H

#include <endian.h>
#include <stdlib.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <functional>

struct Path {
    char *data_;
    size_t size_;
    // whether data is controlled by the Path object
    bool ctrl_;
    
    // copy constructor
    Path(const Path& other) {
        size_ = other.size_;
        if (size_ == 0) {
            data_ = nullptr;
            ctrl_ = false;
        }
        else {
            data_ = static_cast<char*>(malloc(other.size_));
            memcpy(data_, other.data_, size_);
            ctrl_ = true; 
        }
    }

    // copy assignment
    Path& operator=(const Path& other) {
        if (this != &other) {
            if (other.size_ == 0) {
                size_ = 0;
                data_ = nullptr;
                ctrl_ = false;
            }
            else {
                if (ctrl_) {
                    if (size_ >= other.size_) {
                        size_ = other.size_;
                        memcpy(data_, other.data_, size_);
                    }
                    else {
                        free(data_);
                        size_ = other.size_;
                        data_ = static_cast<char*>(malloc(size_));
                        memcpy(data_, other.data_, size_);
                    }
                }
                else {
                    size_ = other.size_;
                    data_ = static_cast<char*>(malloc(size_));
                    memcpy(data_, other.data_, size_);
                    ctrl_ = true;
                }
            }
        }

        return *this;

    }
    
    Path() : data_(nullptr), size_(0), ctrl_(false) { }

    Path(std::string_view path) {
        size_ = sizeof(uint32_t) + path.size();
        ctrl_= true;
        data_ = static_cast<char*>(malloc(size_));
        // initialize path depth
        uint32_t depth = htole32(std::count(path.begin(), path.end(), '/'));
        memcpy(data_, &depth, sizeof(uint32_t));
        // copy path string, excluding null terminator
        memcpy(&data_[sizeof(uint32_t)], path.data(), path.size());
    }

    Path(const char* data, size_t size, bool ctrl = true) : data_(const_cast<char*>(data)), size_(size), ctrl_(ctrl) {
        if (ctrl_) {
            char * temp =  static_cast<char*>(malloc(size_));
            memcpy(temp, data_,size_);
            data_ = temp;
        }
    }

    // move constructor
    Path(Path && other) : data_(other.data_), size_(other.size_), ctrl_(other.ctrl_) {
        other.ctrl_ = false;
    }

    // move assignment
    Path& operator=(Path&& other) {
        if (this != &other) {
            if (ctrl_) {
                free(data_);
            }
            data_ = other.data_;
            size_ = other.size_;
            ctrl_ = other.ctrl_;
            other.ctrl_ = false;

        }

        return *this;
        
    }

    //blindly concatenates other to the path
    void concat(const std::string & other) {
        size_t temp_size = size_ + other.size();
        char * temp_data = static_cast<char*>(malloc(temp_size));
        //copy both path and other to temp buffer
        memcpy(temp_data, data_, size_);
        memcpy(&temp_data[size_], other.data(), other.size());
        
        //update depth
        uint32_t depth;
        uint32_t add_depth = std::count(other.begin(), other.end(), '/');
        memcpy(&depth, data_, sizeof(uint32_t));
        depth = le32toh(depth);
        depth += add_depth;
        depth = htole32(depth);
        memcpy(temp_data, &depth, sizeof(uint32_t));

        if (ctrl_) {
            free(data_);
        }

        data_ = temp_data;
        size_ = temp_size;
        ctrl_ = true;

    }

    ~Path()  {
        if (ctrl_) {
            free(data_);
        }
    }

    Path parent() {
        Path parent_path;
        std::string_view path_str(data_, size_);
        size_t last_slash = path_str.find_last_of('/');
        if (last_slash > sizeof(uint32_t)) {
            parent_path.data_ = static_cast<char*>(malloc(last_slash));
            parent_path.size_ = last_slash;
            parent_path.ctrl_ = true;
            memcpy(parent_path.data_, data_, last_slash);
            
            //overwrite the depth
            uint32_t depth;
            memcpy(&depth, data_, sizeof(uint32_t));
            depth = le32toh(depth);
            depth--;
            depth = htole32(depth);
            memcpy(parent_path.data_, &depth, sizeof(uint32_t));
        }
        //execptional case for root
        else {
            parent_path.data_ = static_cast<char*>(malloc(last_slash + 1));
            parent_path.size_ = last_slash + 1;
            parent_path.ctrl_ = true;
            memcpy(parent_path.data_, data_, last_slash + 1);
        }
     
        return parent_path;
    }

    bool hasPrefix(const Path & prefix) const {
        if (prefix.size_ > size_) {
            return false;
        }
        return (memcmp(prefix.data_, data_, prefix.size_) == 0);
    }

    std::string toString() const {
        return std::string(&data_[sizeof(uint32_t)], size_ - sizeof(uint32_t));
    }

    uint32_t depth() const {
        uint32_t depth;
        memcpy(&depth, data_, sizeof(uint32_t));
        return le32toh(depth);
    }

    int compare(const Path &other) const {
        return compare(data_, other.data_, size_, other.size_);
    }

    static int compare(const char *a_data, const char *b_data, int a_size, int b_size) {
        uint32_t a_depth, b_depth;
        memcpy(&a_depth, a_data, sizeof(uint32_t));
        memcpy(&b_depth, b_data, sizeof(uint32_t));
        a_depth = le32toh(a_depth);
        b_depth = le32toh(b_depth);

        if (a_depth < b_depth) {
            return -1;
            
        }
        else if (a_depth > b_depth) {
            return 1;
             
        }

        const size_t min_len = (a_size < b_size) ? a_size : b_size;
        int r = memcmp(&a_data[sizeof(uint32_t)], &b_data[sizeof(uint32_t)], min_len - sizeof(uint32_t));
        if (r == 0) {
            if (a_size < b_size) {
                r = -1;
            }    
            else if (a_size > b_size) {
                r = 1;
            }
        }
        
        return r;
    }

    bool operator==(const Path &other) const { 
        return (size_ == other.size_ && !memcmp(data_ ,other.data_,size_));
    }

    bool operator<(const Path &other) const { 
        return (compare(other) < 0);
    }
     

}; 

template <>
struct std::hash<Path>
{
  std::size_t operator()(const Path& path) const {
    return std::hash<string_view>()(std::string_view(path.data_, path.size_));       
  }
};

inline std::size_t hash_value(Path const &path)
{
    return std::hash<std::string_view>()(std::string_view(path.data_, path.size_));
}

#endif // PATH_H