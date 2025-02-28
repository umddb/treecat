// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef LF_SKIPLIST_H
#define LF_SKIPLIST_H

#include <atomic>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <chrono>

#include <immintrin.h>

#include "common/atomicmarkablereference.h"
#include "common/randgen.h"

enum class LFSLStatus
{
    NONE = 0x00,
    SUCCESS = 0x01,
    WAS_HEAD = 0x02,
    WAS_TAIL = 0x04
};

inline LFSLStatus operator | (LFSLStatus lhs, LFSLStatus rhs)
{
    using T = std::underlying_type_t <LFSLStatus>;
    return static_cast<LFSLStatus>(static_cast<T>(lhs) | static_cast<T>(rhs));
}
    
inline LFSLStatus& operator |= (LFSLStatus& lhs, LFSLStatus rhs)
{
    lhs = lhs | rhs;
    return lhs;
}

inline LFSLStatus operator & (LFSLStatus lhs, LFSLStatus rhs)
{
    using T = std::underlying_type_t <LFSLStatus>;
    return static_cast<LFSLStatus>(static_cast<T>(lhs) & static_cast<T>(rhs));
}
    
inline LFSLStatus& operator &= (LFSLStatus& lhs, LFSLStatus rhs)
{
    lhs = lhs & rhs;
    return lhs;
}

/* Custom lock free skiplist per Herlihy et al's implementation. Unlike typical 
 * lockfree skiplist, duplciate entries are allowed, but only the first (latest) 
 * entry is searchable and removable (LIFO search and removal). Memory reclamation 
 * is epoch based. For our purpose, threads that add element does not get involved 
 * in memory reclamation process (and hence does not snip any node), unless necessary,
 * as they operate in critical section. Only threads (garbage collector) that 
 * remove any nodes will be involved in snipping them and reclaiming memory.
 */
template<typename K, typename V>
class LFSkipList {
    
    public:
        // thread context necessary for memory reclamation
        struct ThreadContext;
        struct Node;
        class Iterator;
        
        static constexpr int32_t maxHeight = 12;
        static constexpr int32_t branching = 4;
        // there is a single thread context per skiplist instance and thread instance
        static thread_local std::unordered_map<LFSkipList<K,V>*, ThreadContext*> self;
        // public as thread has to add its context to the list itself. 
        std::vector<ThreadContext*> thread_contexts_;
        std::shared_mutex thread_contexts_mtx_;
        struct ThreadContext {
            std::vector<Node*> pending_reclaims_;
            // counter for keeping track of epochs
            std::atomic<uint64_t> counter_;

            ThreadContext(LFSkipList<K,V> * skip_list) {
                counter_.store(0);
                // add oneself to thread_contexts vector
                std::unique_lock lock(skip_list->thread_contexts_mtx_);
                skip_list->thread_contexts_.push_back(this);
            }
        };

        struct Node {
            Node(const K & key, const V & value);
            Node(const K & key, V && value);
            Node(K && key, const V & value);
            Node(K && key, V && value);
            void setHeight(int32_t height);

            int32_t height_;
            K key_;
            V value_;
            std::atomic<int32_t>ref_count_;
            AtomicMarkableReference<Node> next_[maxHeight];
                
        };

        /*
         * Iterator for the lockfree skiplist. Every entry is visible, including duplicate 
         * entries. In our use case of iterators, we don't expect to insert any duplicate 
         * entry so this is fine.    
         */
        class Iterator {
            public:
                Iterator(LFSkipList<K,V> * skip_list);
                ~Iterator();
                void seek(const K& key);
                bool next();
                // if a node is marked, the iterator is no longer valid. This should be fine
                // as nodes are removed so that they are not in the middle of valid range 
                // of values scanned by the iterator
                bool isValid();
                void deactivate();
                // current key
                const K& key();
                // current value
                const V& value();
            private:
                LFSkipList<K,V> * skip_list_;
                // current node;
                Node * curr_node_;
                Node * preds_[maxHeight];
                Node * succs_[maxHeight];
                bool active_;

        };

        LFSkipList(Node * head, Node * tail);
        ~LFSkipList();
        // create thread context and register the thread for memory management
        void registerThread();
        bool head(K & key, V & value);
        LFSLStatus get(const K & key, V & value);
        LFSLStatus tryGet(const K & key, K & real_key, V & value);
        LFSLStatus insert(const K & key, const V & value);
        LFSLStatus insert(Node * new_node);
        LFSLStatus remove(const K & key);
        std::vector<LFSLStatus> bulkRemove(const std::vector<K> & keys);
        Iterator * newIterator();

    private:
        Node * head_;
        Node * tail_;
        LFSLStatus removeImpl(const K & key);
        // find first instance, snipping marked nodes in the way
        bool findFirst(const K & key, Node ** preds, Node ** succs, int32_t bottom_level = 0);
        // find first instance, don't snip marked nodes
        bool searchFirst(const K & key, Node ** preds, Node ** succs, int32_t bottom_level = 0);
        int32_t randomLevel();
        void beginOp();
        void schedForReclaim(Node * node);
        void endOp();
        void waitUntilUnreserved();

};

template<typename K, typename V>
thread_local std::unordered_map<LFSkipList<K,V>*, typename LFSkipList<K, V>::ThreadContext*> LFSkipList<K,V>::self = 
    std::unordered_map<LFSkipList<K,V>*, typename LFSkipList<K, V>::ThreadContext*>();

// Multiple constructors to support move semantics since path cannot be copy constructed...
template<typename K, typename V>
LFSkipList<K,V>::Node::Node(const K & key, const V & value) : height_(0), 
    key_(key), value_(value) {
    ref_count_.store(0);
}

template<typename K, typename V>
LFSkipList<K,V>::Node::Node(const K & key, V && value) : height_(0), 
    key_(key), value_(std::move(value)) {
    ref_count_.store(0);
}

template<typename K, typename V>
LFSkipList<K,V>::Node::Node(K && key, const V & value) : height_(0), 
    key_(std::move(key)), value_(value) {
    ref_count_.store(0);
}

template<typename K, typename V>
LFSkipList<K,V>::Node::Node(K && key, V && value) : height_(0), 
    key_(std::move(key)), value_(std::move(value)) {
    ref_count_.store(0);
}

template<typename K, typename V>
void LFSkipList<K,V>::Node::setHeight(int32_t height) {
    height_ = height;
    ref_count_.store(height);
}

// Iterator for the skiplist. It does not get invalidated when the skiplist 
// is modified, which is perfectly fine for our use case. 
template<typename K, typename V>
LFSkipList<K,V>::Iterator::Iterator(LFSkipList<K,V> * skip_list): skip_list_(skip_list),
    curr_node_(nullptr), active_(false) { }

template<typename K, typename V>
LFSkipList<K,V>::Iterator::~Iterator() {
    deactivate();
}

template<typename K, typename V>
void LFSkipList<K,V>::Iterator::seek(const K & key) {
    if (active_) {
        skip_list_->endOp();
    }

    active_ = true;
    skip_list_->beginOp();

    skip_list_->searchFirst(key, preds_, succs_, 0);

    curr_node_ = succs_[0];
}

template<typename K, typename V>
bool LFSkipList<K,V>::Iterator::next() {
    bool marked;
    Node * succ = curr_node_->next_[0].getReference();

    if (succ == nullptr) {
        return false;    
    }

    curr_node_ = succ;
    succ = curr_node_->next_[0].get(marked);

    while (marked) {
        curr_node_ = succ;
        succ = curr_node_->next_[0].get(marked);
    }

    return true;

}

template<typename K, typename V>
bool LFSkipList<K,V>::Iterator::isValid() {
    return active_ && (curr_node_ != skip_list_->tail_);
}


template<typename K, typename V>
void LFSkipList<K,V>::Iterator::deactivate() {
    if (active_) {
        skip_list_->endOp();
        active_ = false;
    }
}

template<typename K, typename V>
const K& LFSkipList<K,V>::Iterator::key() {
    return curr_node_->key_;
}

template<typename K, typename V>
const V& LFSkipList<K,V>::Iterator::value() {
    return curr_node_->value_;
}

template<typename K, typename V>
LFSkipList<K,V>::LFSkipList(Node * head, Node * tail) : head_(head), tail_(tail) {
    head_->height_ = maxHeight;
    tail_->height_ = maxHeight;

    for (int32_t i = 0; i < maxHeight; i++) {
        head_->next_[i].set(tail_, false); 
    }

    for (int32_t i = 0; i < maxHeight; i++) {
        tail_->next_[i].set(nullptr, false); 
    }

}

// TODO: maybe fix this so we don't have memory leaks
template<typename K, typename V>
LFSkipList<K,V>::~LFSkipList() {
    delete head_;
    delete tail_;
}

// this will result in memory leaks, but we have a fixed number of threads
// accessing a few number of skiplists 
template<typename K, typename V>
void LFSkipList<K,V>::registerThread() {
    if (!self.contains(this)) {
        self[this] = new ThreadContext(this);
    }
    
}


template<typename K, typename V>
bool LFSkipList<K,V>::head(K & key, V & value) {
    beginOp();

    bool marked;
    Node * curr_node = head_->next_[0].getReference();
    Node * succ = curr_node->next_[0].get(marked);

    while (marked) {
        curr_node = succ;
        succ = curr_node->next_[0].get(marked);
    }

    if (curr_node == tail_) {
        endOp();
        return false;
    }

    key = curr_node->key_;
    value = curr_node->value_;

    endOp();

    return true;

}


template<typename K, typename V>
LFSLStatus LFSkipList<K,V>::get(const K & key, V & value) {
    Node * preds[maxHeight];
    Node * succs[maxHeight];
    LFSLStatus status = LFSLStatus::NONE;
    
    beginOp();

    if (searchFirst(key, preds, succs, 0)) {
        value = succs[0]->value_;
        status |= LFSLStatus::SUCCESS;
    }

    endOp();

    return status;
}

template<typename K, typename V>
LFSLStatus LFSkipList<K,V>::tryGet(const K & key, K & real_key, V & value) {
    Node * preds[maxHeight];
    Node * succs[maxHeight];
    LFSLStatus status = LFSLStatus::NONE;
    
    beginOp();

    if (searchFirst(key, preds, succs, 0)) {
        status |= LFSLStatus::SUCCESS;
    }

    real_key = succs[0]->key_;
    value = succs[0]->value_;

    endOp();

    return status;
}




template<typename K, typename V>
LFSLStatus LFSkipList<K,V>::insert(const K & key, const V & value) {
    return insert(new Node(key, value));
}

template<typename K, typename V>
LFSLStatus LFSkipList<K,V>::insert(Node * new_node) {
    new_node->setHeight(randomLevel());
    Node * preds[maxHeight];
    Node * succs[maxHeight];
    LFSLStatus status = LFSLStatus::NONE;

    beginOp();

    int count = 0;
    while(true) {
        // first 2 times, don't snip marked nodes to prevent memory reclamation
        if (count < 2) {
            searchFirst(new_node->key_, preds, succs, 0);
        }
        // if pred is found to be marked multiple times, there is a chance
        // that remove() function is blocked. Snip the marked nodes oneself
        else {
            findFirst(new_node->key_, preds, succs, 0);
        }
        
        Node * pred = preds[0];
        Node * succ = succs[0];
        new_node->next_[0].set(succ, false);
        if (!pred->next_[0].compareAndSet(succ, new_node, false, false)) {
            count++;
            continue;
        }
        for (int32_t level = 1; level < new_node->height_; level++) {
            count = 0;
            while(true) {
                pred = preds[level];
                succ = succs[level];
                // Herlihy et al.'s implementation has a bug
                new_node->next_[level].set(succ, false);
                if (pred->next_[level].compareAndSet(succ, new_node, false, false)) {
                    break;
                }
                if (count < 2) {
                    searchFirst(new_node->key_, preds, succs, level);
                }
                else {
                    findFirst(new_node->key_, preds, succs, level);
                }
                count++;
            }
                        
        }

        if (preds[0] == head_) {
            status |= LFSLStatus::WAS_HEAD; 
        }
        if (succs[0] == tail_) {
            status |= LFSLStatus::WAS_TAIL;
        }
        status |= LFSLStatus::SUCCESS;
        break;

    }

    endOp();

    return status;
}

// NOTE that a thread may fail to remove a key even if it is there
// because some other thread removed the particular node. 
// This could possibly be problematic when there are duplicate keys, but
// in our case, there is only one garbage collector thread removing all 
// relevant entries. 
template<typename K, typename V>
LFSLStatus LFSkipList<K,V>::remove(const K & key) {
    beginOp();
    LFSLStatus status = removeImpl(key);
    endOp();

    return status;
}

template<typename K, typename V>
std::vector<LFSLStatus> LFSkipList<K,V>::bulkRemove(const std::vector<K> & keys) {
    std::vector<LFSLStatus> statuses;
    beginOp();
    for (auto & key : keys) {
        statuses.push_back(removeImpl(key)); 
    }
    endOp();

    return statuses;
}

template<typename K, typename V>
LFSkipList<K,V>::Iterator * LFSkipList<K,V>::newIterator() {
    return new Iterator(this);
}

template<typename K, typename V>
LFSLStatus LFSkipList<K,V>::removeImpl(const K & key) {
    Node * preds[maxHeight];
    Node * succs[maxHeight];
    Node * node_to_remove, * succ;
    LFSLStatus status = LFSLStatus::NONE;

    // if key is found, try to remove
    if (searchFirst(key, preds, succs, 0)) {
        node_to_remove = succs[0];
        // mark at every level except the bottom
        for (int32_t level = node_to_remove->height_ - 1; level > 0; level--) {
            bool marked = false;
            succ = node_to_remove->next_[level].get(marked);
            while (!marked) {
                node_to_remove->next_[level].compareAndSet(succ, succ, false, true);
                succ = node_to_remove->next_[level].get(marked);
            }
        }
        // finally mark the bottom level
        bool marked = false;
        succ = node_to_remove->next_[0].get(marked);
        while (!marked) {
            bool i_marked_it = node_to_remove->next_[0].compareAndSet(succ, succ, false, true);
            succ = node_to_remove->next_[0].get(marked);
            // since this thread marked it, it also tries to snip it
            if (i_marked_it) {
                findFirst(key, preds, succs, 0);
                status |= LFSLStatus::SUCCESS;
            }
        }

        if (preds[0] == head_) {
            status |= LFSLStatus::WAS_HEAD; 
        }
        if (succs[0] == tail_) {
            status |= LFSLStatus::WAS_TAIL;
        }
    }

    return status;
}

template<typename K, typename V>
bool LFSkipList<K,V>::findFirst(const K & key, Node ** preds, Node ** succs, int32_t bottom_level) {
    bool marked = false, snip = false;
    Node * pred = nullptr, *curr = nullptr, *succ = nullptr;
    
    retry:
        pred = head_;
        for (int32_t level = maxHeight - 1; level >= bottom_level; level--) {
            curr = pred->next_[level].getReference();
            while (true) {
                succ = curr->next_[level].get(marked); 
            
                while(marked) {
                    snip = pred->next_[level].compareAndSet(curr, succ, false, false);
                    // snipping failed, try again!
                    if (!snip) {
                        goto retry;
                    }
                    // when ref_count_ reaches 0, snipped at all levels, so 
                    // safe to schedule for reclaim
                    if (curr->ref_count_.fetch_sub(1) == 1) {
                        schedForReclaim(curr);
                    }
                    curr = pred->next_[level].getReference();
                    succ = curr->next_[level].get(marked);
                }
                    
                if (curr->key_ < key) {
                    pred = curr;
                    curr = succ;
                }
                else {
                    break;
                }
            }

            preds[level] = pred;
            succs[level] = curr;

        }
    return (curr->key_ == key);    

}

template<typename K, typename V>
bool LFSkipList<K,V>::searchFirst(const K & key, Node ** preds, Node ** succs, int32_t bottom_level) {
    bool marked = false;
    Node * pred = nullptr, *curr = nullptr, *succ = nullptr;
    
    pred = head_;
    for (int32_t level = maxHeight - 1; level >= bottom_level; level--) {
        curr = pred->next_[level].getReference();
        while (true) {
            succ = curr->next_[level].get(marked);
            while(marked) {
                // Herlihy et al.'s implementation has a bug
                curr = succ;
                succ = curr->next_[level].get(marked);
            }
                
            if (curr->key_ < key) {
                pred = curr;
                curr = succ;
            }
            else {
                break;
            }
        }

        preds[level] = pred;
        succs[level] = curr;

    }
    return (curr->key_ == key);    

}

template<typename K, typename V>
int32_t LFSkipList<K,V>::randomLevel() {
    std::minstd_rand * rand_gen = RandGen::getTLSInstance();
    static thread_local std::uniform_int_distribution<int32_t> dist(1, branching);
    
    int32_t height = 1;
    while (height < maxHeight && (dist(*rand_gen)) < 2) {
        height++;
    }

    return height;
} 

template<typename K, typename V>
void LFSkipList<K,V>::beginOp() {
    self[this]->counter_.fetch_add(1);
}

template<typename K, typename V>
void LFSkipList<K,V>::schedForReclaim(Node * node){
    self[this]->pending_reclaims_.push_back(node);
}

template<typename K, typename V>
void LFSkipList<K,V>::endOp() {
    ThreadContext * thread_context = self[this];
    thread_context->counter_.fetch_add(1);
    if (thread_context->pending_reclaims_.empty()) {
        return;
    }
    waitUntilUnreserved();
    for (auto & reclaim: thread_context->pending_reclaims_) {
        delete reclaim;
    }   
    thread_context->pending_reclaims_.clear();
}

template<typename K, typename V>
void LFSkipList<K,V>::waitUntilUnreserved() {
    // get the epochs/counters for all threads before waiting one by one.
    // shared mtx should be fine as the only time exclusive mtx is required is
    // when registering threads
    std::shared_lock lock(thread_contexts_mtx_);
    std::vector<uint64_t> counters;
    counters.reserve(thread_contexts_.size());
    for (auto & thread_context : thread_contexts_ ) {
        counters.push_back(thread_context->counter_.load());
    }

    for (size_t i = 0; i < counters.size(); i++) {
        if (counters[i] % 2 == 1) {
            int spin_count = 0;
            //spin wait while epoch has not been passed
            while (thread_contexts_[i]->counter_.load() == counters[i]) {
                if (spin_count < 500) {
                    _mm_pause();
                    spin_count++;
                } 
                else {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
        }
    }   
}



#endif