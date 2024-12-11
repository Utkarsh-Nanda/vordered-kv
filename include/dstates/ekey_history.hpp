
#ifndef __EKEY_HISTORY_T
#define __EKEY_HISTORY_T

#include "marker.hpp"
#include "key_info.hpp"
#include <atomic>
#include <vector>
#include <algorithm>
#include <shared_mutex>

template <class V>
class ekey_history_t {
    static const size_t BLOCK_SIZE = 128;

    struct entry_t {
        int ts;
        V val;
        bool marked = false;
        // entry_t() : ts(-1), val(marker_t<V>::low_marker) {}
    };

    struct block_index_entry {
        int timestamp; // starting timestamp of this block
        std::vector<entry_t> entries;
        block_index_entry(int ts) : timestamp(ts), entries(BLOCK_SIZE) {}
    };

    // Rather than having a pair of timestamps, it's a vector of structs having timestamp and vector of entries.
    std::vector<block_index_entry> block_index;
    std::shared_mutex block_index_mutex;
    std::atomic<size_t>tail{0}, pending{0}; // has to be atomic, becuase it is shared among threads.
    int block_len = 0;
    int max_timestamp = -1;
    std::function<int()> tag_function;
public:
    key_info_t info;


    ekey_history_t(std::function<int()> tag_fn = nullptr) : pending(0), tag_function(tag_fn) {
        // unique lock cause the mutex is not shared with other threads, we are adding a new block.
        std::unique_lock lock(block_index_mutex);
        block_index.emplace_back(0); // First block starts with timestamp 0, but no entry has been made yet.
        block_len = 1;
    }

    void set_tag_function(std::function<int()> tag_fn) {
        tag_function = tag_fn;
    }

    void insert(const V &v, bool flag)
    {
        size_t new_slot;
        int t = 0;
        auto &last_block = block_index.back();
        {
            std::unique_lock lock(block_index_mutex);
            if (flag) {
                t = tag_function();
            }
            new_slot = pending.fetch_add(1); // Atomically increment pending and get the previous value
        }
        
        int index = new_slot % BLOCK_SIZE;      // Calculate the index within the block based on pending
        int block_number = new_slot / BLOCK_SIZE;

        if (new_slot >= block_len * BLOCK_SIZE)
        {
            std::unique_lock lock(block_index_mutex);
            for (int i = block_len; i <= block_number; i++)
            {
                block_index.emplace_back(100000000);
                block_len++;
            }
        }

        block_index[block_number].timestamp = std::min(last_block.timestamp, t); // it might not happen that the first entry will reach the block first.
        block_index[block_number].entries[index].ts = t;
        block_index[block_number].entries[index].val = v;
        block_index[block_number].entries[index].marked = true;
    }

    void remove(int t) {
        insert(t, marker_t<V>::low_marker);
    }



    V find(int t){
    
    // If block_index is empty, return low marker
    if (block_index.empty())
        return marker_t<V>::low_marker;

    size_t current_tail = tail.load(); // points to the one more than the last marked entry.

    while (true) {
        size_t block_number = current_tail / BLOCK_SIZE;
        size_t index_in_block = current_tail % BLOCK_SIZE;

        if (block_number >= block_len) {
            break; // No more blocks
        }

        const auto& current_block = block_index[block_number];
        if (index_in_block >= current_block.entries.size()) {
            break; // Reached the end of entries
        }

        const auto& entry = current_block.entries[index_in_block];

        if (entry.ts <= t && entry.marked) {
            size_t expected = current_tail;
            if (tail.compare_exchange_weak(expected, current_tail + 1)) {
                ++current_tail;
                continue; // Proceed to the next entry
            } else {
                current_tail = tail.load(); // Reload if CAS failed
            }
        } else {
            break; // Entry doesn't satisfy conditions
        }
    }
    
    if (current_tail == 0)
        return marker_t<V>::low_marker;
    current_tail -= 1; // points to the last marked entry
    size_t block_number = current_tail / BLOCK_SIZE;
    int index_in_block = current_tail % BLOCK_SIZE;

    const auto& last_block = block_index[block_number];
    const auto& tail_entry = last_block.entries[index_in_block];
    // If the timestamp is the last valid entry
    if (tail_entry.marked && tail_entry.ts <= t) {
        return tail_entry.val;
    }

    auto block_it = std::upper_bound(block_index.begin(), block_index.end(), t,
        [](int timestamp, const block_index_entry& entry) {
            return timestamp < entry.timestamp;
        }
    );

    if (block_it != block_index.begin())
        --block_it;
    else
        return marker_t<V>::low_marker;

    // Now search within the block
    const auto& target_block = *block_it;
    const auto& entries = target_block.entries;
    

    // Find the entry with greatest timestamp <= t
    auto entry_it = std::upper_bound(entries.begin(), entries.end(), t,
        [](int timestamp, const entry_t& entry) {
            return timestamp < entry.ts;
        }
    );
    
    if (entry_it != entries.begin())
        --entry_it;
    else
        return marker_t<V>::low_marker;
    // Check if the entry is marked and timestamp <= t
    if (entry_it->marked && entry_it->ts <= t) {
        return entry_it->val;
    }

    return marker_t<V>::low_marker;
}


void copy_to(std::vector<std::pair<int, V>>& result) {
    std::shared_lock lock(block_index_mutex);
    if (block_index.empty()) {
        return;
    }
    size_t total_entries = pending.load();
    size_t current_tail = tail.load();

    for (size_t idx = 0; idx < total_entries; idx++) {
        size_t block_number = idx / BLOCK_SIZE;
        size_t index_in_block = idx % BLOCK_SIZE;

        if (block_number >= block_len) 
            break; // No more blocks

        const auto& current_block = block_index[block_number];
        const auto& entry = current_block.entries[index_in_block];

        if (idx < current_tail) {
            // Entries before tail: simply emplace into result
            if (entry.marked) {
                result.emplace_back(entry.ts, entry.val);
            }
        } else {
            // Entries at or after tail
            if (entry.marked) {
                size_t expected_tail = current_tail;
                if (tail.compare_exchange_weak(expected_tail, current_tail + 1)) {
                    ++current_tail;
                    result.emplace_back(entry.ts, entry.val);
                } else {
                    current_tail = tail.load();
                }
            } else 
                break;
        }
    }
}


    void cleanup() {
        std::unique_lock lock(block_index_mutex);
        block_index.clear();
        // block_index.emplace_back(std::numeric_limits<int>::min());
        pending.store(0);
        tail.store(0);
    }

    size_t size() {
        return pending.load();
    }

};

#endif // __EKEY_HISTORY_T
