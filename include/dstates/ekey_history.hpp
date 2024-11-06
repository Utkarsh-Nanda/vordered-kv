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
    static const size_t BLOCK_SIZE = 10;

    struct entry_t {
        int ts;
        V val;
        bool marked = false;
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
    int block_len;
    int max_timestamp = -1;
public:
    key_info_t info;


    ekey_history_t() : pending(0) {
        // unique lock cause the mutex is not shared with other threads, we are adding a new block.
        std::unique_lock lock(block_index_mutex);
        block_index.emplace_back(100000000); // First block starts with timestamp 0, but no entry has been made yet.
        block_len = 1;
    }

    // Function to insert a new entry with timestamp 't' and value 'v'
    void insert(int t, const V &v)
    {

        auto &last_block = block_index.back();
        size_t new_slot = pending.fetch_add(1); // Atomically increment pending and get the previous value
        int index = new_slot % BLOCK_SIZE;      // Calculate the index within the block based on pending
        // int sz = block_index.size();
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
        max_timestamp = std::max(max_timestamp, t);
        block_index[block_number].entries[index].marked = true;
    }

    void remove(int t) {
        insert(t, marker_t<V>::low_marker);
    }

    V find(int t){
    // Take a shared lock since we're only reading
    std::shared_lock lock(block_index_mutex);

    // If block_index is empty, return low marker
    if (block_index.empty())
        return marker_t<V>::low_marker;

    size_t current_tail = tail.load();
    size_t block_number = current_tail / BLOCK_SIZE;

    // Update tail by checking consecutive marked entries
    int counter = 1;
    while (true) {
        const auto& current_block = block_index[block_number];
        bool flag = false;
        while(current_block.entries[current_tail % BLOCK_SIZE].ts <= t && current_block.entries[current_tail % BLOCK_SIZE].marked) {
                //std::cout << "Counter : " << counter++ << "\n\n";

            tail.compare_exchange_weak(current_tail, current_tail + 1);
            current_tail = tail.load();
            if (current_tail % BLOCK_SIZE == 0) {
                block_number++;
                flag = true;
                break;
            }
        }
        if (flag == false){
            //std::cout << "flag == false, : " << tail.load() << "\n\n";
            break;
        }
    }
    std::cout << "Tail after going through the increase in tail : " << tail.load() << "\n\n";
    current_tail = tail.load() - 1; // tail points to one index ahead of the last marked entry
    int index_in_block = current_tail % BLOCK_SIZE;
    const auto& last_block2 = block_index[block_number];
    const auto& tail_entry2 = last_block2.entries[index_in_block];

    // If the requested timestamp is greater than or equal to tail's timestamp (last marked entry)
    if (t >= tail_entry2.ts && tail_entry2.marked)
    {
            std::cout << "timestamp greater than tail's timestamp\n\n";
            return tail_entry2.val;
    }
     // Binary search to find the correct block
    auto block_it = std::lower_bound(block_index.begin(), block_index.begin() + block_number + 1, t,
        [](const block_index_entry& entry, int timestamp) {
            return entry.timestamp < timestamp;
        }
    );

    // If t is greater than the starting timestamp of current block.
    if (block_it != block_index.begin() && (*block_it).timestamp > t)
        --block_it;

    std::cout << "Timestamp of Block we are searching in : " << block_it->timestamp << "\n\n";
    // Binary search within the block
    const auto& target_block = *block_it;
    // the ternary operator is used to check if the block is the last block or the previous of it.
    // If last then the size of the block is the current tail % BLOCK_SIZE + 1, else it is BLOCK_SIZE.
    std::cout << "Passed target_block\n";
    std::cout << "Doing binary search of entries of the block : " << target_block.timestamp << "\n";

    return marker_t<V>::low_marker;
    auto entry_it = std::lower_bound(
        target_block.entries.begin(),
        target_block.entries.begin() + (block_it == block_index.begin() + block_number ?
                                      current_tail % BLOCK_SIZE + 1 : BLOCK_SIZE),
        t,
        [](const entry_t& entry, int timestamp) {
            return entry.ts < timestamp;
        }
    );

    if (entry_it == target_block.entries.end()){
            std::cout << "end found.\n";
            return marker_t<V>::low_marker;

    }
            //else
                    //std::cout << " entry >= the timestamp " << entry_it->ts << "\n\n";
    // If exact match found till the tail, return the value
    if (entry_it != target_block.entries.end() && entry_it->ts == t)
    {
        std::cout << "Exact match found, returned : " << entry_it->val << "\n\n";
        return entry_it->val;
    }

    // If no exact match, get previous valid entry
    // If the t is smaller than the first timestamp in the block, go to previous block
    if (entry_it == target_block.entries.begin()) {
        // If we're in the first block and no smaller entry exists
        if (block_it == block_index.begin())
            return marker_t<V>::low_marker;

        // Try previous block, this happens when the timestamp of the block we landed upon was greater than t
        --block_it;
        const auto& prev_block = *block_it;
        // Find last marked entry in previous block, reject the values which are greater than t in previous block.
        for (int i = BLOCK_SIZE - 1; i >= 0; --i) {
            if (prev_block.entries[i].ts <= t)
            {
                std::cout << "first value in the previous block which is <= t : " << prev_block.entries[i].val << "\n\n";
                return prev_block.entries[i].val;
            }
        }
        return marker_t<V>::low_marker;
    }
    // If t is greater than the first timestamp in the block, then move to previous entry in current block
    // Move to previous entry in current block
    --entry_it;
    while (entry_it >= target_block.entries.begin()) {
        if (entry_it->ts <= t)
        {
                std::cout << "value from the current block but earlier timestamp primted : " << entry_it->val << "\n\n";
                return entry_it->val;
        }
                --entry_it;
    }

    return marker_t<V>::low_marker;
    
}


void copy_to(std::vector<std::pair<int, V>>& result) {

    std::shared_lock lock(block_index_mutex);

    if (block_index.empty()) {
        return;
    }

    size_t current_tail = tail.load();
    size_t block_number = current_tail / BLOCK_SIZE;

    // Update tail by checking consecutive marked entries
    while (true) {
        const auto& current_block = block_index[block_number];
        bool flag = false;
        while(current_block.entries[current_tail % BLOCK_SIZE].marked) {
            result.emplace_back(current_block.entries[current_tail % BLOCK_SIZE].ts, current_block.entries[current_tail % BLOCK_SIZE].val);
            tail.compare_exchange_weak(current_tail, current_tail + 1);
            current_tail = tail.load();
            if (current_tail % BLOCK_SIZE == 0) {
                block_number++;
                flag = true;
                break;
            }
        }
        if (flag == false)
            break;
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


