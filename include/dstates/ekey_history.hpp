// Dynamic history but seperated map and linked list

// #ifndef __EKEY_HISTORY_T
// #define __EKEY_HISTORY_T

// #include "marker.hpp"
// #include "key_info.hpp"
// #include <atomic>
// #include <stdexcept>
// #include <vector>
// #include <memory>
// #include <shared_mutex>

// template <class V>
// class ekey_history_t {
//     static const size_t BLOCK_SIZE = 128;

//     struct entry_t {
//         int ts;
//         V val;
//         bool marked = false;
//     };
//     // these are related to a particular block
//     struct block_t {
//         std::vector<entry_t> entries;
//         std::atomic<int> first_ts;
//         std::atomic<int> last_ts;
//         std::atomic<size_t> size;
//         std::shared_ptr<block_t> next;

//         block_t() : entries(BLOCK_SIZE), first_ts(std::numeric_limits<int>::max()),
//                     last_ts(std::numeric_limits<int>::min()), size(0), next(nullptr) {}
//     };
//     // these are related to all of ekey_history
//     // head is not atomic as it doesn't keep getting updated.
//     std::shared_ptr<block_t> head;
//     std::atomic<std::shared_ptr<block_t>> tail;
//     std::atomic<size_t> pending;

//     // Vector to store the starting timestamp and block pointer for efficient access.
//     std::vector<std::pair<int, std::shared_ptr<block_t>>> block_index;
//     mutable std::shared_mutex block_index_mutex;

//     // std::map<int, std::shared_ptr<block_t>> block_map;
//     // mutable std::shared_mutex block_map_mutex;

// public:
//     key_info_t info;
//     // when the object of type ekey_history_t is made a new block_t is formed which is pointed by the head pointer
//     // initially the tail also points to head, and pending = 0
//     ekey_history_t() : head(std::make_shared<block_t>()), tail(head), pending(0) {
//         std::unique_lock lock(block_index_mutex);
//         block_index.emplace_back(std::numeric_limits<int>::min(), head);
//         // block_map[std::numeric_limits<int>::min()] = head;
//     }

//     // void insert(int t, const V &v) {
//     // // std::cout << "Inserting a value - start.\n";
//     // while (true) {
//     //     // std::cout << "A turn.\n";
//     //     auto current_tail = tail.load();
//     //     size_t slot = current_tail->size.fetch_add(1);

//     //     if (slot < BLOCK_SIZE) {
//     //         // std::cout << "Current Block size is enough.\n";
//     //         current_tail->entries[slot].ts = t;
//     //         current_tail->entries[slot].val = v;
//     //         current_tail->entries[slot].marked = true; // inserted
//     //         // std::cout << "inserted the ts val and marked values.\n";
//     //         int expected_first = current_tail->first_ts.load();
//     //         current_tail->first_ts.compare_exchange_strong(expected_first, std::min(expected_first, t));

//     //         int expected_last = current_tail->last_ts.load();
//     //         current_tail->last_ts.compare_exchange_strong(expected_last, std::max(expected_last, t));
//     //         // if (current_tail->last_ts.compare_exchange_strong(expected_last, std::max(expected_last, t))) {
//     //             // std::unique_lock lock(block_map_mutex);
//     //             // block_map[t] = current_tail;
//     //         // }
//     //         // std::cout << "updated the minimum and maximum values for ts for the current block.\n";
//     //         pending.fetch_add(1);
//     //         // std::cout << "boom.\n";
//     //         // info.update(t, v == marker_t<V>::low_marker);
//     //         // std::cout << "end of an insertion if block found.\n";
//     //         return;
//     //     } else {
//     //         // std::cout << "Current Block size is not enough.\n";
//     //         auto new_block = std::make_shared<block_t>();
//     //         std::shared_ptr<block_t> expected_next = nullptr;
//     //         if (std::atomic_compare_exchange_strong(&current_tail->next, &expected_next, new_block)) {
//     //             std::shared_ptr<block_t> expected_tail = current_tail;
//     //             tail.compare_exchange_strong(expected_tail, new_block);

//     //             // std::unique_lock lock(block_map_mutex);
//     //             // block_map[t] = new_block;
//     //         }
//     //     }
//     // }
//     // // std::cout << "Inserting a value ends. After the while loop\n";
//     // }

//     void insert(int t, const V &v) {
//         while (true) {
//             auto current_tail = tail.load();
//             size_t slot = current_tail->size.fetch_add(1);

//             if (slot < BLOCK_SIZE) {
//                 current_tail->entries[slot].ts = t;
//                 current_tail->entries[slot].val = v;
//                 current_tail->entries[slot].marked = true;

//                 // int expected_first = current_tail->first_ts.load();
//                 // current_tail->first_ts.compare_exchange_strong(expected_first, std::min(expected_first, t));

//                 // int expected_last = current_tail->last_ts.load();
//                 // if (current_tail->last_ts.compare_exchange_strong(expected_last, std::max(expected_last, t))) {
//                 //     std::unique_lock lock(block_index_mutex);
//                 //     if (block_index.empty() || block_index.back().first < t) {
//                 //         block_index.emplace_back(t, current_tail);
//                 //     }
//                 // }

//                 // int expected_last = current_tail->last_ts.load();
//                 // current_tail->last_ts.compare_exchange_strong(expected_last, std::max(expected_last, t));

//                 pending.fetch_add(1);

//                 return;
//             } else {
//                 auto new_block = std::make_shared<block_t>();
//                 std::shared_ptr<block_t> expected_next = nullptr;
//                 if (std::atomic_compare_exchange_strong(&current_tail->next, &expected_next, new_block)) {
//                     std::shared_ptr<block_t> expected_tail = current_tail;
//                     tail.compare_exchange_strong(expected_tail, new_block);

//                     std::unique_lock lock(block_index_mutex);
//                     block_index.emplace_back(t, new_block);
//                 }
//             }
//         }
//     }

//     void remove(int t) {
//         insert(t, marker_t<V>::low_marker);
//     }

//     // V find(int t) {
//     //     auto current = head; // points to the head of the linked list of blocks
//     //     while (current) { // iterate through all the blocks
//     //         if ((t >= current->first_ts.load() && t <= current->last_ts.load()) || current->next == NULL) { // if t is in range for current block
//     //             for (size_t i = 0; i < current->size.load(); ++i) { // iterate through all entries of current block
//     //                 if (current->entries[i].marked && current->entries[i].ts <= t) { // if current entry is valid and less than t
//     //                     if (current->entries[i].ts == t) { // if exact same timestamp found
//     //                         return current->entries[i].val; // return value
//     //                     }
//     //                     // if this is the last entry or the first entry of next block is greater than t
//     //                     if (i == current->size.load() - 1 || current->entries[i + 1].ts > t) {
//     //                         // return the value of the just previous timestamp
//     //                         return current->entries[i].val;
//     //                     }
//     //                 }
//     //             }
//     //         }
//     //         current = current->next;
//     //     }
//     //     // if the timestamp didn't belong in between any block, return low marker value.
//     //     return marker_t<V>::low_marker;
//     // }

//         V find(int t) {
//         // std::shared_ptr<block_t> target_block;
//         // {
//         //     std::shared_lock lock(block_index_mutex);
//         //     auto it = std::upper_bound(block_index.begin(), block_index.end(),
//         //         std::make_pair(t, std::shared_ptr<block_t>()),
//         //         [](const auto& a, const auto& b) { return a.first < b.first; });

//         //     if (it != block_index.begin()) {
//         //         --it;
//         //         target_block = it->second;
//         //     } else {
//         //         return marker_t<V>::low_marker;
//         //     }
//         // }

//         std::shared_ptr<block_t> target_block;
//         {
//             std::shared_lock lock(block_index_mutex);

//             if (block_index.empty()) {
//                 return marker_t<V>::low_marker;
//             }

//             int left = 0;
//             int right = block_index.size() - 1;

//             while (left <= right) {
//                 int mid = left + (right - left) / 2;

//                 if (block_index[mid].first <= t) {
//                     // if we have reached the last block, or the starting value of next block is > t, then this is the block
//                     if (mid == block_index.size() - 1 || block_index[mid + 1].first > t) {
//                         target_block = block_index[mid].second;
//                         break;
//                     }
//                     left = mid + 1;
//                 } else {
//                     right = mid - 1;
//                 }
//             }
//             // if after search target_block is not set
//             if (!target_block) {
//                 // right points to the value just less than t
//                 if (right >= 0) {
//                     target_block = block_index[right].second;
//                 } else {
//                     return marker_t<V>::low_marker;
//                 }
//             }
//         }

//         size_t left = 0, right = target_block->size.load() - 1;
//         // std::cout << "Block size : " << right << "\n";
//         while (left <= right) {
//             size_t middle = left + (right - left) / 2;
//             if (t < target_block->entries[middle].ts)
//                 right = middle - 1;
//             else if (t > target_block->entries[middle].ts)
//                 left = middle + 1;
//                 // if t == middle timestamp and it is marked
//             else if (target_block->entries[middle].marked){
//                 // std::cout << "Value returned after finding exactly : " << target_block->entries[middle].val << "\n";
//                 return target_block->entries[middle].val;
//             }
//             else
//                 right = middle - 1;  // If not marked, continue searching for the previous valid entry
//         }

//         // If no exact match, return the most recent value before t
//         while (right >= 0) {
//             if (target_block->entries[right].marked && target_block->entries[right].ts <= t) {
//                 // std::cout << "Value returned after not finding a match : " << target_block->entries[right].val << "\n";
//                 return target_block->entries[right].val;
//             }
//             --right;
//         }
//         // std::cout << "Returning low marker : " << marker_t<V>::low_marker << "\n";
//         return marker_t<V>::low_marker;
//     }

//     void copy_to(std::vector<std::pair<int, V>> &result) {
//         result.clear();
//         auto current = head;
//         // std::cout << "Inside copy function.\n";
//         while (current) {
//             // std::cout << "Going through new block.\n";
//             // std::cout << "size of the current block : " << current->size.load() - 1 << "\n";
//             for (size_t i = 0; i < current->size.load() - 1; ++i) {
//                 // std::cout << "Going through a particular value.  " << i << " \n";
//                 if (current->entries[i].marked) {
//                     // std::cout<< "Inside the if block.\n";
//                     result.emplace_back(current->entries[i].ts, current->entries[i].val);
//                     // std::cout << "After the emplacing.\n";
//                 }

//             }
//             current = current->next;
//         }
//         // std::cout << "End of copy function.\n";
//     }

//     void cleanup() {
//         auto current = head;
//         while (current) {
//             auto next = current->next;
//             current.reset();
//             current = next;
//         }
//         head.reset();
//         tail.store(nullptr);
//         pending.store(0);
//     }

//     size_t size() {
//         return pending.load();
//     }
// };

// #endif // __EKEY_HISTORY_T

// Static history, original.

// #ifndef __EKEY_HISTORY_T
// #define __EKEY_HISTORY_T

// #include "marker.hpp"
// #include "key_info.hpp"

// #include <atomic>
// #include <stdexcept>
// #include <vector>

// template <class V> class ekey_history_t {
//     static const size_t HISTORY_SIZE = 128;

//     struct entry_t {
//         int ts;
//         V val;
//         bool marked = false;
//     };
//     std::vector<entry_t> history{HISTORY_SIZE};
//     std::atomic<int> tail{0}, pending{0};

// public:
//     key_info_t info;

//     void insert(int t, const V &v) {
//         int slot = pending++;
//         if (slot == HISTORY_SIZE)
//             throw std::runtime_error("history full, reallocation not implemented yet");
//         history[slot].ts = t;
//         history[slot].val = v;
//         history[slot].marked = true;
// 	info.update(t, v == marker_t<V>::low_marker);
//     }

//     void remove(int t) {
//         insert(t, marker_t<V>::low_marker);
//     }

//     V find(int t) {
//         int current_tail = tail;
//         while (history[current_tail].marked && history[current_tail].ts <= t) {
//             tail.compare_exchange_weak(current_tail, current_tail + 1);
//             current_tail = tail;
//         }

//         int left = 0, right = current_tail - 1;
//         while (left <= right) {
//             int middle = (left + right) / 2;
//             if (t < history[middle].ts)
//                 right = middle - 1;
//             else if (t > history[middle].ts)
//                 left = middle + 1;
//             else
//                 return history[middle].val;
//         }

//         return (right < 0) ? marker_t<V>::low_marker : history[right].val;
//     }

//     void copy_to(std::vector<std::pair<int, V>> &result) {
//         int current_head = 0;
//         while (history[current_head].marked) {
//             result.emplace_back(std::make_pair(history[current_head].ts, history[current_head].val));
//             if (current_head == tail)
//                 tail.compare_exchange_weak(current_head, current_head + 1);
//             current_head++;
//         }
//     }

//     size_t size() {
// 	return pending.load();
//     }
// };

// #endif // __EKEY_HISTORY_T

// New login, with dynamic history and direct mapping of timestamp and blocks

// #ifndef __EKEY_HISTORY_T
// #define __EKEY_HISTORY_T

// #include "marker.hpp"
// #include "key_info.hpp"
// #include <atomic>
// #include <vector>
// #include <algorithm>
// #include <shared_mutex>

// template <class V>
// class ekey_history_t {
//     static const size_t BLOCK_SIZE = 128;

//     struct entry_t {
//         int ts;
//         V val;
//         bool marked = false;
//     };

//     struct block_t {
//     std::vector<entry_t> entries;
//     std::atomic<size_t> size;

//     block_t() : entries(BLOCK_SIZE), size(0) {}
//     // block_t(const block_t&) = delete;
//     // block_t& operator=(const block_t&) = delete;
//     // block_t(block_t&&) = default;
//     // block_t& operator=(block_t&&) = default;
// };

//     std::vector<std::pair<int, block_t>> block_index;
//     mutable std::shared_mutex block_index_mutex;
//     std::atomic<size_t> pending;

// public:
//     key_info_t info;

//     ekey_history_t() : pending(0) {
//     block_index.emplace_back(std::numeric_limits<int>::min(), block_t());
// }

//     void insert(int t, const V &v) {
//         std::unique_lock lock(block_index_mutex);
//         auto& current_block = *block_index.back().second;  // Dereference the unique_ptr
//         size_t slot = current_block.size.load();

//         if (slot < BLOCK_SIZE) {
//             // Check if timestamp already exists
//             auto it = std::lower_bound(current_block.entries.begin(),
//                                        current_block.entries.begin() + slot,
//                                        t,
//                                        [](const entry_t& entry, int timestamp) {
//                                            return entry.ts < timestamp;
//                                        });

//             if (it != current_block.entries.begin() + slot && it->ts == t) {
//                 // Update existing entry
//                 it->val = v;
//                 return;
//             }

//             // Insert new entry
//             current_block.entries[slot].ts = t;
//             current_block.entries[slot].val = v;
//             current_block.entries[slot].marked = true;
//             current_block.size.fetch_add(1);

//             // int expected_first = current_block.first_ts.load();
//             // current_block.first_ts.compare_exchange_strong(expected_first, std::min(expected_first, t));

//             // int expected_last = current_block.last_ts.load();
//             // current_block.last_ts.compare_exchange_strong(expected_last, std::max(expected_last, t));

//             pending.fetch_add(1);
//         } else {
//             block_index.emplace_back(t, std::make_unique<block_t>());
//             auto& new_block = *block_index.back().second;
//             new_block.entries[0].ts = t;
//             new_block.entries[0].val = v;
//             new_block.entries[0].marked = true;
//             new_block.size.store(1);
//             pending.fetch_add(1);
//         }
//     }

//     void remove(int t) {
//         insert(t, marker_t<V>::low_marker);
//     }

//     V find(int t) {
//         std::shared_lock lock(block_index_mutex);

//     auto block_it = std::lower_bound(block_index.begin(), block_index.end(), t,
//         [](const auto& block_pair, int timestamp) {
//             return block_pair.first <= timestamp;
//         });

//     if (block_it == block_index.end()) {
//         --block_it;
//     }

//     const auto& block = *block_it->second;
//         auto entry_it = std::lower_bound(block.entries.begin(),
//                                          block.entries.begin() + block.size.load(),
//                                          t,
//                                          [](const entry_t& entry, int timestamp) {
//                                              return entry.ts <= timestamp;
//                                          });

//         if (entry_it != block.entries.begin() + block.size.load()) { // if found
//             if (entry_it->ts == t && entry_it->marked) {
//                 return entry_it->val;
//             }
//             if (entry_it != block.entries.begin()) {
//                 --entry_it;
//                 if (entry_it->marked) {
//                     return entry_it->val;
//                 }
//             }
//         }

//         return marker_t<V>::low_marker;
//     }

//     void copy_to(std::vector<std::pair<int, V>>& result) {
//     result.clear();
//     std::shared_lock lock(block_index_mutex);
//     for (const auto& block_pair : block_index) {
//         const auto& block = *block_pair.second;
//         for (size_t i = 0; i < block.size.load(); ++i) {
//             if (block.entries[i].marked) {
//                 result.emplace_back(block.entries[i].ts, block.entries[i].val);
//             }
//         }
//     }
// }

//     void cleanup() {
//     std::unique_lock lock(block_index_mutex);
//     block_index.clear();
//     block_index.emplace_back(std::numeric_limits<int>::min(), std::make_unique<block_t>());
//     pending.store(0);
// }

//     size_t size() {
//         return pending.load();
//     }
// };

// #endif // __EKEY_HISTORY_T

// experimentation

#ifndef __EKEY_HISTORY_T
#define __EKEY_HISTORY_T

#include "marker.hpp"
#include "key_info.hpp"
#include <atomic>
#include <vector>
#include <algorithm>
#include <shared_mutex>
#include <omp.h>
template <class V>
class ekey_history_t
{
    static const size_t BLOCK_SIZE = 128;

    struct entry_t
    {
        int ts;
        V val;
        bool marked = false;
    };
    struct block_index_entry
    {
        int timestamp; // starting timestamp of this block
        std::vector<entry_t> entries;
        block_index_entry(int ts) : timestamp(ts), entries(BLOCK_SIZE) {}
    };
    // Rather than having a pair of timestamps, it's a vector of structs having timestamp and vector of entries.
    std::vector<block_index_entry> block_index;
    std::shared_mutex block_index_mutex;
    std::atomic<size_t> tail{0}, pending{0}; // has to be atomic, becuase it is shared among threads.

public:
    key_info_t info;

    ekey_history_t() : pending(0)
    {
        // unique lock cause the mutex is not shared with other threads, we are adding a new block.
        std::unique_lock lock(block_index_mutex);
        block_index.emplace_back(0); // First block starts with timestamp 0, but no entry has been made yet.
    }

    // Function to insert a new entry with timestamp 't' and value 'v'
    void insert(int t, const V &v)
    {
        // Get the last block in the index
        auto &last_block = block_index.back();
        size_t new_slot = pending.fetch_add(1); // Atomically increment pending and get the previous value
        int index = new_slot % BLOCK_SIZE;      // Calculate the index within the block based on pending
        int sz = block_index.size();
        // it might happen that a new block is created, if the 
        // new_slot <sz * BLOCK_SIZE, is there to prvent the time when a thread 2 has index 1 and is trying to insert but thread 1 hasn't added the block yet.
        // If I just do index != 0, then insert, then in the case of first block, the first entry will be missed.
                                               0         1       128   
        if ((index != 0 || new_slot == 0) && new_slot < sz * BLOCK_SIZE)
        {
            last_block.entries[index].ts = t;
            last_block.entries[index].val = v;
            last_block.entries[index].marked = true;
        }
        else
        {
            // If the current block is full, create a new block
            std::unique_lock lock(block_index_mutex);
            if (sz == block_index.size())         // if the block hasn't been added by another thread
                block_index.emplace_back(t);      // Add a new block to the index
            lock.unlock();                        // Unlock the mutex before modifying the block
            auto &new_block = block_index.back(); // Get a reference to the new block
            new_block.entries[index].ts = t;
            new_block.entries[index].val = v;
            new_block.entries[index].marked = true;
        }
    }

    V find(int t)
    {
        // Take a shared lock since we're only reading
        // std::shared_lock lock(block_index_mutex);

        // If block_index is empty, return low marker
        if (block_index.empty())
            return marker_t<V>::low_marker;

        size_t current_tail = tail.load();
        size_t block_number = current_tail / BLOCK_SIZE;

        // Update tail by checking consecutive marked entries
        while (true)
        {
            const auto &current_block = block_index[block_number];
            bool flag = false;
            while (current_block.entries[current_tail % BLOCK_SIZE].ts <= t && current_block.entries[current_tail % BLOCK_SIZE].marked)
            {
                tail.compare_exchange_weak(current_tail, current_tail + 1);
                current_tail = tail.load();
                if (current_tail % BLOCK_SIZE == 0)
                {
                    block_number++;
                    flag = true;
                    break;
                }
            }
            if (flag == false)
                break;
        }
        current_tail = tail.load() - 1; // tail points to one index ahead of the last marked entry
        index_in_block = current_tail % BLOCK_SIZE;
        const auto &last_block2 = block_index[block_number];
        const auto &tail_entry2 = last_block2.entries[index_in_block];

        // If the requested timestamp is greater than or equal to tail's timestamp (last marked entry)
        if (t >= tail_entry2.ts && tail_entry2.marked)
            return tail_entry2.val;

        // Binary search to find the correct block
        auto block_it = std::lower_bound(block_index.begin(), block_index.begin() + block_number + 1, t,
                                         [](const block_index_entry &entry, int timestamp)
                                         {
                                             return entry.timestamp < timestamp;
                                         });

        // If t is greater than the starting timestamp of current block.
        if ((*block_it).timestamp > t)
            --block_it;

        // Binary search within the block
        const auto &target_block = *block_it;
        // the ternary operator is used to check if the block is the last block or the previous of it.
        // If last then the size of the block is the current tail % BLOCK_SIZE + 1, else it is BLOCK_SIZE.
        auto entry_it = std::lower_bound(
            target_block.entries.begin(),
            target_block.entries.begin() + (block_it == block_index.begin() + block_number ? current_tail % BLOCK_SIZE + 1 : BLOCK_SIZE),
            t,
            [](const entry_t &entry, int timestamp)
            {
                return entry.ts < timestamp;
            });

        // If exact match found till the tail, return the value
        if (entry_it != target_block.entries.end() && entry_it->ts == t)
            return entry_it->val;

        // If no exact match, get previous valid entry
        // If the t is smaller than the first timestamp in the block, go to previous block
        if (entry_it == target_block.entries.begin())
        {
            // If we're in the first block and no smaller entry exists
            if (block_it == block_index.begin())
                return marker_t<V>::low_marker;

            // Try previous block, this happens when the timestamp of the block we landed upon was greater than t
            --block_it;
            const auto &prev_block = *block_it;
            // Find last marked entry in previous block, reject the values which are greater than t in previous block.
            for (int i = BLOCK_SIZE - 1; i >= 0; --i)
            {
                if (prev_block.entries[i].ts <= t)
                    return prev_block.entries[i].val;
            }
            return marker_t<V>::low_marker;
        }
        // If t is greater than the first timestamp in the block, then move to previous entry in current block
        // Move to previous entry in current block
        --entry_it;
        while (entry_it >= target_block.entries.begin())
        {
            if (entry_it->ts <= t)
                return entry_it->val;
            --entry_it;
        }

        return marker_t<V>::low_marker;
    }

    void remove(int t)
    {
        insert(t, marker_t<V>::low_marker);
    }

    void copy_to(std::vector<std::pair<int, V>> &result)
    {
        std::shared_lock lock(block_index_mutex);

        if (block_index.empty())
        {
            return;
        }

        size_t current_tail = tail.load();
        size_t block_number = current_tail / BLOCK_SIZE;

        // Update tail by checking consecutive marked entries
        while (true)
        {
            const auto &current_block = block_index[block_number];
            bool flag = false;
            while (current_block.entries[current_tail % BLOCK_SIZE].marked)
            {
                result.emplace_back(current_block.entries[current_tail % BLOCK_SIZE].ts, current_block.entries[current_tail % BLOCK_SIZE].val);
                tail.compare_exchange_weak(current_tail, current_tail + 1);
                current_tail = tail.load();
                if (current_tail % BLOCK_SIZE == 0)
                {
                    block_number++;
                    flag = true;
                    break;
                }
            }
            if (flag == false)
                break;
        }
    }

    void cleanup()
    {
        std::unique_lock lock(block_index_mutex);
        block_index.clear();
        pending.store(0);
        tail.store(0);
    }

    size_t size()
    {
        return pending.load();
    }
};

#endif // __EKEY_HISTORY_T

void copy_to(std::vector<std::pair<int, V>> &result)
{
    std::shared_lock lock(block_index_mutex);

    if (block_index.empty())
    {
        return;
    }

    size_t current_tail = tail.load();
    size_t block_number = current_tail / BLOCK_SIZE;

    // Update tail by checking consecutive marked entries
    while (true)
    {
        const auto &current_block = block_index[block_number];
        bool flag = false;
        while (current_block.entries[current_tail % BLOCK_SIZE].marked)
        {
            result.emplace_back(current_block.entries[current_tail % BLOCK_SIZE].ts, current_block.entries[current_tail % BLOCK_SIZE].val);
            tail.compare_exchange_weak(current_tail, current_tail + 1);
            current_tail = tail.load();
            if (current_tail % BLOCK_SIZE == 0)
            {
                block_number++;
                flag = true;
                break;
            }
        }
        if (flag == false)
            break;
    }

    // Get current tail value and find which block contains it
    // size_t current_tail = tail.load();
    // size_t block_number = current_tail / BLOCK_SIZE;
    // size_t index_in_block = current_tail % BLOCK_SIZE;
    // size_t current_head = 0;

    // // Iterate through blocks
    // for (size_t current_block = 0; current_block <= block_number; ++current_block) {
    //     const auto& block = block_index[current_block];

    //     // For each block, iterate through entries up to BLOCK_SIZE
    //     // For the last block, only go up to the tail
    //     size_t limit = (current_block == block_number) ? index_in_block : BLOCK_SIZE;
    //     // Iterate through entries
    //     while (block.entries[current_head].marked && current_head < BLOCK_SIZE) {
    //         result.emplace_back(block.entries[current_head].ts, block.entries[current_head].val);
    //         // Update tail if we reached it
    //         if (current_head == current_tail) {
    //             tail.compare_exchange_weak(current_tail, current_tail + 1);
    //             current_tail = tail.load();
    //         }
    //         current_head++;
    //     }
    // }
}