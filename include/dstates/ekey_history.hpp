


#ifndef __EKEY_HISTORY_T
#define __EKEY_HISTORY_T

#include "marker.hpp"
#include "key_info.hpp"
#include <atomic>
#include <stdexcept>
#include <vector>
#include <memory>

template <class V>
class ekey_history_t {
    static const size_t BLOCK_SIZE = 128;

    struct entry_t {
        int ts;
        V val;
        bool marked = false;
    };
    // these are related to a particular block
    struct block_t {
        std::vector<entry_t> entries;
        std::atomic<int> first_ts;
        std::atomic<int> last_ts;
        std::atomic<size_t> size;
        std::shared_ptr<block_t> next;

        block_t() : entries(BLOCK_SIZE), first_ts(std::numeric_limits<int>::max()),
                    last_ts(std::numeric_limits<int>::min()), size(0), next(nullptr) {}
    };
    // these are related to all of ekey_history
    // head is not atomic as it doesn't keep getting updated.
    std::shared_ptr<block_t> head;
    std::atomic<std::shared_ptr<block_t>> tail;
    std::atomic<size_t> total_size;

    // std::map<int, std::shared_ptr<block_t>> block_map;
    // mutable std::shared_mutex block_map_mutex;

public:
    key_info_t info;
    // when the object of type ekey_history_t is made a new block_t is formed which is pointed by the head pointer
    // initially the tail also points to head, and total_size = 0
    ekey_history_t() : head(std::make_shared<block_t>()), tail(head), total_size(0) {
        // block_map[std::numeric_limits<int>::min()] = head;
    }


    void insert(int t, const V &v) {
    // std::cout << "Inserting a value - start.\n";
    while (true) {
        // std::cout << "A turn.\n";
        auto current_tail = tail.load();
        size_t slot = current_tail->size.fetch_add(1);
        
        if (slot < BLOCK_SIZE) {
            // std::cout << "Current Block size is enough.\n";
            current_tail->entries[slot].ts = t;
            current_tail->entries[slot].val = v;
            current_tail->entries[slot].marked = true; // inserted
            // std::cout << "inserted the ts val and marked values.\n";
            int expected_first = current_tail->first_ts.load();
            current_tail->first_ts.compare_exchange_strong(expected_first, std::min(expected_first, t));

            int expected_last = current_tail->last_ts.load();
            current_tail->last_ts.compare_exchange_strong(expected_last, std::max(expected_last, t));
            // if (current_tail->last_ts.compare_exchange_strong(expected_last, std::max(expected_last, t))) {
                // std::unique_lock lock(block_map_mutex);
                // block_map[t] = current_tail;
            // }
            // std::cout << "updated the minimum and maximum values for ts for the current block.\n";
            total_size.fetch_add(1);
            // std::cout << "boom.\n";
            // info.update(t, v == marker_t<V>::low_marker);
            // std::cout << "end of an insertion if block found.\n";
            return;
        } else {
            // std::cout << "Current Block size is not enough.\n";
            auto new_block = std::make_shared<block_t>();
            std::shared_ptr<block_t> expected_next = nullptr;
            if (std::atomic_compare_exchange_strong(&current_tail->next, &expected_next, new_block)) {
                std::shared_ptr<block_t> expected_tail = current_tail;
                tail.compare_exchange_strong(expected_tail, new_block);

                // std::unique_lock lock(block_map_mutex);
                // block_map[t] = new_block;
            }
        }
    }
    // std::cout << "Inserting a value ends. After the while loop\n";
    }

    void remove(int t) {
        insert(t, marker_t<V>::low_marker);                                                                                 
    }

    V find(int t) {
        auto current = head; // points to the head of the linked list of blocks
        while (current) { // iterate through all the blocks
            if ((t >= current->first_ts.load() && t <= current->last_ts.load()) || current->next == NULL) { // if t is in range for current block
                for (size_t i = 0; i < current->size.load(); ++i) { // iterate through all entries of current block
                    if (current->entries[i].marked && current->entries[i].ts <= t) { // if current entry is valid and less than t
                        if (current->entries[i].ts == t) { // if exact same timestamp found
                            return current->entries[i].val; // return value
                        }
                        // if this is the last entry or the first entry of next block is greater than t
                        if (i == current->size.load() - 1 || current->entries[i + 1].ts > t) {
                            // return the value of the just previous timestamp
                            return current->entries[i].val;
                        }
                    }
                }
            }
            current = current->next;
        }
        // if the timestamp didn't belong in between any block, return low marker value.
        return marker_t<V>::low_marker;
    }
    

    void copy_to(std::vector<std::pair<int, V>> &result) {
        result.clear();
        auto current = head;
        // std::cout << "Inside copy function.\n";
        while (current) {
            std::cout << "Going through new block.\n";
            std::cout << "size of the current block : " << current->size.load() - 1 << "\n";
            for (size_t i = 0; i < current->size.load() - 1; ++i) {
                // std::cout << "Going through a particular value.  " << i << " \n";
                if (current->entries[i].marked) {
                    // std::cout<< "Inside the if block.\n";
                    result.emplace_back(current->entries[i].ts, current->entries[i].val);
                    // std::cout << "After the emplacing.\n";
                }

            }
            current = current->next;
        }
        // std::cout << "End of copy function.\n";
    }

    void cleanup() {
        auto current = head;
        while (current) {
            auto next = current->next;
            current.reset();
            current = next;
        }
        head.reset();
        tail.store(nullptr);
        total_size.store(0);
    }
    
    size_t size() {
        return total_size.load();
    }
};

#endif // __EKEY_HISTORY_T