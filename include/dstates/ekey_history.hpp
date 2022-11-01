#ifndef __EKEY_HISTORY_T
#define __EKEY_HISTORY_T

#include "marker.hpp"

#include <atomic>
#include <stdexcept>
#include <vector>

#define __DEBUG
#include "debug.hpp"

template <class V> class ekey_history_t {
    static const size_t HISTORY_SIZE = 128;
    struct entry_t {
        int ts;
        V val;
        bool marked = false;
    };

    std::vector<entry_t> history{HISTORY_SIZE};
    std::atomic<int> tail{0}, pending{0};

public:
    void insert(int t, const V &v) {
        int slot = pending++;
        if (slot == HISTORY_SIZE)
            throw std::runtime_error("history full, reallocation not implemented yet");
        history[slot].ts = t;
        history[slot].val = v;
        history[slot].marked = true;
    }

    void remove(int t) {
        insert(t, marker_t<V>::low_marker);
    }

    V find(int t) {
        int current_tail = tail;
        while (history[current_tail].marked && history[current_tail].ts <= t) {
            tail.compare_exchange_weak(current_tail, current_tail + 1);
            current_tail = tail;
        }

        int left = 0, right = current_tail - 1;
        while (left <= right) {
            int middle = (left + right) / 2;
            if (t < history[middle].ts)
                right = middle - 1;
            else if (t > history[middle].ts)
                left = middle + 1;
            else
                return history[middle].val;
        }

        return (right < 0) ? marker_t<V>::low_marker : history[right].val;
    }

    void copy_to(std::vector<std::pair<int, V>> &result) {
        int current_head = 0;
        while (history[current_head].marked) {
            result.emplace_back(std::make_pair(history[current_head].ts, history[current_head].val));
            if (current_head == tail)
                tail.compare_exchange_weak(current_head, current_head + 1);
            current_head++;
        }
    }

    size_t size() {
	return pending.load();
    }
};

#endif // __EKEY_HISTORY_T
