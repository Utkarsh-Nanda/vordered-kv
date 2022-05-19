#ifndef __HISTORY_T
#define __HISTORY_T

#include <atomic>
#include <iostream>

#define __DEBUG
#include "debug.hpp"

template <class V> class history_opt_t {
    static const size_t HISTORY_SIZE = 16;
    struct entry_t {
        int ts;
        V val;
        bool marked = false;
    };
    entry_t history[HISTORY_SIZE];
    std::atomic<int> tail{0}, pending{0};

public:
    inline static const int marker = std::numeric_limits<V>::min();

    void insert(int t, const V &v, bool persist = true) {
        int slot = pending++;
        while (slot == HISTORY_SIZE)
            throw std::runtime_error("history full, reallocation not implemented yet");
        history[slot].ts = t;
        history[slot].val = v;
        history[slot].marked = true;
    }

    void remove(int t) {
        insert(t, marker);
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
        return (right < 0) ? marker : history[right].val;
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
};

#endif // __HISTORY_T
