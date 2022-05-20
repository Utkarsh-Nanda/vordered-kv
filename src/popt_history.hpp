#ifndef __POPT_HISTORY_T
#define __POPT_HISTORY_T

#include <iostream>

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/container/array.hpp>

#define __DEBUG
#include "debug.hpp"

template <class V> class popt_history_t {
    struct entry_t {
        int ts;
        V val;
        bool marked = false;
    };
    static const size_t HISTORY_SIZE = 16;

    pmem::obj::array<entry_t, HISTORY_SIZE> history;
    pmem::obj::p<int> tail, pending;
    pmem::obj::pool_base pool;
    pmem::obj::mutex tx_mutex;

public:
    inline static const int marker = std::numeric_limits<V>::min();

    popt_history_t() {
        pool = pmem::obj::pool_by_vptr(this);
    }

    void insert(int t, const V &v) {
        int slot;
        pmem::obj::transaction::run(pool, [&] {
            slot = pending++;
        }, tx_mutex);
        if (slot == HISTORY_SIZE)
            throw std::runtime_error("history full, reallocation not implemented yet");
        pmem::obj::transaction::run(pool, [&] {
            history[slot].ts = t;
            history[slot].val = v;
            history[slot].marked = true;
        });
    }

    void remove(int t) {
        insert(t, marker);
    }

    V find(int t) {
        pmem::obj::transaction::run(pool, [&] {
            while (history[tail].marked && history[tail].ts <= t)
                tail++;
        }, tx_mutex);
        int left = 0, right = tail - 1;
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
            current_head++;
        }
    }

    int get_latest() {
        int current_tail = tail;
        return current_tail > 0 ? history[current_tail - 1].val : -1;
    }

    size_t size() {
        return tail;
    }
};

#endif // __POPT_HISTORY_T
