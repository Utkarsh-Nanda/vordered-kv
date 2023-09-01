#ifndef __POPT_HISTORY_T
#define __POPT_HISTORY_T

#include "marker.hpp"

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/container/array.hpp>

template <class V> class popt_history_t {
    typedef typename std::conditional<std::is_same<V, std::string>::value, pmem::obj::string, V>::type PV;
    struct entry_t {
        int ts;
        PV val;
        bool marked = false;
    };
    static const size_t HISTORY_SIZE = 16;

    pmem::obj::array<entry_t, HISTORY_SIZE> history;
    pmem::obj::p<int> tail, pending;
    pmem::obj::pool_base pool;
    pmem::obj::mutex tx_mutex;

public:
    key_info_t info;

    popt_history_t() {
        pool = pmem::obj::pool_by_vptr(this);
        pmem::obj::transaction::run(pool, [&] {
            while (history[tail].marked)
                tail++;
        }, tx_mutex);
        if (tail > 0)
            info.update(history[tail - 1].ts, history[tail - 1].val == marker_t<V>::low_marker);
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
        info.update(t, v == marker_t<V>::low_marker);
    }

    void remove(int t) {
        insert(t, marker_t<V>::low_marker);
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
                return get_volatile(history[middle].val);
        }
        return (right < 0) ? marker_t<V>::low_marker : get_volatile(history[right].val);
    }

    void copy_to(std::vector<std::pair<int, V>> &result) {
        int current_head = 0;
        while (history[current_head].marked) {
            result.emplace_back(std::make_pair(history[current_head].ts, get_volatile(history[current_head].val)));
            current_head++;
        }
    }

    size_t size() {
        return tail;
    }
};

#endif // __POPT_HISTORY_T
