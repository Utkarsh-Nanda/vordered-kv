#ifndef __PERSISTENT_HISTORY_T
#define __PERSISTENT_HISTORY_T

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/vector.hpp>

#define __DEBUG
#include "debug.hpp"

template <class V> class phistory_t {
    typedef std::pair<int, V> entry_t;
    pmem::obj::vector<std::pair<int, V>> log;
    std::mutex tx_mutex;

public:
    inline static const int marker = std::numeric_limits<V>::min();

    phistory_t() = default;

    void insert(int t, const V &v) {
        auto pool = pmem::obj::pool_by_vptr(this);
        std::lock_guard<std::mutex> lock(tx_mutex);
        pmem::obj::transaction::run(pool, [&] {
            log.emplace_back(t, v);
        });
    }

    void remove(int t) {
        insert(t, marker);
    }

    V find(int t) {
        int left = 0, right = log.size() - 1;
        while (left <= right) {
            int middle = (left + right) / 2;
            if (t < log[middle].first)
                right = middle - 1;
            else if (t > log[middle].first)
                left = middle + 1;
            else
                return log[middle].second;
        }
        return (right < 0) ? marker : log[right].second;
    }

    void copy_to(std::vector<std::pair<int, V>> &result) {
        for (int i = 0; i < log.size(); i++)
            result.emplace_back(log[i]);
    }

    int get_latest() {
        return log.size() > 0 ? log.back().first : -1;
    }

    size_t size() {
        return log.size();
    }
};

#endif // __PERSISTENT_HISTORY_T
