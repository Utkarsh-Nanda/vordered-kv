#ifndef __PERSISTENT_HISTORY_T
#define __PERSISTENT_HISTORY_T

#include <limits>

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <libpmemobj++/container/string.hpp>

#define __DEBUG
#include "debug.hpp"

template <class V> class phistory_t {
    typedef typename std::conditional<std::is_same<V, std::string>::value, pmem::obj::string, V>::type PV;
    typedef std::pair<int, PV> entry_t;
    pmem::obj::vector<entry_t> log;
    pmem::obj::mutex tx_mutex;

public:
    static const V low_marker, high_marker;

    phistory_t() = default;

    static V get_volatile(const PV &v) {
	if constexpr(std::is_same<V, std::string>::value)
	    return std::string(v.data(), v.data() + v.size());
	else
	    return v;
    }

    void insert(int t, const V &v) {
        auto pool = pmem::obj::pool_by_vptr(this);
        pmem::obj::transaction::run(pool, [&] {
	    log.emplace_back(t, v);
        }, tx_mutex);
    }

    void remove(int t) {
        insert(t, low_marker);
    }

    V find(int t) {
        int left = 0, right = size() - 1;
        while (left <= right) {
            int middle = (left + right) / 2;
            if (t < log[middle].first)
                right = middle - 1;
            else if (t > log[middle].first)
                left = middle + 1;
            else
                return get_volatile(log[middle].second);
        }
        return (right < 0) ? low_marker : get_volatile(log[right].second);
    }

    void copy_to(std::vector<std::pair<int, V>> &result) {
        for (size_t i = 0; i < size(); i++)
            result.emplace_back(log[i]);
    }

    int get_latest() {
        return size() > 0 ? log.back().first : -1;
    }

    size_t size() {
	std::scoped_lock<pmem::obj::mutex> lock(tx_mutex);
        return log.size();
    }
};

template <> const std::string phistory_t<std::string>::low_marker = "";
template <class V> const V phistory_t<V>::low_marker = std::numeric_limits<V>::min();

template <> const std::string phistory_t<std::string>::high_marker = "\255";
template <class V> const V phistory_t<V>::high_marker = std::numeric_limits<V>::max();

#endif // __PERSISTENT_HISTORY_T
