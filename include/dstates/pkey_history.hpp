#ifndef __PKEY_HISTORY_T
#define __PKEY_HISTORY_T

#include "marker.hpp"

#include <type_traits>
#include <shared_mutex>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/shared_mutex.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <libpmemobj++/container/string.hpp>

template <class V> class pkey_history_t {
    typedef typename std::conditional<std::is_same<V, std::string>::value, pmem::obj::string, V>::type PV;
    typedef std::pair<int, PV> entry_t;
    pmem::obj::vector<entry_t> log;
    pmem::obj::shared_mutex tx_mutex;

public:
    key_info_t info;

    pkey_history_t() {
	if (log.size() > 0)
	    info.update(log.back().first, log.back().second == marker_t<V>::low_marker);
    }

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
	info.update(t, v == marker_t<V>::low_marker);
    }

    void remove(int t) {
        insert(t, marker_t<V>::low_marker);
    }

    V find(int t) {
	std::shared_lock<pmem::obj::shared_mutex> read_lock(tx_mutex);
        int left = 0, right = log.size() - 1;
        while (left <= right) {
            int middle = (left + right) / 2;
            if (t < log[middle].first)
                right = middle - 1;
            else if (t > log[middle].first)
                left = middle + 1;
            else
                return get_volatile(log[middle].second);
        }
        return (right < 0) ? marker_t<V>::low_marker : get_volatile(log[right].second);
    }

    void copy_to(std::vector<std::pair<int, V>> &result) {
	std::shared_lock<pmem::obj::shared_mutex> read_lock(tx_mutex);
	int log_size = log.size();
        for (int i = 0; i < log_size; i++)
            result.emplace_back(log[i].first, get_volatile(log[i].second));
    }

    size_t size() {
	std::shared_lock<pmem::obj::shared_mutex> read_lock(tx_mutex);
        return log.size();
    }
};

#endif // __PKEY_HISTORY_T
