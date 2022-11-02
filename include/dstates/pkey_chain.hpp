#ifndef __PKEY_CHAIN
#define __PKEY_CHAIN

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/array.hpp>

#define __DEBUG
#include "debug.hpp"

template <class T, size_t N> class pkey_chain_t {
    struct link_t {
        typedef pmem::obj::persistent_ptr<link_t> ptr_t;
        pmem::obj::array<T, N> block;
        ptr_t next = nullptr;
    };
    typedef typename link_t::ptr_t plink_t;

    plink_t head, tail;
    pmem::obj::p<size_t> no_blocks;
    pmem::obj::mutex tx_mutex;
    pmem::obj::pool_base pool;
    size_t pending = 0;

public:
    pkey_chain_t() {
        pool = pmem::obj::pool_by_vptr(this);
	std::scoped_lock<pmem::obj::mutex> lock(tx_mutex);
        if (head == nullptr)
            pmem::obj::transaction::run(pool, [&] {
                head = pmem::obj::make_persistent<link_t>();
                tail = head;
                no_blocks = 1;
            });
        else
            while (pending < N && tail->block[pending] == T())
		pending++;
    }

    template<class... Args > void append(Args&&... args) {
        plink_t link;
        size_t slot;
	std::unique_lock<pmem::obj::mutex> lock(tx_mutex);
	if (pending == N) {
	    pmem::obj::transaction::run(pool, [&] {
		plink_t extra = pmem::obj::make_persistent<link_t>();
		tail->next = extra;
		tail = extra;
		no_blocks++;
	    });
	    pending = 0;
	}
	link = tail;
	slot = pending++;
	lock.unlock();
        pmem::obj::transaction::run(pool, [&] {
	    T* pslot = &link->block[slot];
	    pslot->~T();
	    new (pslot) T(std::forward<Args>(args)...);
        });
    }

    size_t size() {
	std::scoped_lock<pmem::obj::mutex> lock(tx_mutex);
        return N * (no_blocks - 1) + pending;
    }

    plink_t get_head() {
        return head;
    }
};

#endif //__PKEY_CHAIN
