#ifndef __KEY_CHAIN
#define __KEY_CHAIN

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/array.hpp>

#define __DEBUG
#include "debug.hpp"

template <class T, size_t N> class key_chain_t {
    struct link_t {
        typedef pmem::obj::persistent_ptr<link_t> ptr_t;
        pmem::obj::array<T, N> block;
        ptr_t next = NULL;
    };
    typedef typename link_t::ptr_t plink_t;

    plink_t head, tail;
    pmem::obj::p<size_t> no_blocks;
    std::mutex append_lock;
    size_t pending;
    pmem::obj::pool_base pool;

public:
    key_chain_t() {
        pool = pmem::obj::pool_by_vptr(this);
        if (head == NULL) {
            pmem::obj::transaction::run(pool, [&] {
                head = pmem::obj::make_persistent<link_t>();
                tail = head;
                no_blocks = 1;
            });
            pending = 0;
        } else {
            for (pending = 0; pending < N; pending++)
                if (tail->block[pending] == T())
                    break;
        }
    }
    template<class... Args > void append(Args&&... args) {
        plink_t link;
        size_t slot;
        {
            std::lock_guard<std::mutex> lock(append_lock);
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
        }
        pmem::obj::transaction::run(pool, [&] {
            link->block[slot] = T(std::forward<Args>(args)...);
        });
    }
    size_t size() {
        return N * (no_blocks - 1) + pending;
    }
    plink_t get_head() {
        return head;
    }
};

#endif //__KEY_CHAIN
