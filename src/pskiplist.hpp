#ifndef __PERSISTENT_SKIP_LIST
#define __PERSISTENT_SKIP_LIST

#include <atomic>
#include <mutex>
#include <strings.h>
#include <unistd.h>

#include "key_chain.hpp"
#include "phistory.hpp"

#define __DEBUG
#include "debug.hpp"

template <typename K, typename V> class pskiplist_t {
    static const int MAX_LEVEL = 24;
    static const size_t BLOCK_SIZE = 1024;

    typedef phistory_t<V> log_t;
    typedef pmem::obj::persistent_ptr<log_t> plog_t;
    typedef key_chain_t<std::pair<K, plog_t>, BLOCK_SIZE> keymap_t;
    typedef pmem::obj::persistent_ptr<keymap_t> pkeymap_t;

    struct root_t {
         pkeymap_t keymap;
    };

    struct node_t {
        typedef std::atomic<node_t *> next_t;
        K key;
        plog_t history = NULL;
        std::vector<next_t> next;

        node_t(const K &k, int levels = MAX_LEVEL) : key(k), next(levels) { }
    };

    node_t head, tail;
    std::atomic<int> version{0};
    pmem::obj::pool<root_t> pool;
    std::mutex persist_mutex;

public:
    pskiplist_t(const std::string &db) : head(std::numeric_limits<K>::min()), tail(std::numeric_limits<K>::max()) {
        for (int j = 0; j < MAX_LEVEL; j++)
            head.next[j].store(&tail);
        if (access(db.c_str(), F_OK) != 0) {
            pool = pmem::obj::pool<root_t>::create(db, "skiplist_pool", 1 << 30);
            pmem::obj::transaction::run(pool, [&] {
                pool.root()->keymap = pmem::obj::make_persistent<keymap_t>();
            });
            DBG("Created a new pskip_list pool, path = " << db);
        } else {
            pool = pmem::obj::pool<root_t>::open(db, "skiplist_pool");
            DBG("Opened an existing pskip_list pool, path = " << db);
            TIMER_START(restore_index);
            std::atomic<int> count{0};
            int thread_no = std::thread::hardware_concurrency();
            #pragma omp parallel num_threads(thread_no)
            {
                auto head = pool.root()->keymap->get_head();
                int block_id = 0;
                while (head) {
                    if (block_id % thread_no == omp_get_thread_num()) {
                        for (int i = 0; i < BLOCK_SIZE; i++) {
                            plog_t log = head->block[i].second;
                            if (log) {
                                if (log->get_latest() > version)
                                    version.store(log->get_latest());
                                insert(head->block[i].first, V(), log);
                                count++;
                            }
                        }
                    }
                    head = head->next;
                    block_id++;
                }
            }
            TIMER_STOP(restore_index, "restored keys = " << count);
        }
    }
    ~pskiplist_t() {
        node_t *curr = head.next[0].load();
        while (curr != &tail) {
            node_t *next = curr->next[0].load();
            delete curr;
            curr = next;
        }
        pool.close();
    }

    node_t *find_node(const K &key, node_t **preds, node_t **succs) {
      retry:
        int level = head.next.size() - 1;
        node_t *pred = &head;
        node_t *curr = pred->next[level].load();
        while (true) {
            node_t *succ = curr->next[level].load();
            if (curr->key < key) {
                pred = curr;
                curr = succ;
            } else {
                preds[level] = pred;
                succs[level] = curr;
                if (level == 0)
                    break;
                curr = pred->next[--level].load();
            }
        }
        return curr->key == key ? curr : NULL;
    }

    bool insert(const K &key, const V &value, plog_t plog = NULL) {
        node_t *preds[MAX_LEVEL], *succs[MAX_LEVEL];
        node_t *pred, *succ, *node = NULL;
        while(true) {
            node_t *found = find_node(key, preds, succs);
            if (found) {
                if (node) {
                    // somebody else was faster at inserting the same key
                    if (node->history != plog) {
                        pmem::obj::transaction::run(pool, [&] {
                            pmem::obj::delete_persistent<log_t>(node->history);
                        });
                    }
                    delete node;
                }
                node = found;
            } else if (node == NULL) {
                int levels = ffs(rand() | (1 << (MAX_LEVEL - 1)));
                node = new node_t(key, levels);
            }
            if (plog == NULL) {
                if (node->history == NULL)
                    pmem::obj::transaction::run(pool, [&] {
                        node->history = pmem::obj::make_persistent<log_t>();
                    });
                node->history->insert(++version, value);
            } else
                node->history = plog;
            succ = succs[0];
            if (succ == node)
                return true;
            for (int level = 0; level < node->next.size(); level++)
                node->next[level].store(succs[level]);
            pred = preds[0];
            if (pred->next[0].compare_exchange_weak(succ, node)) {
                if (plog == NULL)
                    pool.root()->keymap->append(key, node->history);
                break;
            }
        }
        int level = 1;
        while (level < node->next.size()) {
            pred = preds[level];
            succ = succs[level];
            if (!pred->next[level].compare_exchange_weak(succ, node)) {
                find_node(key, preds, succs);
                continue;
            }
            level++;
        }
        return true;
    }

    bool remove(const K &key) {
        node_t *preds[MAX_LEVEL], *succs[MAX_LEVEL];
        node_t *node = find_node(key, preds, succs);
        if (node == NULL)
            return false;
        node->history->remove(++version);
        return true;
    }

    V find(int v, const K &key) {
        node_t *preds[MAX_LEVEL], *succs[MAX_LEVEL];
        node_t *node = find_node(key, preds, succs);
        if (node == NULL)
            return log_t::marker;
        else
            return node->history->find(v);
    }

    void extract_snapshot(int v, std::vector<std::pair<K,V>> &result) {
        result.clear();
        node_t *curr = head.next[0].load();
        while (curr != &tail) {
            auto p = std::make_pair(curr->key, curr->history->find(v));
            if (p.second != log_t::marker)
                result.push_back(p);
            curr = curr->next[0].load();
        }
    }

    void extract_item(const K &key, std::vector<std::pair<K,V>> &result) {
        result.clear();
        node_t *preds[MAX_LEVEL], *succs[MAX_LEVEL];
        node_t *node = find_node(key, preds, succs);
        if (node == NULL)
            return;
        node->history->copy_to(result);
    }

    int latest() {
        return version;
    }
};

#endif // __SKIP_LIST
