#ifndef __VORDERED_KV
#define __VORDERED_KV

#include "marker.hpp"
#include "emem_history.hpp"
#include "pmem_history.hpp"

#include <atomic>
#include <functional>

#define __DEBUG
#include "debug.hpp"

template <typename K, typename V, typename P = pmem_history_t <K, V>> class vordered_kv_t {
    static const int MAX_LEVEL = 24;

    struct node_t {
        typedef std::atomic<node_t *> next_t;

        K key;
        typename P::plog_t history{nullptr};
        std::vector<next_t> next, shortcut;

        node_t(const K &k, int levels = MAX_LEVEL) : key(k), next(levels), shortcut(levels) { }
    };

    node_t head, tail;
    std::atomic<int> version{0};
    P pool;

public:
    inline static const V low_marker = marker_t<V>::low_marker;
    inline static const V high_marker = marker_t<V>::high_marker;

    vordered_kv_t(const std::string &db) : head(low_marker), tail(high_marker), pool(db) {
        for (int i = 0; i < MAX_LEVEL; i++)
            head.next[i].store(&tail);
	using namespace std::placeholders;
	version.store(pool.restore(std::bind(&vordered_kv_t::insert, this, _1, _2, _3)));
    }
    ~vordered_kv_t() {
        node_t *curr = head.next[0].load();
        while (curr != &tail) {
            node_t *next = curr->next[0].load();
	    pool.deallocate(curr->history, true);
            delete curr;
            curr = next;
        }
    }

    node_t *find_node(const K &key, node_t **preds, node_t **succs) {
        int level = head.next.size() - 1;
        node_t *pred = &head, *curr;

        while (true) {
	    curr = pred->next[level].load();
            if (curr->key < key) {
                pred = curr;
		continue;
	    }
	    preds[level] = pred;
	    succs[level] = curr;
	    if (level == 0)
		break;
	    level--;
        }
        return curr->key == key ? curr : nullptr;
    }

    node_t *find_node_with_shortcuts(const K &key) {
	int level = head.next.size() - 1;
        node_t *pred = &head, *curr, *valid_pred = pred, *scut = nullptr;

        while (true) {
	    curr = pred->next[level].load();
	    if (curr->key < key) {
		if (valid_pred == pred) {
		    scut = valid_pred->shortcut[level].load();
		    if (scut != nullptr && scut->key > curr->key && scut->key < key)
			curr = scut;
		} else if (!curr->history->info.latest_removed()) {
		    valid_pred->shortcut[level].store(curr);
		    valid_pred = curr;
		}
		pred = curr;
		continue;
	    }
	    if (level == 0)
		break;
	    level--;
	}
	return curr->key == key ? curr : nullptr;
    }

    bool insert(const K &key, const V &value, typename P::plog_t plog = nullptr) {
        node_t *preds[MAX_LEVEL], *succs[MAX_LEVEL];
        node_t *pred, *succ, *node = nullptr;
        while(true) {
            node_t *found = find_node(key, preds, succs);
            if (found) {
                if (node) {
                    // somebody else was faster at inserting the same key
                    if (node->history != plog)
			pool.deallocate(node->history);
                    delete node;
                }
                node = found;
            } else if (node == nullptr) {
                int levels = ffs(rand() | (1 << (MAX_LEVEL - 1)));
                node = new node_t(key, levels);
            }
            if (plog == nullptr) {
                if (node->history == nullptr)
		    node->history = pool.allocate();
                node->history->insert(++version, value);
            } else
                node->history = plog;
            succ = succs[0];
            if (succ == node)
                return true;
            for (size_t level = 0; level < node->next.size(); level++)
                node->next[level].store(succs[level]);
            pred = preds[0];
            if (pred->next[0].compare_exchange_weak(succ, node)) {
                if (plog == nullptr)
                    pool.append(key, node->history);
                break;
            }
        }
        size_t level = 1;
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
        if (node == nullptr)
            return false;
        node->history->remove(++version);
        return true;
    }

    V find(int v, const K &key) {
        node_t *node = find_node_with_shortcuts(key);
        if (node == nullptr)
            return low_marker;
        else
            return node->history->find(v);
    }

    void get_snapshot(int v, std::vector<std::pair<K, V>> &result) {
        result.clear();
        node_t *curr = head.next[0].load();
        while (curr != &tail) {
            auto p = std::make_pair(curr->key, curr->history->find(v));
            if (p.second != low_marker)
                result.push_back(p);
            curr = curr->next[0].load();
        }
    }

    void get_key_history(const K &key, std::vector<std::pair<int, V>> &result) {
        result.clear();
        node_t *node = find_node_with_shortcuts(key);
        if (node == nullptr)
            return;
        node->history->copy_to(result);
    }

    int latest() {
        return version;
    }
};

#endif // __VORDERED_KV
