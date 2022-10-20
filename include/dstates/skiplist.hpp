#ifndef __SKIP_LIST
#define __SKIP_LIST

#include "history.hpp"

#include <atomic>
#include <strings.h>

#define __DEBUG
#include "debug.hpp"

template <typename K, typename V> class skiplist_t {
    static const int MAX_LEVEL = 24;

    struct node_t {
        typedef std::atomic<node_t *> next_t;
        typedef history_opt_t<V> history_t;
        K key;
        history_t history;
        std::vector<next_t> next;

        node_t(const K &k, int levels = MAX_LEVEL) : key(k), next(levels) { }
    };

    node_t head, tail;
    std::atomic<int> version{0};

public:
    skiplist_t() : head(std::numeric_limits<K>::min()), tail(std::numeric_limits<K>::max()) {
        for (int j = 0; j < MAX_LEVEL; j++)
            head.next[j].store(&tail);
    }
    ~skiplist_t() {
        node_t *curr = head.next[0].load();
        while (curr != &tail) {
            node_t *next = curr->next[0].load();
            delete curr;
            curr = next;
        }
    }

    node_t *find_node(const K &key, node_t **preds, node_t **succs) {
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

    bool insert(const K &key, const V &value) {
        node_t *preds[MAX_LEVEL], *succs[MAX_LEVEL];
        node_t *pred, *succ, *node = NULL;
        do {
            node_t *found = find_node(key, preds, succs);
            if (found) {
                if (node)
                    delete node;
                node = found;
            } else if (node == NULL) {
                int levels = ffs(rand() | (1 << (MAX_LEVEL - 1)));
                node = new node_t(key, levels);
            }
            node->history.insert(++version, value);
            succ = succs[0];
            if (succ == node)
                return true;
            for (size_t level = 0; level < node->next.size(); level++)
                node->next[level].store(succs[level]);
            pred = preds[0];
        } while (!pred->next[0].compare_exchange_weak(succ, node));
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
        if (node == NULL)
            return false;
        node->history.remove(++version);
        return true;
    }

    V find(int v, const K &key) {
        node_t *preds[MAX_LEVEL], *succs[MAX_LEVEL];
        node_t *node = find_node(key, preds, succs);
        if (node == NULL)
            return node_t::history_t::marker;
        else
            return node->history.find(v);
    }

    void extract_snapshot(int v, std::vector<std::pair<K,V>> &result) {
        result.clear();
        node_t *curr = head.next[0].load();
        while (curr != &tail) {
            auto p = std::make_pair(curr->key, curr->history.find(v));
            if (p.second != node_t::history_t::marker)
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
        node->history.copy_to(result);
    }

    int latest() {
        return version;
    }
};

#endif // __SKIP_LIST
