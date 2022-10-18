#ifndef __LOCKEDMAP_HPP
#define __LOCKEDMAP_HPP

#include "history.hpp"

#include <mutex>
#include <memory>

#define __DEBUG
#include "debug.hpp"

template <class K, class V> class locked_map_t {
    typedef history_opt_t<V> interval_t;
    typedef std::shared_ptr<interval_t> pinterval_t;
    std::mutex m;
    std::map<K, pinterval_t> map;
    std::atomic<int> version{0};

public:
    void cleanup() { }
    int latest() const {
        return version;
    }
    void insert(const K &key, const V &value) {
        std::unique_lock<std::mutex> lock(m);
        auto it = map.find(key);
        if (it == map.end()) {
            pinterval_t p(new interval_t());
            it = map.emplace(std::make_pair(key, p)).first;
        }
        it->second->insert(++version, value);
    }
    void remove(const K &key) {
        insert(key, interval_t::marker);
    }
    V find(int v, const K &key) {
        std::unique_lock<std::mutex> lock(m);
        auto it = map.find(key);
        if (it == map.end())
            return interval_t::marker;
        else
            return it->second->find(v);
    }
    void extract_snapshot(int v, std::vector<std::pair<K,V>> &result) {
        std::unique_lock<std::mutex> lock(m);
        result.clear();
        for (auto it = map.begin(); it != map.end(); it++) {
            V res = it->second->find(v);
            if (res != interval_t::marker)
                result.emplace_back(std::make_pair(it->first, res));
        }
    }
    void extract_item(int key, std::vector<std::pair<K,V>> &result) {
        std::unique_lock<std::mutex> lock(m);
        result.clear();
        auto it = map.find(key);
        if (it == map.end())
            return;
        it->second->copy_to(result);
    }
};

#endif //__LOCKEDMAP_H
