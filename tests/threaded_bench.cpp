#include <vector>
#include <map>
#include <thread>
#include <limits>
#include <random>
#include <algorithm>

#include "pskiplist.hpp"
#include "skiplist.hpp"
#include "lockedmap.hpp"
#include "sqlite_wrapper.hpp"
#include "rocksdb_wrapper.hpp"

#define __DEBUG
#include "debug.hpp"

typedef std::pair<int, int> intp_t;

static std::vector<intp_t> ref_vals;
static std::map<int, int> sorted_vals;
static const int N = 1000000;

void create_reference(int n) {
    std::mt19937 rng(112233L);
    ref_vals.resize(n);
    sorted_vals.clear();
    int i = 0;
    while (i < n) {
        int val = rng();
        auto it = sorted_vals.find(val);
        if (it != sorted_vals.end())
            continue;
        auto p = std::make_pair(val, val + 1);
        sorted_vals.insert(it, p);
        ref_vals[i] = p;
        i++;
    }
    std::cout << "Init complete, max element: " << sorted_vals.rbegin()->first + 1 << std::endl;
}

template <class Map> void run_extract_find(Map &vmap, int n, int r, int t) {
    std::atomic<int> extracted(0);
    TIMER_START(t_extract);
    #pragma omp parallel num_threads(t)
    {
        std::mt19937 rng(334455 + omp_get_thread_num());
        std::vector<std::pair<int, int>> result;
        #pragma omp for
        for (int j = 0; j < t; j++) {
            result.clear();
            int v = rng() % r;
            vmap.extract_snapshot(vmap.latest() - v, result);
            extracted += result.size();
        }
    }
    TIMER_STOP(t_extract, "extract snapshot " << t << " times, level = " << r << ", items = " << extracted);

    std::atomic<int> non_empty(0);
    TIMER_START(t_item);
    #pragma omp parallel num_threads(t)
    {
        std::mt19937 rng(445566 + omp_get_thread_num());
        std::vector<std::pair<int, int>> result;
        #pragma omp for
        for (int j = 0; j < n; j++) {
            int i = rng() % n;
            result.clear();
            vmap.extract_item(ref_vals[i].first, result);
            if (result.size() != 0)
                non_empty++;
            else
                DBG("empty history for key: " << ref_vals[i].first);
        }
    }
    TIMER_STOP(t_item, "extract item " << n << " keys, non empty history = " << non_empty);

    std::atomic<int> found(0);
    TIMER_START(t_find);
    #pragma omp parallel num_threads(t)
    {
        std::mt19937 rng(445566 + omp_get_thread_num());
        #pragma omp for
        for (int j = 0; j < n; j++) {
            int i = rng() % n;
            int v = rng() % r;
            if (vmap.find(vmap.latest() - v, ref_vals[i].first) != phistory_t<int>::low_marker)
                found++;
        }
    }
    TIMER_STOP(t_find, "find " << n << " KV pairs, level = " << r << ", items = " << found);
}

template <class Map> void run_insert_remove(Map &vmap, int n, int r, int t) {
    TIMER_START(t_insert);
    #pragma omp parallel num_threads(t)
    {
        #pragma omp for
        for (int i = 0; i < n; i++)
            vmap.insert(ref_vals[i].first, ref_vals[i].second);
    }
    TIMER_STOP(t_insert, "insert " << n << " KV pairs");
    TIMER_START(t_remove);
    #pragma omp parallel num_threads(t)
    {
        std::mt19937 rng(223344 + omp_get_thread_num());
        #pragma omp for
        for (int j = 0; j < r; j++) {
            int i = rng() % n;
            vmap.remove(ref_vals[i].first);
        }
    }
    TIMER_STOP(t_remove, "remove " << r << " KV pairs");
    TIMER_START(t_insert_rm);
    #pragma omp parallel num_threads(t)
    {
        #pragma omp for
        for (int i = 0; i < r; i++)
            vmap.insert(ref_vals[n + i].first, ref_vals[n + i].second);
    }
    TIMER_STOP(t_insert_rm, "insert after remove " << r << " KV pairs");
}

template <class Map> void run_ephemeral_approach(Map &vmap, bool initial, int N, int t) {
    if (initial) {
        DBG("initial pass");
        run_insert_remove(vmap, N, N, t);
        run_extract_find(vmap, N, vmap.latest(), t);
    } else
        DBG("skipping restart pass");
}

template <class Map> void run_persistent_approach(Map &vmap, bool initial, int N, int t) {
    if (initial) {
        DBG("initial pass");
        run_insert_remove(vmap, N, N, t);
    } else
        DBG("restart pass");
    run_extract_find(vmap, N, vmap.latest(), t);
}

void run_tests(const std::string &approach, bool initial, int N, int t, const std::string &db, bool shared) {
    if (approach == "skiplist_t") {
        skiplist_t<int, int> map;
        run_ephemeral_approach(map, initial, N, t);
    } else if (approach == "pskiplist_t") {
        pskiplist_t<int, int> map(db);
        run_persistent_approach(map, initial, N, t);
    } else if (approach == "locked_map_t") {
        locked_map_t<int, int> map;
        run_ephemeral_approach(map, initial, N, t);
    } else if (approach == "sqlite_wrapper_t") {
        sqlite_wrapper_t map(db, t, shared);
        run_persistent_approach(map, initial, N, t);
    } else if (approach == "rocksdb_wrapper_t") {
	rocksdb_wrapper_t<int, int> map(db);
	run_persistent_approach(map, initial, N, t);
    } else
        ERROR("no valid approach selected");
}

int main(int argc, char **argv) {
    if (argc < 5) {
        std::cout << "Usage: " << argv[0] << " <min_threads> <max_threads> <db_path> <approach>" << std::endl;
        return -1;
    }
    int ts = std::stoi(argv[1]), tc = std::stoi(argv[2]);
    std::string db(argv[3]), approach(argv[4]);
    bool shared = argc > 5;
    DBG("Multi-threaded ordered map test, threads: " << ts << "/" << tc);
    create_reference(2 * N);

    for (int t = ts; t <= tc; t <<= 1) {
        DBG("starting test for " << approach << " with " << t << " threads");
        run_tests(approach, true, N, t, db, shared);
        run_tests(approach, false, N, t, db, shared);
        unlink(db.c_str());
    }

    return 0;
}
