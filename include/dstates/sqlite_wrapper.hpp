#ifndef __SQLITE_WRAPPER
#define __SQLITE_WRAPPER

#include <sstream>
#include <cassert>
#include <vector>
#include <sqlite3.h>
#include <atomic>
#include <thread>

#define __DEBUG
#include "debug.hpp"

using namespace std::chrono_literals;
typedef std::vector<std::pair<int,int>> vintint_t;

static int get_latest_version(void *max_version, int count, char **data, char **columns) {
    assert(count == 1);
    if (data[0])
        static_cast<std::atomic<int> *>(max_version)->store(std::stoi(data[0]));
    return 0;
}

class sqlite_wrapper_t {
    struct thread_state_t {
        sqlite3_stmt *insert_stmt = nullptr,
            *find_stmt = nullptr, *snap_stmt = nullptr, *item_stmt = nullptr;
        sqlite3 *LocalDB = nullptr;
    };
    std::string db_file;
    std::vector<thread_state_t> handle;
    std::atomic<int> version{0};
    sqlite3 *MainDB;
    bool shared;

public:
    static const int marker = std::numeric_limits<int>::min();

    sqlite_wrapper_t(const std::string _db_file, int thread_no, bool _sh) : db_file(_db_file), handle(thread_no), shared(_sh) {
        open_db(MainDB);
        std::string sql =
            "create table if not exists history(ts int primary key desc, k int, v int);"
            "create index if not exists key_idx on history (k)";
        if (sqlite3_exec(MainDB, sql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK)
            throw std::runtime_error("cannot create history table: " + std::string(sqlite3_errmsg(MainDB)));
        sqlite3_exec(MainDB, "select max(ts) from history", get_latest_version, (void *)&version, nullptr);
        for (int i = 0; i < thread_no; i++) {
            open_db(handle[i].LocalDB);
            ASSERT(sqlite3_prepare_v2(MainDB, "insert into history values (?, ?, ?)", -1, &handle[i].insert_stmt, nullptr) == SQLITE_OK);
            ASSERT(sqlite3_prepare_v2(handle[i].LocalDB, "select k, v, max(ts) from history where k = ? and ts <= ? group by k",
                                      -1, &handle[i].find_stmt, nullptr) == SQLITE_OK);
            ASSERT(sqlite3_prepare_v2(handle[i].LocalDB, "select k, v, max(ts) from history where ts <= ? group by k order by k",
                                      -1, &handle[i].snap_stmt, nullptr) == SQLITE_OK);
            ASSERT(sqlite3_prepare_v2(handle[i].LocalDB, "select ts, v from history where k = ?",
                                      -1, &handle[i].item_stmt, nullptr) == SQLITE_OK);
        }
        DBG("Initialization complete, latest version = " << version);
    }
    ~sqlite_wrapper_t() {
        for (size_t i = 0; i < handle.size(); i++) {
            sqlite3_finalize(handle[i].insert_stmt);
            sqlite3_finalize(handle[i].find_stmt);
            sqlite3_finalize(handle[i].snap_stmt);
            sqlite3_finalize(handle[i].item_stmt);
            sqlite3_close(handle[i].LocalDB);
        }
        sqlite3_close(MainDB);
    }

    int latest() const {
        return version;
    }

    void open_db(sqlite3 *&DB) {
        if (shared && sqlite3_open_v2("file::memory:?cache=shared", &DB, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, nullptr) != SQLITE_OK)
            throw std::runtime_error("cannot open db in shared mode");
        if (!shared && sqlite3_open_v2(db_file.c_str(), &DB, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK)
            throw std::runtime_error("cannot open db in normal mode");
        if (sqlite3_exec(DB, "pragma journal_mode=WAL", nullptr, nullptr, nullptr) != SQLITE_OK)
            throw std::runtime_error("cannot switch to WAL mode: " + std::string(sqlite3_errmsg(DB)));
    }

    void insert(int key, int value) {
        auto insert_stmt = handle[omp_get_thread_num()].insert_stmt;
        sqlite3_bind_int(insert_stmt, 1, ++version);
        sqlite3_bind_int(insert_stmt, 2, key);
        sqlite3_bind_int(insert_stmt, 3, value);
        int rc = sqlite3_step(insert_stmt);
        if (rc != SQLITE_DONE)
            throw std::runtime_error("cannot insert using prepared statement: " + std::to_string(rc));
        sqlite3_reset(insert_stmt);
    }

    void remove(int key) {
        insert(key, marker);
    }

    int find(int ts, int key) {
        auto find_stmt = handle[omp_get_thread_num()].find_stmt;
        sqlite3_bind_int(find_stmt, 1, key);
        sqlite3_bind_int(find_stmt, 2, ts);
        int ret = marker;
        if (sqlite3_step(find_stmt) == SQLITE_ROW)
            ret = sqlite3_column_int(find_stmt, 1);
        sqlite3_reset(find_stmt);
        return ret;
    }

    void get_snapshot(int ts, vintint_t &res) {
        auto snap_stmt = handle[omp_get_thread_num()].snap_stmt;
        sqlite3_bind_int(snap_stmt, 1, ts);
        int rc = sqlite3_step(snap_stmt);
        while (rc == SQLITE_ROW) {
            int v = sqlite3_column_int(snap_stmt, 1);
            if (v != marker)
                res.push_back(std::make_pair(sqlite3_column_int(snap_stmt, 0), v));
            rc = sqlite3_step(snap_stmt);
        }
        sqlite3_reset(snap_stmt);
    }

    void get_key_history(int key, vintint_t &res) {
        auto item_stmt = handle[omp_get_thread_num()].item_stmt;
        sqlite3_bind_int(item_stmt, 1, key);
        int rc = sqlite3_step(item_stmt);
        while (rc == SQLITE_ROW) {
            res.push_back(std::make_pair(sqlite3_column_int(item_stmt, 0), sqlite3_column_int(item_stmt, 1)));
            rc = sqlite3_step(item_stmt);
        }
        sqlite3_reset(item_stmt);
    }
};

#endif //__SQLITE_WRAPPER
