#ifndef __ROCKSBD_WRAPPER_HPP
#define __ROCKSDB_WRAPPER_HPP

#include "marker.hpp"

#include <rocksdb/db.h>
#include <rocksdb/convenience.h>

template <class K, class V> class rocksdb_wrapper_t {
    rocksdb::DB  *db;
    std::atomic<uint64_t> version{0};

    template <class T> inline rocksdb::Slice get_slice(const T &obj) {
        if constexpr(std::is_same<T, std::string>::value)
            return rocksdb::Slice(obj);
        else
            return rocksdb::Slice((char *)&obj, sizeof(T));
    }
    template <class T, class S> inline T extract_slice(const S &slice) {
        if constexpr(std::is_same<T, std::string>::value)
            return slice.ToString();
        else
            return *((T *)slice.data());
    }

    void rocksdb_assert(const rocksdb::Status &s) {
        if (s.ok())
            return;
        FATAL(s.ToString());
    }

public:
    rocksdb_wrapper_t(const std::string &db_name) {
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status;
        status = rocksdb::Comparator::CreateFromString(
            rocksdb::ConfigOptions(), "leveldb.BytewiseComparator.u64ts", &options.comparator);
        rocksdb_assert(status);
        status = rocksdb::DB::Open(options, db_name, &db);
        rocksdb_assert(status);
        rocksdb::ReadOptions read_opts;
        size_t tv = std::numeric_limits<size_t>::max();
        rocksdb::Slice ts = get_slice(tv);
        read_opts.timestamp = &ts;
        rocksdb::Iterator* it = db->NewIterator(read_opts);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            size_t ts = extract_slice<size_t>(it->timestamp());
            if (ts > version)
                version = ts;
        }
        delete it;
        DBG("successfully opened DB: " << db_name << ", timestamp = " << version);
    }
    ~rocksdb_wrapper_t() {
        delete db;
    }

    int latest() const {
        return version;
    }

    int tag() {
        return version++;
    }

    void insert(const K &key, const V &value) {
        rocksdb::Status status = db->Put(rocksdb::WriteOptions(), get_slice(key), get_slice(version), get_slice(value));
        rocksdb_assert(status);
    }

    void remove(const K &key) {
        insert(key, marker_t<V>::low_marker);
    }

    V find(int v, const K &key) {
        uint64_t tv = v;
        rocksdb::ReadOptions read_opts;
        rocksdb::Slice ts = get_slice(tv);
        read_opts.timestamp = &ts;
        rocksdb::PinnableSlice value;
        rocksdb::Status status = db->Get(read_opts, db->DefaultColumnFamily(), get_slice(key), &value);
        if (status.IsNotFound())
            return marker_t<V>::low_marker;
        else
            return extract_slice<V>(value);
    }

    void get_snapshot(int v, std::vector<std::pair<K,V>> &result) {
        result.clear();
        uint64_t tv = v;
        rocksdb::ReadOptions read_opts;
        rocksdb::Slice ts = get_slice(tv);
        read_opts.timestamp = &ts;
        rocksdb::Iterator* it = db->NewIterator(read_opts);
        for (it->SeekToFirst(); it->Valid(); it->Next())
            result.emplace_back(extract_slice<K>(it->key()), extract_slice<V>(it->value()));
        delete it;
    }

    void get_key_history(const K &key, std::vector<std::pair<int, V>> &result) {
        result.clear();
        rocksdb::ReadOptions read_opts;
        rocksdb::PinnableSlice value;
        rocksdb::Status status;
        V last_val = marker_t<V>::low_marker;

        for (uint64_t tv = 0; tv <= version; tv++) {
            rocksdb::Slice ts = get_slice(tv);
            read_opts.timestamp = &ts;
            status = db->Get(read_opts, db->DefaultColumnFamily(), get_slice(key), &value);
            if (status.ok()) {
                V new_val = extract_slice<V>(value);
                if (new_val != last_val) {
                    result.emplace_back(tv, new_val);
                    last_val = new_val;
                }
            }
            value.Reset();
        }
    }
};

#endif //__ROCKSDB_WRAPPER_T
