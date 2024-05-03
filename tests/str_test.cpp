#include "dstates/vordered_kv.hpp"
//#include "dstates/rocksdb_wrapper.hpp"
#include "dstates/marker.hpp"

#include <iostream>
#include <cassert>
#include <filesystem>

using str_vordered_kv_t = vordered_kv_t<std::string, std::string>;
//using str_vordered_kv_t = rocksdb_wrapper_t<std::string, std::string>;

static const std::string marker = marker_t<std::string>::low_marker;

template<class T> void print_content(const T &map) {
    std::cout << "Result: ";
    for (auto &e: map)
	std::cout << "(" << e.first << ", " << e.second << ") ";
    std::cout << std::endl;
}

int main() {
    std::string db = "/dev/shm/test.db";
    std::filesystem::remove_all(db);
    str_vordered_kv_t vordered_kv(db);

    vordered_kv.insert("key1", "val4");
    vordered_kv.tag();
    std::cout << "inserted (key1, val4) at version 0" << std::endl;
    vordered_kv.insert("key2", "val3");
    vordered_kv.tag();
    std::cout << "inserted (key2, val3) at version 1" << std::endl;
    vordered_kv.insert("key1", "val2");
    vordered_kv.tag();
    std::cout << "inserted (key1, val2) at version 2" << std::endl;
    vordered_kv.insert("key3", "val1");
    vordered_kv.insert("key1", "val7");
    vordered_kv.insert("key3", "val2");
    vordered_kv.tag();
    std::cout << "inserted (key3, val1) (key1, val7) (key3, val2) at version 3" << std::endl;

    assert(vordered_kv.find(0, "key1") == "val4");
    std::cout << "checked (key1, val4) can be found at version 0" << std::endl;
    assert(vordered_kv.find(2, "key3") == marker);
    std::cout << "checked key3 cannot be found at version 2" << std::endl;
    assert(vordered_kv.find(3, "key3") == "val2");
    std::cout << "checked (key3, val2) can be found at version 3" << std::endl;

    std::vector<std::pair<std::string, std::string>> result;
    vordered_kv.get_snapshot(std::numeric_limits<int>::max(), result);
    print_content(result);
    assert(result.size() == 3);
    std::cout << "checked snapshot at version 3 has 3 entries" << std::endl;

    std::vector<std::pair<int, std::string>> key_result;
    vordered_kv.get_key_history("key1", key_result);
    print_content(key_result);
    assert(key_result.size() == 3);
    std::cout << "checked key history of key1 has 3 entries" << std::endl;

    return 0;
}
