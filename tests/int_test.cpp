#include "dstates/vordered_kv.hpp"
//#include "dstates/rocksdb_wrapper.hpp"
#include "dstates/marker.hpp"

#include <iostream>
#include <cassert>
#include <filesystem>

using int_vordered_kv_t = vordered_kv_t<int, int>;
//using int_vordered_kv_t = rocksdb_wrapper_t<int, int>;

static const int marker = marker_t<int>::low_marker;

template<class T> void print_content(const T &map) {
    std::cout << "Result: ";
    for (auto &e: map)
	std::cout << "(" << e.first << ", " << e.second << ") ";
    std::cout << std::endl;
}

int main() {
    std::string db = "/dev/shm/test.db";
    std::filesystem::remove_all(db);
    int_vordered_kv_t vordered_kv(db);

    vordered_kv.insert(1, 4);
    vordered_kv.tag();
    std::cout << "inserted (1, 4) at version 0" << std::endl;
    vordered_kv.insert(2, 3);
    vordered_kv.tag();
    std::cout << "inserted (2, 3) at version 1" << std::endl;
    vordered_kv.insert(1, 2);
    vordered_kv.tag();
    std::cout << "inserted (1, 2) at version 2" << std::endl;
    vordered_kv.insert(3, 1);
    vordered_kv.insert(1, 7);
    vordered_kv.insert(3, 2);
    vordered_kv.tag();
    std::cout << "inserted (3, 1) (1, 7) (3, 2) at version 3" << std::endl;

    assert(vordered_kv.find(0, 1) == 4);
    std::cout << "checked (1, 4) can be found at version 0" << std::endl;
    assert(vordered_kv.find(2, 3) == marker);
    std::cout << "checked 3 cannot be found at version 2" << std::endl;
    assert(vordered_kv.find(3, 3) == 2);
    std::cout << "checked (3, 2) can be found at version 3" << std::endl;

    std::vector<std::pair<int, int>> result;
    vordered_kv.get_snapshot(std::numeric_limits<int>::max(), result);
    print_content(result);
    assert(result.size() == 3);
    std::cout << "checked latest snapshot (version 3) has 3 entries" << std::endl;

    std::vector<std::pair<int, int>> key_result;
    vordered_kv.get_key_history(1, key_result);
    print_content(key_result);
    assert(key_result.size() == 3);
    std::cout << "checked key history of 1 has 3 entries" << std::endl;

    return 0;
}
