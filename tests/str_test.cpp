#include "dstates/vordered_kv.hpp"

#include <iostream>
#include <cassert>

using str_vordered_kv_t = vordered_kv_t<std::string, std::string>;

static const std::string marker = str_vordered_kv_t::low_marker;

template<class T> void print_content(const T &map) {
    std::cout << "Result: ";
    for (auto &e: map)
	std::cout << "(" << e.first << ", " << e.second << ") ";
    std::cout << std::endl;
}

int main() {
    unlink("/dev/shm/test.db");
    str_vordered_kv_t vordered_kv("/dev/shm/test.db");

    vordered_kv.insert("key1", "val4");
    std::cout << "inserted (key1, val4) at version 1" << std::endl;
    vordered_kv.insert("key2", "val3");
    std::cout << "inserted (key2, val3) at version 2" << std::endl;
    vordered_kv.insert("key1", "val2");
    std::cout << "inserted (key1, val2) at version 3" << std::endl;
    vordered_kv.insert("key3", "val1");
    std::cout << "inserted (key3, val1) at version 4" << std::endl;
    assert(vordered_kv.find(1, "key1") == "val4");
    std::cout << "checked (key1, val4) can be found at version 1" << std::endl;
    assert(vordered_kv.find(3, "key3") == marker);
    std::cout << "checked key3 cannot be found at version 3" << std::endl;

    std::vector<std::pair<std::string, std::string>> result;
    vordered_kv.get_snapshot(std::numeric_limits<int>::max(), result);
    print_content(result);
    assert(result.size() == 3);
    std::cout << "checked snapshot at version 4 has 3 entries" << std::endl;

    std::vector<std::pair<int, std::string>> key_result;
    vordered_kv.get_key_history("key1", key_result);
    print_content(key_result);
    assert(key_result.size() == 2);
    std::cout << "checked key history of key1 has 2 entries" << std::endl;

    return 0;
}
