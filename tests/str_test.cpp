#include "dstates/pskiplist.hpp"

#include <iostream>
#include <cassert>

static const std::string marker = phistory_t<std::string>::low_marker;

template<class T> void print_content(const T &map) {
    std::cout << "Snapshot content: ";
    for (auto &e: map)
	std::cout << "(" << e.first << ", " << e.second << ") ";
    std::cout << std::endl;
}

int main() {
    unlink("/dev/shm/test.db");
    pskiplist_t<std::string, std::string> vordered_kv("/dev/shm/test.db");

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
    vordered_kv.extract_snapshot(std::numeric_limits<int>::max(), result);
    print_content(result);

    return 0;
}
