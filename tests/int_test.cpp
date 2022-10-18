#include "pskiplist.hpp"

#include <iostream>
#include <cassert>

static const int marker = phistory_t<int>::low_marker;

template<class T> void print_content(const T &map) {
    std::cout << "Snapshot content: ";
    for (auto &e: map)
	std::cout << "(" << e.first << ", " << e.second << ") ";
    std::cout << std::endl;
}

int main() {
    unlink("/dev/shm/test.db");
    pskiplist_t<int, int> vordered_kv("/dev/shm/test.db");

    vordered_kv.insert(1, 4);
    std::cout << "inserted (1, 4) at version 1" << std::endl;
    vordered_kv.insert(2, 3);
    std::cout << "inserted (2, 3) at version 2" << std::endl;
    vordered_kv.insert(1, 2);
    std::cout << "inserted (1, 2) at version 3" << std::endl;
    vordered_kv.insert(3, 1);
    std::cout << "inserted (3, 1) at version 4" << std::endl;
    assert(vordered_kv.find(1, 1) == 4);
    std::cout << "checked (1, 4) can be found at version 1" << std::endl;
    assert(vordered_kv.find(3, 3) == marker);
    std::cout << "checked 3 cannot be found at version 3" << std::endl;
    std::vector<std::pair<int, int>> result;
    vordered_kv.extract_snapshot(std::numeric_limits<int>::max(), result);
    print_content(result);

    return 0;
}
