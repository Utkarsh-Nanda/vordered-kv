#include "dstates/vordered_kv.hpp"

#include <iostream>
#include <cassert>

using int_vordered_kv_t = vordered_kv_t<int, int>;

static const int marker = int_vordered_kv_t::low_marker;

template<class T> void print_content(const T &map) {
    std::cout << "Result: ";
    for (auto &e: map)
	std::cout << "(" << e.first << ", " << e.second << ") ";
    std::cout << std::endl;
}

int main() {
    unlink("/dev/shm/test.db");
    int_vordered_kv_t vordered_kv("/dev/shm/test.db");

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
    vordered_kv.get_snapshot(std::numeric_limits<int>::max(), result);
    print_content(result);
    assert(result.size() == 3);
    std::cout << "checked snapshot at version 4 has 3 entries" << std::endl;

    std::vector<std::pair<int, int>> key_result;
    vordered_kv.get_key_history(1, key_result);
    print_content(key_result);
    assert(key_result.size() == 2);
    std::cout << "checked key history of 1 has 2 entries" << std::endl;

    return 0;
}
