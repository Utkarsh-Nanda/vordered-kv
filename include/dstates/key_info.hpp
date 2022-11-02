#ifndef __KEY_INFO
#define __KEY_INFO

#include <atomic>

class key_info_t {
    struct info_t {
	int version{0};
	bool removed{false};
    };
    std::atomic<info_t> info;

public:
    void update(int t, bool removed) {
	info_t prev, curr{t, removed};
	do {
	    prev = info.load();
	} while (prev.version < t && !info.compare_exchange_weak(prev, curr));
    }
    int latest_version() const {
	return info.load().version;
    }
    int latest_removed() const {
	return info.load().removed;
    }
};

#endif //__KEY_INFO
