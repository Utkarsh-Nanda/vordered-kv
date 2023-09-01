#ifndef __MARKER
#define __MARKER

#include <limits>
#include <string>
#include <libpmemobj++/container/string.hpp>

template <class T> struct marker_t {
    static const T low_marker, high_marker;
};

template <> const std::string marker_t<std::string>::low_marker = "";
template <class T> const T marker_t<T>::low_marker = std::numeric_limits<T>::min();

template <> const std::string marker_t<std::string>::high_marker = "\255";
template <class T> const T marker_t<T>::high_marker = std::numeric_limits<T>::max();

std::string get_volatile(const pmem::obj::string &v) {
    return std::string(v.data(), v.data() + v.size());
}
template <class T> T get_volatile(const T &v) {
    return v;
}

#endif // __MARKER
