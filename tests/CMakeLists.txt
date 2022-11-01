# Simple tests
add_executable (int_test int_test.cpp)
add_executable (str_test str_test.cpp)
target_link_libraries (int_test ${DSTATES_LIBS})
target_link_libraries (str_test ${DSTATES_LIBS})
