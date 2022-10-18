# Comment as needed to reduce build time

# Simple tests
add_executable (int_test int_test.cpp)
add_executable (str_test str_test.cpp)
target_link_libraries (int_test ${DSTATES_LIBS})
target_link_libraries (str_test ${DSTATES_LIBS})

# Benchmarks
if(SQLite3_FOUND AND RocksDB_FOUND)
    add_executable (threaded_bench threaded_bench.cpp ${DSTATES_HEADERS})
    target_link_libraries (threaded_bench ${DSTATES_LIBS} ${SQLite3_LIBRARIES} ${RocksDB_LIBRARIES})
else()
    message(STATUS "Threaded benchmark not built due to missing SQLite3, RocksDB dependencies")
endif()

if (SQLite3_FOUND)
    add_executable (mpi_naive_merge mpi_naive_merge.cpp)
    add_executable (mpi_opt_merge mpi_opt_merge.cpp)
    target_link_libraries (mpi_naive_merge ${DSTATES_LIBS} ${MPI_CXX_LIBRARIES} ${SQLite3_LIBRARIES})
    target_link_libraries (mpi_opt_merge ${DSTATES_LIBS} ${MPI_CXX_LIBRARIES} ${SQLite3_LIBRARIES})
else()
    message(STATUS "MPI benchmarks not built due to missing SQLite3 dependency")
endif()