find_path(RocksDB_INCLUDE_DIR rocksdb/db.h)
find_library(RocksDB_LIBRARY NAMES rocksdb)

set(RocksDB_LIBRARIES ${RocksDB_LIBRARY})
set(RocksDB_INCLUDE_DIRS ${RocksDB_INCLUDE_DIR})

set(MSG_NOT_FOUND "RocksDB NOT found (set CMAKE_PREFIX_PATH to point the location)")
if(NOT (RocksDB_INCLUDE_DIR AND RocksDB_LIBRARY))
    if(RocksDB_FIND_REQUIRED)
	message(FATAL_ERROR ${MSG_NOT_FOUND})
    else()
	message(WARNING ${MSG_NOT_FOUND})
    endif()
else()
    message(STATUS "Found RocksDB: ${RocksDB_LIBRARY}")
    set(RocksDB_FOUND TRUE)
endif()

mark_as_advanced(RocksDB_LIBRARY RocksDB_INCLUDE_DIR)
