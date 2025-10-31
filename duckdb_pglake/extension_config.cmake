# External extensions to link into libduckdb
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG c93303d2654d1725db3e9f4dbe1c053586f9f3f2
    INCLUDE_DIR extension/httpfs/include
)

# Extension from this repo
duckdb_extension_load(duckdb_pglake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
