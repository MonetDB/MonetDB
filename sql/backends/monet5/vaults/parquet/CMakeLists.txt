
add_library(parquet MODULE)

target_sources(parquet
    PRIVATE
    parquet.c
	  pqc_reader.h
	  pqc_filemetadata.h
	  pqc_thrift.h
	  pqc_reader.c
	  pqc_filemetadata.c
	  pqc_thrift.c)

target_include_directories(parquet
    PRIVATE
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(parquet
    PRIVATE
    monetdb_config_header
    sqlinclude
    sql
    monetdb5
    bat
    stream
    $<$<BOOL:${SNAPPY_FOUND}>:SNAPPY::SNAPPY>
    $<$<BOOL:${ZLIB_FOUND}>:ZLIB::ZLIB>
    $<$<BOOL:${ZSTD_FOUND}>:ZSTD::ZSTD>
    $<$<BOOL:${LZ4_FOUND}>:LZ4::LZ4>
    $<$<BOOL:${BROTLI_FOUND}>:BROTLI::BROTLI>
  )

set_target_properties(parquet
    PROPERTIES
    OUTPUT_NAME
    _parquet)

install(TARGETS
    parquet
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
    COMPONENT server)
