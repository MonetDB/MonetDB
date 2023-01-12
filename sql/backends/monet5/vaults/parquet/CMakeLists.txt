if(Parquet_FOUND)
  add_library(parquetlib MODULE)

  target_sources(parquetlib
    PRIVATE
    parquet.c
    parquet.h
  )



  target_link_libraries(parquetlib
    PRIVATE
    monetdb_config_header
    monetdb5
    bat
    sqlinclude
    arrow_shared 
    arrow-glib 
    gobject-2.0 
    glib-2.0 
    parquet_shared 
    parquet-glib
  )

  target_include_directories(parquetlib
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

  install(TARGETS
    parquetlib
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT server)
endif()

