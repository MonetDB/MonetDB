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

  install(TARGETS
    parquetlib
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT server)
endif()

