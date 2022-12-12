if(Parquet_FOUND)
  add_library(parquet_lib MODULE SHARED)

  target_sources(parquet_lib
    PRIVATE
    parquet.c
    parquet.h
  )

  target_link_libraries(parquet_lib
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
    parquet_lib
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT server)
endif()

