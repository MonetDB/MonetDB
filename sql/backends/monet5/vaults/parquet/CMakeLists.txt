if(Parquet_FOUND)
  add_library(parquet_lib MODULE)

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
    Arrow
    Parquet
  )

  install(TARGETS
    parquet_lib
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT server)
endif()

