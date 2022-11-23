if(PARQUET)
    find_package(PkgConfig REQUIRED)
    pkg_search_module(GLIB REQUIRED glib-2.0)

    include_directories(${GLIB_INCLUDE_DIRS})
    link_directories(${GLIB_LIBRARY_DIRS})

    add_definitions(${GLIB_CFLAGS_OTHER})

    find_package(Arrow CONFIG REQUIRED)
    find_package(Parquet CONFIG REQUIRED
        PATHS ${Arrow_DIR} 
        NO_DEFAULT_PATH
    )

    if(PARQUET_FOUND)
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

    else()
        message(FATAL_ERROR "Parquet was requested but was not found")
    endif()
endif()

