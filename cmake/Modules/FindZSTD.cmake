# - Find ZSTD
# Find the native ZSTD headers and libraries.
#
# ZSTD_INCLUDE_DIR	- where to find ZSTD.h, etc.
# ZSTD_LIBRARIES	- List of libraries when using ZSTD.
# ZSTD_FOUND	- True if ZSTD found.

find_path(ZSTD_INCLUDE_DIR NAMES zstd.h)
find_library(ZSTD_LIBRARIES NAMES zstd)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZSTD
  DEFAULT_MSG
  ZSTD_LIBRARIES
  ZSTD_INCLUDE_DIR)

mark_as_advanced(ZSTD_INCLUDE_DIR ZSTD_LIBRARIES)

if(ZSTD_FOUND)
  if(NOT TARGET ZSTD::ZSTD AND (EXISTS "${ZSTD_LIBRARIES}"))
    add_library(ZSTD::ZSTD UNKNOWN IMPORTED)
    set_target_properties(ZSTD::ZSTD
      	PROPERTIES
      	INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}")

    if(EXISTS "${ZSTD_LIBRARIES}")
      set_target_properties(ZSTD::ZSTD
        PROPERTIES
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
	IMPORTED_LOCATION "${ZSTD_LIBRARIES}")
    endif()
  endif()
endif()
