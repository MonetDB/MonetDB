# - Find snappy
# Find the native snappy headers and libraries.
#
# SNAPPY_INCLUDE_DIR	- where to find snappy.h, etc.
# SNAPPY_LIBRARIES	- List of libraries when using snappy.
# SNAPPY_FOUND	- True if snappy found.

find_path(SNAPPY_INCLUDE_DIR NAMES snappy.h)
find_library(SNAPPY_LIBRARIES NAMES snappy)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Snappy
  DEFAULT_MSG
  SNAPPY_LIBRARIES
  SNAPPY_INCLUDE_DIR)

mark_as_advanced(SNAPPY_INCLUDE_DIR
  SNAPPY_LIBRARIES)

if(SNAPPY_FOUND)
  if(NOT TARGET SNAPPY::SNAPPY AND
      (EXISTS "${SNAPPY_LIBRARIES}"))
    add_library(SNAPPY::SNAPPY UNKNOWN IMPORTED)
    set_target_properties(SNAPPY::SNAPPY
      PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${SNAPPY_INCLUDE_DIR}")

    if(EXISTS "${SNAPPY_LIBRARIES}")
      set_target_properties(SNAPPY::SNAPPY
        PROPERTIES
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${SNAPPY_LIBRARIES}")
    endif()
  endif()
endif()
