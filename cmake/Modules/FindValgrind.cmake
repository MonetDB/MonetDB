# - Find valgrind
# Find the native valgrind headers and libraries.
#
# VALGRIND_INCLUDE_DIR	- where to find valgrind.h, etc.
# VALGRIND_LIBRARIES	- List of libraries when using valgrind.
# VALGRIND_FOUND	- True if valgrind found.

find_path(VALGRIND_INCLUDE_DIR
  NAMES valgrind.h)

find_library(VALGRIND_LIBRARIES
  NAMES valgrind)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Valgrind
  DEFAULT_MSG
  VALGRIND_LIBRARIES
  VALGRIND_INCLUDE_DIR)

mark_as_advanced(VALGRIND_INCLUDE_DIR
  VALGRIND_LIBRARIES)

if(VALGRIND_FOUND)
  if(NOT TARGET VALGRIND::VALGRIND AND
      (EXISTS "${VALGRIND_LIBRARIES}"))
    add_library(VALGRIND::VALGRIND UNKNOWN IMPORTED)
    set_target_properties(VALGRIND::VALGRIND
      PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${VALGRIND_INCLUDE_DIR}")

    if(EXISTS "${VALGRIND_LIBRARIES}")
      set_target_properties(VALGRIND::VALGRIND
        PROPERTIES
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${VALGRIND_LIBRARIES}")
    endif()
  endif()
endif()
