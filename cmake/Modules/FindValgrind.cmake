# - Find valgrind
# Find the native valgrind headers and libraries.
#
# VALGRIND_INCLUDE_DIR	- where to find valgrind.h, etc.
# VALGRIND_LIBRARIES	- List of libraries when using valgrind.
# VALGRIND_FOUND	- True if valgrind found.

find_path(VALGRIND_INCLUDE_DIR
  NAMES valgrind.h
  PATH_SUFFIXES valgrind)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Valgrind
  DEFAULT_MSG
  VALGRIND_INCLUDE_DIR)

mark_as_advanced(VALGRIND_INCLUDE_DIR)

if(VALGRIND_FOUND)
  if(NOT TARGET VALGRIND::VALGRIND)
    add_library(VALGRIND::VALGRIND UNKNOWN IMPORTED)
    set_target_properties(VALGRIND::VALGRIND
      PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${VALGRIND_INCLUDE_DIR}")
  endif()
endif()
