# - Find htslib
# Find the native htslib headers and libraries.
#
# HTSLIB_INCLUDE_DIRS	- where to find hts.h, etc.
# HTSLIB_LIBRARIES	- List of libraries when using htslib.
# HTSLIB_FOUND	- True if htslib found.

find_package(PkgConfig QUIET)
pkg_check_modules(PC_HTSLIB QUIET htslib>=1.10)

if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin" OR PC_HTSLIB_FOUND)
  if(NOT WIN32)

    # Look for the header file.
    find_path(HTSLIB_INCLUDE_DIR NAMES htslib/hts.h
      HINTS
      ${PC_HTSLIB_INCLUDEDIR}
      ${PC_HTSLIB_INCLUDE_DIRS}
      PATH_SUFFIXES htslib
      )

    # Look for the library.
    find_library(HTSLIB_LIBRARIES NAMES hts
      HINTS
      ${PC_HTSLIB_LIBDIR}
      ${PC_HTSLIB_LIBRARY_DIRS}
      )

  endif()
endif()

# Handle the QUIETLY and REQUIRED arguments and set HTSLIB_FOUND to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(HTSLIB DEFAULT_MSG HTSLIB_LIBRARIES HTSLIB_INCLUDE_DIR)

mark_as_advanced(HTSLIB_INCLUDE_DIR HTSLIB_LIBRARIES)

if(HTSLIB_FOUND)
  add_library(HTSLIB::HTSLIB UNKNOWN IMPORTED)
  set_target_properties(HTSLIB::HTSLIB
    PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${HTSLIB_INCLUDE_DIR}")
  set_target_properties(HTSLIB::HTSLIB
    PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${HTSLIB_LIBRARIES}")
endif()
