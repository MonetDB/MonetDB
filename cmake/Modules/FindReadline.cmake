# - Find readline
# Find the native readline headers and libraries.
#
# READLINE_INCLUDE_DIRS	- where to find readline.h, etc.
# READLINE_LIBRARIES	- List of libraries when using readline.
# READLINE_FOUND	- True if readline found.

find_package(PkgConfig QUIET)
pkg_check_modules(PC_READLINE QUIET readline>=8.0)

if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin" OR PC_READLINE_FOUND)

# Look for the header file.
find_path(READLINE_INCLUDE_DIR NAMES readline/readline.h
   HINTS
   ${PC_READLINE_INCLUDEDIR}
   ${PC_READLINE_INCLUDE_DIRS}
   PATH_SUFFIXES readline
   )

# Look for the library.
find_library(READLINE_LIBRARIES NAMES readline
   HINTS
   ${PC_READLINE_LIBDIR}
   ${PC_READLINE_LIBRARY_DIRS}
   )

endif()

# Handle the QUIETLY and REQUIRED arguments and set READLINE_FOUND to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Readline DEFAULT_MSG READLINE_LIBRARIES READLINE_INCLUDE_DIR)

mark_as_advanced(READLINE_INCLUDE_DIR READLINE_LIBRARIES)

if(READLINE_FOUND)
  add_library(Readline::Readline UNKNOWN IMPORTED)
  set_target_properties(Readline::Readline
    PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${READLINE_INCLUDE_DIR}")
  set_target_properties(Readline::Readline
    PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${READLINE_LIBRARIES}")
endif()
