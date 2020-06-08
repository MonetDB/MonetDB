# - Find geos
# Find the native geos headers and libraries.
#
# GEOS_INCLUDE_DIR	- where to find geos_c.h, etc.
# GEOS_LIBRARIES	- List of libraries when using geos.
# GEOS_VERSION	- GEOS_VERSION if found
# GEOS_FOUND	- True if geos found.

# Look for the header file.
find_path(GEOS_INCLUDE_DIR NAMES geos_c.h geos_c_i.h)

# Look for the library.
find_library(GEOS_LIBRARIES NAMES geos_c geos_c_i)

# Handle the QUIETLY and REQUIRED arguments and set GEOS_FOUND to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Geos DEFAULT_MSG GEOS_LIBRARIES GEOS_INCLUDE_DIR)

if(GEOS_FOUND)
  file(STRINGS "${GEOS_INCLUDE_DIR}/geos_c.h" GEOS_VERSION_LINES REGEX "#define[ \t]+GEOS_VERSION_(MAJOR|MINOR|PATCH)")
  string(REGEX REPLACE ".*GEOS_VERSION_MAJOR *\([0-9]*\).*" "\\1" GEOS_VERSION_MAJOR "${GEOS_VERSION_LINES}")
  string(REGEX REPLACE ".*GEOS_VERSION_MINOR *\([0-9]*\).*" "\\1" GEOS_VERSION_MINOR "${GEOS_VERSION_LINES}")
  string(REGEX REPLACE ".*GEOS_VERSION_PATCH *\([0-9]*\).*" "\\1" GEOS_VERSION_PATCH "${GEOS_VERSION_LINES}")
  set(GEOS_VERSION "${GEOS_VERSION_MAJOR}.${GEOS_VERSION_MINOR}.${GEOS_VERSION_PATCH}")
  add_library(Geos::Geos UNKNOWN IMPORTED)
  set_target_properties(Geos::Geos
    PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${GEOS_INCLUDE_DIR}")
  set_target_properties(Geos::Geos
    PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${GEOS_LIBRARIES}")
endif()

mark_as_advanced(GEOS_INCLUDE_DIR GEOS_LIBRARIES GEOS_VERSION)
