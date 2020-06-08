# - Find netcdf
# Find the native netcdf headers and libraries.
#
# NETCDF_INCLUDE_DIR	- where to find netcdf.h, etc.
# NETCDF_LIBRARIES	- List of libraries when using netcdf.
# NETCDF_VERSION	- netcdf version if found
# NETCDF_FOUND	- True if netcdf found.

# Look for the header file.
find_path(NETCDF_INCLUDE_DIR NAMES netcdf.h)

# Look for the library.
find_library(NETCDF_LIBRARIES NAMES netcdf)

# Handle the QUIETLY and REQUIRED arguments and set NETCDF_FOUND
# to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NetCDF DEFAULT_MSG NETCDF_LIBRARIES NETCDF_INCLUDE_DIR)

if(NETCDF_FOUND)
  file(STRINGS "${NETCDF_INCLUDE_DIR}/netcdf_meta.h" NETCDF_VERSION_LINES REGEX "#define[ \t]+NC_VERSION_(MAJOR|MINOR|PATCH|NOTE)")
  string(REGEX REPLACE ".*NC_VERSION_MAJOR *\([0-9]*\).*" "\\1" NETCDF_VERSION_MAJOR "${NETCDF_VERSION_LINES}")
  string(REGEX REPLACE ".*NC_VERSION_MINOR *\([0-9]*\).*" "\\1" NETCDF_VERSION_MINOR "${NETCDF_VERSION_LINES}")
  string(REGEX REPLACE ".*NC_VERSION_PATCH *\([0-9]*\).*" "\\1" NETCDF_VERSION_PATCH "${NETCDF_VERSION_LINES}")
  string(REGEX REPLACE ".*NC_VERSION_NOTE *\"\([^\"]*\)\".*" "\\1" NETCDF_VERSION_NOTE "${NETCDF_VERSION_LINES}")
  set(NETCDF_VERSION "${NETCDF_VERSION_MAJOR}.${NETCDF_VERSION_MINOR}.${NETCDF_VERSION_PATCH}.${NETCDF_VERSION_NOTE}")
endif()

mark_as_advanced(NETCDF_INCLUDE_DIR NETCDF_LIBRARIES NETCDF_VERSION)

if(NETCDF_FOUND)
  set(NETCDF_MINIMUM_VERSION "4.2")
  if(NETCDF_VERSION VERSION_LESS "${NETCDF_MINIMUM_VERSION}")
    message(STATUS "netcdf library found, but the version is too old: ${NETCDF_VERSION} < ${NETCDF_MINIMUM_VERSION}")
    set(NETCDF_FOUND FALSE)
  endif()
endif()

if(NETCDF_FOUND)
  add_library(NetCDF::NetCDF UNKNOWN IMPORTED)
  set_target_properties(NetCDF::NetCDF
    PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${NETCDF_INCLUDE_DIR}")
  set_target_properties(NetCDF::NetCDF
    PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${NETCDF_LIBRARIES}")
endif()
