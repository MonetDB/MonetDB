# - Find liblas
# Find the native liblas headers and libraries.
#
# LIBLAS_INCLUDE_DIR	- where to find liblas.h, etc.
# LIBLAS_LIBRARIES	- List of libraries when using liblas.
# LIBLAS_VERSION	- liblas version if found
# LIBLAS_FOUND	- True if liblas found.

# Look for the header file.
find_path(LIBLAS_INCLUDE_DIR NAMES liblas/capi/liblas.h liblas/capi/las_version.h liblas/capi/las_config.h)

find_library(LIBLAS_LIBRARIES NAMES las)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LIBLAS DEFAULT_MSG LIBLAS_LIBRARIES LIBLAS_INCLUDE_DIR)

# Look for the library.
if(LIBLAS_FOUND)
	file(STRINGS "${LIBLAS_INCLUDE_DIR}/liblas/capi/las_version.h" LIBLAS_VERSION_LINES REGEX "#define[ \t]+LIBLAS_VERSION_(MAJOR|MINOR|REV)")
	string(REGEX REPLACE ".*LIBLAS_VERSION_MAJOR *\([0-9]*\).*" "\\1" LIBLAS_VERSION_MAJOR "${LIBLAS_VERSION_LINES}")
	string(REGEX REPLACE ".*LIBLAS_VERSION_MINOR *\([0-9]*\).*" "\\1" LIBLAS_VERSION_MINOR "${LIBLAS_VERSION_LINES}")
	string(REGEX REPLACE ".*LIBLAS_VERSION_REV *\([0-9]*\).*" "\\1" LIBLAS_VERSION_REV "${LIBLAS_VERSION_LINES}")
	set(LIBLAS_VERSION "${LIBLAS_VERSION_MAJOR}.${LIBLAS_VERSION_MINOR}.${LIBLAS_VERSION_REV}")
endif()

mark_as_advanced(LIBLAS_INCLUDE_DIR LIBLAS_LIBRARIES LIBLAS_VERSION)

if(LIBLAS_FOUND)
  add_library(Liblas::Liblas UNKNOWN IMPORTED)
  set_target_properties(Liblas::Liblas
    PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${LIBLAS_INCLUDE_DIR}")
  set_target_properties(Liblas::Liblas
    PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${LIBLAS_LIBRARIES}")
endif()
