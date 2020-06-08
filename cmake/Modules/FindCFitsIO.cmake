# - Find cfitsio
# Find the native cfitsio headers and libraries.
#
# CFITSIO_INCLUDE_DIR	- where to find fits.h, etc.
# CFITSIO_LIBRARIES	- List of libraries when using cfitsio.
# CFITSIO_FOUND	- True if cfitsio found.

# Look for the header file.
find_path(CFITSIO_INCLUDE_DIR NAMES fitsio.h PATH_SUFFIXES cfitsio) # On some systems fitsio.h is installed under cfitsio directory.

# Look for the library.
find_library(CFITSIO_LIBRARIES NAMES cfitsio)

# Handle the QUIETLY and REQUIRED arguments and set CFITSIO_FOUND to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CFitsIO DEFAULT_MSG CFITSIO_LIBRARIES CFITSIO_INCLUDE_DIR)

mark_as_advanced(CFITSIO_INCLUDE_DIR CFITSIO_LIBRARIES)

if(CFITSIO_FOUND)
  add_library(CFitsIO::CFitsIO UNKNOWN IMPORTED)
  set_target_properties(CFitsIO::CFitsIO
    PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${CFITSIO_INCLUDE_DIR}")
  set_target_properties(CFitsIO::CFitsIO
    PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${CFITSIO_LIBRARIES}")
endif()
