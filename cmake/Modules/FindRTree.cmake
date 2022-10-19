# - Find RTree
# Find the native rtree headers and libraries.
#
# RTREE_INCLUDE_DIR	- where to find rtree.h.
# RTREE_LIBRARY - where to find rtree library.
# RTREE_FOUND	- True if rtree found.

# Look for the header file.
find_path(RTREE_INCLUDE_DIR NAMES rtree.h)

# Look for the library.
find_library(RTREE_LIBRARY NAMES librtree.a)

# Handle the QUIETLY and REQUIRED arguments and set RTREE_FOUND to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(RTree REQUIRED_VARS RTREE_LIBRARY RTREE_INCLUDE_DIR)

if(RTREE_FOUND AND NOT TARGET rtree:rtree)
  add_library(rtree::rtree UNKNOWN IMPORTED)
  set_target_properties(rtree::rtree
    PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${RTREE_INCLUDE_DIR}")
  set_target_properties(rtree::rtree
    PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${RTREE_LIBRARY}")
endif()

mark_as_advanced(RTREE_INCLUDE_DIR RTREE_LIBRARIES)
