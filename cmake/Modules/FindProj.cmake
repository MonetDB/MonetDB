# - Find proj
# Find the native proj headers and libraries.
#
# PROJ_INCLUDE_DIR	- where to find proj_api.h, etc.
# PROJ_LIBRARIES	- List of libraries when using proj.
# PROJ_FOUND	- True if proj found.

find_path(PROJ_INCLUDE_DIR NAMES proj_api.h)
find_library(PROJ_LIBRARIES NAMES proj)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Proj
  DEFAULT_MSG
  PROJ_LIBRARIES
  PROJ_INCLUDE_DIR)

mark_as_advanced(PROJ_INCLUDE_DIR PROJ_LIBRARIES)

if(PROJ_FOUND)
  if(NOT TARGET PROJ::PROJ AND (EXISTS "${PROJ_LIBRARIES}"))
    add_library(PROJ::PROJ UNKNOWN IMPORTED)
    set_target_properties(PROJ::PROJ
      PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${PROJ_INCLUDE_DIR}")
    set_target_properties(PROJ::PROJ
      PROPERTIES
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${PROJ_LIBRARIES}")
  endif()
endif()
