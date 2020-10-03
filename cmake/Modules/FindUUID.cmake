# - Find uuid
# Find the native uuid headers and libraries.
#
# UUID_INCLUDE_DIR	- where to find uuid.h, etc.
# UUID_LIBRARIES	- List of libraries when using uuid.
# UUID_FOUND	- True if uuid found.

cmake_push_check_state()
# Look for the header file.
find_path(UUID_INCLUDE_DIR NAMES uuid/uuid.h)
if(UUID_INCLUDE_DIR)
  set(CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES};${UUID_INCLUDE_DIR}")
else()
  set(UUID_INCLUDE_DIR "" CACHE INTERNAL "uuid include directories")
endif()
# Look for the library.
find_library(UUID_LIBRARIES NAMES uuid) # Linux requires a separate library for UUID
if(NOT UUID_LIBRARIES)
  set(UUID_LIBRARIES "" CACHE INTERNAL "uuid libraries path")
endif()
# Find uuid_generate symbol, which we require and some platforms don't have it
set(CMAKE_REQUIRED_LIBRARIES ${UUID_LIBRARIES})
set(CMAKE_REQUIRED_INCLUDES ${UUID_INCLUDE_DIR})
check_symbol_exists("uuid_generate" "uuid/uuid.h" HAVE_UUID_GENERATE)
cmake_pop_check_state()

if(HAVE_UUID_GENERATE)
  include(FindPackageHandleStandardArgs)
  #if("${UUID_LIBRARIES}" STREQUAL "")
    #find_package_handle_standard_args(UUID DEFAULT_MSG UUID_INCLUDE_DIR)
  #else()
    find_package_handle_standard_args(UUID DEFAULT_MSG UUID_LIBRARIES UUID_INCLUDE_DIR)
  #endif()
  mark_as_advanced(UUID_INCLUDE_DIR UUID_LIBRARIES)
endif()

if(UUID_FOUND)
  if(NOT TARGET UUID::UUID) # AND (EXISTS "${UUID_LIBRARIES}"))
    add_library(UUID::UUID UNKNOWN IMPORTED)
    set_target_properties(UUID::UUID
      PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${UUID_INCLUDE_DIR}")

    #if(EXISTS "${UUID_LIBRARIES}")
      set_target_properties(UUID::UUID
        PROPERTIES
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${UUID_LIBRARIES}")
    #endif()
  endif()
endif()
