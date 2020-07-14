# - Find lz4
# Find the native lz4 headers and libraries.
#
# LZ4_INCLUDE_DIR	- where to find lz4.h, etc.
# LZ4_LIBRARIES	- List of libraries when using lz4.
# LZ4_VERSION	- LZ4_VERSION if found
# LZ4_FOUND	- True if lz4 found.

include(FindPackageHandleStandardArgs)

find_path(LZ4_INCLUDE_DIR NAMES lz4.h)
find_library(LZ4_LIBRARIES NAMES lz4)

if(LZ4_INCLUDE_DIR AND EXISTS "${LZ4_INCLUDE_DIR}/lz4.h")
  file(STRINGS "${LZ4_INCLUDE_DIR}/lz4.h" LZ4_VERSION_LINES REGEX "#define[ \t]+LZ4_VERSION_(MAJOR|MINOR|RELEASE)")
  string(REGEX REPLACE ".*LZ4_VERSION_MAJOR *\([0-9]*\).*" "\\1" LZ4_VERSION_MAJOR "${LZ4_VERSION_LINES}")
  string(REGEX REPLACE ".*LZ4_VERSION_MINOR *\([0-9]*\).*" "\\1" LZ4_VERSION_MINOR "${LZ4_VERSION_LINES}")
  string(REGEX REPLACE ".*LZ4_VERSION_RELEASE *\([0-9]*\).*" "\\1" LZ4_VERSION_RELEASE "${LZ4_VERSION_LINES}")
  set(LZ4_VERSION "${LZ4_VERSION_MAJOR}.${LZ4_VERSION_MINOR}.${LZ4_VERSION_RELEASE}")
endif()

find_package_handle_standard_args(LZ4
  REQUIRED_VARS
  LZ4_LIBRARIES
  LZ4_INCLUDE_DIR
  VERSION_VAR LZ4_VERSION)

if(LZ4_FOUND)
  if(NOT TARGET LZ4::LZ4 AND
      (EXISTS "${LZ4_LIBRARIES}"))
    add_library(LZ4::LZ4 UNKNOWN IMPORTED)
    set_target_properties(LZ4::LZ4
      PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}")

    if(EXISTS "${LZ4_LIBRARIES}")
      set_target_properties(LZ4::LZ4
        PROPERTIES
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${LZ4_LIBRARIES}")
    endif()
  endif()
endif()

mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARIES LZ4_VERSION)
