# - Find Brotli
# Find the native Brotli headers and libraries.
#
# BROTLI_INCLUDE_DIR	- where to find BROTLI.h, etc.
# BROTLI_LIBRARIES	- List of libraries when using BROTLI.
# BROTLI_FOUND	- True if BROTLI found.

find_path(BROTLI_INCLUDE_DIR NAMES brotli/decode.h)
find_library(BROTLI_LIBRARIES NAMES brotlidec)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(BROTLI
  DEFAULT_MSG
  BROTLI_LIBRARIES
  BROTLI_INCLUDE_DIR)

mark_as_advanced(BROTLI_INCLUDE_DIR BROTLI_LIBRARIES)

if(BROTLI_FOUND)
	if(NOT TARGET BROTLI::BROTLI AND (EXISTS "${BROTLI_LIBRARIES}"))
		add_library(BROTLI::BROTLI UNKNOWN IMPORTED)
		set_target_properties(BROTLI::BROTLI
			PROPERTIES
			INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")

		if(EXISTS "${BROTLI_LIBRARIES}")
			set_target_properties(BROTLI::BROTLI
				PROPERTIES
				IMPORTED_LINK_INTERFACE_LANGUAGES "C"
				IMPORTED_LOCATION "${BROTLI_LIBRARIES}")
		endif()
	endif()
endif()
