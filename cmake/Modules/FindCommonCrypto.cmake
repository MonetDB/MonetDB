# - Find CommonCrytpo
# Find the native uuid headers and libraries.
#
# COMMONCRYPTO_INCLUDE_DIR	- where to find  CommonCrypto/CommonDigest.h, etc.
# COMMONCRYPTO_LIBRARIES	- List of libraries when using uuid.
# COMMONCRYPTO_FOUND	- True if crypto found.

# Look for the header file.
find_path(COMMONCRYPTO_INCLUDE_DIR NAMES CommonCrypto/CommonDigest.h)

# Look for the library.
find_library(COMMONCRYPTO_LIBRARIES NAMES crypto)

# Handle the QUIETLY and REQUIRED arguments and set COMMONCRYPTO_FOUND to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CommonCrypto DEFAULT_MSG COMMONCRYPTO_LIBRARIES COMMONCRYPTO_INCLUDE_DIR)

mark_as_advanced(COMMONCRYPTO_INCLUDE_DIR COMMONCRYPTO_LIBRARIES)
