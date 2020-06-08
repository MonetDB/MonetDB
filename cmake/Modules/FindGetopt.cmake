# - Find Getopt
# Find the native getopt headers and libraries.
#
# GETOPT_INCLUDE_DIR	- where to find getopt.h, etc.
# GETOPT_LIBRARIES	- List of libraries when using getopt.
# GETOPT_FOUND	- True if getopt found.

##find_path(HAVE_GETOPT_H "getopt.h")
##check_symbol_exists("getopt_long" "getopt.h" HAVE_GETOPT_LONG)

##find_library(GETOPT_LIB "getopt.lib")
#cmakedefine HAVE_GETOPT_H @HAVE_GETOPT_H@
#cmakedefine HAVE_GETOPT_LONG @HAVE_GETOPT_LONG@

#define HAVE_GETOPT_LONG 1
#cmakedefine HAVE_GETOPT_H @HAVE_GETOPT_H@
#cmakedefine GETOPT_LIB @GETOPT_LIB@
