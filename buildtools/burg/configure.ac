#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.57)
AC_INIT([burg], [1.0], [monet@cwi.nl])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([burs.c])
AC_CONFIG_HEADER([burg_config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_LEX
AM_PROG_LEX()
AC_PROG_YACC()

dnl Pathfinder's and MEL's lexer code do not work properly with flex 2.5.31;
dnl they work fine with both flex 2.5.4 and 2.5.33.
dnl To avoid any problems, we refuse to work with flex 2.5.31 "everywhre",
dnl not only with Pathfider & MEL.

if test "x$LEX" != "x"; then    dnl flex found on the system?

AC_MSG_CHECKING([for flex version])
flex_ver="`$LEX -V | head -n1`"
AC_MSG_RESULT($flex_ver)

case "$flex_ver" in
*2.5.31*)
  AC_MSG_ERROR([MonetDB's lexer code does not work properly with flex 2.5.31;
               please install/use flex 2.5.4 or flex 2.5.33 instead.])
esac

fi   dnl flex found on the system?

# Checks for libraries.

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdbool.h stdlib.h string.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_TYPE_SIZE_T
AC_CHECK_SIZEOF(void *)

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset strdup])

# Produce output files
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
