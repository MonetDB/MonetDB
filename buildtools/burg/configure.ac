#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT([burg], [1.0], [monet@cwi.nl])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([burs.c])
AC_CONFIG_HEADER([burg_config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_LEX
AM_PROG_LEX()
AC_PROG_YACC()

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
