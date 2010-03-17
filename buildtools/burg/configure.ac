#                                               -*- Autoconf -*-

dnl The contents of this file are subject to the MonetDB Public License
dnl Version 1.1 (the "License"); you may not use this file except in
dnl compliance with the License. You may obtain a copy of the License at
dnl http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
dnl
dnl Software distributed under the License is distributed on an "AS IS"
dnl basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
dnl License for the specific language governing rights and limitations
dnl under the License.
dnl
dnl The Original Code is the MonetDB Database System.
dnl
dnl The Initial Developer of the Original Code is CWI.
dnl Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
dnl Copyright August 2008-2010 MonetDB B.V.
dnl All Rights Reserved.

# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.60)
AC_INIT([burg], [1.0], [monet@cwi.nl])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([burs.c])
AC_CONFIG_HEADER([burg_config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_LEX
AM_PROG_LEX()
AC_PROG_YACC()
AC_CHECK_PROG(LOCKFILE,lockfile,lockfile -r 2,echo)
AC_CHECK_PROG(RM,rm,rm -f)
AC_CHECK_PROG(MV,mv,mv -f)

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
AC_CHECK_TYPES(long long)
AC_CHECK_TYPES(__int64)
AC_CHECK_SIZEOF(void *)
AC_CHECK_SIZEOF(int)
AC_CHECK_SIZEOF(long)
AC_CHECK_SIZEOF(long long)
AC_CHECK_SIZEOF(__int64)

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset strdup])

# Produce output files
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
