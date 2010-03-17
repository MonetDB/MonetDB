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
AC_INIT([Mx], [1.0], [monet@cwi.nl])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([Mx.c])
AC_CONFIG_HEADER([mx_config.h])

# Checks for programs.
AC_PROG_CC
AC_PATH_PROG(BASH, bash, /usr/bin/bash, $PATH)

# Checks for libraries.

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([getopt.h limits.h stddef.h stdlib.h string.h unistd.h utime.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STAT
AC_C_CONST
AC_TYPE_SIZE_T
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_STAT
AC_FUNC_UTIME_NULL
AC_FUNC_VPRINTF
AC_CHECK_FUNCS([memset mkdir strchr strcspn strrchr strstr utime])
AC_CHECK_DECLS(getopt)

# Other checks
case "$host_os" in
mac*)	DIRSEP=':'
	AC_ERROR([mac not supported yet])
	;;
*mingw*)
	DIRSEP='\\'
	;;
*)
	DIRSEP='/'
	;;
esac
AC_DEFINE_UNQUOTED(DIR_SEP, '$DIRSEP', [Directory separator])
AC_DEFINE(MX_CXX_SUFFIX, "cc", [Suffix for C++ files])

# Produce output files
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
