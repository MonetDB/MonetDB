#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.57)
AC_INIT([mel], [1.0], [monet@cwi.nl])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([mel.mx])
AC_CONFIG_HEADER([mel_config.h])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
if test $GCC = yes; then
	case "`$CXX -dumpversion`" in
	4.*)
		dnl remove -O flags from CXXFLAGS when using g++ version 4
		dnl otherwise we may get segfaults, at least on x86_64 Linux
		dnl also, g++ 4.1.1 with optimization on Fedora Core 5 causes mel to fail in weird ways
		CXXFLAGS=`echo "$CXXFLAGS" | sed 's/-O[[0-9]]*//'`
		;;
	esac
fi
AC_ARG_WITH(mx,
	AC_HELP_STRING([--with-mx=FILE], [Mx is installed as FILE]),
	have_mx="$withval",
	have_mx=auto)
if test "x$have_mx" = xauto; then
	AC_PATH_PROGS(MX,[ Mx$EXEEXT Mx ],,$PATH)
	if test "x$MX" = x; then
		AC_ERROR([No Mx$EXEEXT found in PATH=$PATH])
	fi
elif test "x$have_mx" = xno; then
	AC_MSG_ERROR([Mx is required])
else
	MX="$withval"
fi
AC_SUBST(MX)
AC_CHECK_PROG(LOCKFILE,lockfile,lockfile -r 2,echo)
AC_PROG_LEX
AM_PROG_LEX()
AC_PROG_YACC()
AC_PROG_LN_S()
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
AC_HEADER_TIME
AC_CHECK_HEADERS([malloc.h stdlib.h string.h sys/time.h unistd.h])
AC_LANG_SAVE
AC_LANG_CPLUSPLUS
AC_CHECK_HEADER(iostream,
	AC_DEFINE(HAVE_IOSTREAM, 1,
		[Define to 1 if you have the <iostream> header file.]),,[ ])
AC_CHECK_HEADER(cstdio,
	AC_DEFINE(HAVE_CSTDIO, 1,
		[Define to 1 if you have the <cstdio> header file.]),,[ ])
AC_LANG_RESTORE

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE
AC_TYPE_SIZE_T

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

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset strchr strdup strrchr])

# Produce output files
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
