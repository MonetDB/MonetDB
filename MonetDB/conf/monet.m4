dnl The contents of this file are subject to the MonetDB Public
dnl License Version 1.0 (the "License"); you may not use this file
dnl except in compliance with the License. You may obtain a copy of
dnl the License at
dnl http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
dnl
dnl Software distributed under the License is distributed on an "AS
dnl IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
dnl implied. See the License for the specific language governing
dnl rights and limitations under the License.
dnl
dnl The Original Code is the Monet Database System.
dnl
dnl The Initial Developer of the Original Code is CWI.
dnl Portions created by CWI are Copyright (C) 1997-2005 CWI.
dnl All Rights Reserved.

dnl VERSION_TO_NUMBER macro (copied from libxslt)
AC_DEFUN([MONETDB_VERSION_TO_NUMBER],
[`$1 | sed 's|[[_\-]][[a-zA-Z0-9]]*$||' | awk 'BEGIN { FS = "."; } { printf "%d", ([$]1 * 1000 + [$]2) * 1000 + [$]3;}'`])

AC_DEFUN([AM_MONET],
[

dnl check for monet
have_monet=auto
MONETDB_CFLAGS=""
MONETDB_LIBS=""
MONETDB_MOD_PATH=""
MONETDB_PREFIX="."
if test "x$1" = "x"; then
  MONETDB_REQUIRED_VERSION="4.4.1"
else
  MONETDB_REQUIRED_VERSION="$1"
fi
AC_ARG_WITH(monet,
	AC_HELP_STRING([--with-monet=DIR], [monet is installed in DIR]),
	have_monet="$withval")
if test "x$have_monet" != xno; then
  MPATH="$withval/bin:$PATH"
  AC_PATH_PROG(MONETDB_CONFIG,monetdb-config,,$MPATH)

  if test "x$MONETDB_CONFIG" != x; then
    AC_MSG_CHECKING(whether MonetDB version $MONETDB_REQUIRED_VERSION or newer is installed) 
    MONETDBVERS=`$MONETDB_CONFIG --version`
    if test MONETDB_VERSION_TO_NUMBER(echo $MONETDBVERS) -ge MONETDB_VERSION_TO_NUMBER(echo $MONETDB_REQUIRED_VERSION); then
      have_monet=yes
      AC_MSG_RESULT($have_monet: found version $MONETDBVERS)
    else
      have_monet=no
      AC_MSG_RESULT($have_monet: found only version $MONETDBVERS)
    fi
  fi

  if test "x$have_monet" != xyes; then
    MONETDB_CFLAGS=""
    MONETDB_INCS=""
    MONETDB_INCLUDEDIR=""
    MONETDB_LIBS=""
    MONETDB_MOD_PATH=""
    MONETDB_PREFIX=""
  else
    MONETDB_CFLAGS=`$MONETDB_CONFIG --cflags`
    MONETDB_INCS=`$MONETDB_CONFIG --includes`
    MONETDB_INCLUDEDIR=`$MONETDB_CONFIG --pkgincludedir`
    MONETDB_LIBS=`$MONETDB_CONFIG --libs`
    MONETDB_MOD_PATH=`$MONETDB_CONFIG --modpath`
    MONETDB_PREFIX=`$MONETDB_CONFIG --prefix`
    CLASSPATH="$CLASSPATH:`$MONETDB_CONFIG --classpath`"
  fi
fi
AC_SUBST(MONETDB_CFLAGS)
AC_SUBST(MONETDB_INCS)
AC_SUBST(MONETDB_INCLUDEDIR)
AC_SUBST(MONETDB_LIBS)
AC_SUBST(MONETDB_MOD_PATH)
AC_SUBST(MONETDB_PREFIX)
AC_SUBST(CLASSPATH)
AM_CONDITIONAL(HAVE_MONET,test x$have_monet = xyes)
]) dnl AC_DEFUN AM_MONET

AC_DEFUN([AM_MONET5],
[

dnl check for monet5
have_monet5=auto
MONET5_CFLAGS=""
MONET5_LIBS=""
MONET5_MOD_PATH=""
MONET5_PREFIX="."
if test "x$1" = "x"; then
  MONET5_REQUIRED_VERSION="4.99.19"
else
  MONET5_REQUIRED_VERSION="$1"
fi
AC_ARG_WITH(monet5,
	AC_HELP_STRING([--with-monet5=DIR], [monet5 is installed in DIR]),
	have_monet5="$withval")
if test "x$have_monet5" != xno; then
  MPATH="$withval/bin:$PATH"
  AC_PATH_PROG(MONET5_CONFIG,monetdb-config,,$MPATH)

  if test "x$MONET5_CONFIG" != x; then
    AC_MSG_CHECKING(whether MonetDB version $MONET5_REQUIRED_VERSION or newer is installed) 
    MONET5VERS=`$MONET5_CONFIG --version`
    if test MONETDB_VERSION_TO_NUMBER(echo $MONET5VERS) -ge MONETDB_VERSION_TO_NUMBER(echo $MONET5_REQUIRED_VERSION); then
      have_monet5=yes
      AC_MSG_RESULT($have_monet5: found version $MONET5VERS)
    else
      have_monet5=no
      AC_MSG_RESULT($have_monet5: found only version $MONET5VERS)
    fi
  fi

  if test "x$have_monet5" != xyes; then
    MONET5_CFLAGS=""
    MONET5_INCS=""
    MONET5_INCLUDEDIR=""
    MONET5_LIBS=""
    MONET5_MOD_PATH=""
    MONET5_PREFIX=""
  else
    MONET5_CFLAGS=`$MONET5_CONFIG --cflags`
    MONET5_INCS=`$MONET5_CONFIG --includes`
    MONET5_INCLUDEDIR=`$MONET5_CONFIG --pkgincludedir`
    MONET5_LIBS=`$MONET5_CONFIG --libs`
    MONET5_MOD_PATH=`$MONET5_CONFIG --modpath`
    MONET5_PREFIX=`$MONET5_CONFIG --prefix`
  fi
fi
AC_SUBST(MONET5_CFLAGS)
AC_SUBST(MONET5_INCS)
AC_SUBST(MONET5_INCLUDEDIR)
AC_SUBST(MONET5_LIBS)
AC_SUBST(MONET5_MOD_PATH)
AC_SUBST(MONET5_PREFIX)
AM_CONDITIONAL(HAVE_MONET5,test x$have_monet5 = xyes)
]) dnl AC_DEFUN AM_MONET5

AC_DEFUN([AM_MONETDB_COMPILER],
[

dnl Some special requirements for MacOS X/Darwin
case "$host" in
powerpc-apple-darwin*)
	CPPFLAGS="$CPPFLAGS -I/sw/include"
	LDFLAGS="$LDFLAGS -L/sw/lib"
	if test "$host" = "powerpc-apple-darwin6.8"; then
		dnl  Jaguar still requires this...
		CFLAGS="$CFLAGS -Ddlsym=dlsym_prepend_underscore"
	fi
	;;
esac

dnl check for compiler
icc_ver=""
gcc_ver=""

AC_ARG_WITH(gcc,
	AC_HELP_STRING([--with-gcc=<compiler>], [which C compiler to use])
AC_HELP_STRING([--without-gcc], [do not use GCC]), [
	case $withval in
	yes)	CC=gcc CXX=g++;;
	no)	case $host_os-$host in
		linux*-i?86*)	CC=icc	CXX=icpc;;
		linux*-x86_64*)	CC=icc	CXX=icpc;;
		linux*-ia64*)	CC=ecc	CXX=ecpc;;
		aix*)		CC=xlc_r	CXX=xlC_r;;
		*)		CC=cc	CXX=CC;;
		esac
		case $host_os in
		linux*)
		    dnl  Since version 8.0, ecc/ecpc are also called icc/icpc,
		    dnl  and icc/icpc requires "-no-gcc" to avoid predefining
		    dnl  __GNUC__, __GNUC_MINOR__, and __GNUC_PATCHLEVEL__ macros.
		    icc_ver="`$CC --version 2>/dev/null`"
		    case $icc_ver in
		    8.*)	CC="icc -no-gcc"	CXX="icpc -no-gcc";;
		    esac
		    ;;
		esac
		;;
	*)	CC=$withval
		case "$CC" in
		dnl  Portland Group compiler (pgcc/pgCC)
		pgcc*)	CC="$CC -fPIC";;
		esac
		;;
	esac])

AC_ARG_WITH(gxx,
	AC_HELP_STRING([--with-gxx=<compiler>], [which C++ compiler to use]), [
	case $withval in
	yes|no)	AC_ERROR(must supply a compiler when using --with-gxx);;
	*)	CXX=$withval
		case "$CXX" in
		dnl  Portland Group compiler (pgcc/pgCC)
		pgcc*)	CXX="$CXX -fPIC";;
		esac
		;;
	esac])

AC_PROG_CC()
AC_PROG_CXX()
AC_PROG_CPP()
AC_PROG_GCC_TRADITIONAL()

case $GCC-$host_os in
yes-*)	gcc_ver="`$CC --version | head -n1 | sed -e 's|^[[^0-9]]*\([[0-9]][[0-9\.]]*[[0-9]]\)\([[^0-9]].*\)*$|\1|'`";;
esac

dnl  Set compiler switches.
dnl  The idea/goal is to be as strict as possible, i.e., enable preferable
dnl  *all* warnings and make them errors. This should help keeping the code
dnl  as clean and portable as possible.
dnl  It turned out, though, that this, especially turning all warnings into 
dnl  errors is a bit too ambitious for configure/autoconf. Hence, we set
dnl  the standard CFLAGS & CXXFLAGS to what configure/autoconf can cope with
dnl  (basically everything except "-Werror"). For "-Werror" and some
dnl  switches that disable selected warnings that haven't been sorted out,
dnl  yet, we set X_CFLAGS & X_CXXFLAGS, which are added to the standard
dnl  CFLAGS & CXXFLAGS once configure/autoconf are done with their job,
dnl  i.e., at the end of the configure.m4 file that includes this monet.m4.
dnl  Only GNU (gcc/g++) and Intel ([ie]cc/[ie]cpc on Linux) are done so far.
X_CFLAGS=''
X_CXXFLAGS=''
NO_X_CFLAGS='_NO_X_CFLAGS_'
case "$GCC-$CC-$host_os" in
yes-*-*)
	dnl  GNU (gcc/g++)
	dnl  We need more features than the C89 standard offers, but not all
	dnl  (if any at all) C/C++ compilers implements the complete C99
	dnl  standard.  Moreover, there seems to be no standard for the
	dnl  defines that enable the features beyond C89 in the various
	dnl  platforms.  Here's what we found working so far...
	case "$gcc_ver-$host_os" in
	*-cygwin*)
		dnl  MonetDB/src/testing/Mtimeout.c fails to compile with
		dnl  "--std=c99" as the compiler then refuses to recognize
		dnl  the "sa_handler" member of the "sigaction" struct,
		dnl  which is defined in an unnamed union in
		dnl  /usr/include/cygwin/signal.h ...
		;;
	*-irix*|*-darwin*)
		CFLAGS="$CFLAGS -std=c99"
		;;
	*-solaris*)
		CFLAGS="$CFLAGS -std=c99"
		CFLAGS="$CFLAGS -D__EXTENSIONS__"
		;;
	3.*-*)
		AC_DEFINE(_POSIX_C_SOURCE, 200112L, [Compiler flag])
		AC_DEFINE(_POSIX_SOURCE, 1, [Compiler flag])
		AC_DEFINE(_XOPEN_SOURCE, 600, [Compiler flag])
		CFLAGS="$CFLAGS -std=c99"
		CXXFLAGS="$CXXFLAGS -ansi"
		;;
	esac
	dnl  Be picky; "-Werror" seems to be too rigid for autoconf...
	CFLAGS="$CFLAGS -Wall -W"
	CXXFLAGS="$CXXFLAGS -Wall -W"
	dnl  Be rigid; "MonetDB code is supposed to adhere to this... ;-)
	X_CFLAGS="$X_CFLAGS -Werror-implicit-function-declaration"
	dnl X_CXXFLAGS="$X_CXXFLAGS -Werror-implicit-function-declaration"
	X_CFLAGS="$X_CFLAGS -Werror"
	X_CXXFLAGS="$X_CXXFLAGS -Werror"
	dnl  ... however, some things aren't solved, yet ...
	X_CFLAGS="$X_CFLAGS -Wno-format"
	X_CXXFLAGS="$X_CXXFLAGS -Wno-format"
	dnl  ... and some are beyond our control:
	case "$gcc_ver" in
		dnl  Some versions of flex and bison require these:
	3.*)	dnl  (Don't exist for gcc < 3.0.)
		X_CFLAGS="$X_CFLAGS -Wno-unused-function -Wno-unused-label"
		X_CXXFLAGS="$X_CXXFLAGS -Wno-unused-function -Wno-unused-label"
		;;
	*)	dnl  gcc < 3.0 also complains about "value computed is not used"
		dnl  in src/monet/monet_context.mx:
		dnl  #define VARfixate(X)   ((X) && ((X)->constant=(X)->frozen=TRUE)==TRUE)
		dnl  But there is no "-Wno-unused-value" switch for gcc < 3.0 either...
		X_CFLAGS="$X_CFLAGS -Wno-unused"
		X_CXXFLAGS="$X_CXXFLAGS -Wno-unused"
		;;
	esac
	case $gcc_ver-$host_os in
	2.9*-aix*)
		dnl  In some cases, there is a (possibly) uninitialized
		dnl  variable in bison.simple ... |-(
		dnl  However, gcc-2.9-aix51-020209 on SARA's solo
		dnl  seems to ignore "-Wno-uninitialized";
		dnl  hence, we switch-off "-Werror" by disabling
		dnl  X_CFLAGS locally in src/monet/Makefile.ag:
		dnl  @NO_X_CFLAGS@ with NO_X_CFLAGS='X_CFLAGS'
		dnl  generates "X_CFLAGS =" in the generated Makefile.
		NO_X_CFLAGS='X_CFLAGS'
		;;
	*-solaris*|*-darwin*|*-aix*)
		dnl  In some cases, there is a (possibly) uninitialized
		dnl  variable in bison.simple ... |-(
		X_CFLAGS="$X_CFLAGS -Wno-uninitialized"
		X_CXXFLAGS="$X_CXXFLAGS -Wno-uninitialized"
		;;
	3.3*-*)
		dnl  gcc 3.3* --- at least on Linux64 (Red Hat Enterprise
		dnl  Linux release 2.9.5AS (Taroon)) and the cross-compiler
		dnl  for arm-linux --- seem to require this to avoid
		dnl  "warning: dereferencing type-punned pointer will break strict-aliasing rules"
		X_CFLAGS="$X_CFLAGS -Wno-strict-aliasing"
		;;
	esac
	;;
-icc*-linux*|-ecc*-linux*)
	dnl  Intel ([ie]cc/[ie]cpc on Linux)
 	LDFLAGS="$LDFLAGS -i_dynamic"
	dnl  Let warning #140 "too many arguments in function call"
	dnl  become an error to make configure tests work properly.
	CFLAGS="$CFLAGS -we140"
	CXXFLAGS="$CXXFLAGS -we140"
	dnl  Version 8.0 doesn't find sigset-t when -ansi is set... !?
	case $icc_ver in
	8.*)	;;
	*)	CFLAGS="$CFLAGS -ansi"	CXXFLAGS="$CXXFLAGS -ansi";
	esac
	dnl Define the same settings as for gcc, as we use the same
	dnl header files
	AC_DEFINE(_POSIX_C_SOURCE, 200112L, [Compiler flag])
	AC_DEFINE(_POSIX_SOURCE, 1, [Compiler flag])
	AC_DEFINE(_XOPEN_SOURCE, 600, [Compiler flag])
	dnl  Be picky; "-Werror" seems to be too rigid for autoconf...
	CFLAGS="$CFLAGS -c99 -Wall -w2"
	CXXFLAGS="$CXXFLAGS -c99 -Wall -w2"
	dnl  Be rigid; "MonetDB code is supposed to ahear to this... ;-)
	dnl  Let warning #266 "function declared implicitly" become an error.
	X_CFLAGS="$X_CFLAGS -we266"
	X_CXXFLAGS="$X_CXXFLAGS -we266"
	X_CFLAGS="$X_CFLAGS -Werror"
	X_CXXFLAGS="$X_CXXFLAGS -Werror"
	dnl  ... however, some things aren't solved, yet:
	dnl  (for the time being,) we need to disable some warnings (making them remarks doesn't seem to work with -Werror):
	X_CFLAGS="$X_CFLAGS -wd1418,1419,279,310,981,810,444,193,111,177,171,181,764,269,108,188,1357,102,70"
	case $icc_ver in
	8.[[1-9]]*)	X_CFLAGS="$X_CFLAGS,1572" ;;
	esac
	X_CXXFLAGS="$X_CXXFLAGS -wd1418,1419,279,310,981,810,444,193,111,177,171,181,764,269,108,188,1357,102,70"
	dnl  #1418: external definition with no prior declaration
	dnl  #1419: external declaration in primary source file
	dnl  # 279: controlling expression is constant
	dnl  # 310: old-style parameter list (anachronism)
	dnl  # 981: operands are evaluated in unspecified order
	dnl  # 810: conversion from "." to "." may lose significant bits
	dnl  # 444: destructor for base class "." is not virtual
	dnl  # 193: zero used for undefined preprocessing identifier
	dnl  # 111: statement is unreachable
	dnl  # 177: function "." was declared but never referenced
	dnl  # 171: invalid type conversion: "." to "."
	dnl  # 181: argument is incompatible with corresponding format string conversion
	dnl  # 764: nonstandard format string conversion
	dnl  # 269: invalid format string conversion
	dnl  # 108: implicitly-signed bit field of length 1
	dnl  # 188: enumerated type mixed with another type
	dnl  #1357: optimization disabled due to excessive resource requirements; contact Intel Premier Support for assistance
	dnl  # 102: forward declaration of enum type is nonstandard
	dnl  #  70: incomplete type is not allowed
	dnl  #1572: floating-point equality and inequality comparisons are unreliable
	;;
-pgcc*-linux*)
	dnl  Portland Group (PGI) (pgcc/pgCC on Linux)
	dnl Define the same settings as for gcc, as we use the same
	dnl header files
	AC_DEFINE(_POSIX_C_SOURCE, 200112L, [Compiler flag])
	AC_DEFINE(_POSIX_SOURCE, 1, [Compiler flag])
	AC_DEFINE(_XOPEN_SOURCE, 600, [Compiler flag])
	dnl  required for "scale" in module "decimal"
	CFLAGS="$CFLAGS -Msignextend"
	CFLAGS="$CFLAGS -c9x"
	CXXFLAGS="$CXXFLAGS -c9x"
	;;
esac
AC_SUBST(CFLAGS)
AC_SUBST(CXXFLAGS)
AC_SUBST(X_CFLAGS)
AC_SUBST(X_CXXFLAGS)
AC_SUBST(NO_X_CFLAGS)

dnl  default javac flags
JAVACFLAGS="$JAVACFLAGS -g:none -O"
AC_SUBST(JAVACFLAGS)

AC_ARG_WITH(bits,
	AC_HELP_STRING([--with-bits=BITS],
		[obsolete: use --enable-bits instead]),
	AC_MSG_ERROR([argument --with-bits is obsolete: use --enable-bits instead]))

bits=32
AC_ARG_ENABLE(bits,
	AC_HELP_STRING([--enable-bits=BITS],
		[specify number of bits (32 or 64)]), [
case $enableval in
32)	case "$host" in
	ia64*)	AC_ERROR([we do not support 32 bits on $host, yet]);;
	esac
	;;
64)	case "$host-$GCC-$CC" in
	i?86*-*-*)  AC_ERROR([$host does not support 64 bits]);;
	x86_64*--icc*) AC_ERROR([$CC on $host does not support 64 bits]);;
	esac
	;;
*)	AC_ERROR(--enable-bits argument must be either 32 or 64);;
esac
bits=$enableval
])
if test "$bits" = "64"; then
	dnl  Keep in mind how to call the 32-bit compiler.
	case "$GCC-$CC-$host_os-$host" in
	yes-*-linux*-x86_64*)
		dnl  On our x86_64 machine, "gcc" defaults to "gcc -m64" ...
		CC32="$CC -m32";;
	-pgcc*-linux*-x86_64*)
		dnl  On our x86_64 machine, "pgcc" defaults to "pgcc -tp=k8-64" ...
		CC32="$CC -tp=k8-32";;
	*)	CC32="$CC";;
	esac
fi
case "$GCC-$CC-$host_os-$host-$bits" in
yes-*-solaris*-64)
	case `$CC -v 2>&1` in
	*'gcc version 3.'*)	;;
	*)	AC_ERROR([need GCC version 3.X for 64 bits]);;
	esac
	CC="$CC -m$bits"
	CXX="$CXX -m$bits"
	;;
-*-solaris*-64)
	CC="$CC -xarch=v9"
	CXX="$CXX -xarch=v9"
	;;
yes-*-irix*-64)
	CC="$CC -mabi=$bits"
	CXX="$CXX -mabi=$bits"
	;;
-*-irix*-64)
	CC="$CC -$bits"
	CXX="$CXX -$bits"
	;;
yes-*-aix*-64)
	CC="$CC -maix$bits"
	CXX="$CXX -maix$bits"
	AR="ar -X64"
	NM="nm -X64 -B"
	;;
-*-aix*-64)
	CC="$CC -q$bits"
	CXX="$CXX -q$bits"
	AR="ar -X64"
	NM="nm -X64 -B"
	;;
yes-*-linux*-x86_64*-*)
	CC="$CC -m$bits"
	CXX="$CXX -m$bits"
	;;
-pgcc*-linux*-x86_64*-*)
	CC="$CC -tp=k8-$bits"
	CXX="$CXX -tp=k8-$bits"
	;;
esac

AC_ARG_ENABLE(oid32,
	AC_HELP_STRING([--enable-oid32],
		[use 32 bits vor OIDs on a 64-bit architecture]),
	enable_oid32=$enableval,
	enable_oid32=no)
case $enable_oid32 in
yes)	AC_DEFINE(MONET_OID32, 1, [Define if the oid type should use 32 bits on a 64-bit architecture]);;
esac


dnl find out, whether the C compiler is C99 compliant
AC_MSG_CHECKING([if your compiler is C99 compliant])
have_c99=no
AC_RUN_IFELSE(AC_LANG_PROGRAM(,
[[
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901
return 0;
#else
return 1;
#endif
]]
),
[AC_DEFINE([HAVE_C99], 1, [Is your compiler C99 compliant?])
have_c99=yes
AC_MSG_RESULT(yes)],
AC_MSG_RESULT(no))


dnl some dirty hacks
dnl we use LEXLIB=-ll because this is usually correctly installed 
dnl and -lfl usually only in the 32bit version
THREAD_SAVE_FLAGS="\$(thread_safe_flag_spec) -D_REENTRANT"
# only needed in monet
MEL_LIBS=""
NO_OPTIMIZE_FILES=""
case "$host_os" in
solaris*)
    case "$GCC" in
      yes) ;;
      *) 
	MEL_LIBS="-z muldefs"
        THREAD_SAVE_FLAGS="$THREAD_SAVE_FLAGS -mt"
        ;;
    esac
    LEXLIB=-ll
    ;;
irix*)
    LEXLIB=-ll
    ;;
aix*)
    THREAD_SAVE_FLAGS="$THREAD_SAVE_FLAGS -D_THREAD_SAFE"
    case "$GCC" in
      yes)
        THREAD_SAVE_FLAGS="$THREAD_SAVE_FLAGS -mthreads"
        dnl  With "-On" (n>0), compilation of monet_multiplex.mx fails on sara's solo with
        dnl  "Assembler: /tmp/cc8qluZf.s: line 33198: Displacement must be divisible by 4.".
        dnl  Likewise, the MIL parser does not work correctly, unless compile monet_parse.yy.c
        dnl  without optimization (i.e., with "-O0").
        dnl  Hence:
        NO_OPTIMIZE_FILES="monet_multiplex.mx monet_parse.yy.mx"
        ;;
      *)
        THREAD_SAVE_FLAGS="$THREAD_SAVE_FLAGS -qthreaded"
        MEL_LIBS="-qstaticinline"
        ;;
    esac
    ;;
esac
AC_SUBST(MEL_LIBS)
AC_SUBST(thread_safe_flag_spec)
AC_SUBST(THREAD_SAVE_FLAGS)
AC_SUBST(NO_OPTIMIZE_FILES)

JAVA_VERSION=""
JAVA="java"
JAVAC="javac"
JAR="jar"
AC_ARG_WITH(java,
	AC_HELP_STRING([--with-java=DIR],
		[javac and jar are installed in DIR/bin]),
	have_java="$withval",
	have_java=auto)
if test "x$have_java" != xno; then
  JPATH=$PATH
  if test "x$have_java" != xauto; then
     JPATH="$withval/bin:$JPATH"
  fi
  AC_PATH_PROG(JAVA,java,,$JPATH)
  if test "x$JAVA" != "x"; then
    AC_MSG_CHECKING(for Java >= 1.4)
    JAVA_VERSION=[`$JAVA -version 2>&1 | head -n1 | sed -e 's|^[^0-9]*||' -e 's|[^0-9]*$||'`]
    if test MONETDB_VERSION_TO_NUMBER(echo $JAVA_VERSION) -ge MONETDB_VERSION_TO_NUMBER(echo "1.4"); then
      have_java_1_4=yes
    else
      have_java_1_4=no
    fi
    AC_MSG_RESULT($have_java_1_4 -> $JAVA_VERSION found)
  fi

  AC_PATH_PROG(JAVAC,javac,,$JPATH)
  AC_PATH_PROG(JAR,jar,,$JPATH)
  if test x$have_java_1_4 != xyes; then
     have_java=no
  elif test "x$JAVAC" = "x"; then
     have_java=no
  elif test "x$JAR" = "x"; then
     have_java=no
  else
     have_java=yes
  fi

  if test "x$have_java" != xyes; then
    JAVA_VERSION=""
    JAVA=""
    JAVAC=""
    JAR=""
    CLASSPATH=""
  fi
fi
AC_SUBST(JAVAVERS)
AC_SUBST(JAVA)
AC_SUBST(JAVAC)
AC_SUBST(JAR)
AC_SUBST(CLASSPATH)
AM_CONDITIONAL(HAVE_JAVA,test x$have_java = xyes)

]) dnl AC_DEFUN AM_MONETDB_COMPILER

AC_DEFUN([AM_MONETDB_TOOLS],[

dnl AM_PROG_LIBTOOL has loads of required macros, when those are not satisfied within
dnl this macro block the requirement is pushed to the next level, e.g. configure.ag
dnl this can lead to unwanted orders of checks and thus to wrong settings, e.g.
dnl - the compilers are set for 64 bit, but the linker for 32 bit;
dnl - --enable-shared and --disabled-static are ignored as AC_LIBTOOL_SETUP is
dnl   called earlier then AC_DISABLE_STATIC and AC_ENABLE_SHARED
dnl To prevent this we take over some of these required macros and call them explicitly.

AC_PROG_INSTALL()
AC_PROG_LD()
AC_DISABLE_STATIC()
AC_ENABLE_SHARED()

AC_LIBTOOL_SETUP()
AC_PROG_LIBTOOL()
AM_PROG_LIBTOOL()

dnl AC_PROG_CC_STDC()
if test "$bits" = "64"; then
	dnl  On 64-bit systems, there might be no 64-bit libfl or libl, which is not a major problem,
	dnl  as we define our own yywrap function, and hence don't need these libraries.
	dnl  However, the standard "AC_PROG_LEX" & "AM_PROG_LEX" tests fail to correctly determine,
	dnl  whether [f]lex defines yytext as pointer or array, in case there is no proper lib[f]l.
	dnl  Hence, we first check, whether there is a suitable lib[f]l --- we check for function
	dnl  main instead of yywrap, as otherwise configure would cache the result, and check again
	dnl  in the 32-bit version hereafter. In case there is no suitable lib[f]l, we temporarly 
	dnl  switch back to the 32-bit version for "AC_PROG_LEX" & "AM_PROG_LEX".
	AC_CHECK_LIB(fl, main, 
		[ AC_DEFINE(HAVE_LIBFL, 1, [Define if you have the fl[ex] library]) 
		  have_libfl=yes ] , 
		[ have_libfl=no
		  AC_CHECK_LIB(l, main, 
			[ AC_DEFINE(HAVE_LIBL, 1, [Define if you have the l[ex] library]) 
		  	  have_libl=yes ] ,
			[ have_libl=no
			  CC64="$CC"
			  CC="$CC32"
			] )
		] )
fi
AC_PROG_LEX
AM_PROG_LEX()
if test "$CC64" != ""; then
	dnl  Back to 64-bit, and don't use the 32-bit lib[f]l that might have been found.
	CC="$CC64"
	LEXLIB=""
fi
AC_PROG_YACC()
AC_PROG_LN_S()
AC_CHECK_PROG(RM,rm,rm -f)
AC_CHECK_PROG(MV,mv,mv -f)
AC_CHECK_PROG(LOCKFILE,lockfile,lockfile -r 2,echo)
AC_PATH_PROG(BASH,bash, /usr/bin/bash, $PATH)
AC_CHECK_PROGS(RPMBUILD,rpmbuild rpm)

AC_ARG_WITH(swig,
	AC_HELP_STRING([--with-swig=FILE], [swig is installed as FILE]),
	SWIG="$withval",
	SWIG=swig)
AC_MSG_CHECKING([for swig >= 1.3.20])
if test "x$SWIG" != xno; then
  case "$SWIG" in
  yes|auto)
    SWIG=swig;;
  esac
  # we want the right version...
  case `$SWIG -version 2>&1` in
  *Version\ 1.3.2*)
    # ...and it must support -outdir
    case `$SWIG -help 2>&1` in
    *-outdir*) ;;
    *) SWIG=no;;
    esac ;;
  *) SWIG=no;;
  esac
fi
have_swig=no
if test x"$SWIG" != xno; then
	have_swig=yes
fi
AC_MSG_RESULT($have_swig)
AC_SUBST(SWIG)
AM_CONDITIONAL(HAVE_SWIG, test x"$SWIG" != xno)

AC_ARG_WITH(python,
	AC_HELP_STRING([--with-python=FILE], [python is installed as FILE]),
	python="$withval",
	python=auto)
case "$python" in
yes|auto)
  PYTHON=python;;
no)
  ;;
*)
  PYTHON="$python"
  python=yes
  ;;
esac
AC_MSG_CHECKING([for python >= 2.0])
if test "x$python" != xno; then
  case `"$PYTHON" -c 'import sys; print sys.version[[:3]]' 2>/dev/null` in
  2.*) ;;
  '')
     case "$python" in
     auto) ;;
     *) AC_MSG_ERROR([$PYTHON not found])
	;;
     esac
     python=no;;
  *) case "$python" in
     auto) ;;
     *) AC_MSG_ERROR([Your Python version is too old, at least 2.X required])
	;;
     esac
     python=no;;
  esac
fi
if test "x$python" != xno; then
  PYTHONINC=`"$PYTHON" -c 'import distutils.sysconfig; print distutils.sysconfig.get_python_inc()' 2>/dev/null`
  if test ! "$PYTHONINC"; then
    case "$python" in
    auto)
      ;;
    *)
      AC_MSG_ERROR([No Python include directory found.  Is Python installed properly?])
      ;;
    esac
    python=no
  elif test ! -f "$PYTHONINC/Python.h"; then
    case "$python" in
    auto)
      ;;
    *)
      AC_MSG_ERROR([No Python.h include file found.  Is Python installed properly?])
      ;;
    esac
    python=no
  fi
fi
if test "x$python" != xno; then
  PYTHON_LIBDIR=`"$PYTHON" -c 'import distutils.sysconfig; print distutils.sysconfig.get_python_lib(1,0,"")' 2>/dev/null`
  if test ! "$PYTHON_LIBDIR"; then
    case "$python" in
    auto)
      ;;
    *)
      AC_MSG_ERROR([No Python library directory found.  Is Python installed properly?])
      ;;
    esac
    python=no
  fi
fi
if test x"$python" != xno; then
	python=yes
fi
AC_MSG_RESULT($python)
AC_SUBST(PYTHONINC)
AC_SUBST(PYTHON)
AC_SUBST(PYTHON_LIBDIR)
AM_CONDITIONAL(HAVE_PYTHON, test x"$python" != xno)
AM_CONDITIONAL(HAVE_PYTHON_SWIG, test x"$python" != xno -a x"$SWIG" != xno)

AC_ARG_WITH(perl,
	AC_HELP_STRING([--with-perl=FILE], [perl is installed as FILE]),
	perl="$withval",
	perl=auto)
case "$perl" in
yes|auto)
  PERL=perl;;
no)
  ;;
*)
  PERL="$perl"
  perl=yes
  ;;
esac
AC_MSG_CHECKING([for perl])
if test "x$perl" != xno; then
  PERLINC=`"$PERL" -MConfig -e 'print "$Config{archlibexp}/CORE"'`
  if test ! "$PERLINC"; then
    case "$perl" in
    auto)
      ;;
    *)
      AC_MSG_ERROR([No Perl include directory found.  Is Perl installed properly?])
      ;;
    esac
    perl=no
  elif test ! -f "$PERLINC/perl.h"; then
    case "$perl" in
    auto)
      ;;
    *)
      AC_MSG_ERROR([No perl.h include file found.  Is Perl installed properly?])
      ;;
    esac
    perl=no
  fi
fi
if test "x$perl" != xno; then
  PERL_LIBDIR=`"$PERL" -MConfig -e '$x=$Config{installvendorarch}; $x =~ s|$Config{vendorprefix}/||; print $x;' 2>/dev/null`
  if test ! "$PERL_LIBDIR"; then
    case "$perl" in
    auto)
      ;;
    *)
      AC_MSG_ERROR([No Perl library directory found.  Is Perl installed properly?])
      ;;
    esac
    perl=no
  fi
fi
if test x"$perl" != xno; then
	perl=yes
fi
AC_MSG_RESULT($perl)
AC_SUBST(PERLINC)
AC_SUBST(PERL)
AC_SUBST(PERL_LIBDIR)
AM_CONDITIONAL(HAVE_PERL, test x"$perl" != xno)
AM_CONDITIONAL(HAVE_PERL_SWIG, test x"$perl" != xno -a x"$SWIG" != xno)

dnl to shut up automake (.m files are used for mel not for objc)
AC_CHECK_TOOL(OBJC,objc)

#AM_DEPENDENCIES(CC)
#AM_DEPENDENCIES(CXX)

dnl Checks for header files.
AC_HEADER_STDC()

case "$host_os" in
    cygwin*)
	;;
    *)
	CYGPATH_W=echo
	CYGPATH_WP=echo
	AC_SUBST(CYGPATH_W)
	AC_SUBST(CYGPATH_WP)
	;;
esac

]) dnl AC_DEFUN AM_MONETDB_TOOLS

AC_DEFUN([AM_MONETDB_OPTIONS],
[
dnl --enable-noexpand
AC_ARG_ENABLE(noexpand,
	AC_HELP_STRING([--enable-noexpand],
		[do not expand the comma-separated list of MIL types given as argument, or "all" if no expansion should be done [default=]]),
	enable_noexpand=$enableval,
	enable_noexpand=)
case $enable_noexpand in
bat|bat,*|*,bat|*,bat,*|all)
	AC_DEFINE(NOEXPAND_BAT, 1, [Define if you don't want to expand the bat type]);;
esac
case $enable_noexpand in
bit|bit,*|*,bit|*,bit,*|all)
	AC_DEFINE(NOEXPAND_BIT, 1, [Define if you don't want to expand the bit type]);;
esac
case $enable_noexpand in
chr|chr,*|*,chr|*,chr,*|all)
	AC_DEFINE(NOEXPAND_CHR, 1, [Define if you don't want to expand the chr type]);;
esac
case $enable_noexpand in
dbl|dbl,*|*,dbl|*,dbl,*|all)
	AC_DEFINE(NOEXPAND_DBL, 1, [Define if you don't want to expand the dbl type]);;
esac
case $enable_noexpand in
flt|flt,*|*,flt|*,flt,*|all)
	AC_DEFINE(NOEXPAND_FLT, 1, [Define if you don't want to expand the flt type]);;
esac
case $enable_noexpand in
int|int,*|*,int|*,int,*|all)
	AC_DEFINE(NOEXPAND_INT, 1, [Define if you don't want to expand the int type]);;
esac
case $enable_noexpand in
lng|lng,*|*,lng|*,lng,*|all)
	AC_DEFINE(NOEXPAND_LNG, 1, [Define if you don't want to expand the lng type]);;
esac
case $enable_noexpand in
oid|oid,*|*,oid|*,oid,*|all)
	AC_DEFINE(NOEXPAND_OID, 1, [Define if you don't want to expand the oid type]);;
esac
case $enable_noexpand in
ptr|ptr,*|*,ptr|*,ptr,*|all)
	AC_DEFINE(NOEXPAND_PTR, 1, [Define if you don't want to expand the ptr type]);;
esac
case $enable_noexpand in
sht|sht,*|*,sht|*,sht,*|all)
	AC_DEFINE(NOEXPAND_SHT, 1, [Define if you don't want to expand the sht type]);;
esac
case $enable_noexpand in
str|str,*|*,str|*,str,*|all)
	AC_DEFINE(NOEXPAND_STR, 1, [Define if you don't want to expand the str type]);;
esac

dnl --enable-debug
AC_ARG_ENABLE(debug,
	AC_HELP_STRING([--enable-debug],
		[enable full debugging [default=no]]),
	enable_debug=$enableval,
	enable_debug=no)
if test "x$enable_debug" = xyes; then
  if test "x$enable_optim" = xyes; then
    AC_ERROR([combining --enable-optimize and --enable-debug is not possible.])
  else
    dnl  remove "-Ox" as some compilers don't like "-g -Ox" combinations
    CFLAGS=" $CFLAGS "
    CFLAGS="`echo "$CFLAGS" | sed -e 's| -O[[0-9]] | |g' -e 's| -g | |g' -e 's|^ ||' -e 's| $||'`"
    CXXFLAGS=" $CXXFLAGS "
    CXXFLAGS="`echo "$CXXFLAGS" | sed -e 's| -O[[0-9]] | |g' -e 's| -g | |g' -e 's|^ ||' -e 's| $||'`"
    JAVACFLAGS=" $JAVACFLAGS "
    JAVACFLAGS="`echo "$JAVACFLAGS" | sed -e 's| -O | |g' -e 's| -g | |g' -e 's| -g:[[a-z]]* | |g' -e 's|^ ||' -e 's| $||'`"
    dnl  add "-g"
    CFLAGS="$CFLAGS -g"
    CXXFLAGS="$CXXFLAGS -g"
    JAVACFLAGS="$JAVACFLAGS -g"
    case "$GCC-$host_os" in
      yes-aix*)
        CFLAGS="$CFLAGS -gxcoff"
        CXXFLAGS="$CXXFLAGS -gxcoff"
        ;;
    esac
  fi
fi

dnl --enable-assert
AC_ARG_ENABLE(assert,
	AC_HELP_STRING([--enable-assert],
		[enable assertions in the code [default=yes]]),
	enable_assert=$enableval,
	enable_assert=yes)
if test "x$enable_assert" = xno; then
  AC_DEFINE(NDEBUG, 1, [Define if you do not want assertions])
fi

dnl --enable-optimize
NO_INLINE_CFLAGS=""
AC_ARG_ENABLE(optimize,
	AC_HELP_STRING([--enable-optimize],
		[enable extra optimization [default=no]]),
	enable_optim=$enableval, enable_optim=no)
if test "x$enable_optim" = xyes; then
  if test "x$enable_debug" = xyes; then
    AC_ERROR([combining --enable-optimize and --enable-debug is not possible.])
  elif test "x$enable_prof" = xyes; then
    AC_ERROR([combining --enable-optimize and --enable-profile is not (yet?) possible.])
  elif test "x$enable_instrument" = xyes; then
    AC_ERROR([combining --enable-optimize and --enable-instrument is not (yet?) possible.])
  else
    dnl  remove "-g" as some compilers don't like "-g -Ox" combinations
    CFLAGS=" $CFLAGS "
    CFLAGS="`echo "$CFLAGS" | sed -e 's| -g | |g' -e 's|^ ||' -e 's| $||'`"
    CXXFLAGS=" $CXXFLAGS "
    CXXFLAGS="`echo "$CXXFLAGS" | sed -e 's| -g | |g' -e 's|^ ||' -e 's| $||'`"
    JAVACFLAGS=" $JAVACFLAGS "
    JAVACFLAGS="`echo "$JAVACFLAGS" | sed -e 's| -g | |g' -e 's| -g:[[a-z]]* | |g' -e 's|^ ||' -e 's| $||'`"
    dnl  Optimization flags
    JAVACFLAGS="$JAVACFLAGS -g:none -O"
    if test "x$GCC" = xyes; then
      dnl -fomit-frame-pointer crashes memprof
      case "$host-$gcc_ver" in
      x86_64-*-*-3.[[2-9]]*|i*86-*-*-3.[[2-9]]*)
                      CFLAGS="$CFLAGS -O6"
                      case "$host" in
                      i*86-*-cygwin) 
                           dnl  With gcc 3.2, the combination of "-On -fomit-frame-pointer" (n>1)
                           dnl  does not seem to produce stable/correct? binaries under CYGWIN
                           dnl  (Mdiff and Mserver crash with segmentation faults);
                           dnl  hence, we omit -fomit-frame-pointer, here.
                           ;;
                      *)   CFLAGS="$CFLAGS -fomit-frame-pointer";;
                      esac
                      CFLAGS="$CFLAGS                          -finline-functions -falign-loops=4 -falign-jumps=4 -falign-functions=4 -fexpensive-optimizations                     -funroll-loops -frerun-cse-after-loop -frerun-loop-opt"
                      dnl  With gcc 3.2, the combination of "-On -funroll-all-loops" (n>1)
                      dnl  does not seem to produce stable/correct? binaries
                      dnl  (Mserver produces tons of incorrect BATpropcheck warnings);
                      dnl  hence, we omit -funroll-all-loops, here.
                      ;;
      x86_64-*-*|i*86-*-*)
                      CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions -malign-loops=4 -malign-jumps=4 -malign-functions=4 -fexpensive-optimizations -funroll-all-loops  -funroll-loops -frerun-cse-after-loop -frerun-loop-opt";;
      ia64-*-*)       CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions                                                     -fexpensive-optimizations                                    -frerun-cse-after-loop -frerun-loop-opt"
                      dnl  Obviously, 4-byte alignment doesn't make sense on Linux64; didn't try 8-byte alignment, yet.
                      dnl  Further, when combining either of "-funroll-all-loops" and "-funroll-loops" with "-On" (n>1),
                      dnl  gcc (3.2.1 & 2.96) does not seem to produce stable/correct? binaries under Linux64
                      dnl  (Mserver crashes with segmentation fault);
                      dnl  hence, we omit both "-funroll-all-loops" and "-funroll-loops", here
                      ;;
      *-sun-solaris*) CFLAGS="$CFLAGS -O2 -fomit-frame-pointer -finline-functions"
                      if test "$bits" = "64" ; then
                        NO_INLINE_CFLAGS="-O1"
                      fi
                      ;;
      *irix*)         CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions"
                      NO_INLINE_CFLAGS="-fno-inline"
                      ;;
      *aix*)          CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions"
                      if test "$bits" = "64" ; then
                        NO_INLINE_CFLAGS="-O0"
                      else
                        NO_INLINE_CFLAGS="-fno-inline"
                      fi
                      ;;
      *)              CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions";;
      esac
    else
      case "$host-$icc_ver" in
      dnl Portland Group compiler (pgcc/pgCC) has $icc_ver=""
      *-*-*-)    ;;
      dnl  With icc-8.0, Interprocedural (IP) Optimization does not seem to work with MonetDB:
      dnl  With "-ipo -ipo_obj", pass-through linker options ("-Wl,...") are not handled correctly,
      dnl  and with "-ip -ipo_obj", the resulting Mserver segfaults immediately.
      dnl  Hence, we skip Interprocedural (IP) Optimization with icc-8.0.
      x86_64-*-*-8.*) CFLAGS="$CFLAGS -mp1 -O3 -restrict -unroll               -tpp6 -axKWNPB";;
      i*86-*-*-8.*)   CFLAGS="$CFLAGS -mp1 -O3 -restrict -unroll               -tpp6 -axKWNPB";;
      ia64-*-*-8.*)   CFLAGS="$CFLAGS -mp1 -O2 -restrict -unroll               -tpp2 -mcpu=itanium2";;
      i*86-*-*)       CFLAGS="$CFLAGS -mp1 -O3 -restrict -unroll -ipo -ipo_obj -tpp6 -axiMKW";;
      ia64-*-*)       CFLAGS="$CFLAGS -mp1 -O2 -restrict -unroll -ipo -ipo_obj -tpp2 -mcpu=itanium2"
                      dnl  With "-O3", ecc does not seem to produce stable/correct? binaries under Linux64
                      dnl  (Mserver produces some incorrect BATpropcheck warnings);
                      dnl  hence, we use only "-O2", here.
                      ;;
#      *irix*)        CFLAGS="$CFLAGS -O3 -Ofast=IP27 -OPT:alias=restrict -IPA"
      *irix*)         CFLAGS="$CFLAGS -O3 -OPT:div_split=ON:fast_complex=ON:fast_exp=ON:fast_nint=ON:Olimit=2147483647:roundoff=3 -TARG:processor=r10k -IPA"
                      LDFLAGS="$LDFLAGS -IPA"
                      ;;
      *-sun-solaris*) CFLAGS="$CFLAGS -xO5";;
      *aix*)          CFLAGS="$CFLAGS -O3"
                      NO_INLINE_CFLAGS="-qnooptimize"
                      ;;
      esac   
    fi
  fi
fi
AC_SUBST(NO_INLINE_CFLAGS)

dnl --enable-warning (only gcc & icc/ecc)
AC_ARG_ENABLE(warning,
	AC_HELP_STRING([--enable-warning],
		[enable extended compiler warnings [default=no]]),
	enable_warning=$enableval,
	enable_warning=no)
if test "x$enable_warning" = xyes; then
  dnl  Basically, we disable/overule X_C[XX]FLAGS, i.e., "-Werror" and some "-Wno-*".
  dnl  All warnings should be on by default (see above).
  case $GCC-$host_os in
  yes-*)
	dnl  GNU (gcc/g++)
	X_CFLAGS="-pedantic -Wno-long-long"
	X_CXXFLAGS="-pedantic -Wno-long-long"
	;;
  -linux*)
	dnl  Intel ([ie]cc/[ie]cpc on Linux)
	X_CFLAGS=""
	X_CXXFLAGS=""
	;;
  esac
fi

dnl --enable-profile
need_profiling=no
AC_ARG_ENABLE(profile,
	AC_HELP_STRING([--enable-profile], [enable profiling [default=no]]),
	enable_prof=$enableval,
	enable_prof=no)
if test "x$enable_prof" = xyes; then
  if test "x$enable_optim" = xyes; then
    AC_ERROR([combining --enable-optimize and --enable-profile is not (yet?) possible.])
  else
    AC_DEFINE(PROFILE, 1, [Compiler flag])
    need_profiling=yes
    if test "x$GCC" = xyes; then
      CFLAGS="$CFLAGS -pg"
    fi
  fi
fi
AM_CONDITIONAL(PROFILING,test "x$need_profiling" = xyes)

dnl --enable-instrument
need_instrument=no
AC_ARG_ENABLE(instrument,
	AC_HELP_STRING([--enable-instrument],
		[enable instrument [default=no]]),
	enable_instrument=$enableval,
	enable_instrument=no)
if test "x$enable_instrument" = xyes; then
  if test "x$enable_optim" = xyes; then
    AC_ERROR([combining --enable-optimize and --enable-instrument is not (yet?) possible.])
  else
    AC_DEFINE(PROFILE, 1, [Compiler flag])
    need_instrument=yes
    if test "x$GCC" = xyes; then
      CFLAGS="$CFLAGS -finstrument-functions -g"
    fi
  fi
fi

dnl static or shared linking
SHARED_LIBS=''
[
if [ "$enable_static" = "yes" ]; then
	CFLAGS="$CFLAGS -DSTATIC"
	SHARED_LIBS='$(STATIC_LIBS) $(smallTOC_SHARED_LIBS) $(largeTOC_SHARED_LIBS)'
	case "$host_os" in
	aix*)	
		if test "x$GCC" = xyes; then
			LDFLAGS="$LDFLAGS -Xlinker"
		fi
		LDFLAGS="$LDFLAGS -bbigtoc"
		;;
	irix*)
		if test "x$GCC" != xyes; then
			SHARED_LIBS="$SHARED_LIBS -lm"
		fi
		;;
	esac
else
	case "$host_os" in
	aix*)	LDFLAGS="$LDFLAGS -Wl,-brtl";;
	esac
fi
]
AC_SUBST(SHARED_LIBS)
AM_CONDITIONAL(LINK_STATIC,test "x$enable_static" = xyes)

]) dnl AC_DEFUN AM_MONETDB_OPTIONS

AC_DEFUN([AM_MONETDB_LIBS],
[
dnl libpthread
have_pthread=auto
PTHREAD_LIBS=""
PTHREAD_INCS=""
AC_ARG_WITH(pthread,
	AC_HELP_STRING([--with-pthread=DIR],
		[pthread library is installed in DIR]), 
	have_pthread="$withval")
if test "x$have_pthread" != xno; then
  if test "x$have_pthread" != xauto; then
    PTHREAD_LIBS="-L$withval/lib"
    PTHREAD_INCS="-I$withval/include"
  fi

  save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS $PTHREAD_INCS"
  AC_CHECK_HEADERS(pthread.h semaphore.h) 
  CPPFLAGS="$save_CPPFLAGS"

  save_LDFLAGS="$LDFLAGS"
  LDFLAGS="$LDFLAGS $PTHREAD_LIBS"
  AC_CHECK_LIB(pthread, sem_init, 
	[ PTHREAD_LIBS="$PTHREAD_LIBS -lpthread" 
          AC_DEFINE(HAVE_LIBPTHREAD, 1, [Define if you have the pthread library]) 
	  have_pthread=yes ] , 
	dnl sun
	[ AC_CHECK_LIB(pthread, sem_post, 
		[ PTHREAD_LIBS="$PTHREAD_LIBS -lpthread -lposix4" 
          	AC_DEFINE(HAVE_LIBPTHREAD, 1, [Define if you have the pthread library]) 
	  	have_pthread=yes ] , [ have_pthread=no], "-lposix4" )
	] )
  AC_CHECK_LIB(pthread, pthread_sigmask, AC_DEFINE(HAVE_PTHREAD_SIGMASK, 1, [Define if you have the pthread_sigmask function]))
  AC_CHECK_LIB(pthread, pthread_kill_other_threads_np, AC_DEFINE(HAVE_PTHREAD_KILL_OTHER_THREADS_NP, 1, [Define if you have the pthread_kill_other_threads_np function]))
  LDFLAGS="$save_LDFLAGS"

  if test "x$have_pthread" != xyes; then
    PTHREAD_LIBS=""
    PTHREAD_INCS=""
  fi
fi
AC_SUBST(PTHREAD_LIBS)
AC_SUBST(PTHREAD_INCS)

dnl libreadline
have_readline=auto
READLINE_LIBS=""
READLINE_INCS=""
AC_ARG_WITH(readline,
	AC_HELP_STRING([--with-readline=DIR],
		[readline library is installed in DIR]), 
	have_readline="$withval")
if test "x$have_readline" != xno; then
  if test "x$have_readline" != xauto; then
    READLINE_LIBS="-L$have_readline/lib"
    READLINE_INCS="-I$have_readline/include"
  fi

  save_LDFLAGS="$LDFLAGS"
  LDFLAGS="$LDFLAGS $READLINE_LIBS"
  dnl use different functions in the cascade of AC_CHECK_LIB
  dnl calls since configure may cache the results
  AC_CHECK_LIB(readline, readline, 
	[ READLINE_LIBS="$READLINE_LIBS -lreadline" 
	  have_readline=yes ]
	, [ AC_CHECK_LIB(readline, rl_history_search_forward, 
	[ READLINE_LIBS="$READLINE_LIBS -lreadline -ltermcap" 
	  have_readline=yes ]
	, [ AC_CHECK_LIB(readline, rl_reverse_search_history,
	[ READLINE_LIBS="$READLINE_LIBS -lreadline -lncurses" 
	  have_readline=yes ] 
	, have_readline=no, "-lncurses" ) ], "-ltermcap" ) ],)
  LDFLAGS="$save_LDFLAGS"

  if test "x$have_readline" = xyes; then
    AC_CHECK_LIB(readline, rl_completion_matches,
	have_readline=yes, have_readline=no,
	$READLINE_LIBS)
  fi
  if test "x$have_readline" = xyes; then
    AC_DEFINE(HAVE_LIBREADLINE, 1, [Define if you have the readline library])
  else
    READLINE_LIBS=""
    READLINE_INCS=""
  fi
fi
AC_SUBST(READLINE_LIBS)
AC_SUBST(READLINE_INCS)

dnl OpenSSL
dnl change "no" in the next line to "auto" to get OpenSSL automatically
dnl when available
have_openssl=no
OPENSSL_LIBS=""
OPENSSL_INCS=""
AC_ARG_WITH(openssl,
	AC_HELP_STRING([--with-openssl=DIR],
		[OpenSSL library is installed in DIR]), 
	have_openssl="$withval")
if test "x$have_openssl" != xno; then
  if test "x$have_openssl" != xauto; then
    OPENSSL_LIBS="-L$withval/lib"
    OPENSSL_INCS="-I$withval/include"
  fi

  save_LDFLAGS="$LDFLAGS"
  LDFLAGS="$LDFLAGS $OPENSSL_LIBS"
  AC_CHECK_LIB(ssl, SSL_read,
	[ OPENSSL_LIBS="$OPENSSL_LIBS -lssl"
	  have_openssl=yes ]
	, have_openssl=no )
  dnl on some systems, -lcrypto needs to be passed as well
  AC_CHECK_LIB(crypto, ERR_get_error, OPENSSL_LIBS="$OPENSSL_LIBS -lcrypto")
  LDFLAGS="$save_LDFLAGS"

  if test "x$have_openssl" = xyes; then
    AC_COMPILE_IFELSE(AC_LANG_PROGRAM([#include <openssl/ssl.h>],[]), ,
	save_CPPFLAGS="$CPPFLAGS"
	CPPFLAGS="$CPPFLAGS -DOPENSSL_NO_KRB5"
	AC_COMPILE_IFELSE(AC_LANG_PROGRAM([#include <openssl/ssl.h>],[]),
		AC_DEFINE(OPENSSL_NO_KRB5, 1, [Define if OpenSSL should not use Kerberos 5]),
		have_openssl=no))
	CPPFLAGS="$save_CPPFLAGS"
  fi

  if test "x$have_openssl" = xyes; then
    AC_DEFINE(HAVE_OPENSSL, 1, [Define if you have the OpenSSL library])
  else
    OPENSSL_LIBS=""
    OPENSSL_INCS=""
  fi
fi
AC_SUBST(OPENSSL_LIBS)
AC_SUBST(OPENSSL_INCS)

DL_LIBS=""
AC_CHECK_LIB(dl, dlopen, [ DL_LIBS="-ldl" ] )
AC_SUBST(DL_LIBS)

MALLOC_LIBS=""
AC_CHECK_LIB(malloc, malloc, [ MALLOC_LIBS="-lmalloc" ] )
AC_SUBST(MALLOC_LIBS)

save_LIBS="$LIBS"
LIBS="$LIBS $MALLOC_LIBS"
AC_CHECK_FUNCS(mallopt mallinfo)
LIBS="$save_LIBS"

SOCKET_LIBS=""
AC_CHECK_FUNC(gethostbyname_r, [], [
  AC_CHECK_LIB(nsl_r, gethostbyname_r, [ SOCKET_LIBS="-lnsl_r" ],
    AC_CHECK_LIB(nsl, gethostbyname_r, [ SOCKET_LIBS="-lnsl"   ] ))])
AC_CHECK_FUNC(setsockopt, , AC_CHECK_LIB(socket, setsockopt, 
	[ SOCKET_LIBS="-lsocket $SOCKET_LIBS" ] ))

#AC_CHECK_LIB(setsockopt, socket, [ SOCKET_LIBS="-lsocket -lnsl" ], [], "-lnsl" )
AC_SUBST(SOCKET_LIBS)

dnl check for NetCDF io library (default /usr and /usr/local)
have_netcdf=auto
NETCDF_CFLAGS=""
NETCDF_LIBS=""
AC_ARG_WITH(netcdf,
	AC_HELP_STRING([--with-netcdf=DIR],
		[netcdf library is installed in DIR]),
	have_netcdf="$withval")
if test "x$have_netcdf" != xno; then
  if test "x$have_netcdf" != xauto; then
    NETCDF_CFLAGS="-I$withval/include"
    NETCDF_LIBS="-L$withval/lib"
  fi

  save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS $NETCDF_CFLAGS"
  AC_CHECK_HEADER(netcdf.h, have_netcdf=yes, have_netcdf=no)
  CPPFLAGS="$save_CPPFLAGS"

  if test "x$have_netcdf" = xyes; then
  	save_LDFLAGS="$LDFLAGS"
  	LDFLAGS="$LDFLAGS $NETCDF_LIBS"
  	AC_CHECK_LIB(netcdf, nc_open, NETCDF_LIBS="$NETCDF_LIBS -lnetcdf"
        	AC_DEFINE(HAVE_LIBNETCDF, 1, [Define if you have the netcdf library]) have_netcdf=yes, have_netcdf=no)
  	LDFLAGS="$save_LDFLAGS"
  fi

  if test "x$have_netcdf" != xyes; then
    NETCDF_CFLAGS=""
    NETCDF_LIBS=""
  fi
fi
AC_SUBST(NETCDF_CFLAGS)
AC_SUBST(NETCDF_LIBS)
AM_CONDITIONAL(HAVE_NETCDF, test "x$have_netcdf" = xyes)

dnl check for z (de)compression library (default /usr and /usr/local)
have_z=auto
Z_CFLAGS=""
Z_LIBS=""
AC_ARG_WITH(z,
	AC_HELP_STRING([--with-z=DIR],
		[z library is installed in DIR]),
	have_z="$withval")
if test "x$have_z" != xno; then
  if test "x$have_z" != xauto; then
    Z_CFLAGS="-I$withval/include"
    Z_LIBS="-L$withval/lib"
  fi

  save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS $Z_CFLAGS"
  AC_CHECK_HEADER(zlib.h, have_z=yes, have_z=no)
  CPPFLAGS="$save_CPPFLAGS"

  if test "x$have_z" = xyes; then
  	save_LDFLAGS="$LDFLAGS"
  	LDFLAGS="$LDFLAGS $Z_LIBS"
  	AC_CHECK_LIB(z, gzopen, Z_LIBS="$Z_LIBS -lz"
        	AC_DEFINE(HAVE_LIBZ, 1, [Define if you have the z library]) have_z=yes, have_z=no)
  	LDFLAGS="$save_LDFLAGS"
  fi

  if test "x$have_z" != xyes; then
    Z_CFLAGS=""
    Z_LIBS=""
  fi
fi
AC_SUBST(Z_CFLAGS)
AC_SUBST(Z_LIBS)

dnl check for bz2 (de)compression library (default /usr and /usr/local)
have_bz=auto
BZ_CFLAGS=""
BZ_LIBS=""
AC_ARG_WITH(bz2,
	AC_HELP_STRING([--with-bz2=DIR],
		[bz2 library is installed in DIR]),
	have_bz="$withval")
if test "x$have_bz" != xno; then
  if test "x$have_bz" != xauto; then
    BZ_CFLAGS="-I$withval/include"
    BZ_LIBS="-L$withval/lib"
  fi

  save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS $BZ_CFLAGS"
  AC_CHECK_HEADER(bzlib.h, have_bz=yes, have_bz=no)
  CPPFLAGS="$save_CPPFLAGS"

  if test "x$have_bz" = xyes; then
  	save_LDFLAGS="$LDFLAGS"
  	LDFLAGS="$LDFLAGS $BZ_LIBS"
  	AC_CHECK_LIB(bz2, BZ2_bzopen, BZ_LIBS="$BZ_LIBS -lbz2"
        	AC_DEFINE(HAVE_LIBBZ2, 1, [Define if you have the bz2 library]) have_bz=yes, have_bz=no)
  	LDFLAGS="$save_LDFLAGS"
  fi

  if test "x$have_bz" != xyes; then
    BZ_CFLAGS=""
    BZ_LIBS=""
  fi
fi
AC_SUBST(BZ_CFLAGS)
AC_SUBST(BZ_LIBS)

dnl check for getopt in standard library
AC_CHECK_FUNCS(getopt_long , need_getopt=, need_getopt=getopt; need_getopt=getopt1)
if test x$need_getopt = xgetopt; then
  AC_LIBOBJ(getopt)
elif test x$need_getopt = xgetopt1; then
  AC_LIBOBJ(getopt1)
fi

dnl hwcounters
have_hwcounters=auto
HWCOUNTERS_LIBS=""
HWCOUNTERS_INCS=""
AC_ARG_WITH(hwcounters,
	AC_HELP_STRING([--with-hwcounters=DIR],
		[hwcounters library is installed in DIR]), 
	have_hwcounters="$withval")
if test "x$have_hwcounters" != xno; then
  if test "x$have_hwcounters" != xauto; then
    HWCOUNTERS_LIBS="-L$withval/lib"
    HWCOUNTERS_INCS="-I$withval/include"
  fi

  case "$host_os-$host" in
    linux*-i?86*) HWCOUNTERS_INCS="$HWCOUNTERS_INCS -I/usr/src/linux-`uname -r | sed 's|smp$||'`/include"
  esac
  save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS $HWCOUNTERS_INCS"
  save_LIBS="$LIBS"
  LIBS="$LIBS $HWCOUNTERS_LIBS"
  have_hwcounters=no
  case "$host_os-$host" in
   linux*-i?86*)
	AC_CHECK_HEADERS( libperfctr.h ,
	 AC_CHECK_LIB( perfctr, vperfctr_open , 
	  [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lperfctr" 
	    AC_DEFINE(HAVE_LIBPERFCTR, 1, [Define if you have the perfctr library])
	    have_hwcounters=yes
	  ]
         )
	)
	if test "x$have_hwcounters" != xyes; then
        	AC_CHECK_HEADERS( libpperf.h,
	 	 AC_CHECK_LIB( pperf, start_counters, 
	  	  [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lpperf" 
	    	    AC_DEFINE(HAVE_LIBPPERF, 1, [Define if you have the pperf library])
	    	    have_hwcounters=yes
		  ]
		 )
		)
	fi
	;;
   linux*-ia64*)
	AC_CHECK_HEADERS( perfmon/pfmlib.h ,
	 AC_CHECK_LIB( pfm, pfm_initialize , 
	  [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lpfm" 
	    AC_DEFINE(HAVE_LIBPFM, 1, [Define if you have the pfm library])
	    have_hwcounters=yes
	  ]
         )
	)
	;;
   solaris*)
	AC_CHECK_HEADERS( libcpc.h ,
	 AC_CHECK_LIB( cpc, cpc_access , 
	  [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lcpc" 
	    AC_DEFINE(HAVE_LIBCPC, 1, [Define if you have the cpc library])
	    have_hwcounters=yes
	  ]
	 )
	)
	if test "x$have_hwcounters" != xyes; then
		AC_CHECK_HEADERS( perfmon.h ,
		 AC_CHECK_LIB( perfmon, clr_pic , 
		  [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lperfmon" 
		    AC_DEFINE(HAVE_LIBPERFMON, 1, [Define if you have the perfmon library])
		    have_hwcounters=yes
		  ]
		 )
  		)
	fi
	;;
   irix*)
	AC_CHECK_LIB( perfex, start_counters , 
	 [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lperfex" 
	   have_hwcounters=yes
	 ]
	)
 	;;
  esac
  LIBS="$save_LIBS"
  CPPFLAGS="$save_CPPFLAGS"

  if test "x$have_hwcounters" != xyes; then
    HWCOUNTERS_LIBS=""
    HWCOUNTERS_INCS=""
   else
    CFLAGS="$CFLAGS -DHWCOUNTERS -DHW_`uname -s` -DHW_`uname -m`"
  fi
fi
AC_SUBST(HWCOUNTERS_LIBS)
AC_SUBST(HWCOUNTERS_INCS)

dnl check for the performance counters library 
have_pcl=auto
PCL_CFLAGS=""
PCL_LIBS=""
AC_ARG_WITH(pcl,
	AC_HELP_STRING([--with-pcl=DIR],
		[pcl library is installed in DIR]),
	have_pcl="$withval")
if test "x$have_pcl" != xno; then
  if test "x$have_pcl" != xauto; then
    PCL_CFLAGS="-I$withval/include"
    PCL_LIBS="-L$withval/lib"
  fi

  save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS $PCL_CFLAGS"
  AC_CHECK_HEADER(pcl.h, have_pcl=yes, have_pcl=no)
  CPPFLAGS="$save_CPPFLAGS"

  if test "x$have_pcl" = xyes; then
  	save_LDFLAGS="$LDFLAGS"
  	LDFLAGS="$LDFLAGS $PCL_LIBS"
  	AC_CHECK_LIB(pcl, PCLinit, PCL_LIBS="$PCL_LIBS -lpcl"
        	AC_DEFINE(HAVE_LIBPCL, 1, [Define if you have the pcl library]) have_pcl=yes, have_pcl=no)
  	LDFLAGS="$save_LDFLAGS"
  fi

  if test "x$have_pcl" != xyes; then
    PCL_CFLAGS=""
    PCL_LIBS=""
  fi
fi
AC_SUBST(PCL_CFLAGS)
AC_SUBST(PCL_LIBS)

dnl check for the Perl-compatible regular expressions library 
have_pcre=auto
PCRE_CFLAGS=""
PCRE_LIBS=""
PCRE_CONFIG=""
AC_ARG_WITH(pcre,
	AC_HELP_STRING([--with-pcre=DIR],
		[pcre library is installed in DIR]),
	have_pcre="$withval")
if test "x$have_pcre" != xno; then

    MPATH="$withval/bin:$PATH"
    AC_PATH_PROG(PCRE_CONFIG,pcre-config,,$MPATH)
    if test "x$PCRE_CONFIG" = x; then
    	AC_MSG_RESULT(pcre-config not found; please use --with-pcre=<path>)
    else
    	PCRE_CFLAGS="`$PCRE_CONFIG --cflags`"
    	PCRE_LIBS="`$PCRE_CONFIG --libs`"
    fi

    req_pcre_ver='4.5'
    AC_MSG_CHECKING(for pcre >= $req_pcre_ver)
    if test "x$PCRE_CONFIG" = x; then
    	have_pcre=no
    	AC_MSG_RESULT($have_pcre (without pcre-config to query the pcre version, we assume it's < $req_pcre_ver))
    else
    	pcre_ver="`$PCRE_CONFIG --version 2>/dev/null`"
    	if test MONETDB_VERSION_TO_NUMBER(echo $pcre_ver) -ge MONETDB_VERSION_TO_NUMBER(echo "$req_pcre_ver"); then
      		have_pcre=yes
      		AC_MSG_RESULT($have_pcre (found $pcre_ver))
    	else
      		have_pcre=no
      		AC_MSG_RESULT($have_pcre (found only $pcre_ver))
    	fi
    fi

    if test "x$have_pcre" = xyes; then
    	save_CPPFLAGS="$CPPFLAGS"
    	CPPFLAGS="$CPPFLAGS $PCRE_CFLAGS"
    	AC_CHECK_HEADER(pcre.h, have_pcre=yes, have_pcre=no)
    	CPPFLAGS="$save_CPPFLAGS"
    fi

    if test "x$have_pcre" = xyes; then
    	save_LDFLAGS="$LDFLAGS"
    	LDFLAGS="$LDFLAGS $PCRE_LIBS"
    	AC_CHECK_LIB(pcre, pcre_compile, PCRE_LIBS="$PCRE_LIBS -lpcre" have_pcre=yes, have_pcre=no)
    	LDFLAGS="$save_LDFLAGS"
    fi

    if test "x$have_pcre" = xyes; then
    	AC_DEFINE(HAVE_LIBPCRE, 1, [Define if you have the pcre library])
    else
    	PCRE_CFLAGS=""
    	PCRE_LIBS=""
    fi
fi
AC_SUBST(PCRE_CFLAGS)
AC_SUBST(PCRE_LIBS)

AC_CHECK_HEADERS(iconv.h locale.h langinfo.h)
AC_CHECK_FUNCS(nl_langinfo setlocale)

dnl  If not present in libc, the iconv* functions might be in a separate libiconv;
dnl  on CYGWIN, they are even called libiconv*; hence, we define some wrappers to
dnl  handle this.
ICONV_LIBS=""
have_iconv=no
libiconv=no
AC_CHECK_LIB(iconv, iconv, 
  [ ICONV_LIBS="-liconv" have_iconv=yes ],
  [ AC_CHECK_LIB(iconv, libiconv, [ ICONV_LIBS="-liconv" have_iconv=yes libiconv=yes ], 
    [ AC_CHECK_FUNC(iconv, [ have_iconv=yes ], []) 
    ]) 
  ]
)
if test "x$have_iconv" = xyes; then
	AC_DEFINE(HAVE_ICONV, 1, [Define if you have the iconv function])
	if test "x$libiconv" = xyes; then
		AC_DEFINE(iconv, libiconv, [Wrapper])
		AC_DEFINE(iconv_open, libiconv_open, [Wrapper])
		AC_DEFINE(iconv_close, libiconv_close, [Wrapper])
	fi
	AC_TRY_COMPILE([
#include <stdlib.h>
#include <iconv.h>
extern
#ifdef __cplusplus
"C"
#endif
#if defined(__STDC__) || defined(__cplusplus)
size_t iconv (iconv_t cd, char * *inbuf, size_t *inbytesleft, char * *outbuf, size_t *outbytesleft);
#else
size_t iconv();
#endif
], [], iconv_const="", iconv_const="const")
	AC_DEFINE_UNQUOTED(ICONV_CONST, $iconv_const, 
	 [Define as const if the declaration of iconv() needs const for 2nd argument.])
fi
AC_SUBST(ICONV_LIBS)   

AC_CHECK_PROG(LATEX2HTML,latex2html,latex2html)
AC_CHECK_PROG(LATEX,latex,latex)
AC_CHECK_PROG(PDFLATEX,pdflatex,pdflatex)
AC_CHECK_PROG(DVIPS,dvips,dvips)
AC_CHECK_PROG(FIG2DEV,fig2dev,fig2dev)
FIG2DEV_EPS=eps
AC_MSG_CHECKING([$FIG2DEV postscript option])
[ if [ "$FIG2DEV" ]; then
        echo "" > $FIG2DEV -L$FIG2DEV_EPS 2>/dev/null
        if [ $? -ne 0 ]; then
                FIG2DEV_EPS=ps
        fi
fi ]
AC_MSG_RESULT($FIG2DEV_EPS)
AC_SUBST(FIG2DEV_EPS)
AM_CONDITIONAL(DOCTOOLS, test -n "$LATEX2HTML" -a -n "$LATEX" -a -n "$PDFLATEX" -a -n "$FIG2DEV" -a -n "$DVIPS") 

INSTALL_BACKUP=""
AC_MSG_CHECKING([$INSTALL --backup option])
[ if [ "$INSTALL" ]; then
	inst=`echo $INSTALL | sed 's/ .*//'`
	if [ ! "`file $inst | grep 'shell script' 2>/dev/null`" ] ; then
	    echo "" > c 2>/dev/null
            $INSTALL --backup=nil c d 1>/dev/null 2>/dev/null
            if [ $? -eq 0 ]; then
                INSTALL_BACKUP="--backup=nil" 
            fi
            $INSTALL -C --backup=nil c e 1>/dev/null 2>/dev/null
            if [ $? -eq 0 ]; then
                INSTALL_BACKUP="-C --backup=nil" 
       	    fi
	fi 
	rm -f c d e 2>/dev/null
fi ]
AC_MSG_RESULT($INSTALL_BACKUP)
AC_SUBST(INSTALL_BACKUP)

PHP_INCS=""
PHP_EXTENSIONDIR=""
AC_ARG_WITH(php,
	AC_HELP_STRING([--with-php=<value>], [PHP support (yes/no/auto)]),
	have_php="$withval",
	have_php=auto)
AC_ARG_WITH(php-config, AC_HELP_STRING([--with-php-config=FILE], [Path to php-config script]),
	PHP_CONFIG="$withval",
	PHP_CONFIG=php-config)

if test "x$have_php" != xno; then
	AC_CHECK_PROG(have_php, $PHP_CONFIG, $have_php, no)
	if test $have_php = no; then
		AC_MSG_ERROR(Cannot find php-config. Please use --with-php-config=PATH)
	fi
fi
AC_MSG_CHECKING([for PHP])
if test "x$have_php" != xno; then
	php_prefix="`$PHP_CONFIG --prefix`"
	if test -z "$php_prefix"; then
		have_php=no
	else
		PHP_INCS=" `$PHP_CONFIG --includes`"
		PHP_EXTENSIONDIR="`$PHP_CONFIG --extension-dir | sed -e s+$php_prefix++g`"
	fi
fi
if test x"$have_php" != xno; then
	have_php=yes
fi
AC_MSG_RESULT($have_php)
AC_SUBST(PHP_INCS)
AC_SUBST(PHP_EXTENSIONDIR)
AM_CONDITIONAL(HAVE_PHP, test x"$have_php" != xno)



AC_SUBST(CFLAGS)
AC_SUBST(CXXFLAGS)
]) dnl AC_DEFUN AM_MONETDB_LIBS

AC_DEFUN([AM_MONETDB_CLIENT],[

dnl check for Monet and some basic utilities
AM_MONET($1)
MPATH="$MONETDB_PREFIX/bin:$PATH"
AC_PATH_PROG(MX,Mx$EXEEXT,,$MPATH)
AC_PATH_PROG(MEL,mel$EXEEXT,,$MPATH)

]) dnl AC_DEFUN AM_MONETDB_CLIENT
