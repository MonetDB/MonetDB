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
dnl Portions created by CWI are Copyright (C) 1997-2003 CWI.
dnl All Rights Reserved.
dnl 
dnl Contributor(s):
dnl 		Martin Kersten <Martin.Kersten@cwi.nl>
dnl 		Peter Boncz <Peter.Boncz@cwi.nl>
dnl 		Niels Nes <Niels.Nes@cwi.nl>
dnl 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

dnl VERSION_TO_NUMBER macro (copied from libxslt)
AC_DEFUN(MONET_VERSION_TO_NUMBER,
[`$1 | awk 'BEGIN { FS = "."; } { printf "%d", ([$]1 * 1000 + [$]2) * 1000 + [$]3;}'`])

AC_DEFUN(AM_MONET,
[

dnl check for monet
have_monet=auto
MONET_CFLAGS=""
MONET_LIBS=""
MONET_MOD_PATH=""
MONET_PREFIX="."
if test "x$1" = "x"; then
  MONET_REQUIRED_VERSION="4.3.10"
else
  MONET_REQUIRED_VERSION="$1"
fi
AC_ARG_WITH(monet,
[  --with-monet=DIR     monet is installed in DIR], have_monet="$withval")
if test "x$have_monet" != xno; then
  MPATH="$withval/bin:$PATH"
  AC_PATH_PROG(MONET_CONFIG,monet-config,,$MPATH)

  if test "x$MONET_CONFIG" != x; then
    AC_MSG_CHECKING(for Monet >= $MONET_REQUIRED_VERSION) 
    MONETVERS=`$MONET_CONFIG --version`
    if test MONET_VERSION_TO_NUMBER(echo $MONETVERS) -ge MONET_VERSION_TO_NUMBER(echo $MONET_REQUIRED_VERSION); then
      have_monet=yes
    else
      have_monet=no
    fi
    AC_MSG_RESULT($have_monet -> $MONETVERS found)
  fi

  if test "x$have_monet" != xyes; then
    MONET_CFLAGS=""
    MONET_INCS=""
    MONET_INCLUDEDIR=""
    MONET_LIBS=""
    MONET_MOD_PATH=""
    MONET_PREFIX=""
  else
    MONET_CFLAGS=`$MONET_CONFIG --cflags`
    MONET_INCS=`$MONET_CONFIG --includes`
    MONET_INCLUDEDIR=`$MONET_CONFIG --includedir`
    MONET_LIBS=`$MONET_CONFIG --libs`
    MONET_MOD_PATH=`$MONET_CONFIG --modpath`
    MONET_PREFIX=`$MONET_CONFIG --prefix`
    CLASSPATH="$CLASSPATH:`$MONET_CONFIG --classpath`"
  fi
fi
AC_SUBST(MONET_CFLAGS)
AC_SUBST(MONET_INCS)
AC_SUBST(MONET_INCLUDEDIR)
AC_SUBST(MONET_LIBS)
AC_SUBST(MONET_MOD_PATH)
AC_SUBST(MONET_PREFIX)
AC_SUBST(CLASSPATH)
AM_CONDITIONAL(HAVE_MONET,test x$have_monet = xyes)
])

AC_DEFUN(AM_MONET_COMPILER,
[

dnl Some special requirements for MacOS X/Darwin
case "$host" in
powerpc-apple-darwin*)
	CPPFLAGS="$CPPFLAGS -I/sw/include"
	LDFLAGS="$LDFLAGS -L/sw/lib"
	CFLAGS="$CFLAGS -Ddlsym=dlsym_prepend_underscore"
	;;
esac

dnl check for compiler
AC_ARG_WITH(gcc,
[  --with-gcc=<compiler>   which C compiler to use
  --without-gcc           do not use GCC], [
	case $withval in
	no)	CC=cc CXX=CC;;
	yes)	CC=gcc CXX=g++;;
	*)	CC=$withval;;
	esac])

AC_ARG_WITH(gxx,
[  --with-gxx=<compiler>   which C++ compiler to use], [
	case $withval in
	yes|no)	AC_ERROR(must supply a compiler when using --with-gxx);;
	*)	CXX=$withval;;
	esac])

AC_PROG_CC()
AC_PROG_CXX()
AC_PROG_CPP()
AC_PROG_GCC_TRADITIONAL()
AC_SUBST(CFLAGS)

bits=32
AC_ARG_WITH(bits,
[  --with-bits=<#bits>     specify number of bits (32 or 64)],[
case $withval in
32)	case "$host" in
	ia64*)	AC_ERROR([we do not support 32 bits on $host, yet]);;
	esac
	;;
64)	case "$host" in
	i?86*)	AC_ERROR([$host does not support 64 bits]);;
	esac
	;;
*)	AC_ERROR(--with-bits argument must be either 32 or 64);;
esac
bits=$withval
])
case "$GCC-$host_os-$bits" in
yes-solaris*-64)
	case `$CC -v 2>&1` in
	*'gcc version 3.'*)	;;
	*)	AC_ERROR([need GCC version 3.X for 64 bits]);;
	esac
	CC="$CC -m$bits"
	CXX="$CXX -m$bits"
	;;
-solaris*-64)
	CC="$CC -xarch=v9"
	CXX="$CXX -xarch=v9"
	;;
yes-irix*-64)
	CC="$CC -mabi=$bits"
	CXX="$CXX -mabi=$bits"
	;;
-irix*-64)
	CC="$CC -$bits"
	CXX="$CXX -$bits"
	;;
esac

dnl some dirty hacks
dnl we use LEXLIB=-ll because this is usually correctly installed 
dnl and -lfl usually only in the 32bit version
thread_safe_flag_spec="-D_REENTRANT"
# only needed in monet
MEL_LIBS=""
case "$host_os" in
solaris*)
    case "$CC" in
      cc*) 
	echo "$CC=cc"
	MEL_LIBS="-z muldefs"
        thread_safe_flag_spec="-mt" ;;
      *) ;;
    esac
    LEXLIB=-ll
    ;;
irix*)
    LEXLIB=-ll
    ;;
aix*)
    thread_safe_flag_spec="-D_THREAD_SAFE $thread_safe_flag_spec"
    ;;
esac
AC_SUBST(MEL_LIBS)
AC_SUBST(thread_safe_flag_spec)

have_java=auto
JAVAC="javac"
JAR="jar"
AC_ARG_WITH(java,
[  --with-java=DIR     javac and jar are installed in DIR/bin], have_java="$withval")
if test "x$have_java" != xno; then
  JPATH="$withval/bin:$PATH"
  AC_PATH_PROG(JAVAC,javac,,$JPATH)
  AC_PATH_PROG(JAR,jar,,$JPATH)
  if test "x$JAVAC" = "x"; then
     have_java=no
  elif test "x$JAR" = "x"; then
     have_java=no
  else
     have_java=yes
  fi

  if test "x$have_java" != xyes; then
    JAVAC=""
    JAR=""
    CLASSPATH=""
  fi
fi
AC_SUBST(JAVAC)
AC_SUBST(JAR)
AC_SUBST(CLASSPATH)
AM_CONDITIONAL(HAVE_JAVA,test x$have_java = xyes)

])

AC_DEFUN(AM_MONET_TOOLS,[

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
AC_PROG_LEX
AM_PROG_LEX()
AC_PROG_YACC()
AC_PROG_LN_S()
AC_CHECK_PROG(RM,rm,rm -f)
AC_CHECK_PROG(MV,mv,mv -f)
AC_CHECK_PROG(LOCKFILE,lockfile,lockfile -r 2,echo)
AC_PATH_PROG(BASH,bash, /usr/bin/bash, $PATH)

dnl to shut up automake (.m files are used for mel not for objc)
AC_CHECK_TOOL(OBJC,objc)

#AM_DEPENDENCIES(CC)
#AM_DEPENDENCIES(CXX)

dnl Checks for header files.
AC_HEADER_STDC()

])

AC_DEFUN(AM_MONET_OPTIONS,
[
dnl --enable-debug
AC_ARG_ENABLE(debug,
[  --enable-debug          enable full debugging [default=off]],
  enable_debug=$enableval, enable_debug=no)
if test "x$enable_debug" = xyes; then
  if test "x$GCC" = xyes; then
    CFLAGS="$CFLAGS -O0"
    CXXFLAGS="$CXXFLAGS -O0"
  fi
  CFLAGS="$CFLAGS -g"
  CXXFLAGS="$CXXFLAGS -g"
fi

dnl --enable-optimize
NO_INLINE_CFLAGS=""
AC_ARG_ENABLE(optimize,
[  --enable-optimize       enable extra optimization [default=off]],
  enable_optim=$enableval, enable_optim=no)
if test "x$enable_optim" = xyes; then
  dnl Optimization flags
  if test "x$enable_debug" = xno; then
    if test "x$GCC" = xyes; then
      dnl -fomit-frame-pointer crashes memprof
      gcc_ver="`$CC --version | head -1 | sed -e 's|^[[^0-9]]*\([[0-9]][[0-9\.]]*[[0-9]]\)[[^0-9]].*$|\1|'`"
      case "$host-$gcc_ver" in
      i*86-*-*-3.2)   CFLAGS="$CFLAGS -O6"
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
      i*86-*-*)       CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions -malign-loops=4 -malign-jumps=4 -malign-functions=4 -fexpensive-optimizations -funroll-all-loops  -funroll-loops -frerun-cse-after-loop -frerun-loop-opt";;
      ia64-*-*)       CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions                                                     -fexpensive-optimizations                                    -frerun-cse-after-loop -frerun-loop-opt"
                      dnl  Obviously, 4-byte alignment doesn't make sense on Linux64; didn't try 8-byte alignment, yet.
                      dnl  Further, when combining either of "-funroll-all-loops" and "-funroll-loops" with "-On" (n>1),
                      dnl  gcc (3.2.1 & 2.96) does not seem to produce stable/correct? binaries under Linux64
                      dnl  (Mserver crashes with segmentation fault);
                      dnl  hence, we omit both "-funroll-all-loops" and "-funroll-loops", here
                      ;;
      *-sun-solaris*) CFLAGS="$CFLAGS -O2 -fomit-frame-pointer -finline-functions"
                      if test "$CC" = "gcc -m64" ; then
                        NO_INLINE_CFLAGS="-O1"
                      fi
                      ;;
      *irix6.5*)      CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions"
                      NO_INLINE_CFLAGS="-fno-inline"
                      ;;
      *)              CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions";;
      esac
    else
      case "$host" in
      i*86-*-*)       CFLAGS="$CFLAGS -mp1 -O3 -tpp6 -restrict -axiMK -unroll -ipo -ipo_obj";;
      ia64-*-*)       CFLAGS="$CFLAGS -mp1 -O2 -tpp2 -restrict -mcpu=itanium2 -unroll -ipo -ipo_obj "
                      dnl  With "-O3", ecc does not seem to produce stable/correct? binaries under Linux64
                      dnl  (Mserver produces some incorrect BATpropcheck warnings);
                      dnl  hence, we use only "-O2", here.
                      ;;
#      *irix6.5*)      CFLAGS="$CFLAGS -O3 -Ofast=IP27 -OPT:alias=restrict -IPA"
      *irix6.5*)      CFLAGS="$CFLAGS -O3 -OPT:div_split=ON:fast_complex=ON:fast_exp=ON:fast_nint=ON:Olimit=2147483647:roundoff=3 -TARG:processor=r10k -IPA"
                      LDFLAGS="$LDFLAGS -IPA"
                      ;;
      *-sun-solaris*) CFLAGS="$CFLAGS -xO5";;
      esac   
    fi

  fi
fi
AC_SUBST(NO_INLINE_CFLAGS)

dnl --enable-warning (only gcc)
AC_ARG_ENABLE(warning,
[  --enable-warning           enable extended compiler warnings [default=off]],
  enable_warning=$enableval, enable_warning=no)
if test "x$enable_warning" = xyes; then
  if test "x$GCC" = xyes; then
    CFLAGS="$CFLAGS -Wall -std=c99"
    CPPFLAGS="$CPPFLAGS -Wall -std=c99"
  fi
fi

dnl --enable-profile
need_profiling=no
AC_ARG_ENABLE(profile,
[  --enable-profile        enable profiling [default=off]],
  enable_prof=$enableval, enable_prof=no)
if test "x$enable_prof" = xyes; then
  CFLAGS="$CFLAGS -DPROFILE"
  need_profiling=yes
  if test "x$GCC" = xyes; then
    CFLAGS="$CFLAGS -pg"
  fi
fi
AM_CONDITIONAL(PROFILING,test "x$need_profiling" = xyes)

dnl --enable-instrument
need_instrument=no
AC_ARG_ENABLE(instrument,
[  --enable-instrument        enable instrument [default=off]],
  enable_instrument=$enableval, enable_instrument=no)
if test "x$enable_instrument" = xyes; then
  CFLAGS="$CFLAGS -DPROFILE"
  need_instrument=yes
  if test "x$GCC" = xyes; then
    CFLAGS="$CFLAGS -finstrument-functions -g"
  fi
fi

dnl static or shared linking
SHARED_LIBS=''
[
if [ "$enable_static" = "yes" ]; then
	CFLAGS="$CFLAGS -DSTATIC"
	SHARED_LIBS='$(STATIC_LIBS) $(smallTOC_SHARED_LIBS)'
	case "$host_os" in
	aix*)	CFLAGS="$CFLAGS -DsmallTOC";;
	*)	SHARED_LIBS="$SHARED_LIBS "'$(largeTOC_SHARED_LIBS)';;
	esac
	if test "x$GCC" != xyes; then
		case "$host_os" in
		irix*)	SHARED_LIBS="$SHARED_LIBS -lm";;
		esac
	fi
fi
]
AC_SUBST(SHARED_LIBS)
AM_CONDITIONAL(LINK_STATIC,test "x$enable_static" = xyes)

])

AC_DEFUN(AM_MONET_LIBS,
[
dnl libpthread
have_pthread=auto
PTHREAD_LIBS=""
PTHREAD_INCS=""
AC_ARG_WITH(pthread,
[  --with-pthread=DIR     pthread library is installed in DIR], 
	[have_pthread="$withval"])
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
[  --with-readline=DIR     readline library is installed in DIR], 
	[have_readline="$withval"])
if test "x$have_readline" != xno; then
  if test "x$have_readline" != xauto; then
    READLINE_LIBS="-L$withval/lib"
    READLINE_INCS="-I$withval/include"
  fi

  save_LDFLAGS="$LDFLAGS"
  LDFLAGS="$LDFLAGS $READLINE_LIBS"
  AC_CHECK_LIB(readline, readline, 
	[ READLINE_LIBS="$READLINE_LIBS -lreadline" 
          AC_DEFINE(HAVE_LIBREADLINE, 1, [Define if you have the readline library]) 
	  have_readline=yes ]
	, [ AC_CHECK_LIB(readline, rl_history_search_forward, 
	[ READLINE_LIBS="$READLINE_LIBS -lreadline -ltermcap" 
          AC_DEFINE(HAVE_LIBREADLINE, 1, [Define if you have the readline library]) 
	  have_readline=yes ]
	, have_readline=no, "-ltermcap" ) ], )
  LDFLAGS="$save_LDFLAGS"

  if test "x$have_readline" != xyes; then
    READLINE_LIBS=""
    READLINE_INCS=""
  fi
fi
AC_SUBST(READLINE_LIBS)
AC_SUBST(READLINE_INCS)

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

dnl check for z (de)compression library (default /usr and /usr/local)
have_z=auto
Z_CFLAGS=""
Z_LIBS=""
AC_ARG_WITH(z,
[  --with-z=DIR     z library is installed in DIR], have_z="$withval")
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
[  --with-bz2=DIR     bz2 library is installed in DIR], have_bz="$withval")
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
AC_CHECK_FUNCS(getopt_long , , AC_LIBOBJ(getopt); AC_LIBOBJ(getopt1))

dnl hwcounters
have_hwcounters=auto
HWCOUNTERS_LIBS=""
HWCOUNTERS_INCS=""
AC_ARG_WITH(hwcounters,
[  --with-hwcounters=DIR     hwcounters library is installed in DIR], 
	[have_hwcounters="$withval"])
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
[  --with-pcl=DIR     pcl library is installed in DIR], have_pcl="$withval")
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

AC_CHECK_HEADERS(iconv.h locale.h langinfo.h)
AC_CHECK_FUNCS(nl_langinfo setlocale)

dnl  If not present in libc, the iconv* functions might be in a separate libiconv;
dnl  on CYGWIN, they are even called libiconv* (iconv.h takes care of mapping
dnl  iconv* to libiconv*).
ICONV_LIBS=""
have_iconv=no
AC_CHECK_LIB(iconv, iconv, 
  [ ICONV_LIBS="-liconv" have_iconv=yes ],
  [ AC_CHECK_LIB(iconv, libiconv, [ ICONV_LIBS="-liconv" have_iconv=yes ], 
    [ AC_CHECK_FUNC(iconv, [ have_iconv=yes ], []) 
    ]) 
  ]
)
if test "x$have_iconv" = xyes; then
	AC_DEFINE(HAVE_ICONV, 1, [Define if you have the iconv function])
fi
AC_SUBST(ICONV_LIBS)   

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
AM_CONDITIONAL(DOCTOOLS, test -n "$LATEX" -a -n "$FIG2DEV" -a -n "$DVIPS") 

INSTALL_BACKUP=""
AC_MSG_CHECKING([$INSTALL --backup option])
[ if [ "$INSTALL" ]; then
	if [ ! "`file $INSTALL | grep 'shell script' 2>/dev/null`" ] ; then
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

AC_SUBST(CFLAGS)
AC_SUBST(CXXFLAGS)
])

AC_DEFUN(AM_MONET_CLIENT,[

dnl check for Monet and some basic utilities
AM_MONET($1)
MPATH="$MONET_PREFIX/bin:$PATH"
AC_PATH_PROG(MX,Mx$EXEEXT,,$MPATH)
AC_PATH_PROG(MEL,mel$EXEEXT,,$MPATH)

])
