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
  MONET_REQUIRED_VERSION="4.3.5"
else
  MONET_REQUIRED_VERSION="$1"
fi
AC_ARG_WITH(monet,
[  --with-monet=DIR     monet is installed in DIR], have_monet="$withval")
if test "x$have_monet" != xno; then
  AC_PATH_PROG(MONET_CONFIG,monet-config,,$withval/bin:$PATH)

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
  fi
fi
AC_SUBST(MONET_CFLAGS)
AC_SUBST(MONET_INCS)
AC_SUBST(MONET_INCLUDEDIR)
AC_SUBST(MONET_LIBS)
AC_SUBST(MONET_MOD_PATH)
AC_SUBST(MONET_PREFIX)
AM_CONDITIONAL(HAVE_MONET,test x$have_monet = xyes)
])

AC_DEFUN(AM_MONET_COMPILER,
[
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

AC_PROG_CC
AC_PROG_CXX
AC_PROG_CPP
AC_PROG_GCC_TRADITIONAL

bits=32
AC_ARG_WITH(bits,
[  --with-bits=<#bits>     specify number of bits (32 or 64)],[
case $withval in
32)	;;
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
      case "$host" in
      i*86-*-*)       CFLAGS="$CFLAGS -O6 -fomit-frame-pointer -finline-functions -malign-loops=4 -malign-jumps=4 -malign-functions=4 -ffast-math -fexpensive-optimizations -funroll-all-loops  -funroll-loops -frerun-cse-after-loop -frerun-loop-opt";;
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
      i*86-*-*)       CFLAGS="$CFLAGS -O3 -tpp6 -axiMK -unroll -wp_ipo -ipo_obj";;
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
    CFLAGS="$CFLAGS -ansi -pedantic"
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
          AC_DEFINE(HAVE_LIBPTHREAD) 
	  have_pthread=yes ] , 
	dnl sun
	[ AC_CHECK_LIB(pthread, sem_post, 
		[ PTHREAD_LIBS="$PTHREAD_LIBS -lpthread -lposix4" 
          	AC_DEFINE(HAVE_LIBPTHREAD) 
	  	have_pthread=yes ] , [ have_pthread=no], "-lposix4" )
	] )
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
          AC_DEFINE(HAVE_LIBREADLINE) 
	  have_readline=yes ]
	, [ AC_CHECK_LIB(readline, rl_history_search_forward, 
	[ READLINE_LIBS="$READLINE_LIBS -lreadline -ltermcap" 
          AC_DEFINE(HAVE_LIBREADLINE) 
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
        	AC_DEFINE(HAVE_LIBZ) have_z=yes, have_z=no)
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
        	AC_DEFINE(HAVE_LIBBZ2) have_bz=yes, have_bz=no)
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
AC_SUBST(LIBOBJS)
AC_CHECK_FUNCS(getopt_long , , [LIBOBJS="getopt.lo getopt1.lo"] ) 

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

  save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS $HWCOUNTERS_INCS"
  save_LIBS="$LIBS"
  LIBS="$LIBS $HWCOUNTERS_LIBS"
  have_hwcounters=no
  case "$host_os" in
   linux*)
	AC_CHECK_HEADERS( libperfctr.h ,
	 AC_CHECK_LIB( perfctr, vperfctr_open , 
	  [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lperfctr" 
	    AC_DEFINE(HAVE_LIBPERFCTR)
	    have_hwcounters=yes ] ) ,
        AC_CHECK_HEADERS( libpperf.h,
	 AC_CHECK_LIB( pperf, start_counters, 
	  [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lpperf" 
	    AC_DEFINE(HAVE_LIBPPERF)
	    have_hwcounters=yes ] )
	)) ;;
   solaris*)
	AC_CHECK_HEADERS( libcpc.h ,
	 AC_CHECK_LIB( cpc, cpc_access , 
	  [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lcpc" 
	    AC_DEFINE(HAVE_LIBCPC)
	    have_hwcounters=yes ] ) ,
	AC_CHECK_HEADERS( perfmon.h ,
	 AC_CHECK_LIB( perfmon, clr_pic , 
	  [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lperfmon" 
	    AC_DEFINE(HAVE_LIBPERFMON)
	    have_hwcounters=yes ] )
  	)) ;;
   irix*)
	AC_CHECK_LIB( perfex, start_counters , 
	 [ HWCOUNTERS_LIBS="$HWCOUNTERS_LIBS -lperfex" 
	   have_hwcounters=yes ] )
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
	echo "" > c 2>/dev/null
        $INSTALL -C --backup=nil c d 1>/dev/null 2>/dev/null
        if [ $? -ne 0 ]; then
                INSTALL_BACKUP="-C --backup=nil" 
        fi
	rm -f c d  2>/dev/null
fi ]
AC_MSG_RESULT($INSTALL_BACKUP)
AC_SUBST(INSTALL_BACKUP)
])
