/* -*-C-*- */

/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

/* Manual config.h. needed for win32 .  */

/* We use #if _MSC_VER >= 1300 to identify Visual Studio .NET 2003 in
 * which the value is actually 0x1310 (in Visual Studio 6 the value is
 * 1200; in Visual Studio 8 the value is 1400).
 */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif

/* Visual Studio 8 has deprecated lots of stuff: suppress warnings */
#define _CRT_SECURE_NO_DEPRECATE 1

#if defined(_DEBUG) && defined(_CRTDBG_MAP_ALLOC)
/* In this case, malloc and friends are redefined in crtdbg.h to debug
   versions.  We need to include stdlib.h and malloc.h first or else
   we get conflicting declarations.
*/
#include <stdlib.h>
#include <malloc.h>
#include <crtdbg.h>
#endif

#include <process.h>
#include <windows.h>
#include <stddef.h>

/* indicate to sqltypes.h that windows.h has already been included and
   that it doesn't have to define Windows constants */
#define ALREADY_HAVE_WINDOWS_TYPE 1

#define NATIVE_WIN32 1

#define DIR_SEP '\\'
#define DIR_SEP_STR "\\"
#define PATH_SEP ';'
#define PATH_SEP_STR ";"
#define SO_EXT ".dll"
#define SO_PREFIX "lib"

#define isatty _isatty

/* Define to 1 if you have the <alloca.h> header file. */
/* #undef HAVE_ALLOCA_H */

/* Define to 1 if you have the `asctime_r' function. */
/* #undef HAVE_ASCTIME_R */

/* Define if you have asctime_r(struct tm*,char *buf,size_t s) */
/* #undef HAVE_ASCTIME_R3 */

/* Define to 1 if you have the `basename' function. */
/* #undef HAVE_BASENAME */

/* Define to 1 if you have the `crypt' function. */
/* #undef HAVE_CRYPT */

/* Define to 1 if you have the <crypt.h> header file. */
/* #undef HAVE_CRYPT_H */

/* Define to 1 if you have the `ctime_r' function. */
/* #undef HAVE_CTIME_R */

/* Define if you have ctime_r(time_t*,char *buf,size_t s) */
/* #undef HAVE_CTIME_R3 */

/* Define if you have the cURL library */
/* #undef HAVE_CURL */

/* Define to 1 if you have the declaration of `strdup', and to 0 if you don't.
   */
#define HAVE_DECL_STRDUP 1

/* Define to 1 if you have the declaration of `strndup', and to 0 if you
   don't. */
#define HAVE_DECL_STRNDUP 0

/* Define to 1 if you have the declaration of `strtof', and to 0 if you don't.
   */
#define HAVE_DECL_STRTOF 0

/* Define to 1 if you have the declaration of `strtoll', and to 0 if you
   don't. */
#if _MSC_VER >= 1300
#define HAVE_DECL_STRTOLL 1
#else
#define HAVE_DECL_STRTOLL 0
#endif

/* Define to 1 if you have the <dirent.h> header file. */
/* #undef HAVE_DIRENT_H */

/* Define to 1 if you have the <dlfcn.h> header file. */
/* #undef HAVE_DLFCN_H */

/* Define to 1 if you have the `drand48' function. */
/* #undef HAVE_DRAND48 */

/* Define to 1 if you have the `fcntl' function. */
/* #undef HAVE_FCNTL */

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if you have the `fdatasync' function. */
/* #undef HAVE_FDATASYNC */

/* Define to 1 if you have the `fpclass' function. */
#define HAVE_FPCLASS 1

/* Define to 1 if you have the `fpclassify' function. */
/* #undef HAVE_FPCLASSIFY */

/* Define to 1 if you have the `fstat' function. */
#define HAVE_FSTAT 1

/* Define to 1 if you have the `fsync' function. */
/* #undef HAVE_FSYNC */

/* Define to 1 if you have the `ftime' function. */
#define HAVE_FTIME 1

/* Define to 1 if you have the `ftruncate' function. */
/* #undef HAVE_FTRUNCATE */

/* Does your compiler support function attributes (__attribute__)? */
/* #undef HAVE_FUNCTION_ATTRIBUTES */

/* Define to 1 if you have the `getlogin' function. */
/* #undef HAVE_GETLOGIN */

/* Define to 1 if you have the `getopt' function. */
/* #undef HAVE_GETOPT */

/* Define to 1 if you have the <getopt.h> header file. */
/* #undef HAVE_GETOPT_H */

/* Define to 1 if you have the `getopt_long' function. */
/* #undef HAVE_GETOPT_LONG */

/* Define to 1 if you have the `getrlimit' function. */
/* #undef HAVE_GETRLIMIT */

/* Define to 1 if you have the `getsysteminfo' function. */
#define HAVE_GETSYSTEMINFO 1

/* Define to 1 if you have the `gettimeofday' function. */
/* #undef HAVE_GETTIMEOFDAY */

/* Define to 1 if you have the `getuid' function. */
/* #undef HAVE_GETUID */

/* Define to 1 if you have the `GlobalMemoryStatus' function. */
#define HAVE_GLOBALMEMORYSTATUS 1

/* Define to 1 if you have the `GlobalMemoryStatusEx' function. */
#define HAVE_GLOBALMEMORYSTATUSEX 1 /* only on >= NT 5 */

/* Define if you have the iconv library */
/* #undef HAVE_ICONV */		/* defined in winrules.msc */

/* Define to 1 if you have the <iconv.h> header file. */
#define HAVE_ICONV_H 1

/* Define to 1 if you have the <ieeefp.h> header file. */
/* #undef HAVE_IEEEFP_H */

/* Define to 1 if you have the `inet_ntop' function. */
/* #undef HAVE_INET_NTOP */

/* Define to 1 if the system has the type `__int64'. */
#define HAVE___INT64 1

/* Define to 1 if you have the <inttypes.h> header file. */
/* #undef HAVE_INTTYPES_H */

/* Define to 1 if you have the <io.h> header file. */
/* #undef HAVE_IO_H */

/* Define to 1 if you have the `isinf' function. */
/* #undef HAVE_ISINF */

/* Define to 1 if you have the `kill' function. */
/* #undef HAVE_KILL */

/* Define to 1 if you have the <langinfo.h> header file. */
/* #undef HAVE_LANGINFO_H */

/* Define if you have the bz2 library */
/* #undef HAVE_LIBBZ2 */	/* defined in winrules.msc */

/* Define to 1 if you have the <libgen.h> header file. */
/* #undef HAVE_LIBGEN_H */

/* Define if you have the cpc library */
/* #undef HAVE_LIBPCL */

/* Define if you have the pcre library */
/* #undef HAVE_LIBPCRE */	/* defined in winrules.msc */

/* Define if you have the perfctr library */
/* #undef HAVE_LIBPERFCTR */

/* Define if you have the perfctr library */
/* #undef HAVE_LIBPERFMON */

/* Define if you have the pfm library */
/* #undef HAVE_LIBPFM */

/* Define if you have the pperf library */
/* #undef HAVE_LIBPPERF */

/* Define if you have the readline library */
/* #undef HAVE_LIBREADLINE */

/* Do we have the libxml2 library available to support XML Schema? */
/* #undef HAVE_LIBXML2 */	/* defined in winrules.msc */

/* Define if you have the z library */
/* #undef HAVE_LIBZ */		/* defined in winrules.msc */

/* Define to 1 if you have the <locale.h> header file. */
#define HAVE_LOCALE_H 1

/* Define to 1 if you have the `localtime_r' function. */
/* #undef HAVE_LOCALTIME_R */

/* Define to 1 if you have the `lockf' function. */
/* #undef HAVE_LOCKF */

/* Define to 1 if the system has the type `long long'. */
#if _MSC_VER >= 1300
/* Visual Studio .NET 2003 does have long long, but the printf %lld
 * format is interpreted the same as %ld, i.e. useless
 */
/* #define HAVE_LONG_LONG 1 */
#else
/* #undef HAVE_LONG_LONG */
#endif

/* Define to 1 if you have the `mallinfo' function. */
/* #undef HAVE_MALLINFO */

/* Define to 1 if you have the <malloc.h> header file. */
#define HAVE_MALLOC_H 1

/* Define to 1 if you have the `mallopt' function. */
/* #undef HAVE_MALLOPT */

/* Define to 1 if you have the `mrand48' function. */
/* #undef HAVE_MRAND48 */

/* Define to 1 if you have the `nanosleep' function. */
/* #undef HAVE_NANOSLEEP */

/* Define to 1 if you have the <netdb.h> header file. */
/* #undef HAVE_NETDB_H */

/* Define to 1 if you have the `nl_langinfo' function. */
/* #undef HAVE_NL_LANGINFO */

/* Define to 1 if you have the <odbcinst.h> header file. */
#define HAVE_ODBCINST_H 1

/* Define to 1 if you have the `opendir' function. */
/* #undef HAVE_OPENDIR */

/* Define if you have the OpenSSL library */
/* #undef HAVE_OPENSSL */

/* Define if you want TIJAH */
#define HAVE_PFTIJAH 1

/* Define to 1 if you have the `pipe' function. */
#define HAVE_PIPE 1
#define pipe(p) _pipe(p, 8192, O_BINARY)

/* Define to 1 if you have the `popen' function. */
/* #undef HAVE_POPEN */

/* Define to 1 if you have the `posix_fadvise' function. */
/* #undef HAVE_POSIX_FADVISE */

/* Define to 1 if you have the `posix_madvise' function. */
/* #undef HAVE_POSIX_MADVISE */

/* Define if you want PROBXML */
/* #undef HAVE_PROBXML */

/* Define to 1 if you have the <pthread.h> header file. */
#define HAVE_PTHREAD_H 1

/* Define if you have the pthread_kill function */
/* #undef HAVE_PTHREAD_KILL */

/* Define if you have the pthread_setschedprio function */
/* #undef HAVE_PTHREAD_SETSCHEDPRIO */

/* Define if you have the pthread_sigmask function */
/* #undef HAVE_PTHREAD_SIGMASK */

/* Define to 1 if the system has the type `ptrdiff_t'. */
#define HAVE_PTRDIFF_T 1

/* Define to 1 if you have the `putenv' function. */
#define HAVE_PUTENV 1

/* Define to 1 if you have the <pwd.h> header file. */
/* #undef HAVE_PWD_H */

/* Define to 1 if you have the <regex.h> header file. */
/* #undef HAVE_REGEX_H */

/* Define if the compiler supports the restrict keyword */
/* #undef HAVE_RESTRICT */

/* Define if the compiler supports the __restrict__ keyword */
/* #undef HAVE___RESTRICT__ */

/* Define to 1 if you have the `sbrk' function. */
/* #undef HAVE_SBRK */

/* Define to 1 if you have the <semaphore.h> header file. */
#define HAVE_SEMAPHORE_H 1

/* Define to 1 if you have the `setenv' function. */
/* #undef HAVE_SETENV */

/* Define to 1 if you have the `setlocale' function. */
#define HAVE_SETLOCALE 1

/* Define to 1 if you have the `setsid' function. */
/* #undef HAVE_SETSID */

/* Define to 1 if you have the `shutdown' function. */
#define HAVE_SHUTDOWN 1

/* Define to 1 if you have the `sigaction' function. */
/* #undef HAVE_SIGACTION */

/* Define to 1 if you have the <signal.h> header file. */
#define HAVE_SIGNAL_H 1

/* Define if your mallinfo struct has signed elements */
/* #undef HAVE_SIGNED_MALLINFO) */

/* Define to 1 if you have the `snprintf' function. */
#define HAVE_SNPRINTF 1
#ifndef snprintf
#define snprintf _snprintf
#endif

/* Define to 1 if the system has the type `socklen_t'. */
#define HAVE_SOCKLEN_T 1
typedef int socklen_t;

/* Define if you have the SQLGetPrivateProfileString function */
#define HAVE_SQLGETPRIVATEPROFILESTRING 1

/* Define to 1 if the system has the type `ssize_t'. */
#define HAVE_SSIZE_T 1		/* see below */

/* Define to 1 if you have the <stdbool.h> header file. */
/* #undef HAVE_STDBOOL_H */

/* Define to 1 if you have the <stdint.h> header file. */
/* #undef HAVE_STDINT_H */

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `strcasecmp' function. */
#define HAVE_STRCASECMP 1
#ifndef strcasecmp
#define strcasecmp(x,y) _stricmp(x,y)
#endif

/* Define to 1 if you have the `strdup' function. */
#define HAVE_STRDUP 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the <strings.h> header file. */
/* #undef HAVE_STRINGS_H */

/* Define to 1 if you have the `strncasecmp' function. */
#define HAVE_STRNCASECMP 1
#ifndef strncasecmp
#define strncasecmp(x,y,z) _strnicmp(x,y,z)
#endif

/* Define to 1 if you have the `strndup' function. */
/* #undef HAVE_STRNDUP */

/* Define to 1 if you have the `strsignal' function. */
/* #undef HAVE_STRSIGNAL */

/* Define to 1 if you have the `strtod' function. */
#define HAVE_STRTOD 1

/* Define to 1 if you have the `strtof' function. */
/* #undef HAVE_STRTOF */

/* Define to 1 if you have the `strtoll' function. */
#if _MSC_VER >= 1300
#define HAVE_STRTOLL 1
#ifndef strtoll
#define strtoll _strtoi64
#endif
#else
/* #undef HAVE_STRTOLL */
#endif

/* Define to 1 if you have the `strtoull' function. */
#if _MSC_VER >= 1300
#define HAVE_STRTOULL 1
#ifndef strtoull
#define strtoull _strtoui64
#endif
#else
/* #undef HAVE_STRTOULL */
#endif

/* Define if you have struct mallinfo */
/* #undef HAVE_STRUCT_MALLINFO */

/* Define to 1 if you have the `sysconf' function. */
/* #undef HAVE_SYSCONF */

/* Define to 1 if you have the <sys_file.h> header file. */
/* #undef HAVE_SYS_FILE_H */

/* Define to 1 if you have the <sys_ioctl.h> header file. */
/* #undef HAVE_SYS_IOCTL_H */

/* Define to 1 if you have the <sys_mman.h> header file. */
/* #undef HAVE_SYS_MMAN_H */

/* Define to 1 if you have the <sys_param.h> header file. */
/* #undef HAVE_SYS_PARAM_H */

/* Define to 1 if you have the <sys_resource.h> header file. */
/* #undef HAVE_SYS_RESOURCE_H */

/* Define if you have _sys_siglist */
/* #undef HAVE__SYS_SIGLIST */

/* Define to 1 if you have the <sys_socket.h> header file. */
/* #undef HAVE_SYS_SOCKET_H */

/* Define to 1 if you have the <sys_stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys_times.h> header file. */
/* #undef HAVE_SYS_TIMES_H */

/* Define to 1 if you have the <sys_types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys_un.h> header file. */
/* #undef HAVE_SYS_UN_H */

/* Define to 1 if you have the <sys_wait.h> header file. */
/* #undef HAVE_SYS_WAIT_H */

/* Define to 1 if you have the <termios.h> header file. */
/* #undef HAVE_TERMIOS_H */

/* Define to 1 if you have the `times' function. */
/* #undef HAVE_TIMES */

/* Define to 1 if you have the `uname' function. */
/* #undef HAVE_UNAME */

/* Define to 1 if you have the <unistd.h> header file. */
/* #undef HAVE_UNISTD_H */

/* Define to 1 if you have the `vsnprintf' function. */
#define HAVE_VSNPRINTF 1
#ifndef vsnprintf
#define vsnprintf _vsnprintf
#endif

/* Define to 1 if you have the <winsock.h> header file. */
#define HAVE_WINSOCK_H 1

/* Define to 1 if you have the <xmmintrin.h> header file. */
/* #undef HAVE_XMMINTRIN_H */

/* Host identifier */
#define HOST "i686-pc-win32"

/* Define as const if the declaration of iconv() needs const for 2nd argument.
   */
#define ICONV_CONST const

/* Format to print 64 bit signed integers. */
#define LLFMT "%I64d"

/* Define if the oid type should use 32 bits on a 64-bit architecture */
/* 32-bit OIDs are used by default on 64-bit Windows;
 * compile with `nmake MONET_OID64=1` to use 64-bit OIDs on 64-bit Windows;
 * remove the following 5 lines (and this comment) to make 64-bit OIDs
 * default on 64-bit Windows (then to be overruled via `nmake MONET_OID32=1`;
 * see also NT/winrules.msc .
 */
#ifdef _WIN64
#ifndef MONET_OID64
#define MONET_OID32 1
#endif
#endif

/* Define if you do not want assertions */
/* #undef NDEBUG */

/* Define if you don't want to expand the bat type */
/* #undef NOEXPAND_BAT */

/* Define if you don't want to expand the bit type */
/* #undef NOEXPAND_BIT */

/* Define if you don't want to expand the bte type */
/* #undef NOEXPAND_BTE */

/* Define if you don't want to expand the chr type */
/* #undef NOEXPAND_CHR */

/* Define if you don't want to expand the dbl type */
/* #undef NOEXPAND_DBL */

/* Define if you don't want to expand the flt type */
/* #undef NOEXPAND_FLT */

/* Define if you don't want to expand the int type */
/* #undef NOEXPAND_INT */

/* Define if you don't want to expand the lng type */
/* #undef NOEXPAND_LNG */

/* Define if you don't want to expand the oid type */
/* #undef NOEXPAND_OID */

/* Define if you don't want to expand the ptr type */
/* #undef NOEXPAND_PTR */

/* Define if you don't want to expand the sht type */
/* #undef NOEXPAND_SHT */

/* Define if you don't want to expand the str type */
/* #undef NOEXPAND_STR */

/* Define if you don't want to expand the wrd type */
/* #undef NOEXPAND_WRD */

/* Define if OpenSSL should not use Kerberos 5 */
/* #undef OPENSSL_NO_KRB5 */

/* Name of package */
#define PACKAGE "MonetDB"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT ""

/* Define to the full name of this package. */
#define PACKAGE_NAME ""

/* Define to the full name and version of this package. */
#define PACKAGE_STRING ""

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME ""

/* Define to the version of this package. */
#define PACKAGE_VERSION ""

/* Compiler flag */
/* #undef PROFILE */

/* Define as the return type of signal handlers (`int' or `void'). */
#define RETSIGTYPE void

/* Define to 1 if the `setpgrp' function takes no argument. */
/* #undef SETPGRP_VOID */

/* The size of a `char', as computed by sizeof. */
#define SIZEOF_CHAR 1

/* The size of a `int', as computed by sizeof. */
#define SIZEOF_INT 4

/* The size of a `long', as computed by sizeof. */
#define SIZEOF_LONG 4

/* The size of a `short', as computed by sizeof. */
#define SIZEOF_SHORT 2

#ifdef _WIN64
/* The size of a `ptrdiff_t', as computed by sizeof. */
#define SIZEOF_PTRDIFF_T 8

/* The size of a `size_t', as computed by sizeof. */
#define SIZEOF_SIZE_T 8
typedef __int64 ssize_t;

/* The size of a `void *', as computed by sizeof. */
#define SIZEOF_VOID_P 8
#else
/* The size of a `ptrdiff_t', as computed by sizeof. */
#define SIZEOF_PTRDIFF_T 4

/* The size of a `size_t', as computed by sizeof. */
#define SIZEOF_SIZE_T 4

/* The size of a `void *', as computed by sizeof. */
#define SIZEOF_VOID_P 4

/* The size of a `ssize_t', as computed by sizeof. */
#define SIZEOF_SSIZE_T SIZEOF_SIZE_T
typedef int ssize_t;
#endif

/* The size of a `__int64', as computed by sizeof. */
#define SIZEOF___INT64 8

/* If using the C implementation of alloca, define if you know the
   direction of stack growth for your system; otherwise it will be
   automatically deduced at run-time.
	STACK_DIRECTION > 0 => grows toward higher addresses
	STACK_DIRECTION < 0 => grows toward lower addresses
	STACK_DIRECTION = 0 => direction of growth unknown */
/* #undef STACK_DIRECTION */

/* Define to 1 if the `S_IS*' macros in <sys/stat.h> do not work properly. */
/* #undef STAT_MACROS_BROKEN */

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
/* #undef TIME_WITH_SYS_TIME */

/* Define to 1 if your <sys/time.h> declares `struct tm'. */
/* #undef TM_IN_SYS_TIME */

/* Format to print 64 bit unsigned integers. */
#define ULLFMT "%I64u"

/* Define on MS Windows (also under Cygwin) */
#ifndef WIN32
#define WIN32 1
#endif

/* Define to 1 if your processor stores words with the most significant byte
   first (like Motorola and SPARC, unlike Intel and VAX). */
/* #undef WORDS_BIGENDIAN */

/* Number of bits in a file offset, on hosts where this is settable. */
#define _FILE_OFFSET_BITS 32

/* Define for large files, on AIX-style hosts. */
/* #undef _LARGE_FILES */

/* Compiler flag */
/* #undef _POSIX_C_SOURCE */

/* Compiler flag */
/* #undef _POSIX_SOURCE */

/* Compiler flag */
/* #undef _XOPEN_SOURCE */

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* Wrapper */
/* #undef iconv */

/* Wrapper */
/* #undef iconv_close */

/* Wrapper */
/* #undef iconv_open */

/* Define to `__inline__' or `__inline' if that's what the C compiler
   calls it, or to nothing if 'inline' is not supported under any name.  */
#ifndef inline
#define inline __inline
#endif

/* Define to `long' if <sys/types.h> does not define. */
/* #undef off_t */

/* Define to `int' if <sys/types.h> does not define. */
/* #undef pid_t */

/* Define to `unsigned' if <sys/types.h> does not define. */
/* #undef size_t */
