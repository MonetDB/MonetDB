/* Manual config.h. needed for win32 .  */

#define WIN32

/* Define if on AIX 3.
   System headers sometimes define this.
   We just want to avoid a redefinition error message.  */
#ifndef _ALL_SOURCE
/* #undef _ALL_SOURCE */
#endif

/* Define if using alloca.c.  */
/* #undef C_ALLOCA */

/* Define to empty if the keyword does not work.  */
/* #undef const */

/* Define to one of _getb67, GETB67, getb67 for Cray-2 and Cray-YMP systems.
   This function is required for alloca.c support on those systems.  */
/* #undef CRAY_STACKSEG_END */

/* Define if you have alloca, as a function or macro.  */
#define HAVE_ALLOCA 1

/* Define if you have <alloca.h> and it should be used (not on Ultrix).  */
#undef HAVE_ALLOCA_H 

/* Define if you don't have vprintf but do have _doprnt.  */
/* #undef HAVE_DOPRNT */

/* Define if you have a working `mmap' system call.  */
#define HAVE_MMAP 1

/* Define if you have <sys/wait.h> that is POSIX.1 compatible.  */
#undef HAVE_SYS_WAIT_H 

/* Define if your struct tm has tm_zone.  */
#undef HAVE_TM_ZONE

/* Define if you don't have tm_zone but do have the external array
   tzname.  */
#define HAVE_TZNAME 

/* Define if utime(file, NULL) sets file's timestamp to the present.  */
#define HAVE_UTIME_NULL 1

/* Define if you have the vprintf function.  */
#define HAVE_VPRINTF 1

/* Define as __inline if that's what the C compiler calls it.  */
#define inline __inline

/* Define to `long' if <sys/types.h> doesn't define.  */
/* #undef off_t */

/* Define to `int' if <sys/types.h> doesn't define.  */
#define pid_t int

/* Define as the return type of signal handlers (int or void).  */
#define RETSIGTYPE void

/* Define if the `setpgrp' function takes no argument.  */
#undef SETPGRP_VOID 

/* Define to `unsigned' if <sys/types.h> doesn't define.  */
/* #undef size_t */

/* If using the C implementation of alloca, define if you know the
   direction of stack growth for your system; otherwise it will be
   automatically deduced at run-time.
 STACK_DIRECTION > 0 => grows toward higher addresses
 STACK_DIRECTION < 0 => grows toward lower addresses
 STACK_DIRECTION = 0 => direction of growth unknown
 */
/* #undef STACK_DIRECTION */

/* Define if the `S_IS*' macros in <sys/stat.h> do not work properly.  */
/* #undef STAT_MACROS_BROKEN */

/* Define if you have the ANSI C header files.  */
#define STDC_HEADERS 1

/* Define if you can safely include both <sys/time.h> and <time.h>.  */
#undef TIME_WITH_SYS_TIME 

/* Define if your <sys/time.h> declares struct tm.  */
/* #undef TM_IN_SYS_TIME */

/* Define if your processor stores words with the most significant
   byte first (like Motorola and SPARC, unlike Intel and VAX).  */
/* #undef WORDS_BIGENDIAN */

/* Define if lex declares yytext as a char * by default, not a char[].  */
#define YYTEXT_POINTER 1

/* Define this if the compiler has bool type defined */
#define HAVE_BOOL 1

/* Define this if you have the readline library */
#undef HAVE_LIBREADLINE 

/* Define this if you have ctime_r(time_t*,char *buf,size_t s) */
/* #undef HAVE_CTIME_R3 */

/* The number of bytes in a long.  */
#define SIZEOF_LONG 4

/* Define if you have the ctime_r function.  */
#undef HAVE_CTIME_R 

/* Define if you have the getcwd function.  */
#define HAVE_GETCWD 1

/* Define if you have the gethostname function.  */
#define HAVE_GETHOSTNAME 1

/* Define if you have the getopt function.  */
#undef HAVE_GETOPT 

/* Define if you have the getpagesize function.  */
#undef HAVE_GETPAGESIZE 

/* Define if you have the getpgid function.  */
#undef HAVE_GETPGID 

/* Define if you have the gettimeofday function.  */
#undef HAVE_GETTIMEOFDAY 

/* Define if you have the mkdir function.  */
#define HAVE_MKDIR 1

/* Define if you have the putenv function.  */
#define HAVE_SETENV 1
#undef HAVE_PUTENV 

/* Define if you have the rlimit function.  */
/* #undef HAVE_RLIMIT */

/* Define if you have the rmdir function.  */
#define HAVE_RMDIR 1

/* Define if you have the select function.  */
#define HAVE_SELECT 1

/* Define if you have the strcspn function.  */
#define HAVE_STRCSPN 1

/* Define if you have the strdup function.  */
#define HAVE_STRDUP 1

/* Define if you have the strerror function.  */
#define HAVE_STRERROR 

/* Define if you have the strstr function.  */
#define HAVE_STRSTR 1

/* Define if you have the strtod function.  */
#define HAVE_STRTOD 1

/* Define if you have the strtol function.  */
#define HAVE_STRTOL 1

/* Define if you have the <dirent.h> header file.  */
#undef HAVE_DIRENT_H 

/* Define if you have the <fcntl.h> header file.  */
#define HAVE_FCNTL_H 1
#define HAVE_FCNTL  1
#define HAVE_SETSOCKOPT 0

/* Define if you have the <dlfcn.h> header file.  */
#undef HAVE_DLFCN_H 

/* Define if you have the <getopt.h> header file.  */
#undef HAVE_GETOPT_H 

/* Define if you have the <limits.h> header file.  */
#define HAVE_LIMITS_H 1

/* Define if you have the <pwd.h> header file.  */
#undef HAVE_PWD_H 

/* Define if you have the <malloc.h> header file.  */
#define HAVE_MALLOC_H 1

/* Define if you have the <ndir.h> header file.  */
/* #undef HAVE_NDIR_H */

/* Define if you have the <pthread.h> header file.  */
#define HAVE_PTHREAD_H 1

/* Define if you have the <semaphore.h> header file.  */
#define HAVE_SEMAPHORE_H 1

/* Define if you have the <rlimit.h> header file.  */
/* #undef HAVE_RLIMIT_H */

/* Define if you have the <sys/dir.h> header file.  */
/* #undef HAVE_SYS_DIR_H */

/* Define if you have the <sys/file.h> header file.  */
#undef HAVE_SYS_FILE_H 

/* Define if you have the <sys/param.h> header file.  */
#undef HAVE_SYS_PARAM_H 

/* Define if you have the <sys/times.h> header file.  */
#undef HAVE_SYS_TIMES_H 

/* Define if you have the <sys/mman.h> header file.  */
#undef HAVE_SYS_MMAN_H 

/* Define if you have the <sys/ndir.h> header file.  */
/* #undef HAVE_SYS_NDIR_H */

/* Define if you have the <sys/resource.h> header file.  */
#undef HAVE_SYS_RESOURCE_H 

/* Define if you have the <sys/time.h> header file.  */
#undef HAVE_SYS_TIME_H 

/* Define if you have the <sys/utime.h> header file.  */
#define HAVE_SYS_UTIME_H 1

/* Define if you have the <netdb.h> header file.  */
#undef HAVE_NETDB_H 

/* Define if you have the <thread.h> header file.  */
/* #undef HAVE_THREAD_H */

/* Define if you have the <time.h> header file.  */
#define HAVE_TIME_H 1

/* Define if you have the <unistd.h> header file.  */
#undef HAVE_UNISTD_H 

/* Name of package */
#define PACKAGE "MonetDB"

/* Version number of package */
#define VERSION "4.3.5"

/* Host identifier */
#define HOST "i686-pc-win32"

#undef HAVE_LONGLONG
#define HAVE__INT64

/* dirty hack */
#include <winlibc.h>

#define HAVE_NOMALLINFO
