#ifndef WINLIBC_H
#define WINLIBC_H

#define NATIVE_WIN32

/* Windows emulation stubs for common Unix functions
 */
#ifdef NATIVE_WIN32
#define DIR_SEP '\\'
#  define MAXPATHLEN 1024
#  ifdef _MSC_VER

/* These POSIXish functions are available in the Microsoft C library
 * prefixed with underscore (which of course technically speaking is
 * the Right Thing, as they are non-ANSI. Not that being non-ANSI
 * prevents Microsoft from practically requiring you to include
 * <windows.h> every now and then...).
 *
 * You still need to include the appropriate headers to get the
 * prototypes, <io.h> or <direct.h>.
 *
 * For some functions, we provide emulators in glib, which are prefixed
 * with win_.
 */
#include <winsock.h>
#define sleep(s)
/* first some dummies, later move to pwd.h */
#define getpwuid(uid) NULL
#define getuid() 0
#define cuserid() "monet"
struct passwd {
  char *pw_name;	/* Username.  */
  char *pw_passwd;	/* Password.  */
  int  pw_uid;		/* User ID.  */
};

/* first dummy, later in sys/wait.h */
#define waitpid(pid,statusp,sig) 0

/* define some more signals */ 
#include <signal.h>
#define SIGPIPE SIGTERM
#define SIGBUS  SIGSEGV
#define SIGQUIT	SIGBREAK
#define SIGHUP  SIGINT
#define SIGALRM 0
#define kill(s,sig) 
#define alarm(x) 0

#define sbrk(x)	0
#define sysconf(flag) 0

#define sigset_t int
#define sigfillset(s) 0
#define sigdelset(s,m) 0

#define pthread_sigmask(v,mnew,morg) 0
#define pthread_kill(tid,sig) 0

#define __getpgid(pid) getpid()

#define setbuffer(x,y,z)	setbuf(x,y)
#define S_ISCHR(mode)		1

#define link rename
#define setenv SetEnvironmentVariableA
#define RUSAGE_SELF = 1

/* Structure which says how much of each resource has been used.  */
struct rusage
  {
    /* Total amount of user time used.  */
    struct timeval ru_utime;
    /* Total amount of system time used.  */
    struct timeval ru_stime;
    /* Maximum resident set size (in kilobytes).  */
    long int ru_maxrss;
    /* Amount of sharing of text segment memory
       with other processes (kilobyte-seconds).  */
    long int ru_ixrss;
    /* Amount of data segment memory used (kilobyte-seconds).  */
    long int ru_idrss;
    /* Amount of stack memory used (kilobyte-seconds).  */
    long int ru_isrss;
    /* Number of soft page faults (i.e. those serviced by reclaiming
       a page from the list of pages awaiting reallocation.  */
    long int ru_minflt;
    /* Number of hard page faults (i.e. those that required I/O).  */
    long int ru_majflt;
    /* Number of times a process was swapped out of physical memory.  */
    long int ru_nswap;
    /* Number of input operations via the file system.  Note: This
       and `ru_oublock' do not include operations with the cache.  */
    long int ru_inblock;
    /* Number of output operations via the file system.  */
    long int ru_oublock;
    /* Number of IPC messages sent.  */
    long int ru_msgsnd;
    /* Number of IPC messages received.  */
    long int ru_msgrcv;
    /* Number of signals delivered.  */
    long int ru_nsignals;
    /* Number of voluntary context switches, i.e. because the process
       gave up the process before it had to (usually to wait for some
       resource to be available).  */
    long int ru_nvcsw;
    /* Number of involuntary context switches, i.e. a higher priority process
       became runnable or the current process used up its time slice.  */
    long int ru_nivcsw;
  };

#define getrusage(flag,usage) (int)memset((void*)usage, 0, sizeof(struct rusage))

struct tms
  {
    int tms_utime;		/* User CPU time.  */
    int tms_stime;		/* System CPU time.  */

    int tms_cutime;		/* User CPU time of dead children.  */
    int tms_cstime;		/* System CPU time of dead children.  */
  };

#define times(t) (int)memset(t,0,sizeof(struct tms))

#define HZ 60
#define asctime_r( _tm, _buf ) \
        ( strcpy( (_buf), asctime( (_tm) ) ), \
          (_buf) )

#define ctime_r( _clock, _buf ) \
        ( strcpy( (_buf), ctime( (_tm) ) ),  \
          (_buf) )

#define gmtime_r( _clock, _result ) \
        ( *(_result) = *gmtime( (_clock) ), \
          (_result) )

#define localtime_r( _clock, _result ) \
        ( *(_result) = *localtime( (_clock) ), \
          (_result) )

#define rand_r( _seed ) \
        rand()

#define mrand48()  (long)rand()
#define drand48()  (double)rand()

/* not for cplusplus else problems with open and close of streams */
#ifndef  __cplusplus
#    define getcwd		_getcwd
#    define getpid		_getpid
#    define access		_access
#	 define W_OK		1
#	 define R_OK		1
#	 define F_OK		0
#    define open		_open
#    define read		_read
#    define write		_write
#    define lseek		_lseek
#    define close		_close
#    define pipe(phandles)	_pipe (phandles, 4096, _O_BINARY)
#    define popen		_popen
#    define pclose		_pclose
#    define fdopen		_fdopen
#endif /*__cplusplus */


#define	ENOTBLK 15	/* Block device required */
#define	ETXTBSY 26	/* Text file busy */
#define	ENOMSG 35	/* No message of desired type */
#define	EIDRM 45	/* Identifier removed */
#define	ECHRNG 37	/* Channel number out of range */
#define	EL2NSYNC 91	/* Level 2 not synchronized */
#define	EL3HLT 46	/* Level 3 halted */
#define	EL3RST 88	/* Level 3 reset */
#define	ELNRNG 90	/* Link number out of range */
#define	EUNATCH 56	/* Protocol driver not attached */
#define	ENOCSI 43	/* No CSI structure available */
#define	EL2HLT 44	/* Level 2 halted */
#define EBADE 50	/* Invalid exchange */
#define EBADR 51	/* Invalid request descriptor */
#define EXFULL 52	/* Exchange full */
#define ENOANO 53	/* No anode */
#define EBADRQC 54	/* Invalid request code */
#define EBADSLT 55	/* Invalid slot */
#define EBFONT 57	/* Bad font file fmt */
#define ENOSTR 60	/* Device not a stream */
#define ENODATA 61	/* No data (for no delay io) */
#define ETIME 62	/* Timer expired */
#define ENOSR 63	/* Out of streams resources */
#define ENONET 64	/* Machine is not on the network */
#define ENOPKG 65	/* Package not installed */
#define EREMOTE 66	/* The object is remote */
#define ENOLINK 67	/* The link has been severed */
#define EADV 68		/* Advertise error */
#define ESRMNT 69	/* Srmount error */
#define	ECOMM 70	/* Communication error on send */
#define EPROTO 71	/* Protocol error */
#define	EMULTIHOP 74	/* Multihop attempted */
#define	ELBIN 75	/* Inode is remote (not really error) */
#define	EDOTDOT 76	/* Cross mount point (not really error) */
#define EBADMSG 77	/* Trying to read unreadable message */
#define ENOTUNIQ 80	/* Given log. name not unique */
#define EBADFD 81	/* f.d. invalid for this operation */
#define EREMCHG 82	/* Remote address changed */
#define ELIBACC 83	/* Can't access a needed shared lib */
#define ELIBBAD 84	/* Accessing a corrupted shared lib */
#define ELIBSCN 85	/* .lib section in a.out corrupted */
#define ELIBMAX 86	/* Attempting to link in too many libs */
#define ELIBEXEC 87	/* Attempting to exec a shared library */
#define ENMFILE 89      /* No more files */
#define ELOOP 92	/* Too many symbolic links */
#define EOPNOTSUPP 95	/* Operation not supported on transport endpoint */
#define EPFNOSUPPORT 96 /* Protocol family not supported */
#define ECONNRESET 104  /* Connection reset by peer */
#define ENOBUFS 105	/* No buffer space available */
#define EAFNOSUPPORT 106
#define EPROTOTYPE 107
#define ENOTSOCK 108
#define ENOPROTOOPT 109
#define ESHUTDOWN 110
#define ECONNREFUSED 111	/* Connection refused */
#define EADDRINUSE 112		/* Address already in use */
#define ECONNABORTED 113	/* Connection aborted */
#define ENETUNREACH 114
#define ENETDOWN 115
#define ETIMEDOUT 116
#define EHOSTDOWN 117
#define EHOSTUNREACH 118
#define EINPROGRESS 119
#define EALREADY 120
#define EDESTADDRREQ 121
#define EMSGSIZE 122
#define EPROTONOSUPPORT 123
#define ESOCKTNOSUPPORT 124
#define EADDRNOTAVAIL 125
#define ENETRESET 126
#define EISCONN 127
#define ENOTCONN 128
#define ETOOMANYREFS 129
#define EPROCLIM 130
#define EUSERS 131
#define EDQUOT 132
#define ESTALE 133
#define ENOTSUP 134


#ifdef  __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#  endif /* _MSC_VER */
#endif	 /* NATIVE_WIN32 */


#endif /* WINLIBC_H */
