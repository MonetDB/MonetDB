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
#ifndef  __cplusplus
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

#define sbrk(x)	0

#define lockf(fd,locktype,nr) 0
#define F_TLOCK
#define F_ULOCK
#define F_LOCK

#define sleep(s)

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

#    define getcwd		_getcwd
#    define getpid		_getpid
#    define access		_access
#	 define R_OK		1
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
#    define ftruncate(fd, size)	win_ftruncate (fd, size)
#    define opendir		win_opendir
#    define readdir		win_readdir
#    define rewinddir		win_rewinddir
#    define closedir		win_closedir
#    define NAME_MAX 255
struct DIR
{
  char    	*dir_name;
  int       	just_opened;
  unsigned int  find_file_handle;
  char		*find_file_data;
};
typedef struct DIR DIR;
struct direct
{
  char  d_name[NAME_MAX + 1];
  int   d_namelen;
};
#ifdef  __cplusplus
extern "C" {
#endif

/* emulation functions */
extern int	win_ftruncate	(int		 f, unsigned int  size);
DIR*		win_opendir	(const char	*dirname);
struct direct*	win_readdir  	(DIR		*dir);
void		win_rewinddir 	(DIR		*dir);
int		win_closedir  	(DIR		*dir);

#ifdef __cplusplus
}
#endif

#  endif /* _MSC_VER */
#endif	 /* NATIVE_WIN32 */

#endif /* WINLIBC_H */
