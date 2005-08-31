/*
 * Copyright (c) 2004-2005 Sergey Lyubka <valenok@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include <pf_config.h>

#if ! HAVE_SOCKLEN_T
#define NO_SOCKLEN_T 1
#endif

/*
 * Simple and portable HTTP server, http://shttpd.sourceforge.net
 *
 * Compilation:
 *    SSL support:		-DWITH_SSL -lssl -lcrypto
 *    No CGI:			-DNO_CGI
 *    No Authorization:		-DNO_AUTH
 *    PUT,DELETE support:	-DWITH_PUT_AND_DELETE
 *    Override max request size	-DIO_MAX=xxxx
 *    Default config file:	-DCONFIG=\"/etc/shttpd.conf\"
 *    Typedef socklen_t		-DNO_SOCKLEN_T
 *    Use embedded:		-DEMBEDDED
 *    Use multithreading:	-DMT
 */

#define	VERSION		"1.26"		/* Version */
#ifndef CONFIG
#define	CONFIG		"shttpd.conf"	/* Configuration file */
#endif /* CONFIG */
#define	HTPASSWD	".htpasswd"	/* Per-directory passwords file */
#define	EXPIRE_TIME	30		/* Expiration time, seconds */
#ifndef IO_MAX
#define	IO_MAX		(1024*1024)    /* Max request size */
#endif /* IO_MAX */
#define	METHOD_MAX	15		/* Max HTTP method and protocol size */
#define	USER_MAX	64		/* Remote user name maxsize */
#define	AUTH_MAX	1024		/* Authorization line */
#define	NVAR_MAX	128		/* Maximum variables in request */

#define	NELEMS(ar)	(sizeof(ar) / sizeof(ar[0]))
#define	_STR(x)		#x
#define	_STR2(x)	_STR(x)

#ifdef _WIN32					/* Windows specific */
#ifndef __MINGW32__
#pragma comment(lib,"ws2_32")
#endif
#include <winsock.h>
#include <windows.h>
#include <process.h>
#include <direct.h>
#include <io.h>
#define	ERRNO			GetLastError()
typedef unsigned int		uint32_t;
typedef unsigned short		uint16_t;
typedef	DWORD			pthread_t;
#define	DIRSEP			'\\'
#define	O_NONBLOCK		0
#define	waitpid(a,b,c)		0
#define	EWOULDBLOCK		WSAEWOULDBLOCK
#define	snprintf		_snprintf
#define	vsnprintf		_vsnprintf
#define	getcwd			_getcwd
#define	mkdir			_mkdir
#define	setuid(a)		0
#define	pthread_create(a,b,c,d)	CreateThread(0, 0, c, d, 0, a)
#define	pthread_exit(x)		ExitThread(x)
#define	pthread_detach(x)	0
#define	_POSIX_

/* POSIX dirent interface */
struct dirent {
	char	*d_name;
};
typedef struct DIR {
	long			handle;
	struct _finddata_t	info;
	struct dirent		result;
	char			*name;
} DIR;

#else						/* UNIX specific */

#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>		/* Some linuxes put struct timeval there */

#include <pwd.h>
#include <unistd.h>
#include <dirent.h>
#define	DIRSEP			'/'
#define	O_BINARY		0
#define	closesocket(a)		close(a)
#define	ERRNO			errno
#define	WINAPI
#define	LPTHREAD_START_ROUTINE	void * (*) (void *)
#endif	/* _WIN32 */

/* Darwin prior to 7.0 and Win32 do not have socklen_t */
#ifdef NO_SOCKLEN_T
typedef int socklen_t;
#endif

#include <sys/types.h>				/* Common */
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <assert.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <stddef.h>
#include <fcntl.h>

#ifdef WITH_SSL
#include <openssl/ssl.h>
#endif /* WITH_SSL */

#ifdef WITH_DMALLOC
#include <dmalloc.h>
#endif /* WITH_DMALLOC */

/* Macros for doubly-linked circular linked lists. */
struct llhead {
	struct llhead *prev, *next;
};
#define	LL_INIT(N)	((N)->next = (N)->prev = (N))
#define LL_HEAD(H)	struct llhead H = { &H, &H }
#define	LL_ENTRY(P,T,N)	((T *)((char *)(P) - offsetof(T, N)))
#define	LL_TAIL(H, N)							\
	do {								\
		((H)->prev)->next = (N);				\
		(N)->prev = ((H)->prev);				\
		(N)->next = (H);					\
		(H)->prev = (N);					\
	} while (0)
#define	LL_DEL(N)							\
	do {								\
		((N)->next)->prev = ((N)->prev);			\
		((N)->prev)->next = ((N)->next);			\
		LL_INIT(N);						\
	} while (0)
#define	LL_FOREACH(H,N)	for (N = (H)->next; N != (H); N = (N)->next)
#define LL_FOREACH_SAFE(H,N,T)						\
	for (N = (H)->next, T = (N)->next; N != (H);			\
			N = (T), T = (N)->next)

/* Unified socket address */
struct usa {
	socklen_t			len;
	union {
		struct sockaddr		sa;
		struct sockaddr_in	sin;
	} u;
};

/* Mime type entry */
struct mimetype {
	struct mimetype	*next;
	char		*ext;			/* File extention */
	char		*mime;			/* Mime type */
	size_t		extlen;			/* Extention length */
};

/* Return values for getreqlen() function (finds HTTP request length) */
enum { REQ_INVALID = -1, REQ_INCOMPLETE = 0 };

/* Known HTTP methods */
enum { HTTP_GET, HTTP_POST, HTTP_PUT, HTTP_DELETE };

/* I/O buffer */
struct io {
	char	buf[IO_MAX];		/* Buffer */
	int	done;				/* IO finished */
	size_t	nread;				/* Bytes read */
	size_t	nwritten;			/* Bytes written */
};
#define	IO_SPACELEN(io)		(sizeof((io)->buf) - (io)->nread)
#define	IO_DATALEN(io)		((io)->nread - (io)->nwritten)

/* Connection descriptor */
struct conn {
	struct llhead	link;			/* Connections chain */
	struct usa	sa;			/* Remote socket address */
	time_t		expire;			/* Expiration time */
	time_t		ims;			/* If-Modified-Since: */
	int		sock;			/* Remote socket */
	int		reqlen;			/* Request length */
	unsigned	status;
	unsigned	http_method;		/* HTTP method */
	unsigned long	clength;		/* Content-Length */
	unsigned long	nsent;			/* Bytes sent to client */

	struct io	local;			/* Local IO buffer */
	struct io	remote;			/* Remote IO buffer */
	void (*io)(struct conn *);		/* Local IO function */
	
	char		method[METHOD_MAX + 1];	/* Used method */
	char		uri[IO_MAX + 1];	/* Requested URI */
	char		ouri[IO_MAX + 1];	/* Original unmodified URI */
	char		proto[METHOD_MAX + 1];	/* HTTP protocol  */

	char		*user;			/* Remote user name */
	char		*auth;			/* Authorization */
	char		*useragent;		/* User-Agent: */
	char		*path;			/* Path for get_dir */
	char		*referer;		/* Referer: */
	char		*cookie;		/* Cookie: */
	char		*ctype;			/* Content-Type: */
	char		*location;		/* Location: */
	char		*query;			/* QUERY_STRING */
	char		*range;			/* Range: */

	unsigned long	nposted;		/* Emb. POST bytes buffered*/
	void		*userurl;		/* For embedded data */
	char		*vars[NVAR_MAX];	/* Variables */

	int		rfd;			/* Opened read file descr */
	int		wfd;			/* Opened write file descr */
	int		cgisock;		/* Socket to CGI script */
	int		dummy[2];		/* Used to fake IO */
	struct stat	st;			/* Stats of requested file */
	DIR		*dirp;			/* Opened directory */

	/* Flags */
	char		parsed;			/* Request has been parsed */
	char		done;			/* Connection to be closed */
	char		cgiparsed;		/* CGI headers parsed */

#ifdef WITH_SSL
	int		sslaccepted;		/* Flag, was accepted or not */
	SSL		*ssl;			/* SSL descriptor */
#endif /* WITH_SSL */

#ifdef _WIN32
	HANDLE		hChild;			/* Spawned CGI process */
	int		childsock;		/* Child's socket */
#endif /* _WIN32 */
};


enum opt_type		{OPT_STR, OPT_INT};

/*
 * Configuration
 */
static struct opt {
	const char	*varname;	/* Option name in config file */
	int		set;		/* Option is set, don't apply default */
	const char	*flag;		/* Placeholder for cmd line flag */
	enum opt_type	type;		/* Option type, string or integer */
	union {
		char	*value_str;
		int	value_int;
	} value;			/* Option value */
	const char	*deflt;		/* Default option value */
} options[] = {
	{"document_root",	0, NULL, OPT_STR, {0}, NULL},
	{"index_files",		0, NULL, OPT_STR, {0}, "index.html,index.cgi"},
	{"list_directories",	0, NULL, OPT_INT, {0}, "0"},
	{"cgi_extention",	0, NULL, OPT_STR, {0}, ".cgi"},
	{"server_name",		0, NULL, OPT_STR, {0}, "mydomain.com"},
	{"listen_port",		0, NULL, OPT_INT, {0}, "80"},
	{"access_log",		0, NULL, OPT_STR, {0}, "access.log"},
	{"error_log",		0, NULL, OPT_STR, {0}, NULL},
	{"mime_types",		0, NULL, OPT_STR, {0}, NULL},
	{"global_htpasswd",	0, NULL, OPT_STR, {0}, NULL},
	{"debug",		0, NULL, OPT_INT, {0}, "0"},
	{"ssl_certificate",	0, NULL, OPT_STR, {0}, "shttpd.pem"},
	{"inetd_mode",		0, NULL, OPT_INT, {0}, "0"},
	{"runtime_uid",		0, NULL, OPT_STR, {0}, NULL},
	{"use_ssl",		0, NULL, OPT_INT, {0}, "0"}
};

/*
 * For easy access to configuration parameters
 * XXX this enum must be in sync with options array!
 */
enum {
	OPT_DOCROOT	= 0,
	OPT_INDEX	= 1,
	OPT_DIRLIST	= 2,
	OPT_CGIEXT	= 3,
	OPT_REALM	= 4,
	OPT_LISTENPORT	= 5,
	OPT_ACCESSLOG	= 6,
	OPT_ERRORLOG	= 7,
	OPT_MIMEFILE	= 8,
	OPT_HTPASSWD	= 9,
	OPT_DEBUG	= 10,
	OPT_SSLCERT	= 11,
	OPT_INETD	= 12,
	OPT_UID		= 13,
	OPT_USESSL	= 14
};

#define	STROPT(x)	(options[x].value.value_str)
#define	INTOPT(x)	(options[x].value.value_int)

/*
 * Globals variables
 */
#ifndef EMBEDDED
static const char	*config = CONFIG;	/* Configuration file */
#endif /* !EMBEDDED */
static int		quit;			/* Exit flag */
static FILE		*logfp;			/* Log stream */
static time_t		now;			/* Current time */
static int		nconns;			/* Max parallel connections */
static struct mimetype	*mimetypes;		/* Known mime types */
static LL_HEAD(conns);				/* List of connections */

#ifdef WITH_SSL
static SSL_CTX		*ctx;			/* SSL context */
#endif /* WITH_SSL */

/*
 * Prototypes
 */
static void	io_inc_nwritten(struct io *io, size_t n);
static void	copypath(const char *src, char *dst, size_t dstlen);
static void	elog(int fatal, const char *fmt, ...);
static int	casecmp(register const char *s1, register const char *s2);
static int	ncasecmp(register const char *, register const char *, size_t);
static char	*mystrdup(const char *str);
static int	myopen(const char *path, int flags, int mode);
static int	mystat(const char *path, struct stat *stp);
static int	mysocketpair(int sp[2]);
static int	setdummy(struct conn *c);
static void	disconnect(struct conn *c);
static int	writeremote(struct conn *c, const char *buf, size_t len);
static int	readremote(struct conn *c, char *buf, size_t len);
static int	nonblock(int fd);
static void	newconnection(int sock, struct usa *);
static void	errtofile(const char *filename);
static int	getreqlen(const char *buf, size_t buflen);
static void	accesslog(const struct conn *c, int status);
static void	senderr(struct conn *c, int status, const char *descr,
                	const char *headers, const char *fmt, ...);
static int	montoi(const char *s);
static time_t	datetosec(const char *s);
static void	get_dir(struct conn *c);
static void	do_dir(struct conn *c);
static void	get_file(struct conn *c);
static void	do_get(struct conn *c);
static void	urldecode(char *from, char *to);
static void	killdots(char *file);
static char	*fetch(const char *src, char *dst, size_t len);
static void	parseheaders(struct conn *c, char *s);
static int	useindex(struct conn *, char *path, size_t maxpath);
static void	decide(struct conn *c);
static void	addmimetype(const char *ext, const char *mime);
static void	freemime(void);
static void	setmimetypes(const char *file);
static void	setdefmimetypes(void);
static void	serve(struct conn *c, fd_set *rset, fd_set *wset);

#ifdef EMBEDDED

#include "shttpd.h"

/* List of registered URLs */
struct userurl {
	struct llhead		link;
	const char		*url;
	shttpd_callback_t	func;
	void			*data;
};

/* List of authorization files */
struct userauth {
	struct llhead		link;
	const char		*url;
	const char		*filename;
};

static LL_HEAD(urls);		/* List head of user defined URLs */
static LL_HEAD(auths);		/* List head of user defined auth files */

static struct userurl *
isregistered(const char *url)
{
	struct llhead	*lp;
	struct userurl	*p;

	LL_FOREACH(&urls, lp) {
		p = LL_ENTRY(lp, struct userurl, link);
		if (strcmp(p->url, url) == 0)
			return (p);
	}

	return (NULL);
}

#ifdef MT
#ifndef _WIN32
#include <pthread.h>
#endif /* _WIN32 */

struct thrarg {
	struct userurl			userurl;
	struct shttpd_callback_arg	arg;
};

/*
 * This function is run in dedicated thread.
 * It executes user-defined callback function, and exits.
 * Return value is discarded.
 */
static void * WINAPI
do_thread(struct thrarg *p)
{
	int	flags;
	
	/* Put client socket to blocking mode */
#ifdef _WIN32
	flags = 0;
	(void) ioctlsocket(p->arg.connection->sock, FIONBIO, &flags);
#else
	flags = fcntl(p->arg.connection->sock, F_GETFL);
	flags &= ~O_NONBLOCK;
	(void) fcntl(p->arg.connection->sock, F_SETFL, flags);
	(void) signal(SIGPIPE, SIG_IGN);
#endif /* _WIN32 */

	/* Call user function */
	p->userurl.func(&p->arg);

	/* Free up the resources */
	disconnect(p->arg.connection);
	free(p);

	pthread_exit(0);

	return (NULL);
}
#endif /* MT */

/*
 * The URI should be handled is user-registered callback function.
 * In MT scenario, call user-defined function in dedicated thread (or process)
 * and discard the return value.
 * In nonMT scenario, call the user function. Return value should is the
 * number of bytes copied to the local IO buffer. Mark local IO as done,
 * and shttpd will take care about passing data back to client.
 */
static void
do_embedded(struct conn *c)
{
	struct shttpd_callback_arg	arg;
	const struct userurl		*p = c->userurl;
	unsigned long			n;
#ifdef MT
	pthread_t			thr;
	struct thrarg			*param;
#endif /* MT */

	arg.connection		= c;
	arg.callback_data	= p->data;
	arg.buf			= c->local.buf;
	arg.buflen		= sizeof(c->local.buf);

	if (c->http_method == HTTP_POST) {
		/* For POST, we _need_ Content-Length */
		if (c->clength == 0) {
			senderr(c, 411, "Length Required", "", "");
		} else if (c->query == NULL) {
			/* Allocate POST buffer, and copy initial data to it */
			if ((c->query = malloc(c->clength + 1)) == NULL) {
				senderr(c, 413, "Too Large", "", "");
			} else {
				assert(c->remote.nread >= (size_t) c->reqlen);
				n = c->remote.nread - c->reqlen;
				if ((size_t) n > c->clength)
					n = c->clength;
				if (n > 0) {
					(void) memcpy(c->query,
					    c->remote.buf + c->reqlen, n);
					c->nposted += n;
				}
				c->remote.nread = c->remote.nwritten = 0;
			}
		}

		/* Buffer in POST data */
		n = IO_DATALEN(&c->remote);
		if (n > c->clength - c->nposted)
			n = c->clength - c->nposted;
		if (n > 0) {
			(void) memcpy(c->query + c->nposted,
			    c->remote.buf + c->remote.nwritten, n);
			c->nposted += n;
			io_inc_nwritten(&c->remote, n);
		}

		/* Return if not all POST data buffered */
		if (c->nposted < c->clength || c->clength == 0)
			return;
		
		/* Null-terminate query data */
		c->query[c->clength] = '\0';
	}

#ifdef MT
	/* Multi-Threaded scenario. Run dedicated thread for connection. */
	if ((param = malloc(sizeof(*param))) == NULL)
		return;
	param->userurl	= *p;
	param->arg	= arg;
	
	/* FIXME check return value */
	(void) pthread_create(&thr, 0,(LPTHREAD_START_ROUTINE)do_thread, param);
	(void) pthread_detach(thr);
	LL_DEL(&c->link);

#else
	
	/* Single-Threaded scenario. Run function and mark IO as done. */
	c->local.nread += p->func(&arg);
	if (c->local.nread > sizeof(c->local.buf))
		c->local.nread = sizeof(c->local.buf);
	c->local.done++;
	c->io = NULL;
#endif /* MT */
}

#endif /* EMBEDDED */

/*
 * Sane snprintf(). Acts like snprintf(), but never return -1 or the
 * value bigger than supplied buffer.
 * Thanks Adam Zeldis to pointing snprintf()-caused vulnerability
 * in his audit report.
 */
static int
Snprintf(char *buf, size_t buflen, const char *fmt, ...)
{
	va_list		ap;
	int		n;

	if (buflen == 0)
		return (0);

	va_start(ap, fmt);
	n = vsnprintf(buf, buflen, fmt, ap);
	va_end(ap);

	if (n < 0) {
		elog(0, "%s: snprintf returned -1, fmt [%s]", "Snprintf", fmt);
		n = 0;
	} else if (n > (int) buflen - 1) {
		elog(0, "%s: truncating from %d to %u [%s]",
		    "Snprintf", n, buflen - 1, buf);
		n = buflen - 1;
		buf[n] = '\0';
	}

	return (n);
}

/*
 * Increment nwritten counter. If it becomes == nread, flush both to 0
 */
static void
io_inc_nwritten(struct io *io, size_t n)
{
	assert(io->nwritten <= io->nread);
	assert(io->nread <= sizeof(io->buf));
	io->nwritten += n;
	assert(io->nwritten <= io->nread);
	if (io->nwritten == io->nread)
		io->nread = io->nwritten = 0;

}

/*
 * Change all occurences of '/' characters to OS-specific directory separator
 */
static void
copypath(const char *src, char *dst, size_t dstlen)
{
	char *end = dst + dstlen - 1;

	/* Copy the string, substituting the directory separator */
	for (; dst < end && *src != '\0'; src++, dst++)
#ifdef _WIN32
		*dst = *src == '/' ? '\\' : *src;
#else
		*dst = *src;
#endif /* _WIN32 */
	*dst = '\0';
}

#if (defined(_WIN32) && !defined(__GNUC__)) || defined(__MINGW32__)
/*
 * POSIX directory management (limited implementation, enough for shttpd)
 */
static DIR *
opendir(const char *name)
{
	DIR		*dir = NULL;
	size_t		base_length;
	const char	*all;

	if (name && name[0]) {
		base_length = strlen(name);
		all = strchr("/\\", name[base_length - 1]) ? "*" : "/*";

		if ((dir = malloc(sizeof *dir)) != NULL &&
		    (dir->name = malloc(base_length + strlen(all) + 1)) != 0) {
			(void) strcat(strcpy(dir->name, name), all);

			if ((dir->handle = (long) _findfirst(dir->name,
			    &dir->info)) != -1) {
				dir->result.d_name = 0;
			} else {
				free(dir->name);
				free(dir);
				dir = 0;
			}
		} else {
			free(dir);
			dir = NULL;
			errno = ENOMEM;
		}
	} else {
		errno = EINVAL;
	}

	return (dir);
}

static int
closedir(DIR *dir)
{
	int result = -1;

	if (dir) {
		if(dir->handle != -1)
			result = _findclose(dir->handle);

		free(dir->name);
		free(dir);
	}

	if (result == -1) 
		errno = EBADF;

	return (result);
}

static struct dirent *
readdir(DIR *dir)
{
	struct dirent *result = 0;

	if (dir && dir->handle != -1) {
		if(!dir->result.d_name ||
		    _findnext(dir->handle, &dir->info) != -1) {
			result = &dir->result;
			result->d_name = dir->info.name;
		}
	} else {
		errno = EBADF;
	}

	return (result);
}
#endif /* _WIN32 */

/*
 * Log function
 */
static void
elog(int fatal, const char *fmt, ...)
{
	va_list	ap;

	if (INTOPT(OPT_DEBUG) == 0 && fatal == 0)
		return;

	(void) fprintf(stderr, "%lu ", (unsigned long) now);

	va_start(ap, fmt);
	(void) vfprintf(stderr, fmt, ap);
	va_end(ap);

	(void) fputc('\n', stderr);
	(void) fflush(stderr);

	if (fatal)
		exit(EXIT_FAILURE);
}

/*
 * Case-insensitive string comparison, a-la strcmp()
 */
static int
casecmp(register const char *s1, register const char *s2)
{
	for (; *s1 != '\0' && *s2 != '\0'; s1++, s2++)
		if (tolower(*s1) != tolower(*s2))
			break;
	
	return (*s1 - *s2);
}

/* Case insensitive memory comparison, strncasecmp() */
static int
ncasecmp(register const char *s1, register const char *s2, size_t len)
{
	register const char	*e = s1 + len - 1;
	int			ret;
	
	for (; s1 < e && *s1 != '\0' && *s2 != '\0' &&
	    tolower(*s1) == tolower(*s2); s1++, s2++) ;
	ret = tolower(*s1) - tolower(*s2);

	return (ret);
}

/* strdup() is not standard, define it here */
static char *
mystrdup(const char *str)
{
	int	len;
	char	*p;

	len = strlen(str);
	if ((p = malloc(len + 1)) != NULL)
		(void) strcpy(p, str);

	return (p);
}

/*
 * Wrapper around open(), that takes care about directory separators
 */
static int
myopen(const char *path, int flags, int mode)
{
#ifdef _WIN32
	char	buf[FILENAME_MAX];
#endif /* _WIN32 */
	int	fd;
	
#ifdef _WIN32
	copypath(path, buf, sizeof(buf));
	fd = open(buf, flags, mode);
#else
	fd = open(path, flags, mode);
	(void) fcntl(fd, F_SETFD, FD_CLOEXEC);
#endif /* _WIN32 */
	
	return (fd);
}

/*
 * The wrapper around stat(), that takes care about directory separators
 */
static int
mystat(const char *path, struct stat *stp)
{
#ifdef _WIN32
	char	buf[FILENAME_MAX];
	int	len;
#endif /* _WIN32  */
	int	ret;
	
#ifdef _WIN32
	copypath(path, buf, sizeof(buf));
	len = strlen(buf);
	if (buf[len - 1] == '\\')
		buf[len - 1] = '\0';
	ret = stat(buf, stp);
#else
	ret = stat(path, stp);
#endif /* _WIN32 */

	return (ret);
}

/*
 * Create a pair of connected sockets, that can be used a duplex data channel.
 * Return 0 on success, -1 on error
 */
static int
mysocketpair(int sp[2])
{
	struct sockaddr_in	sa;
	int			sock, ret = -1, yes=1;
	socklen_t		len = sizeof(sa);

	(void) memset(&sa, 0, sizeof(sa));
	sa.sin_family 		= AF_INET;
	sa.sin_port		= htons(0);
	sa.sin_addr.s_addr	= htonl(INADDR_LOOPBACK);

	if ((sock = socket(AF_INET, SOCK_STREAM, 6)) == -1) {
		elog(0, "mysocketpair: socket(): %d", ERRNO);
	} else if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (void *) &yes, sizeof(int)) < 0) {
		/* get rid of the "Address already in use" error message */
		elog(0, "mysocketpair: setsockopt(): %d", ERRNO);
		(void) closesocket(sock);
	} else if (bind(sock, (struct sockaddr *) &sa, len) != 0) {
		elog(0, "mysocketpair: bind(): %d", ERRNO);
		(void) closesocket(sock);
	} else if (listen(sock, 1) != 0) {
		elog(0, "mysocketpair: listen(): %d", ERRNO);
		(void) closesocket(sock);
	} else if (getsockname(sock, (struct sockaddr *) &sa, &len) != 0) {
		elog(0, "mysocketpair: getsockname(): %d", ERRNO);
		(void) closesocket(sock);
	} else if ((sp[0] = socket(AF_INET, SOCK_STREAM, 6)) == -1) {
		elog(0, "mysocketpair: socket(): %d", ERRNO);
		(void) closesocket(sock);
	} else if (connect(sp[0], (struct sockaddr *) &sa, len) != 0) {
		elog(0, "mysocketpair: connect(): %d", ERRNO);
		(void) closesocket(sock);
		(void) closesocket(sp[0]);
	} else if ((sp[1] = accept(sock,(struct sockaddr *) &sa, &len)) == -1) {
		elog(0, "mysocketpair: accept(): %d", ERRNO);
		(void) closesocket(sock);
		(void) closesocket(sp[0]);
	} else {
		/* Success */
		ret = 0;
		(void) closesocket(sock);
	}

	return (ret);
}

/*
 * shttpd does select() to multiplex IO. But some channels do not
 * have opened file descriptors, like opened directory or user-defined
 * function. On win32 we cannot even pass regular file descriptor to
 * the select(). Therefore, we do the following:
 *   o in such a connection, create a socket pair.
 *   o write one byte of data into pair[0]
 *   o pass pair[0] to the select(), so this connection will be always
 *     marked 'active'.
 * Return values: 0 on success, -1 on error
 */
static int
setdummy(struct conn *c)
{
	int	ret = -1;

	if (mysocketpair(c->dummy) == 0) {
		(void) send(c->dummy[1], "", 1, 0);
		ret = 0;
	}

	return (ret);
}

/*
 * Disconnect from remote side, free resources
 */
static void
disconnect(struct conn *c)
{
	LL_DEL(&c->link);

	/* If parseheaders() allocated any data, free it */
	if (c->useragent)	free(c->useragent);
	if (c->user)		free(c->user);
	if (c->cookie)		free(c->cookie);
	if (c->ctype)		free(c->ctype);
	if (c->referer)		free(c->referer);
	if (c->location)	free(c->location);
	if (c->auth)		free(c->auth);
	if (c->path)		free(c->path);
	if (c->query)		free(c->query);
	if (c->range)		free(c->range);

	/* Free resources */
	if (c->dummy[0] != -1)	(void) closesocket(c->dummy[0]);
	if (c->dummy[1] != -1)	(void) closesocket(c->dummy[1]);
	if (c->rfd != -1)	(void) close(c->rfd);
	if (c->wfd != -1)	(void) close(c->wfd);
	if (c->cgisock != -1)	(void) closesocket(c->cgisock);
	if (c->dirp)		(void) closedir(c->dirp);

#ifdef WITH_SSL
	if (c->ssl)		SSL_free(c->ssl);
#endif /* WITH_SSL */
	
	(void) closesocket(c->sock);
	free(c);

	/* In inetd mode, exit if request is finished. */
	if (INTOPT(OPT_INETD))
		quit++;
}

#ifdef WITH_SSL
/*
 * Perform SSL handshake
 */
static void
handshake(struct conn *c)
{
	int	n;

	if ((n = SSL_accept(c->ssl)) == 0) {
		n = SSL_get_error(c->ssl, n);
		if (n != SSL_ERROR_WANT_READ && n != SSL_ERROR_WANT_WRITE)
			disconnect(c);
		elog(0, "handshake: SSL_accept error %d", n);
	} else {
		elog(0, "handshake: SSL accepted");
		c->sslaccepted = 1;
	}
}
#endif /* WITH_SSL */

/*
 * Send data to a remote end. Return bytes sent.
 */
static int
writeremote(struct conn *c, const char *buf, size_t len)
{
	int	n;

	/* Send the data via socket or SSL connection */
#ifdef WITH_SSL
	if (c->ssl)
		n = SSL_write(c->ssl, buf, len);
	else
#endif /* WITH_SSL */
		n = send(c->sock, buf, len, 0);
		
	if (n > 0)
		c->nsent += n;
	else if (n  == 0 || (n < 0 && ERRNO != EWOULDBLOCK) ||
	    c->nsent >= c->clength)
		c->remote.done = 1;
		
	return (n);
}

/*
 * Read data from the remote end. Return bytes read.
 */
static int
readremote(struct conn *c, char *buf, size_t len)
{
	int	n = -1;
#ifdef WITH_SSL
	if (c->ssl && c->sslaccepted == 0)
		handshake(c);
	else if (c->ssl)
		n = SSL_read(c->ssl, buf, len);
	else
#endif /* WITH_SSL*/
		n = recv(c->sock, buf, len, 0);
	
	if (n == 0 || (n < 0 && ERRNO != EWOULDBLOCK))
		c->remote.done = 1;

	return (n);
}

/*
 * Put given file descriptor in blocking (block == 1)
 * or non-blocking (block == 0) mode.
 * Return 0 if success, or -1
 */
static int
nonblock(int fd)
{
#ifdef	_WIN32
	unsigned long	on = 1;

	return (ioctlsocket(fd, FIONBIO, &on));
#else
	int	ret = -1;
	int	flags;

	if ((flags = fcntl(fd, F_GETFL, 0)) == -1)
		elog(0, "nonblock: fcntl(F_GETFL): %d", ERRNO);
	else if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0)
		elog(0, "nonblock: fcntl(F_SETFL): %d", ERRNO);
	else
		ret = 0;	/* Success */

	return (ret);
#endif /* _WIN32 */
}

/*
 * Setup listening socket on given port, return socket
 */
int
shttpd_open_port(int port)
{
	int		sock, on = 1;
	struct usa	sa;

	sa.len				= sizeof(sa.u.sin);
	sa.u.sin.sin_family		= AF_INET;
	sa.u.sin.sin_port		= htons((uint16_t) port);
	sa.u.sin.sin_addr.s_addr	= htonl(INADDR_ANY);

	if ((sock = socket(PF_INET, SOCK_STREAM, 6)) == -1)
		elog(1, "shttpd_open_port: socket: %s", strerror(ERRNO));
	else if (nonblock(sock) != 0)
		elog(1, "shttpd_open_port: nonblock");
	else if (setsockopt(sock, SOL_SOCKET,
	    SO_REUSEADDR,(void *) &on, sizeof(on)) != 0)
		elog(1, "shttpd_open_port: setsockopt");
	else if (bind(sock, &sa.u.sa, sa.len) < 0)
		elog(1, "shttpd_open_port: bind(%d): %s", port,strerror(ERRNO));
	else if (listen(sock, 128) != 0)
		elog(1, "shttpd_open_port: listen: %s", strerror(ERRNO));
#ifndef _WIN32
	(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif /* !_WIN32 */

	return (sock);
}

/*
 * newconnection new connection
 */
static void
newconnection(int sock, struct usa *usa) {
	struct conn	*c;
	struct usa	sa;
#ifdef WITH_SSL
	SSL		*ssl = NULL;
#endif /* WITH_SSL */
			
	sa.len = sizeof(sa.u.sin);

	if (nonblock(sock) != 0) {
		elog(0, "newconnection: nonblock: %s", strerror(ERRNO));
		(void) close(sock);
#ifdef WITH_SSL
	} else if (INTOPT(OPT_USESSL) && (ssl = SSL_new(ctx)) == NULL) {
		elog(0, "newconnection: SSL_new: %s", strerror(ERRNO));
		(void) close(sock);
	} else if (INTOPT(OPT_USESSL) && SSL_set_fd(ssl, sock) == 0) {
		elog(0, "newconnection: SSL_set_fd: %s", strerror(ERRNO));
		(void) close(sock);
		SSL_free(ssl);
#endif /* WITH_SSL */
	} else if ((c = calloc(1, sizeof(*c))) == NULL) {
		(void) close(sock);
		elog(0, "newconnection: calloc: %s", strerror(ERRNO));
	} else {
		LL_TAIL(&conns, &c->link);
		if (usa) {
			c->sa = *usa;
		} else {
			(void) getpeername(sock, &sa.u.sa, &sa.len);
			c->sa = sa;
		}
		c->sock = sock;
		
		c->cgisock = c->rfd = c->wfd = c->dummy[0] = c->dummy[1] = -1;
#ifndef _WIN32
		(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif /* _WIN32 */

#ifdef WITH_SSL
		c->ssl = ssl;
		if (ssl)
			handshake(c);
#endif /* WITH_SSL */
		
		c->expire = now + EXPIRE_TIME;
		elog(0, "%p %d connected.", c, sock);
	}
}

/*
 * Redirect error log to a file
 */
static void
errtofile(const char *filename)
{
	int fd;
	
	if ((fd = myopen(filename, O_WRONLY | O_APPEND | O_CREAT, 0666)) == -1)
		elog(1, "open(%s): %s", STROPT(OPT_ERRORLOG), strerror(ERRNO));

	(void) dup2(fd, 2);
	(void) close(fd);
}

/*
 * Check whether full request is buffered
 * Return request length, or 0
 */
static int
getreqlen(const char *buf, size_t buflen)
{
	const char	*s, *e;
	int		len = REQ_INCOMPLETE;

	for (s = buf, e = s + buflen - 1; len == REQ_INCOMPLETE && s < e; s++)
		if (!isprint(*(unsigned char *) s) && *s != '\r' && *s != '\n')
			len = REQ_INVALID;
		else if (s[0] == '\n' && s[1] == '\n')
			len = s - buf + 2;
		else if (s[0] == '\n' && &s[1] < e &&
		    s[1] == '\r' && s[2] == '\n')
			len = s - buf + 3;

	return (len);
}

/*
 * Write an HTTP access log into a file `logfile'
 */
static void
accesslog(const struct conn *c, int status)
{
	char	date[64];

	if (logfp == NULL)
		return;
	
	(void) strftime(date, sizeof(date),"%d/%b/%Y %H:%M:%S",localtime(&now));
	(void) fprintf(logfp, "%s - %s [%s] \"%s %s %s\" %d\n",
	    inet_ntoa(c->sa.u.sin.sin_addr), c->user ? c->user : "-",
	    date, c->method, c->ouri, c->proto, status);
	(void) fflush(logfp);
}

/*
 * Send an error back to a client.
 */
static void
senderr(struct conn *c, int status, const char *descr,
		const char *headers, const char *fmt, ...)
{
	va_list	ap;
	char	msg[sizeof(c->local.buf)];
	int	n;

	n = Snprintf(msg, sizeof(msg),
	    "HTTP/1.1 %d %s\r\n%s%s\r\n%d ",
	    status, descr, headers, headers[0] == '\0' ? "" : "\r\n", status);
	va_start(ap, fmt);
	n += vsnprintf(msg + n, sizeof(msg) - n, fmt, ap);
	if (n > (int) sizeof(msg))
		n = sizeof(msg);
	va_end(ap);
	(void) strncpy(c->local.buf, msg, sizeof(c->local.buf));
	c->local.nread = n;
	c->local.nwritten = 0;
	elog(0, "%s: [%s]", "senderr", c->local.buf);
	accesslog(c, status);
	c->local.done++;
}

/*
 * Convert month to the month number. Return -1 on error, or month number
 */
static int
montoi(const char *s)
{
	static const char *ar[] = {
		"Jan", "Feb", "Mar", "Apr", "May", "Jun",
		"Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
	};
	size_t	i;
	
	for (i = 0; i < NELEMS(ar); i++)
		if (casecmp(s, ar[i]) == 0)
			return (i);
			
	return (-1);
}

/*
 * Parse date-time string, and return the corresponding time_t value
 */
static time_t
datetosec(const char *s)
{
	struct tm	tm;
	char		mon[32];
	int		sec, min, hour, mday, month, year;

	(void) memset(&tm, 0, sizeof(tm));
	tm.tm_isdst = 1;	/* Daylight saving time is in effect */
	sec = min = hour = mday = month = year = 0;

	if (((sscanf(s, "%d/%3s/%d %d:%d:%d",
	    &mday, mon, &year, &hour, &min, &sec) == 6) ||
	    (sscanf(s, "%d %3s %d %d:%d:%d",
	    &mday, mon, &year, &hour, &min, &sec) == 6) ||
	    (sscanf(s, "%*3s, %d %3s %d %d:%d:%d",
	    &mday, mon, &year, &hour, &min, &sec) == 6) ||
	    (sscanf(s, "%d-%3s-%d %d:%d:%d",
	    &mday, mon, &year, &hour, &min, &sec) == 6)) &&
	    (month = montoi(mon)) != -1) {
		tm.tm_mday	= mday;
		tm.tm_mon	= month;
		tm.tm_year	= year;
		tm.tm_hour	= hour;
		tm.tm_min	= min;
		tm.tm_sec	= sec;
	}
	
	if (tm.tm_year > 1900)
		tm.tm_year -= 1900;
	else if (tm.tm_year < 70)
		tm.tm_year += 100;

	elog(0, "datetosec: \"%s\" == %u (mday %d, mon %d, year %d,"
	    "hour %d, min %d, sec %d", s, mktime(&tm), mday, month,
	    year, hour, min, sec);

	return (mktime(&tm));
}

#ifdef WITH_PUT_AND_DELETE
/*
 * For a given PUT path, create all intermediate subdirectories
 * for given path. Return 0 if the path itself is a directory,
 * or -1 on error, 1 if OK.
 */
static int
put_dir(const char *path)
{
	char		buf[FILENAME_MAX];
	const char	*s, *p;
	struct stat	st;
	size_t		len;

	for (s = p = path + 2; (p = strchr(s, '/')) != NULL; s = ++p) {
		len = p - path;
		assert(len < sizeof(buf));
		(void) memcpy(buf, path, len);
		buf[len] = '\0';

		/* Try to create intermediate directory */
		if (mystat(buf, &st) == -1 && mkdir(buf, 0755) != 0)
			return (-1);

		/* Is path itself a directory ? */
		if (p[1] == '\0')
			return (0);
	}

	return (1);
}

/*
 * PUT request
 */
static void
put_file(struct conn *c)
{
	int	n, len;

	assert(c->wfd != -1);
	if ((len = IO_DATALEN(&c->remote)) <= 0)
		return;

	n = write(c->wfd, c->remote.buf + c->remote.nwritten, len);
	elog(0, "put_file(%p): %d bytes", c, n);
	
	if (n > 0) {
		io_inc_nwritten(&c->remote, n);
	} else {
		c->local.done++;
		c->io = NULL;
	}
}
#endif /* WITH_PUT_AND_DELETE */

/*
 * GET the directory
 */
static void
get_dir(struct conn *c)
{
	struct dirent	*dp = NULL;
	char		file[FILENAME_MAX], line[FILENAME_MAX + 512],
				size[64], mod[64];
	const char	*slash;
	struct stat	st;
	int		n, left;

	assert(c->dirp != NULL);
	assert(c->uri[0] != '\0');

	left = IO_SPACELEN(&c->local);
	slash = c->uri[strlen(c->uri) - 1] == '/' ? "" : "/";

	do {
		if (left < (int) sizeof(line))
			break;

		if ((dp = readdir(c->dirp)) == NULL) {
			/* Finished reading directory */
			c->local.done++;
			c->io = NULL;
			break;
		}
		
		/* Do not show current dir and passwords file */
		if (strcmp(dp->d_name, ".") == 0 ||
		   strcmp(dp->d_name, HTPASSWD) == 0)
			continue;
		
		(void) snprintf(file, sizeof(file),
		    "%s%s%s",c->path, slash, dp->d_name);
		(void) mystat(file, &st);
		if (S_ISDIR(st.st_mode))
			snprintf(size,sizeof(size),"        %s","&lt;DIR&gt;");
		else
			(void) snprintf(size, sizeof(size),"%10.2f kB",
			    (float) st.st_size / 1024);
		(void) strftime(mod, sizeof(mod),
		    "%d-%b-%Y %H:%M", localtime(&st.st_mtime));

		n = Snprintf(line, sizeof(line),
		    "<tr><td><a href=\"%s%s%s\">%s%s</a></td>"
		    "<td>&nbsp;&nbsp;%s</td>"
		    "<td><pre>%s</pre></td></tr>\n",
		    c->uri, slash, dp->d_name, dp->d_name,
		    S_ISDIR(st.st_mode) ? "/" : "", mod, size);
		(void) memcpy(c->local.buf + c->local.nread, line, n);
		c->local.nread += n;
		left -= n;
	} while (dp != NULL);
}

/*
 * Schedule GET for the directory
 */
static void
do_dir(struct conn *c)
{
	elog(0, "do_dir");

	if ((c->dirp = opendir(c->path)) == NULL) {
		accesslog(c, 500);
		senderr(c, 500, "Error","", "Cannot open dir");
	} else if (setdummy(c) != 0) {
		accesslog(c, 500);
		senderr(c, 500, "Error","", "Cannot open dir (dummy)");
	} else {
		c->local.nread = Snprintf(c->local.buf, sizeof(c->local.buf),
		    "HTTP/1.1 200 OK\r\n"
		    "Content-Type: text/html\r\n"
		    "\r\n"
		    "<html><head><title>Index of %s</title></head>"
		    "<body><h1>Index of %s</h1><pre><table>"
		    "<tr><th align=\"left\">Name</th>"
		    "<th align=\"left\">&nbsp;&nbsp;Modified</th>"
		    "<th align=\"right\">Size</th></tr>",
		    c->uri, c->uri);
		c->io = get_dir;
		c->remote.nread = 0;
		accesslog(c, 200);
	}
}

/*
 * GET regular file
 */
static void
get_file(struct conn *c)
{
	int	n;

	assert(c->rfd != -1);
	n = read(c->rfd, c->local.buf + c->local.nread, IO_SPACELEN(&c->local));
	
	if (n > 0) {
		c->local.nread += n;
	} else {
		c->local.done++;
		c->io = NULL;
	}
}

/*
 * Schedule GET for regular file
 */
static void
do_get(struct conn *c)
{
	char		date[64], lm[64], etag[64], range[64] = "";
	int		n, status = 200;
	unsigned long	r1, r2;
	const char	*mime = "text/plain", *msg = "OK";
	const char	*fmt, *s = c->uri + strlen(c->uri);
	struct mimetype	*p;

#ifdef _WIN32
	if (setdummy(c) != 0) {
		senderr(c, 500, "Error", "", "Error opening dummy file");
		return;
	}
#endif /* _WIN32 */
	
	/* Figure out the mime type */
	for (p = mimetypes; p != NULL; p = p->next)
		if (strlen(c->uri) > p->extlen &&
		    *(s - p->extlen - 1) == '.' &&
		    !ncasecmp(p->ext, s - p->extlen, p->extlen)) {
			mime = p->mime;
			break;
		}

	c->clength = (unsigned long) c->st.st_size;

	/* If Range: header specified, act accordingly */
	if (c->range && (n = sscanf(c->range, "bytes=%lu-%lu", &r1, &r2)) > 0) {
		status = 206;
		(void) lseek(c->rfd, r1, SEEK_SET);
		c->clength = n == 2 ? r2 - r1 : c->clength - r1;
		(void) Snprintf(range, sizeof(range),
		    "Content-Range: bytes %lu-%lu/%lu\r\n",
		    r1, r1 + c->clength, (unsigned long) c->st.st_size);
		msg = "Partial Content";
	}

	/* Prepare Etag, Date, Last-Modified headers */
	fmt = "%a, %d %b %Y %H:%M:%S GMT";
	(void) strftime(date, sizeof(date), fmt, localtime(&now));
	(void) strftime(lm, sizeof(lm), fmt, localtime(&c->st.st_mtime));
	(void) snprintf(etag, sizeof(etag), "%lx.%lx",
	    (unsigned long) c->st.st_mtime, (unsigned long) c->st.st_size);

	/* Local read buffer should be empty */
	c->local.nread = Snprintf(c->local.buf, sizeof(c->local.buf),
	    "HTTP/1.1 %d %s\r\n"
	    "Connection: close\r\n"
	    "Date: %s\r\n"
	    "Last-Modified: %s\r\n"
	    "Content-Type: %s\r\n"
	    "Content-Length: %lu\r\n"
	    "%s"
	    "\r\n", status, msg, date, lm, mime, c->clength, range);
	c->clength += c->local.nread;
	accesslog(c, status);
	elog(0, "get_file: [%s]", c->local.buf);
	c->io = get_file;
	c->remote.nread = 0;
}

/*
 * Decode urlencoded string. from and to may point to the same location
 */
static void
urldecode(char *from, char *to)
{
	int	i, a, b;
#define	HEXTOI(x)  (isdigit(x) ? x - '0' : x - 'W')

	for (i = 0; *from != '\0'; i++, from++) {
		if (from[0] == '%' &&
		    isxdigit((unsigned char) from[1]) &&
		    isxdigit((unsigned char) from[2])) {
			a = tolower(from[1]);
			b = tolower(from[2]);
			to[i] = (HEXTOI(a) << 4) | HEXTOI(b);
			from += 2;
		} else if (from[0] == '+') {
			to[i] = ' ';
		} else {
			to[i] = from[0];
		}
	}
	to[i] = '\0';
}

/*
 * Protect from ..
 */
static void
killdots(char *file)
{
	char	good[FILENAME_MAX], *s;
	size_t	i;

#define	BADCHAR(x)	((x) == '.')

	/* Get rid of dots */
	for (s = file, i = 0; *s != '\0'; s++) {
		good[i++] = *s;
		while (BADCHAR(s[0]) && BADCHAR(s[1]))
			s++;
	}
	assert(i < sizeof(good));
	good[i] = '\0';
	(void) strcpy(file, good);
}

/*
 * Fetch header value into specified destination
 */
static char *
fetch(const char *src, char *dst, size_t len)
{
	const char	*p;

	*dst = '\0';
	if ((p = strchr(src, '\r')) || (p = strchr(src, '\n'))) {
		len--;	/* For terminating '\0' */
		if (p - src < (int) len)
			len = p - src;
		(void) memcpy(dst, src, len);
		dst[len] = '\0';
	}

	return (dst);
}

/*
 * Parse HTTP headers, filling values in struct conn
 */
static void
parseheaders(struct conn *c, char *s)
{
	char	val[IO_MAX];

	while (s != NULL && s[0] != '\n' && s[0] != '\r') {
		if (ncasecmp(s, "User-Agent: ", 12) == 0)
			c->useragent = mystrdup(fetch(s + 12, val,sizeof(val)));
		else if (ncasecmp(s, "Content-Type: ", 14) == 0)
			c->ctype = mystrdup(fetch(s + 14, val, sizeof(val)));
		else if (ncasecmp(s, "Content-Length: ", 16) == 0)
			c->clength = strtoul(fetch(s + 15,
			    val, sizeof(val)), NULL, 10);
		else if (ncasecmp(s, "If-Modified-Since: ", 19) == 0)
			c->ims = datetosec(fetch(s + 19, val, sizeof(val)));
		else if (ncasecmp(s, "Authorization: ", 15) == 0)
			c->auth = mystrdup(fetch(s + 15, val, sizeof(val)));
		else if (ncasecmp(s, "Referer: ", 9) == 0)
			c->referer = mystrdup(fetch(s + 9, val, sizeof(val)));
		else if (ncasecmp(s, "Cookie: ", 8) == 0)
			c->cookie = mystrdup(fetch(s + 8, val, sizeof(val)));
		else if (ncasecmp(s, "Location: ", 10) == 0)
			c->location = mystrdup(fetch(s + 10, val, sizeof(val)));
		else if (ncasecmp(s, "Status: ", 8) == 0)
			c->status = strtoul(fetch(s+8, val, sizeof(val)),0,10);
		else if (ncasecmp(s, "Range: ", 7) == 0)
			c->range = mystrdup(fetch(s + 7, val, sizeof(val)));
	
		if ((s = strchr(s, '\n')) != NULL)
			s++;
	}
}

#ifndef NO_AUTH
/*
 * MD5 crypto algorithm.
 */
typedef struct _MD5Context {
	uint32_t	buf[4];
	uint32_t	bits[2];
	unsigned char	in[64];
} _MD5_CTX;

#if __BYTE_ORDER == 1234
#define byteReverse(buf, len)	/* Nothing */
#else
/*
 * Note: this code is harmless on little-endian machines.
 */
static void byteReverse(unsigned char *buf, unsigned longs)
{
	uint32_t t;
	do {
		t = (uint32_t) ((unsigned) buf[3] << 8 | buf[2]) << 16 |
			((unsigned) buf[1] << 8 | buf[0]);
		*(uint32_t *) buf = t;
		buf += 4;
	} while (--longs);
}
#endif /* __BYTE_ORDER */


/* The four core functions - F1 is optimized somewhat */

/* #define F1(x, y, z) (x & y | ~x & z) */
#define F1(x, y, z) (z ^ (x & (y ^ z)))
#define F2(x, y, z) F1(z, x, y)
#define F3(x, y, z) (x ^ y ^ z)
#define F4(x, y, z) (y ^ (x | ~z))

/* This is the central step in the MD5 algorithm. */
#define MD5STEP(f, w, x, y, z, data, s) \
( w += f(x, y, z) + data,  w = w<<s | w>>(32-s),  w += x )

/*
 * Start MD5 accumulation.  Set bit count to 0 and buffer to mysterious
 * initialization constants.
 */
static void _MD5Init(_MD5_CTX *ctx)
{
	ctx->buf[0] = 0x67452301;
	ctx->buf[1] = 0xefcdab89;
	ctx->buf[2] = 0x98badcfe;
	ctx->buf[3] = 0x10325476;

	ctx->bits[0] = 0;
	ctx->bits[1] = 0;
}


/*
 * The core of the MD5 algorithm, this alters an existing MD5 hash to
 * reflect the addition of 16 longwords of new data.  MD5Update blocks
 * the data and converts bytes into longwords for this routine.
 */
static void _MD5Transform(uint32_t buf[4], uint32_t const in[16])
{
	register uint32_t a, b, c, d;

	a = buf[0];
	b = buf[1];
	c = buf[2];
	d = buf[3];

	MD5STEP(F1, a, b, c, d, in[0] + 0xd76aa478, 7);
	MD5STEP(F1, d, a, b, c, in[1] + 0xe8c7b756, 12);
	MD5STEP(F1, c, d, a, b, in[2] + 0x242070db, 17);
	MD5STEP(F1, b, c, d, a, in[3] + 0xc1bdceee, 22);
	MD5STEP(F1, a, b, c, d, in[4] + 0xf57c0faf, 7);
	MD5STEP(F1, d, a, b, c, in[5] + 0x4787c62a, 12);
	MD5STEP(F1, c, d, a, b, in[6] + 0xa8304613, 17);
	MD5STEP(F1, b, c, d, a, in[7] + 0xfd469501, 22);
	MD5STEP(F1, a, b, c, d, in[8] + 0x698098d8, 7);
	MD5STEP(F1, d, a, b, c, in[9] + 0x8b44f7af, 12);
	MD5STEP(F1, c, d, a, b, in[10] + 0xffff5bb1, 17);
	MD5STEP(F1, b, c, d, a, in[11] + 0x895cd7be, 22);
	MD5STEP(F1, a, b, c, d, in[12] + 0x6b901122, 7);
	MD5STEP(F1, d, a, b, c, in[13] + 0xfd987193, 12);
	MD5STEP(F1, c, d, a, b, in[14] + 0xa679438e, 17);
	MD5STEP(F1, b, c, d, a, in[15] + 0x49b40821, 22);

	MD5STEP(F2, a, b, c, d, in[1] + 0xf61e2562, 5);
	MD5STEP(F2, d, a, b, c, in[6] + 0xc040b340, 9);
	MD5STEP(F2, c, d, a, b, in[11] + 0x265e5a51, 14);
	MD5STEP(F2, b, c, d, a, in[0] + 0xe9b6c7aa, 20);
	MD5STEP(F2, a, b, c, d, in[5] + 0xd62f105d, 5);
	MD5STEP(F2, d, a, b, c, in[10] + 0x02441453, 9);
	MD5STEP(F2, c, d, a, b, in[15] + 0xd8a1e681, 14);
	MD5STEP(F2, b, c, d, a, in[4] + 0xe7d3fbc8, 20);
	MD5STEP(F2, a, b, c, d, in[9] + 0x21e1cde6, 5);
	MD5STEP(F2, d, a, b, c, in[14] + 0xc33707d6, 9);
	MD5STEP(F2, c, d, a, b, in[3] + 0xf4d50d87, 14);
	MD5STEP(F2, b, c, d, a, in[8] + 0x455a14ed, 20);
	MD5STEP(F2, a, b, c, d, in[13] + 0xa9e3e905, 5);
	MD5STEP(F2, d, a, b, c, in[2] + 0xfcefa3f8, 9);
	MD5STEP(F2, c, d, a, b, in[7] + 0x676f02d9, 14);
	MD5STEP(F2, b, c, d, a, in[12] + 0x8d2a4c8a, 20);

	MD5STEP(F3, a, b, c, d, in[5] + 0xfffa3942, 4);
	MD5STEP(F3, d, a, b, c, in[8] + 0x8771f681, 11);
	MD5STEP(F3, c, d, a, b, in[11] + 0x6d9d6122, 16);
	MD5STEP(F3, b, c, d, a, in[14] + 0xfde5380c, 23);
	MD5STEP(F3, a, b, c, d, in[1] + 0xa4beea44, 4);
	MD5STEP(F3, d, a, b, c, in[4] + 0x4bdecfa9, 11);
	MD5STEP(F3, c, d, a, b, in[7] + 0xf6bb4b60, 16);
	MD5STEP(F3, b, c, d, a, in[10] + 0xbebfbc70, 23);
	MD5STEP(F3, a, b, c, d, in[13] + 0x289b7ec6, 4);
	MD5STEP(F3, d, a, b, c, in[0] + 0xeaa127fa, 11);
	MD5STEP(F3, c, d, a, b, in[3] + 0xd4ef3085, 16);
	MD5STEP(F3, b, c, d, a, in[6] + 0x04881d05, 23);
	MD5STEP(F3, a, b, c, d, in[9] + 0xd9d4d039, 4);
	MD5STEP(F3, d, a, b, c, in[12] + 0xe6db99e5, 11);
	MD5STEP(F3, c, d, a, b, in[15] + 0x1fa27cf8, 16);
	MD5STEP(F3, b, c, d, a, in[2] + 0xc4ac5665, 23);

	MD5STEP(F4, a, b, c, d, in[0] + 0xf4292244, 6);
	MD5STEP(F4, d, a, b, c, in[7] + 0x432aff97, 10);
	MD5STEP(F4, c, d, a, b, in[14] + 0xab9423a7, 15);
	MD5STEP(F4, b, c, d, a, in[5] + 0xfc93a039, 21);
	MD5STEP(F4, a, b, c, d, in[12] + 0x655b59c3, 6);
	MD5STEP(F4, d, a, b, c, in[3] + 0x8f0ccc92, 10);
	MD5STEP(F4, c, d, a, b, in[10] + 0xffeff47d, 15);
	MD5STEP(F4, b, c, d, a, in[1] + 0x85845dd1, 21);
	MD5STEP(F4, a, b, c, d, in[8] + 0x6fa87e4f, 6);
	MD5STEP(F4, d, a, b, c, in[15] + 0xfe2ce6e0, 10);
	MD5STEP(F4, c, d, a, b, in[6] + 0xa3014314, 15);
	MD5STEP(F4, b, c, d, a, in[13] + 0x4e0811a1, 21);
	MD5STEP(F4, a, b, c, d, in[4] + 0xf7537e82, 6);
	MD5STEP(F4, d, a, b, c, in[11] + 0xbd3af235, 10);
	MD5STEP(F4, c, d, a, b, in[2] + 0x2ad7d2bb, 15);
	MD5STEP(F4, b, c, d, a, in[9] + 0xeb86d391, 21);

	buf[0] += a;
	buf[1] += b;
	buf[2] += c;
	buf[3] += d;
}

/*
 * Update context to reflect the concatenation of another buffer full
 * of bytes.
 */
static void
_MD5Update(_MD5_CTX *ctx, unsigned char const *buf, unsigned len)
{
	uint32_t t;

	/* Update bitcount */

	t = ctx->bits[0];
	if ((ctx->bits[0] = t + ((uint32_t) len << 3)) < t)
		ctx->bits[1]++;		/* Carry from low to high */
	ctx->bits[1] += len >> 29;

	t = (t >> 3) & 0x3f;	/* Bytes already in shsInfo->data */

	/* Handle any leading odd-sized chunks */

	if (t) {
		unsigned char *p = (unsigned char *) ctx->in + t;

		t = 64 - t;
		if (len < t) {
			memcpy(p, buf, len);
			return;
		}
		memcpy(p, buf, t);
		byteReverse(ctx->in, 16);
		_MD5Transform(ctx->buf, (uint32_t *) ctx->in);
		buf += t;
		len -= t;
	}
	/* Process data in 64-byte chunks */

	while (len >= 64) {
		memcpy(ctx->in, buf, 64);
		byteReverse(ctx->in, 16);
		_MD5Transform(ctx->buf, (uint32_t *) ctx->in);
		buf += 64;
		len -= 64;
	}

	/* Handle any remaining bytes of data. */

	memcpy(ctx->in, buf, len);
}

/*
 * Final wrapup - pad to 64-byte boundary with the bit pattern 
 * 1 0* (64-bit count of bits processed, MSB-first)
 */
static void
_MD5Final(unsigned char digest[16], _MD5_CTX *ctx)
{
	unsigned count;
	unsigned char *p;

	/* Compute number of bytes mod 64 */
	count = (ctx->bits[0] >> 3) & 0x3F;

	/* Set the first char of padding to 0x80.  This is safe since there is
	   always at least one byte free */
	p = ctx->in + count;
	*p++ = 0x80;

	/* Bytes of padding needed to make 64 bytes */
	count = 64 - 1 - count;

	/* Pad out to 56 mod 64 */
	if (count < 8) {
		/* Two lots of padding:  Pad the first block to 64 bytes */
		memset(p, 0, count);
		byteReverse(ctx->in, 16);
		_MD5Transform(ctx->buf, (uint32_t *) ctx->in);

		/* Now fill the next block with 56 bytes */
		memset(ctx->in, 0, 56);
	} else {
		/* Pad block to 56 bytes */
		memset(p, 0, count - 8);
	}
	byteReverse(ctx->in, 14);

	/* Append length in bits and transform */
	((uint32_t *) ctx->in)[14] = ctx->bits[0];
	((uint32_t *) ctx->in)[15] = ctx->bits[1];

	_MD5Transform(ctx->buf, (uint32_t *) ctx->in);
	byteReverse((unsigned char *) ctx->buf, 4);
	memcpy(digest, ctx->buf, 16);
	memset((char *) ctx, 0, sizeof(ctx));	/* In case it's sensitive */
}

/*
 * Stringify binary data. Output buffer must be twice as big as input,
 * because each byte takes 2 bytes in string representation
 */
static void
bin2str(char *to, const unsigned char *p, size_t len)
{
	const char	*hex = "0123456789abcdef";
	
	for (;len--; p++) {
		*to++ = hex[p[0] >> 4];
		*to++ = hex[p[0] & 0x0f];
		
	}
	*to = '\0';
}

/*
 * Return stringified MD5 hash for list of vectors.
 * buf must point to at least 32-bytes long buffer
 */
static void
md5(char *buf, ...)
{
	unsigned char		hash[16];
	const unsigned char	*p;
	va_list			ap;
	_MD5_CTX		ctx;

	_MD5Init(&ctx);

	va_start(ap, buf);
	while ((p = va_arg(ap, const unsigned char *)) != NULL)
		_MD5Update(&ctx, p, strlen((char *) p));
	va_end(ap);

	_MD5Final(hash, &ctx);
	bin2str(buf, hash, sizeof(hash));
}

#ifndef EMBEDDED
/*
 * Edit the passwords file.
 */
static int
editpass(const char *fname, const char *domain,
		const char *user, const char *pass)
{

#define	LSIZ		512
	int		ret = EXIT_SUCCESS, found = 0;
	char		line[LSIZ], tmp[FILENAME_MAX],
				u[LSIZ], d[LSIZ], ha1[LSIZ];
	FILE		*fp = NULL, *fp2 = NULL;

	(void) snprintf(tmp, sizeof(tmp), "%s.tmp", fname);

	/* Create the file if does not exist */
	if ((fp = fopen(fname, "a+")))
		(void) fclose(fp);

	/* Open the given file and temporary file */
	if ((fp = fopen(fname, "r")) == NULL)
		elog(1, "Cannot open %s: %s", fname, strerror(errno));
	else if ((fp2 = fopen(tmp, "w+")) == NULL)
		elog(1, "Cannot open %s: %s", tmp, strerror(errno));

	/* Copy the stuff to temporary file */
	while (fgets(line, sizeof(line), fp) != NULL) {
		if (sscanf(line, "%[^:]:%[^:]:%s", u, d, ha1) == 3 &&
		    strcmp(user, u) == 0 &&
		    strcmp(domain, d) == 0) {
			found++;
			md5(ha1, user, ":", domain, ":", pass, NULL);
			(void) snprintf(line, sizeof(line),
			    "%s:%s:%s\n", user, domain, ha1);
		}
		(void) fprintf(fp2, "%s", line);
	}

	/* If new user, just add it */
	if (found == 0) {
		md5(ha1, user, ":", domain, ":", pass, NULL);
		(void) snprintf(line, sizeof(line),
		    "%s:%s:%s\n", user, domain, ha1);
		(void) fprintf(fp2, "%s", line);
	}
	
	/* Close files */
	(void) fclose(fp);
	(void) fclose(fp2);

	/* Put the temp file in place of real file */
	(void) rename(tmp, fname);
	(void) remove(tmp);

	return (ret);
}
#endif /* !EMBEDDED */

/*
 * HTTP digest authentication
 */
struct digest {
	char		user[USER_MAX];
	char		uri[IO_MAX];
	char		cnonce[33];
	char		nonce[33];
	char		resp[33];
	char		qop[16];
	char		nc[16];
};

static void
fetchfield(const char **from, char *to, int len, int shift)
{
	int		n;
	char		fmt[20];
	const char	*p = *from + shift;

	*from = p;

	if (*p == '"') {
		Snprintf(fmt, sizeof(fmt), "%%%d[^\"]%%n", len - 1);
		p++;
	} else {
		Snprintf(fmt, sizeof(fmt), "%%%d[^ \t,]%%n", len - 1);
	}

	elog(0, "fetchfield: [%s] [%s]", fmt, p);
	
	if (sscanf(p, fmt, to, &n)) {
		p += n;
		*from = p;
	}
}

/*
 * Fetch a password provided by user.
 * Return 1 if success, 0 otherwise.
 */
static int
getauth(struct conn *c, struct digest *dig)
{
	const char	*p = c->auth, *e = p + strlen(c->auth);
	
	if (ncasecmp(p, "Digest ", 7) != 0)
		return (0);

	(void) memset(dig, 0, sizeof(dig));

	for (p += 7; p < e; p++)
		if (ncasecmp(p, "username=", 9) == 0)
			fetchfield(&p, dig->user, sizeof(dig->user), 9);
		else if (ncasecmp(p, "nonce=", 6) == 0)
			fetchfield(&p, dig->nonce, sizeof(dig->nonce), 6);
		else if (ncasecmp(p, "response=", 9) == 0)
			fetchfield(&p, dig->resp, sizeof(dig->resp), 9);
		else if (ncasecmp(p, "uri=", 4) == 0)
			fetchfield(&p, dig->uri, sizeof(dig->uri), 4);
		else if (ncasecmp(p, "qop=", 4) == 0)
			fetchfield(&p, dig->qop, sizeof(dig->qop), 4);
		else if (ncasecmp(p, "cnonce=", 7) == 0)
			fetchfield(&p, dig->cnonce, sizeof(dig->cnonce), 7);
		else if (ncasecmp(p, "nc=", 3) == 0)
			fetchfield(&p, dig->nc, sizeof(dig->cnonce), 3);

	elog(0, "[%s] [%s] [%s] [%s] [%s] [%s]",
	    dig->user, dig->uri, dig->resp, dig->qop, dig->cnonce, dig->nc);

	return (1);
}

/*
 * Check the user's password, return 1 if OK
 */
static int
checkpass(const struct conn *c, const char *a1, const struct digest *dig)
{
	char		a2[33], resp[33];

	/* XXX  Due to a bug in MSIE, we do not compare the URI	 */
	/* Also, we do not check for authentication timeout */
	if (/*strcmp(dig->uri, c->ouri) != 0 || */
	    strlen(dig->resp) != 32 /*||
	    now - strtoul(dig->nonce, NULL, 10) > 3600 */)
		return (0);

	md5(a2, c->method, ":", dig->uri, NULL);
	md5(resp, a1, ":", dig->nonce, ":", dig->nc, ":",
	    dig->cnonce, ":", dig->qop, ":", a2, NULL);
	elog(0, "checkpass: [%s] [%s]", resp, dig->resp);

	return (strcmp(resp, dig->resp) == 0);
}

static FILE *
open_auth_file(const char *path)
{
	char 		name[FILENAME_MAX];
	const char	*p, *e;

	if (STROPT(OPT_HTPASSWD)) {
		/* Use global passwords file */
		(void) snprintf(name, sizeof(name), "%s", STROPT(OPT_HTPASSWD));
	} else {
		/* Try to find .htpasswd in requested directory */
		for (p = path, e = p + strlen(p) - 1; e > p; e--)
			if (*e == '/')
				break;
		assert(*e == '/');
		(void) snprintf(name, sizeof(name), "%.*s/%s",
		    (int) (e - p), p, HTPASSWD);
	
		/* Fix directory separators */
		copypath(name, name, strlen(name) + 1);
	}

	return (fopen(name, "r"));
}

/*
 * Check authorization. Return 1 if not needed or authorized, 0 otherwise
 */
static int
checkauth(struct conn *c, const char *file)
{
	int		authorized = 1;
	char		line[512], user[65], ha1[65], dom[65];
	struct digest	digest;
	FILE		*fp = NULL;

#ifdef EMBEDDED
	struct userauth	*uap;
	struct llhead	*lp;

	/* Check, is this URL protected by shttpd_protect_url() */
	LL_FOREACH(&auths, lp) {
		uap = LL_ENTRY(lp, struct userauth, link);
		elog(0, "checkauth: %s %s %s", c->ouri,uap->url, uap->filename);
		if (strncmp(uap->url, c->ouri, strlen(uap->url)) == 0) {
			fp = fopen(uap->filename, "r");
			break;
		}
	}
#endif /* EMBEDDED */

	if (fp == NULL)
		fp = open_auth_file(file);

	/* Try to open passwords file. If opened, set authorized to 0  */
	if (fp != NULL) {
		authorized = 0;

		if (c->auth && getauth(c, &digest) &&
		    (c->user = mystrdup(digest.user)) != NULL)
			/* Each line should be "USER:REALM:HASH" */
			while (fgets(line, sizeof(line), fp) != NULL) {
				if (sscanf(line, "%64[^:]:%64[^:]:%64s",
				    user, dom, ha1) != 3)
					continue;
				elog(0, "checkauth: [%s] [%s,%s] [%s,%s]",
				    line, c->user, user, STROPT(OPT_REALM),dom);
				if (strcmp(c->user, user) == 0 &&
				    strcmp(STROPT(OPT_REALM), dom) == 0) {
					authorized = checkpass(c, ha1, &digest);
					break;
				}
			}
		(void) fclose(fp);
	}

	return (authorized);
}
#endif /* NO_AUTH */

#ifndef NO_CGI
/*
 * Open program, and return read/write pipes to it
 * Return 0 if OK, -1 if error
 */
static int
redirect(struct conn *c, char *prog, char *envblk, char **envp)
{
	int			pair[2];
	char			dir[FILENAME_MAX], *p;
#ifdef _WIN32
	int			sock;	/* non-inheritable copy */
	char			cmdline[FILENAME_MAX], line[FILENAME_MAX];
	FILE			*fp;
	STARTUPINFO		si;
	PROCESS_INFORMATION	pi;
#else
	pid_t			pid;
#endif /* _WIN32 */
	
	if (mysocketpair(pair) != 0)
		return (-1);

	/* CGI must be executed in its own directory */
	(void) strcpy(dir, prog);
	for (p = dir + strlen(dir) - 1; p > dir; p--)
		if (*p == '/') {
			*p++ = '\0';
			break;
		}
	copypath(dir, dir, strlen(dir) + 1);

#ifdef _WIN32
	if (!DuplicateHandle(GetCurrentProcess(),(HANDLE) pair[0],
	    GetCurrentProcess(), (HANDLE *) &sock,
	    0, FALSE, DUPLICATE_SAME_ACCESS))
		elog(1, "main: DuplicateHandle: %d", ERRNO);
	(void) closesocket(pair[0]);
	(void) memset(&si, 0, sizeof(si));
	(void) memset(&pi, 0, sizeof(pi));

	si.cb = sizeof(si);
	si.dwFlags = STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;
	si.wShowWindow = SW_HIDE;
	si.hStdInput = si.hStdOutput = (HANDLE) pair[1];

	/* If CGI file is a script, try to read the interpreter line */
	if ((fp = fopen(prog, "r")) != NULL) {
		(void) fgets(line, sizeof(line), fp);
		if (memcmp(line, "#!", 2) != 0)
			line[2] = '\0';
		/* Trim whitespaces from interpreter name */
		for (p = &line[strlen(line) - 1]; p > line && isspace(*p); p--)
			*p = '\0';
		(void) fclose(fp);
	}

	/* Prepare command line */
	(void) snprintf(cmdline, sizeof(cmdline), "%s%s%s",
	    line + 2, line[2] == '\0' ? "" : " ", prog);
#if 0
	copypath(cmdline, cmdline, strlen(cmdline) + 1);
#endif
	elog(0, "redirect: cmdline [%s], dir [%s]", cmdline, dir);

	if (CreateProcess(NULL, cmdline, NULL, NULL, TRUE,
	    0, envblk, dir, &si, &pi) == 0) {
		elog(0, "redirect: CreateProcess(%s): %d", cmdline, ERRNO);
		(void) closesocket(sock);
		(void) closesocket(pair[1]);
		return (-1);
	} else {
#if 0
		CloseHandle(pi.hProcess);
		(void) closesocket(pair[1]);
#endif
		c->hChild = pi.hProcess;
		c->childsock = pair[1];
		CloseHandle(pi.hThread);
		c->cgisock = pair[0];
	}
	return (sock);
#else
	envblk = NULL;	/* unused */

	if ((pid = fork()) == -1) {
		elog(0, "redirect: fork");
		(void) closesocket(pair[0]);
		(void) closesocket(pair[1]);
		return (-1);
	} else if (pid == 0) {
		/* Child */
		(void) closesocket(pair[0]);
		(void) dup2(pair[1], 0);
		(void) dup2(pair[1], 1);
		(void) chdir(dir);
		(void) execle(prog, prog, NULL, envp);
		elog(1, "redirect: exec(%s)", prog);	/* Error, exit*/
	} else {
		/* Parent */
		(void) closesocket(pair[1]);
		c->cgisock = pair[0];
	}

	elog(0, "redirect: %d %d", pair[0], pair[1]);
	
	return (pair[0]);
#endif /* _WIN32 */
}

static void
addenv(char **env, int *len, char **penv, const char *fmt, const char *val)
{
	int	n;
	char	buf[ARG_MAX];

	n = Snprintf(buf, sizeof(buf), fmt, val);
	if (n > 0 && n < *len - 1) {
		*penv = *env;
		n++;	/* Include \0 terminator */
		(void) memcpy(*env, buf, n);
		(*env) += n;
		(*len) -= n;
	}
}

/*
 * Prepare the environment for the CGI program, and start CGI program.
 */
static int
spawncgi(struct conn *c, char *prog)
{
	char	env[ARG_MAX], *penv[64], var[IO_MAX], val[IO_MAX], hdr[IO_MAX],
		*s = env, *p, *p2;
	int	i, n = sizeof(env), k = 0;

#define	ADDENV(x,y)	if (y) addenv(&s, &n, &penv[k++], x, y)

	/* Prepare the environment block */
	ADDENV("%s", "GATEWAY_INTERFACE=CGI/1.1");
	ADDENV("%s", "SERVER_PROTOCOL=HTTP/1.1");
	ADDENV("%s", "REDIRECT_STATUS=200");			/* For PHP */

	ADDENV("SERVER_PORT=%d", (char *) INTOPT(OPT_LISTENPORT));
	ADDENV("SERVER_NAME=%s", STROPT(OPT_REALM));
	ADDENV("DOCUMENT_ROOT=%s", STROPT(OPT_DOCROOT));
	ADDENV("REQUEST_METHOD=%s", c->method);
	ADDENV("QUERY_STRING=%s", c->http_method == HTTP_GET ? c->query : NULL);
	ADDENV("REMOTE_ADDR=%s", inet_ntoa(c->sa.u.sin.sin_addr));
	ADDENV("SCRIPT_NAME=%s", c->uri);
	ADDENV("CONTENT_TYPE=%s", c->ctype);
	ADDENV("PATH_TRANSLATED=%s", prog);
	ADDENV("CONTENT_LENGTH=%lu", (char *) c->clength);
	ADDENV("PATH=%s", getenv("PATH"));
	ADDENV("COMSPEC=%s", getenv("COMSPEC"));
	ADDENV("LD_LIBRARY_PATH=%s", getenv("LD_LIBRARY_PATH"));
	ADDENV("PERLLIB=%s", getenv("PERLLIB"));
	
	if (c->user) {
		ADDENV("REMOTE_USER=%s", c->user);
		ADDENV("%s", "AUTH_TYPE=Digest");
	}

	/* Add all headers as HTTP_* variables */
	for (p2 = strchr(c->remote.buf, '\n') + 1;
	    k < (int) NELEMS(penv) - 2 && n > 2 &&
	    p2 != NULL && p2[0] != '\n' && p2[0] != '\r'; p2 = p) {
		
		/* Remember the beginning of the next header */
		if ((p = strchr(p2, '\n')) != NULL)
			p++;

		/* Fetch header name and value */
		if (sscanf(p2, "%[-a-zA-Z0-9]: %[^\r\n]", var, val) != 2)
			continue;

		/* Upper-case header name. Convert - to _ */
		for (i = 0; i < (int) strlen(var); i++) {
			var[i] = toupper(((unsigned char *) var)[i]);
			if (var[i] == '-')
				var[i] = '_';
		}

		/* Add the header to the environment */
		(void) Snprintf(hdr, sizeof(hdr), "HTTP_%s=%s", var, val);
		ADDENV("%s", hdr);
	}

	penv[k] = NULL;
	env[sizeof(env) - n] = '\0';

	assert(k < (int) NELEMS(penv));
	assert(n < (int) NELEMS(env));
	
	return (redirect(c, prog, env, penv));
}

static void
cgiparse(struct conn *c)
{
	char	l[64];
	int	n;

	if ((n = getreqlen(c->local.buf, c->local.nread)) == REQ_INVALID) {
		senderr(c, 500, "CGI Error", "", "Script sent invalid headers");
	} else if (n == REQ_INCOMPLETE) {
		/* Do not send anything to the client */
		c->local.nwritten = c->local.nread;
	} else {
		/* Received all headers. Set status. */
		parseheaders(c, c->local.buf);
		if (c->location)
			c->status = 302;
		if (c->status == 0)
			c->status = 200;
		
		accesslog(c, c->status);

		/* Output the status line */
		n = Snprintf(l, sizeof(l),  "HTTP/1.1 %u OK\r\n", c->status);
		(void) writeremote(c, l, n);

		/* Set flags, so script output will be passed directly out */
		c->cgiparsed++;
		c->local.nwritten = 0;
	}
}

/*
 * GET or POST for cgi scripts
 */
static void
get_cgi(struct conn *c)
{
	int	len, n;

	elog(0, "%s enter: %d", "get_cgi", c->cgisock);
	assert(c->cgisock != -1);

	/* Push data received from remote side to a CGI program */
	if (c->http_method == HTTP_POST && (len = IO_DATALEN(&c->remote)) > 0) {
		n = send(c->cgisock, c->remote.buf + c->remote.nwritten,len, 0);
		elog(0, "get_cgi: sent %d out of %d [%d]", n, len, ERRNO);
		if (n > 0) {
			io_inc_nwritten(&c->remote, n);
#if 0
		} else if (n == 0 || (n == -1 && ERRNO == EWOULDBLOCK)) {
			c->local.done++;
#endif
		}
	}

#if _WIN32
	/*
	 * On windows, if child CGI process terminates, recv() from cgisock
	 * return -1 with errno ECONRESET. There is no way to receive
	 * the rest of data from socket, so we loose some data if we're
	 * not reading fast enough. So we do not close our part of the
	 * socket pair, and save the child process handle.
	 * We do not close pair[0] so we never get ECONRESET.
	 * we use handle to check is the child still running. If it has
	 * been terminated, we close everything.
	 */
	if (c->local.done)
		TerminateProcess(c->hChild, 0);
	if (WaitForSingleObject(c->hChild, 1) == 0) {
		(void) CloseHandle(c->hChild);
		(void) closesocket(c->childsock);
	}
#endif /* _WIN32 */

	
	/*
	 * Script may output Status: and Location: headers,
	 * which may alter the response code. So buffer in headers,
	 * parse them, send correct status code and then forward
	 * all data from CGI script back to the remote end.
	 */
	len = IO_SPACELEN(&c->local);
	if ((n = recv(c->cgisock, c->local.buf + c->local.nread, len, 0)) > 0) {
		c->local.nread += n;
		if (c->cgiparsed == 0)
			cgiparse(c);
	} else if (n == 0 || (n == -1 && ERRNO != EWOULDBLOCK)) {
		c->local.done++;
		c->io = NULL;
		if (c->cgiparsed == 0)
			senderr(c, 500, "CGI Error", "",
			    "Bad headers sent:\n\"%.*s\"\n",
			c->local.nread, c->local.buf);
	}

	elog(0, "%s exit, recv-ed %d out of %d [%d]", "get_cgi", n, len, ERRNO);
}

/*
 * GET or POST cgi file
 */
static void
do_cgi(struct conn *c)
{
	c->remote.nwritten = c->reqlen;
	io_inc_nwritten(&c->remote, 0);
	(void) nonblock(c->cgisock);
	if (c->http_method == HTTP_GET)
		c->remote.nread = 0;
	c->io = get_cgi;
}

/*
 * Verify that given file has CGI extension
 */
static int
iscgi(const char *path)
{
	const char	*cgipat = STROPT(OPT_CGIEXT);
	int		len1 = strlen(path), len2 = strlen(cgipat);
	return  (len1 > len2 && !ncasecmp(path + len1 - len2, cgipat, len2));
}
#endif /* NO_CGI */

/*
 * For given directory path, substitute it to valid index file.
 * Return 0 if index file has been found, -1 if not found
 */
static int
useindex(struct conn *c, char *path, size_t maxpath)
{
	struct stat	st;
	char		ftry[FILENAME_MAX], name[FILENAME_MAX];
	const char	*p, *s = STROPT(OPT_INDEX);
	size_t		len;

	do {
		if ((p = strchr(s, ',')) != NULL) {
			len = p - s;
			assert(len < sizeof(name));
			(void) memcpy(name, s, len);
			name[len] = '\0';
		} else {
			(void) strncpy(name, s, sizeof(name));
			name[sizeof(name) - 1] = '\0';
		}

		(void) snprintf(ftry, sizeof(ftry), "%s%c%s", path,DIRSEP,name);
		if (mystat(ftry, &st) == 0) {
			/* Found ! */
			(void) strncpy(path, ftry, maxpath);
			path[maxpath - 1] = '\0';
			(void) strncat(c->uri, name,
			    sizeof(c->uri) - strlen(c->uri) - 1);
			c->st = st;
			return (0);
		}

		/* Move to the next index file */
		s = p ? p + 1 : NULL;
	} while (s != NULL);

	return (-1);
}

/*
 * Handle request
 */
static void
decide(struct conn *c)
{
	char		path[FILENAME_MAX], buf[1024];
	size_t		docrootlen;
#ifdef EMBEDDED
	struct userurl	*p;
#endif /* EMBEDDED */

	elog(0, "%s: [%s], uri [%s]", "decide", c->remote.buf, c->uri);
	
	if ((c->query = strchr(c->uri, '?')) != NULL) {
		*c->query++ = '\0';
		c->query = mystrdup(c->query);
		urldecode(c->query, c->query);
	}
	urldecode(c->uri, c->uri);
	docrootlen = strlen(STROPT(OPT_DOCROOT));
	(void) snprintf(path, sizeof(path), "%s%s",STROPT(OPT_DOCROOT),c->uri);
	killdots(path + docrootlen);

#ifndef NO_AUTH
	if (checkauth(c, path) != 1) {
		(void) snprintf(buf, sizeof(buf),
		    "WWW-Authenticate: Digest qop=\"auth\", realm=\"%s\", "
		    "nonce=\"%lu\"", STROPT(OPT_REALM), (unsigned long) now);

		senderr(c, 401, "Unauthorized", buf, "Authorization required");
	} else
#endif /* NO_AUTH */

#if EMBEDDED
	if ((p = isregistered(c->uri)) != NULL) {
		c->userurl = p;
		c->io = do_embedded;
	}
#endif /* EMBEDDED*/
    else if (strstr(path, HTPASSWD)) {
		senderr(c, 403, "Forbidden","", "Permission Denied");
	}
#ifdef WITH_PUT_AND_DELETE
    else if (c->http_method == HTTP_PUT) {
		int	rc;
		if ((rc = put_dir(path)) == 0) {
			senderr(c, 200, "OK","","");
		} else if (rc == -1) {
			senderr(c, 500, "Error","","%s", strerror(errno));
		} else if ((c->wfd = myopen(path, O_WRONLY | O_BINARY |
		    O_CREAT | O_NONBLOCK | O_TRUNC, 0644)) == -1) {
			elog(0, "handle: open(%s): %s", path, strerror(errno));
			senderr(c, 500, "Error","","PUT error");
		} else {
			accesslog(c, 200);
			c->local.nread = Snprintf(c->local.buf,
			    sizeof(c->local.buf), "%s",
			    "HTTP/1.1 200 OK\r\n\r\n");
			c->remote.nwritten = c->reqlen;
			io_inc_nwritten(&c->remote, 0);
			(void) nonblock(c->wfd);
			c->io = put_file;
		}
	} else if (c->http_method == HTTP_DELETE) {
		if (remove(path) == 0)
			senderr(c, 200, "OK", "", "");
		else
			senderr(c, 500, "Error", "", "%s", strerror(errno));
	}
#endif /* WITH_PUT_AND_DELETE */
    else if (mystat(path, &c->st) != 0) {
		senderr(c, 404, "Not Found","", "Not Found");
	} else if (S_ISDIR(c->st.st_mode) && path[strlen(path) - 1] != '/') {
		(void) snprintf(buf, sizeof(buf), "Location: %s/", c->uri);
		senderr(c, 301, "Moved Permanently", buf, "Moved, %s", buf);
	} else if (S_ISDIR(c->st.st_mode) &&
	    useindex(c, path, sizeof(path) - 1) == -1 &&
	    INTOPT(OPT_DIRLIST) == 0) {
		senderr(c, 403, "Forbidden", "", "Directory Listing Denied");
	} else if (S_ISDIR(c->st.st_mode) && INTOPT(OPT_DIRLIST)) {
		if ((c->path = mystrdup(path)) != NULL)
			do_dir(c);
		else
			senderr(c, 500, "Error", "", "strdup");
	} else if (S_ISDIR(c->st.st_mode) && INTOPT(OPT_DIRLIST) == 0) {
		elog(0, "decide: %s: Denied", path);
		senderr(c, 403, "Forbidden", "", "Directory listing denied");
#ifndef NO_CGI
	} else if (iscgi(path)) {
		if ((c->cgisock = spawncgi(c, path)) == -1)
			senderr(c, 500, "Server Error", "", "Cannot exec CGI");
		else
			do_cgi(c);
#endif /* NO_CGI */
	} else if (c->ims && c->ims >= c->st.st_mtime) {
		char buf[222], buf2[222];
		strftime(buf, sizeof(buf),"%d/%b/%Y %H:%M:%S",
				localtime(&c->st.st_mtime));
		strftime(buf2, sizeof(buf2),"%d/%b/%Y %H:%M:%S",
				localtime(&c->ims));
		elog(0, "decide: %s: %lu > %lu (%s>%s) (Not Modified)",
		    path, c->ims, c->st.st_mtime, buf2, buf);
		senderr(c, 304, "Not Modified","", "");
	} else if ((c->rfd = myopen(path, O_RDONLY | O_BINARY, 0644)) != -1) {
		do_get(c);
	} else {
		senderr(c, 500, "Error", "", "Internal Error");
		c->done++;
	}
}

/*
 * Parse HTTP request
 */
static void
parse(struct conn *c, const char *s)
{
	/* Will pre-process in something like "15s 1024s 15s" */
	const char	*fmt =
		"%" _STR2(METHOD_MAX) "s "
		"%" _STR2(IO_MAX) "s "
		"%" _STR2(METHOD_MAX) "s";

	/* Get the request line */
	if (sscanf(s, fmt, c->method, c->uri, c->proto) != 3) {
		senderr(c, 404, "Bad Request","", "Bad Request");
	} else if (c->uri[0] != '/') {
		senderr(c, 404, "Bad Request","", "Bad Request");
	} else if (ncasecmp(c->proto, "HTTP", 4) != 0) {
		senderr(c, 501, "Bad Protocol","", "Procotol Not Supported");
	} else if (ncasecmp(c->method, "GET", 3) == 0) {
		c->http_method = HTTP_GET;
	} else if (ncasecmp(c->method, "POST", 4) == 0) {
			c->http_method = HTTP_POST;
#ifdef WITH_PUT_AND_DELETE
	} else if (ncasecmp(c->method, "PUT", 3) == 0) {
		c->http_method = HTTP_PUT;
	} else if (ncasecmp(c->method, "DELETE", 6) == 0) {
		c->http_method = HTTP_DELETE;
#endif /* WITH_PUT_AND_DELETE */
	} else {
		senderr(c, 501, "Not Implemented", "","Method Not Implemented");
	}
	
	/* XXX senderr() should set c->local.done flag! */
	if (c->local.done == 0) {
		parseheaders(c, strchr(s, '\n') + 1);
		(void) strcpy(c->ouri, c->uri);
		decide(c);
		c->parsed++;
	}
}

/*
 * Add mime type to the known mime type list
 */
static void
addmimetype(const char *ext, const char *mime)
{
	struct mimetype	*p;

	/* XXX possible resource leak  */
	if ((p = calloc(1, sizeof(*p))) != NULL &&
	    (p->ext = mystrdup(ext)) != NULL &&
	    (p->mime = mystrdup(mime)) != NULL) {
		p->extlen = strlen(p->ext);
		p->next = mimetypes;
		mimetypes = p;
	}
}

/*
 * Free mime types list
 */
static void
freemime(void)
{
	struct mimetype	*p, *tmp;

	for (p = mimetypes; p != NULL; p = tmp) {
		tmp = p->next;
		free(p->ext);
		free(p->mime);
		free(p);
	}

	mimetypes = NULL;
}

/*
 * Set mime type list from file
 */
static void
setmimetypes(const char *file)
{
	FILE	*fp;
	char	line[512], ext[sizeof(line)], mime[sizeof(line)], *p;
	
	if ((fp = fopen(file, "r")) == NULL)
		elog(1, "setmimetypes: fopen(%s): %s", file, strerror(errno));

	while (fgets(line, sizeof(line), fp) != NULL) {
		/* Skip empty lines */
		if (line[0] == '#' || line[0] == '\n')
			continue;
		if (sscanf(line, "%s", mime)) {
			p = line + strlen(mime);
			while (*p && *p != '\n' && sscanf(p, "%s", ext)) {
				addmimetype(ext, mime);
				p += strlen(mime);
			}
		}
	}

	(void) fclose(fp);
}

/*
 * Set default mime types list
 */
static void
setdefmimetypes(void)
{
	addmimetype("torrent", "application/x-bittorrent");
	addmimetype("wav", "audio/x-wav");
	addmimetype("mp3", "audio/x-mp3");
	addmimetype("mid", "audio/mid");
	addmimetype("m3u", "audio/x-mpegurl");
	addmimetype("ram", "audio/x-pn-realaudio");
	addmimetype("ra", "audio/x-pn-realaudio");
	addmimetype("doc", "application/msword");
	addmimetype("exe", "application/octet-stream");
	addmimetype("zip", "application/x-zip-compressed");
	addmimetype("xls", "application/excel");
	addmimetype("tgz", "application/x-tar-gz");
	addmimetype("tar.gz", "application/x-tar-gz");
	addmimetype("tar", "application/x-tar");
	addmimetype("gz", "application/x-gunzip");
	addmimetype("arj", "application/x-arj-compressed");
	addmimetype("rar", "application/x-arj-compressed");
	addmimetype("rtf", "application/rtf");
	addmimetype("pdf", "application/pdf");
	addmimetype("jpeg", "image/jpeg");
	addmimetype("png", "image/png");
	addmimetype("mpg", "video/mpeg");
	addmimetype("mpeg", "video/mpeg");
	addmimetype("asf", "video/x-ms-asf");
	addmimetype("avi", "video/x-msvideo");
	addmimetype("bmp", "image/bmp");
	addmimetype("jpg", "image/jpeg");
	addmimetype("gif", "image/gif");
	addmimetype("ico", "image/x-icon");
	addmimetype("css", "text/css");
	addmimetype("htm", "text/html");
	addmimetype("html", "text/html");
}

/*
 * Process given connection
 */
static void
serve(struct conn *c, fd_set *rset, fd_set *wset)
{
	int	n, len;

#if 1
	elog(0, "%s enter %p: local %d.%d.%d, remote %d.%d.%d", "serve", c,
	    c->local.done, c->local.nread, c->local.nwritten,
	    c->remote.done, c->remote.nread, c->remote.nwritten);
#endif

	/* Read from remote end  */
	assert(c->sock != -1);
	if ((len = IO_SPACELEN(&c->remote) - 1)> 0 && FD_ISSET(c->sock, rset)) {
		n = readremote(c, c->remote.buf + c->remote.nread, len);
		if (n > 0) {
			c->remote.nread += n;
			c->remote.buf[c->remote.nread] = '\0';
			c->expire += EXPIRE_TIME;
		} else if (c->parsed == 0 && c->remote.done) {
			c->done++;
		}
	}

	/* Try to parse the request from remote endpoint */
	if (c->parsed == 0) {
		c->reqlen = getreqlen(c->remote.buf, c->remote.nread);
		if (c->reqlen == REQ_INVALID)
			senderr(c, 400, "Bad Request", "", "Bad request");
		else if (IO_SPACELEN(&c->remote) <= 1)
			senderr(c, 400, "Bad Request", "","Request is too big");
		else if (c->reqlen > 0)
			parse(c, c->remote.buf);
	}

	/* Read from the local endpoint */
	if (c->io && IO_SPACELEN(&c->local)) {
		c->io(c); /* do_embedded is called here */
		c->expire += EXPIRE_TIME;
	}

	if ((len = IO_DATALEN(&c->local)) > 0 && FD_ISSET(c->sock, wset)){
	    n = writeremote(c, c->local.buf + c->local.nwritten, len);
		if (n > 0)
			io_inc_nwritten(&c->local, n);
	}

	if ((c->remote.done && c->remote.nread == 0) ||
	    (c->local.done && c->local.nread == 0))
		c->done++;
#if 1
	elog(0, "%s exit %p: local %d.%d.%d, remote %d.%d.%d", "serve", c,
	    c->local.done, c->local.nread, c->local.nwritten,
	    c->remote.done, c->remote.nread, c->remote.nwritten);
#endif
}

/*
 * Add file descriptors of opened connections to the given
 * file descriptors set. Bump up the maxfd.
 */
void
shttpd_merge_fds(fd_set *rfds, fd_set *wfds, int *maxfd)
{
	struct llhead	*lp;
	struct conn	*c;
	int		n = 0;

#define	MERGEFD(fd,set)	\
	do {FD_SET(fd, set); if (fd > *maxfd) *maxfd=fd; } while (0)

	LL_FOREACH(&conns, lp) {
		c = LL_ENTRY(lp, struct conn, link);
		n++;	/* Number of active connections */
		
		/* Remote socket always in read set */
		MERGEFD(c->sock, rfds); 
		
		/* If there is data in local buffer, add to write set */
		if (IO_DATALEN(&c->local))
			MERGEFD(c->sock, wfds);

		/* If there is space left in local buffer, read more */
		if (IO_SPACELEN(&c->local)) {
			/* XXX the order is important here! dummy before rfd */
			if (c->cgisock != -1)
				MERGEFD(c->cgisock, rfds);
			else if (c->dummy[0] != -1)
				MERGEFD(c->dummy[0], rfds);
			else if (c->rfd != -1)
				MERGEFD(c->rfd, rfds);
		}

		/* If we have data from client, pass it on to io function */
		else if (IO_DATALEN(&c->remote) && c->wfd != -1)
			MERGEFD(c->wfd, wfds);
	}

	if (n > nconns)
		nconns = n;	/* Maximum active connections handled */
}

/*
 * One iteration of server loop.
 */
void
shttpd_poll(int listen_socket, unsigned milliseconds)
{
	struct llhead	*lp, *tmp;
	struct usa	sa;
	struct conn	*c;
	int		sock, maxfd = listen_socket;
	struct timeval	tv;			/* Timeout for select() */
	fd_set		rset, wset;

	now = time(0);
	sa.len = sizeof(sa.u.sin);
	
	/* Populate the descriptor set */
	FD_ZERO(&rset);
	FD_ZERO(&wset);
	if (listen_socket != -1)
		FD_SET(listen_socket, &rset);

	shttpd_merge_fds(&rset, &wset, &maxfd);
	tv.tv_sec = milliseconds / 1000;
	tv.tv_usec = milliseconds % 1000;

	/* Find out who is ready for IO */
	if (select(maxfd + 1, &rset, &wset, NULL, &tv) < 0) {
		elog(0, "select: %s", strerror(errno));
	} else {
		/* If listening socket is ready, accept new connection */
		if (listen_socket != -1 && FD_ISSET(listen_socket, &rset) &&
		    (sock = accept(listen_socket, &sa.u.sa, &sa.len)) != -1)
			newconnection(sock, &sa);

		/* Loop through all connections, handle if needed */
		LL_FOREACH_SAFE(&conns, lp, tmp) {
			c = LL_ENTRY(lp, struct conn, link);
			serve(c, &rset, &wset);
			if (c->done || c->expire < now)
				disconnect(c);
		}
	}
}

/*
 * Set the option value
 */
static void
setopt(int index, const char *value)
{
	struct opt	*o = &options[index];

	if (o->type == OPT_INT) {
		if (value != NULL)
			o->value.value_int = atoi(value);
	} else {
		/* Free previous value */
		if (o->value.value_str)
			free(o->value.value_str);
		
		/* Set new value */
		if (value == NULL)
			o->value.value_str = NULL;
		else
			o->value.value_str = mystrdup(value);
	}
}

/*
 * Initialize shttpd
 */
void
shttpd_init(const char *fname)
{
	char		line[FILENAME_MAX],var[sizeof(line)],val[sizeof(line)];
	size_t		i;
	FILE 		*fp;

	/* Init global variable 'now', needed for elog() etc */
	now = time(0);

	/* If config file is there, read it */
	if (fname != NULL && (fp = fopen(fname, "r")) != NULL) {
		elog(0, "using config file %s", fname);

		/* Loop through the lines in config file */
		while (fgets(line, sizeof(line), fp) != NULL) {

			/* Skip comments and empty lines */
			if (line[0] == '#' || line[0] == '\n')
				continue;

			/* Trim trailing newline character */
			line[strlen(line) - 1] = '\0';

			if (sscanf(line, "%s %s", var, val) != 2)
				elog(1, "%s: Bad line: [%s]."
				    "Each line in config file must be "
				    "in format '<var> <value>'",
				    "shttpd_init", line);

			/* Set the option */
			for (i = 0; i < NELEMS(options); i++)
				if (strcmp(options[i].varname, var) == 0) {
					elog(0, "%s %s", var, val);
					setopt(i, val);
					options[i].set = 1;
					break;
				}

			if (i == NELEMS(options))
				elog(1, "%s: Unknown variable [%s]",fname,var);
		}
		(void) fclose(fp);
	}

	/* Cmd flag overrides config file option */
	for (i = 0; i < NELEMS(options); i++) {
		if (options[i].flag) {
			setopt(i, options[i].flag);
			options[i].set = 1;
		} else if (options[i].set == 0) {
			setopt(i, options[i].deflt);
		}
	}

	/* If use_ssl is on, and port is not overridden, set it to 443 */
	if (options[OPT_LISTENPORT].set == 0 && INTOPT(OPT_USESSL))
		setopt(OPT_LISTENPORT, "443");

	/* If document_root is not set, set it to current directory */
	if (STROPT(OPT_DOCROOT) == NULL)
		STROPT(OPT_DOCROOT) = getcwd(NULL, 0);
	assert(STROPT(OPT_DOCROOT) != NULL);

	/* Redirect stderr to a file if requested */
	if (STROPT(OPT_ERRORLOG))
		errtofile(STROPT(OPT_ERRORLOG));
	
	/* Set mime types */
	if (STROPT(OPT_MIMEFILE))
		setmimetypes(STROPT(OPT_MIMEFILE));
	else
		setdefmimetypes();

	/* Open access log file */
	if ((logfp = fopen(STROPT(OPT_ACCESSLOG), "a")) == NULL)
		elog(0, "cannot open logfile %s: %s",
		    STROPT(OPT_ACCESSLOG), strerror(errno));

#ifdef _WIN32
	/* Initialize winsock library */
	{
		WSADATA data;
		int	opt = SO_SYNCHRONOUS_NONALERT;
		WSAStartup(MAKEWORD(2,2), &data);
		if (setsockopt(INVALID_SOCKET, SOL_SOCKET, SO_OPENTYPE,
		    (void *)&opt,sizeof(opt)) < 0)
			elog(1, "setsockopt: %d", ERRNO);
	}
#endif /* _WIN32 */

#if WITH_SSL
	if (INTOPT(OPT_USESSL)) {
		/* Initialize SSL crap */
		SSL_load_error_strings();
		SSLeay_add_ssl_algorithms();
		if ((ctx = SSL_CTX_new(SSLv23_server_method())) == NULL)
			elog(1, "main: error: SSL_CTX_new %p", ctx);
		else if (SSL_CTX_use_certificate_file(ctx, STROPT(OPT_SSLCERT),
		    SSL_FILETYPE_PEM) == 0)
			elog(1, "main: please put %s here",STROPT(OPT_SSLCERT));
		else if (SSL_CTX_use_PrivateKey_file(ctx, STROPT(OPT_SSLCERT),
		    SSL_FILETYPE_PEM) == 0)
			elog(1, "main: please put %s here",STROPT(OPT_SSLCERT));
	}
#endif /* WITH_SSL */
}

/*
 * Deallocate shttpd object, free up the resources
 */
void
shttpd_fini(void)
{
	size_t	i;
	
	/* Free configuration */
	for (i = 0; i < NELEMS(options); i++) {
		if (options[i].type == OPT_STR && options[i].value.value_str)
			free(options[i].value.value_str);
		options[i].set = 0;
		options[i].value.value_str = NULL;
	}

	/* Free allocated mime-types */
	freemime();

	/* If log stream was opened, close it now */
	if (logfp != NULL) {
		(void) fclose(logfp);
		logfp = NULL;
	}
}

#ifdef EMBEDDED
void
shttpd_register_url(const char *url, shttpd_callback_t cb, void *data)
{
	struct userurl	*p;

	if ((p = calloc(1, sizeof(*p))) != NULL) {
		p->func = cb;
		p->data = data;
		p->url = mystrdup(url);
		LL_TAIL(&urls, &p->link);
	}
}

void
shttpd_protect_url(const char *url, const char *filename)
{
	struct userauth	*p;

	if ((p = calloc(1, sizeof(*p))) != NULL) {
		p->url = mystrdup(url);
		p->filename = mystrdup(filename);
		LL_TAIL(&auths, &p->link);
	}
}

const char *
shttpd_get_var(struct conn *c, const char *var)
{
	char	*p, *s, *e;
	size_t	len, i;

	if ((p = c->query) == NULL)
		return (NULL);

	/* If the buffer has not been scanned yet, do it now */
	if (c->vars[0] == NULL)
		for (i = 0; *p && i < NELEMS(c->vars); i++, p = e) {
			c->vars[i] = p;
			if ((s = strchr(p, '=')) == NULL)
				break;
			*s++ = '\0';
			if ((e = strchr(s, '&')) != NULL)
				*e++ = '\0';
			else
				e = s + strlen(s);
		}

	/* Now, loop over all variables, find the right one, return value */
	len = strlen(var);
	for (i = 0; i < NELEMS(c->vars) && c->vars[i] != NULL; i++)
		if (memcmp(var, c->vars[i], len) == 0)
			return (c->vars[i] + len + 1);

	return (NULL);
}

/*
 * Returns everything contained in the connection's "query" buffer
 */
char *
shttpd_get_msg(struct conn *c)
{
    return c->query;
}

static void
substitute(char *buf, const char *kw, const char *subst)
{
	char	buf2[IO_MAX], *s = buf, *p, *p2 = buf2;
	int	len, kwlen = strlen(kw), slen = strlen(subst);

	do {
		if ((p = strstr(s, kw)) == NULL) {
			len = strlen(s);
			(void) memcpy(p2, s, len);
			p2 += len;
			s += len;
		} else {
			len = p - s;
			
			if (&p2[len] < &buf2[sizeof(buf2)]) {
				(void) memcpy(p2, s, len);
				p2 += len;
			}

			if (&p2[slen] < &buf2[sizeof(buf2)]) {
				(void) memcpy(p2, subst, slen);
				p2 += slen;
			}

			s = p + kwlen;
		}

	} while (*s != '\0');

	assert(p2 < &buf2[sizeof(buf2)]);
	*p2 = '\0';

	(void) memcpy(buf, buf2, p2 - buf2 + 1);
}

int
shttpd_template(struct conn *conn, const char *headers, const char *file, ...)
{
	va_list		ap;
	char		buf[IO_MAX], *keyword, *subst;
	FILE		*fp;
	int		n = 0;

	/* Read the file in */
	if ((fp = fopen(file, "r")) == NULL) {
		elog(0, "%s: fopen(%s): %s",
		    "shttd_template", file, strerror(errno));
	} else if ((n = fread(buf, 1, sizeof(buf) - 1, fp)) <= 0) {
		elog(0, "%s: fread(%s): %s",
		    "shttpd_template", file, strerror(errno));
		(void) fclose(fp);
	} else {
		buf[n] = '\0';
		
		va_start(ap, file);
		while ((keyword = va_arg(ap, char *)) != NULL &&
		    (subst = va_arg(ap, char *)) != NULL)
			if (keyword[0] != '\0')
				substitute(buf, keyword, subst);
		va_end(ap);

		n = Snprintf(conn->local.buf, sizeof(conn->local.buf),
		    "%s%s\r\n%s", "HTTP/1.1 200 OK\r\n", headers, buf);
		
		(void) fclose(fp);
	}

	return (n);
}

#ifdef MT
int
shttpd_push(struct conn *conn, const void *buf, size_t len)
{
	int		n;
	size_t		left = len;
	const char	*p = buf;

	do {
		n = writeremote(conn, p, left);
		if (n <= 0)
			return (-1);
		left -= n;
		p += n;
	} while (left > 0);

	return (len);
}

int
shttpd_printf(struct conn *conn, const char *fmt, ...)
{
	va_list	ap;
	int	len;
	char	buf[IO_MAX];

	va_start(ap, fmt);
	len = vsnprintf(buf, sizeof(buf), fmt, ap);
	if (len > (int) sizeof(buf))
		len = sizeof(buf);
	else if (len == -1)
		len = 0;
	va_end(ap);

	return (shttpd_push(conn, buf, len));
}
#endif /* MT */

#else

/*
 * Stand-alone configuration. Define signal handlers, usage() and main()
 */

/*
 * SIGTERM, SIGINT signal handler
 */
static void
sigterm(int signo)
{
	quit = signo;
}

/*
 * Grim reaper of innocent children: SIGCHLD signal handler
 */
static void
sigchild(int signo)
{
	while (waitpid(-1, &signo, WNOHANG) > 0) ;
}


/*
 * Show usage string and exit.
 */
static void
usage(const char *prog)
{
	(void) fprintf(stderr,
	    "shttpd version %s (c) Sergey Lyubka\n"
	    "usage: %s [OPTIONS]\n"
	    "-d <directory>	wwwroot (dflt: current directory)\n"
	    "-D 		no directory listing\n"
	    "-p <port>	listening port (dflt: 80 or 443)\n"
	    "-l <logfile> 	access log file (dflt: %s)\n"
	    "-I 		inetd mode\n"
	    "-e <errorlog>	error log file\n"
	    "-i <files> 	index files (dflt: %s)\n"
	    "-C <config> 	configuration file (dflt: %s)\n"
	    , VERSION, prog, options[OPT_ACCESSLOG].deflt,
	    options[OPT_INDEX].deflt, CONFIG);

	(void) fprintf(stderr,
#ifndef NO_CGI
	    "-c <cgi_ext>	CGI file pattern (dflt: %s)\n"
#endif /* NO_CGI */
#ifndef NO_AUTH
	    "-P <passfile>	global auth file\n"
	    "-A <passfile> <realm> <user> <password> edit .htpasswd file\n"
#endif /* NO_AUTH */
	    "-u <login>	switch UID to user <login_name>\n"
	    "-N <domain>	server domain name (auth realm) (dflt: %s)\n"
	    "-m <mimefile>	mime types file\n"
	    "-v 		debug mode\n"
#ifdef WITH_SSL
	    "-s <pem_file>	SSL certificate (dflt: %s)\n"
	    "-S 		Switch to SSL mode\n"
#endif /* WITH_SSL */
#ifndef NO_CGI
	    , options[OPT_CGIEXT].deflt
#endif /* NO_CGI */
	    , options[OPT_REALM].deflt
#ifdef WITH_SSL
	    , options[OPT_SSLCERT].deflt
#endif /* WITH_SSL */
	    );

	exit(EXIT_FAILURE);
}

int
main(int argc, char *argv[])
{
	int		i, lsn;
	/*
	 * Parse command-line options. Not every system has getopt()
	 * function, so we use the good old method.
	 */
#define	GETARG(p)							\
	if ((p = (argv[i][2] ? &argv[i][2] : argv[++i])) == NULL)	\
		usage(argv[0])

	for (i = 1; i < argc && argv[i][0] == '-'; i++)
		switch (argv[i][1]) {
		case 'd': GETARG(options[OPT_DOCROOT].flag);	break;
		case 'p': GETARG(options[OPT_LISTENPORT].flag);	break;
		case 'l': GETARG(options[OPT_ACCESSLOG].flag);	break;
		case 'e': GETARG(options[OPT_ERRORLOG].flag);	break;
		case 'i': GETARG(options[OPT_INDEX].flag);	break;
		case 'I': options[OPT_INETD].flag = "1";	break;
		case 'P': GETARG(options[OPT_HTPASSWD].flag);	break;
		case 'c': GETARG(options[OPT_CGIEXT].flag);	break;
		case 's': GETARG(options[OPT_SSLCERT].flag);	break;
		case 'S': options[OPT_USESSL].flag = "1";	break;
		case 'u': GETARG(options[OPT_UID].flag);	break;
		case 'D': options[OPT_DIRLIST].flag = "0";	break;
		case 'N': GETARG(options[OPT_REALM].flag);	break;
		case 'm': GETARG(options[OPT_MIMEFILE].flag);	break;
		case 'v': options[OPT_DEBUG].flag = "1";	break;
		case 'C': GETARG(config);			break;
		case 'A':
			if (argc != 6)
				usage(argv[0]);
			exit(editpass(argv[2], argv[3], argv[4], argv[5]));
			break;
		default:
			usage(argv[0]);
			break;
		}

	shttpd_init(config);

	if (INTOPT(OPT_INETD)) {
		/* In inetd mode, stdin is the connected socket. */
		newconnection(0, NULL);
		lsn = -1;
	} else {
		lsn = shttpd_open_port(INTOPT(OPT_LISTENPORT));
	}
	
	/* Switch to alternate UID, it is safe now, after shttpd_open_port() */
#ifndef _WIN32
	if (STROPT(OPT_UID)) {
		const char	*uid = STROPT(OPT_UID);
		struct passwd	*pw;

		if ((pw = getpwnam(uid)) == NULL)
			elog(1, "main: unknown user [%s]", uid);
		else if (setgid(pw->pw_gid) == -1)
			elog(1, "main: setgid(%s): %s", uid, strerror(ERRNO));
		else if (setuid(pw->pw_uid) == -1)
			elog(1, "main: setuid(%s): %s", uid, strerror(ERRNO));
		endpwent();
	}
#endif /* _WIN32 */

	(void) signal(SIGTERM, sigterm);
	(void) signal(SIGINT, sigterm);
#ifndef _WIN32
	(void) signal(SIGCHLD, sigchild);
	(void) signal(SIGPIPE, SIG_IGN);
#endif /* _WIN32 */

	if (INTOPT(OPT_INETD) == 0)
		(void) printf("shttpd %s started on port %d, "
		    "wwwroot: [%s]\n",
		    VERSION, INTOPT(OPT_LISTENPORT), STROPT(OPT_DOCROOT));

	while (quit == 0)
		shttpd_poll(lsn, 1000);

	shttpd_fini();

	/* Close listening socket */
	if (lsn != -1)
		(void) close(lsn);

	if (INTOPT(OPT_INETD) == 0)
		(void) printf("shttpd: exit on signal %d, "
		    "max concurrent connections: %d.\n", quit, nconns);
	
	return (EXIT_SUCCESS);
}
#endif /* !EMBEDDED */
