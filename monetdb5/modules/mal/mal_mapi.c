/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a N.J. Nes P. Boncz, S. Mullender, M. Kersten
 * @v 1.1
 * @+ MAPI interface
 * The complete Mapi library is available to setup
 * communication with another Mserver.
 *
 * Clients may initialize a private listener to implement
 * specific services. For example, in an OLTP environment
 * it may make sense to have a listener for each transaction
 * type, which simply parses a sequence of transaction parameters.
 *
 * Authorization of access to the server is handled as part
 * of the client record initialization phase.
 *
 * This library internally uses pointer handles, which we replace with
 * an index in a locally maintained table. It provides a handle
 * to easily detect havoc clients.
 *
 * A cleaner and simplier interface for distributed processing is available in
 * the module remote.
 */
#include "monetdb_config.h"
#ifdef HAVE_MAPI
#include "mal_mapi.h"
#include <sys/types.h>
#include <stream_socket.h>
#include <mapi.h>
#ifdef HAVE_OPENSSL
# include <openssl/rand.h>		/* RAND_bytes() */
#endif
#ifdef _WIN32   /* Windows specific */
# include <winsock.h>
#else           /* UNIX specific */
# include <sys/select.h>
# include <sys/socket.h>
# include <unistd.h>     /* gethostname() */
# include <netinet/in.h> /* hton and ntoh */
# include <arpa/inet.h>  /* addr_in */
#endif
#ifdef HAVE_SYS_UN_H
# include <sys/un.h>
#endif
#ifdef HAVE_NETDB_H
# include <netdb.h>
# include <netinet/in.h>
#endif
#ifdef HAVE_SYS_UIO_H
# include <sys/uio.h>
#endif

#define SOCKPTR struct sockaddr *
#ifdef HAVE_SOCKLEN_T
#define SOCKLEN socklen_t
#else
#define SOCKLEN int
#endif

static char seedChars[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
	'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
	'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
	'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};


static void generateChallenge(str buf, int min, int max) {
	size_t size;
	size_t bte;
	size_t i;

	/* don't seed the randomiser here, or you get the same challenge
	 * during the same second */
#ifdef HAVE_OPENSSL
	if (RAND_bytes((unsigned char *) &size, (int) sizeof(size)) < 0)
#endif
		size = rand();
	size = (size % (max - min)) + min;
#ifdef HAVE_OPENSSL
	if (RAND_bytes((unsigned char *) buf, (int) size) >= 0) {
		for (i = 0; i < size; i++)
			buf[i] = seedChars[((unsigned char *) buf)[i] % 62];
	} else {
#endif
		for (i = 0; i < size; i++) {
			bte = rand();
			bte %= 62;
			buf[i] = seedChars[bte];
		}
#ifdef HAVE_OPENSSL
	}
#endif
	buf[i] = '\0';
}

struct challengedata {
	stream *in;
	stream *out;
};

static void
doChallenge(void *data)
{
#ifdef DEBUG_SERVER
	Client cntxt= mal_clients;
#endif
	char *buf = (char *) GDKmalloc(BLOCK + 1);
	char challenge[13];
	char *algos;
	stream *fdin = block_stream(((struct challengedata *) data)->in);
	stream *fdout = block_stream(((struct challengedata *) data)->out);
	bstream *bs;
	int len = 0;

#ifdef _MSC_VER
	srand((unsigned int) GDKusec());
#endif
	GDKfree(data);
	if (buf == NULL || fdin == NULL || fdout == NULL){
		if (fdin) {
			mnstr_close(fdin);
			mnstr_destroy(fdin);
		}
		if (fdout) {
			mnstr_close(fdout);
			mnstr_destroy(fdout);
		}
		if (buf)
			GDKfree(buf);
		GDKsyserror("SERVERlisten:"MAL_MALLOC_FAIL);
		return;
	}

	/* generate the challenge string */
	generateChallenge(challenge, 8, 12);
	algos = mcrypt_getHashAlgorithms();
	/* note that we claim to speak proto 9 here for hashed passwords */
	mnstr_printf(fdout, "%s:mserver:9:%s:%s:%s:",
			challenge,
			algos,
#ifdef WORDS_BIGENDIAN
			"BIG",
#else
			"LIT",
#endif
			MONETDB5_PASSWDHASH
			);
	free(algos);
	mnstr_flush(fdout);
	/* get response */
	if ((len = (int) mnstr_read_block(fdin, buf, 1, BLOCK)) < 0) {
		/* the client must have gone away, so no reason to write something */
		mnstr_close(fdin);
		mnstr_destroy(fdin);
		mnstr_close(fdout);
		mnstr_destroy(fdout);
		GDKfree(buf);
		return;
	}
	buf[len] = 0;
#ifdef DEBUG_SERVER
	printf("mal_mapi:Client accepted %s\n", buf);
	fflush(stdout);

	mnstr_printf(cntxt->fdout, "#SERVERlisten:client accepted\n");
	mnstr_printf(cntxt->fdout, "#SERVERlisten:client string %s\n", buf);
#endif
	bs = bstream_create(fdin, 128 * BLOCK);

	if (bs == NULL){
		if (fdin) {
			mnstr_close(fdin);
			mnstr_destroy(fdin);
		}
		if (fdout) {
			mnstr_close(fdout);
			mnstr_destroy(fdout);
		}
		if (buf)
			GDKfree(buf);
		GDKsyserror("SERVERlisten:"MAL_MALLOC_FAIL);
		return;
	}
	bs->eof = 1;
	MSscheduleClient(buf, challenge, bs, fdout);
}

static volatile ATOMIC_TYPE nlistener = 0; /* nr of listeners */
static volatile ATOMIC_TYPE serveractive = 0;
static volatile ATOMIC_TYPE serverexiting = 0; /* listeners should exit */
#ifdef ATOMIC_LOCK
/* lock for all three ATOMIC_TYPE variables above */
static MT_Lock atomicLock MT_LOCK_INITIALIZER("atomicLock");
#endif

static void
SERVERlistenThread(SOCKET *Sock)
{
	char *msg = 0;
	int retval;
	struct timeval tv;
	fd_set fds;
	SOCKET sock = INVALID_SOCKET;
	SOCKET usock = INVALID_SOCKET;
	SOCKET msgsock = INVALID_SOCKET;
	struct challengedata *data;
	MT_Id tid;

	if (*Sock) {
		sock = Sock[0];
		usock = Sock[1];
		GDKfree(Sock);
	}

	(void) ATOMIC_INC(nlistener, atomicLock);

	do {
		FD_ZERO(&fds);
		if (sock != INVALID_SOCKET)
			FD_SET(sock, &fds);
#ifdef HAVE_SYS_UN_H
		if (usock != INVALID_SOCKET)
			FD_SET(usock, &fds);
#endif
		/* Wait up to 0.025 seconds (0.01 if testing) */
		tv.tv_sec = 0;
		tv.tv_usec = GDKdebug & FORCEMITOMASK ? 10000 : 25000;

		/* temporarily use msgsock to record the larger of sock and usock */
		msgsock = sock;
#ifdef HAVE_SYS_UN_H
		if (usock != INVALID_SOCKET && (sock == INVALID_SOCKET || usock > sock))
			msgsock = usock;
#endif
		retval = select((int)msgsock + 1, &fds, NULL, NULL, &tv);
		if (ATOMIC_GET(serverexiting, atomicLock) ||
			GDKexiting())
			break;
		if (retval == 0) {
			/* nothing interesting has happened */
			continue;
		}
		if (retval == SOCKET_ERROR) {
			if (
#ifdef _MSC_VER
				WSAGetLastError() != WSAEINTR
#else
				errno != EINTR
#endif
				) {
				msg = "select failed";
				goto error;
			}
			continue;
		}
		if (sock != INVALID_SOCKET && FD_ISSET(sock, &fds)) {
			if ((msgsock = accept(sock, (SOCKPTR)0, (socklen_t *)0)) == INVALID_SOCKET) {
				if (
#ifdef _MSC_VER
					WSAGetLastError() != WSAEINTR
#else
					errno != EINTR
#endif
					|| !ATOMIC_GET(serveractive, atomicLock)) {
					msg = "accept failed";
					goto error;
				}
				continue;
			}
#ifdef HAVE_SYS_UN_H
		} else if (usock != INVALID_SOCKET && FD_ISSET(usock, &fds)) {
			struct msghdr msgh;
			struct iovec iov;
			char buf[1];
			int rv;
			char ccmsg[CMSG_SPACE(sizeof(int))];
			struct cmsghdr *cmsg;

			if ((msgsock = accept(usock, (SOCKPTR)0, (socklen_t *)0)) == INVALID_SOCKET) {
				if (
#ifdef _MSC_VER
					WSAGetLastError() != WSAEINTR
#else
					errno != EINTR
#endif
					) {
					msg = "accept failed";
					goto error;
				}
				continue;
			}

			/* BEWARE: unix domain sockets have a slightly different
			 * behaviour initialy than normal sockets, because we can
			 * send filedescriptors or credentials with them.  To do so,
			 * we need to use sendmsg/recvmsg, which operates on a bare
			 * socket.  Unfortunately we *have* to send something, so it
			 * is one byte that can optionally carry the ancillary data.
			 * This byte is at this moment defined to contain a character:
			 *  '0' - there is no ancillary data
			 *  '1' - ancillary data for passing a file descriptor
			 * The future may introduce a state for passing credentials.
			 * Any unknown character must be interpreted as some unknown
			 * action, and hence not supported by the server. */

			iov.iov_base = buf;
			iov.iov_len = 1;

			msgh.msg_name = 0;
			msgh.msg_namelen = 0;
			msgh.msg_iov = &iov;
			msgh.msg_iovlen = 1;
			msgh.msg_control = ccmsg;
			msgh.msg_controllen = sizeof(ccmsg);

			rv = recvmsg(msgsock, &msgh, 0);
			if (rv == -1) {
				closesocket(msgsock);
				continue;
			}

			switch (*buf) {
				case '0':
					/* nothing special, nothing to do */
				break;
				case '1':
				{	int *c_d;
					/* filedescriptor, put it in place of msgsock */
					cmsg = CMSG_FIRSTHDR(&msgh);
					closesocket(msgsock);
					if (!cmsg || cmsg->cmsg_type != SCM_RIGHTS) {
						fprintf(stderr, "!mal_mapi.listen: "
								"expected filedescriptor, but "
								"received something else\n");
						continue;
					}
					/* HACK to avoid
					 * "dereferencing type-punned pointer will break strict-aliasing rules"
					 * (with gcc 4.5.1 on Fedora 14)
					 */
					c_d = (int*)CMSG_DATA(cmsg);
					msgsock = *c_d;
				}
				break;
				default:
					/* some unknown state */
					closesocket(msgsock);
					fprintf(stderr, "!mal_mapi.listen: "
							"unknown command type in first byte\n");
					continue;
			}
#endif
		} else {
			continue;
		}
#ifdef DEBUG_SERVER
		printf("server:accepted\n");
		fflush(stdout);
#endif
		data = GDKmalloc(sizeof(*data));
		data->in = socket_rastream(msgsock, "Server read");
		data->out = socket_wastream(msgsock, "Server write");
		if (MT_create_thread(&tid, doChallenge, data, MT_THR_JOINABLE)) {
			mnstr_printf(data->out, "!internal server error (cannot fork new "
						 "client thread), please try again later\n");
			mnstr_flush(data->out);
			showException(GDKstdout, MAL, "initClient",
						  "cannot fork new client thread");
			GDKfree(data);
		}
		GDKregister(tid);
	} while (!ATOMIC_GET(serverexiting, atomicLock) &&
			 !GDKexiting());
	(void) ATOMIC_DEC(nlistener, atomicLock);
	return;
error:
	fprintf(stderr, "!mal_mapi.listen: %s, terminating listener\n", msg);
}

/**
 * Small utility function to call the sabaoth marchConnection function
 * with the right arguments.  If the socket is bound to 0.0.0.0 the
 * hostname address is used, to make the info usable for servers outside
 * localhost.
 */
static void SERVERannounce(struct in_addr addr, int port, str usockfile) {
	str buf;
	char host[128];

	if (port > 0) {
		if (addr.s_addr == INADDR_ANY) {
			gethostname(host, sizeof(host));
			host[sizeof(host) - 1] = '\0';
		} else {
			/* avoid doing this, it requires some includes that probably
			 * give trouble on windowz
			host = inet_ntoa(addr);
			 */
			sprintf(host, "%u.%u.%u.%u",
					(unsigned) ((ntohl(addr.s_addr) >> 24) & 0xff),
					(unsigned) ((ntohl(addr.s_addr) >> 16) & 0xff),
					(unsigned) ((ntohl(addr.s_addr) >> 8) & 0xff),
					(unsigned) (ntohl(addr.s_addr) & 0xff));
		}
		if ((buf = msab_marchConnection(host, port)) != NULL)
			free(buf);
		else
			/* announce that we're now reachable */
			printf("# Listening for connection requests on "
					"mapi:monetdb://%s:%i/\n", host, port);
	}
	if (usockfile != NULL) {
		port = 0;
		if ((buf = msab_marchConnection(usockfile, port)) != NULL)
			free(buf);
		else
			/* announce that we're now reachable */
			printf("# Listening for UNIX domain connection requests on "
					"mapi:monetdb://%s\n", usockfile);
	}
}

str
SERVERlisten(int *Port, str *Usockfile, int *Maxusers)
{
	struct sockaddr_in server;
	SOCKET sock = INVALID_SOCKET;
	SOCKET *psock;
	char accept_any = 0;
	char autosense = 0;
#ifdef HAVE_SYS_UN_H
	struct sockaddr_un userver;
	SOCKET usock = INVALID_SOCKET;
#endif
	SOCKLEN length = 0;
	int on = 1;
	int i = 0;
	MT_Id pid;
	int port;
	int maxusers;
	char *usockfile;
#ifdef DEBUG_SERVER
	char msg[512], host[512];
	Client cntxt= mal_clients;
#endif

	accept_any = GDKgetenv_istrue("mapi_open");
	autosense = GDKgetenv_istrue("mapi_autosense");

	/* early way out, we do not want to listen on any port when running in embedded mode */
	if (GDKgetenv_istrue("mapi_disable")) {
		return MAL_SUCCEED;
	}

	psock = GDKmalloc(sizeof(SOCKET) * 3);
	if (psock == NULL)
		throw(MAL,"mal_mapi.listen", MAL_MALLOC_FAIL);

	port = *Port;
	if (Usockfile == NULL || *Usockfile == 0 ||
		*Usockfile[0] == '\0' || strcmp(*Usockfile, str_nil) == 0)
	{
		usockfile = NULL;
	} else {
#ifdef HAVE_SYS_UN_H
		usockfile = GDKstrdup(*Usockfile);
#else
		usockfile = NULL;
		GDKfree(psock);
		throw(IO, "mal_mapi.listen", OPERATION_FAILED ": UNIX domain sockets are not supported");
#endif
	}
	maxusers = *Maxusers;
	maxusers = (maxusers ? maxusers : SERVERMAXUSERS);

	if (port <= 0 && usockfile == NULL) {
		GDKfree(psock);
		throw(ILLARG, "mal_mapi.listen", OPERATION_FAILED ": no port or socket file specified");
	}

	if (port > 65535) {
		GDKfree(psock);
		if (usockfile)
			GDKfree(usockfile);
		throw(ILLARG, "mal_mapi.listen", OPERATION_FAILED ": port number should be between 1 and 65535");
	}

	if (port > 0) {
		sock = socket(AF_INET, SOCK_STREAM, 0);
		if (sock == INVALID_SOCKET) {
			GDKfree(psock);
			if (usockfile)
				GDKfree(usockfile);
			throw(IO, "mal_mapi.listen",
				  OPERATION_FAILED ": creation of stream socket failed: %s",
#ifdef _MSC_VER
				  wsaerror(WSAGetLastError())
#else
				  strerror(errno)
#endif
				);
		}

		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &on, sizeof on) == SOCKET_ERROR) {
#ifdef _MSC_VER
			const char *err = wsaerror(WSAGetLastError());
#else
			const char *err = strerror(errno);
#endif
			GDKfree(psock);
			if (usockfile)
				GDKfree(usockfile);
			closesocket(sock);
			throw(IO, "mal_mapi.listen", OPERATION_FAILED ": setsockptr failed %s", err);
		}

		server.sin_family = AF_INET;
		if (accept_any)
			server.sin_addr.s_addr = htonl(INADDR_ANY);
		else
			server.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		for (i = 0; i < 8; i++)
			server.sin_zero[i] = 0;
		length = (SOCKLEN) sizeof(server);

		do {
			server.sin_port = htons((unsigned short) ((port) & 0xFFFF));
			if (bind(sock, (SOCKPTR) &server, length) == SOCKET_ERROR) {
				if (
#ifdef _MSC_VER
					WSAGetLastError() == WSAEADDRINUSE &&
#else
#ifdef EADDRINUSE
					errno == EADDRINUSE &&
#else
#endif
#endif
					autosense && port <= 65535)
				{
					port++;
					continue;
				}
				closesocket(sock);
				GDKfree(psock);
				if (usockfile)
					GDKfree(usockfile);
				throw(IO, "mal_mapi.listen",
					  OPERATION_FAILED ": bind to stream socket port %d "
					  "failed: %s", port,
#ifdef _MSC_VER
					  wsaerror(WSAGetLastError())
#else
					  strerror(errno)
#endif
					);
			} else {
				break;
			}
		} while (1);

		if (getsockname(sock, (SOCKPTR) &server, &length) == SOCKET_ERROR) {
			closesocket(sock);
			GDKfree(psock);
			if (usockfile)
				GDKfree(usockfile);
			throw(IO, "mal_mapi.listen",
				  OPERATION_FAILED ": failed getting socket name: %s",
#ifdef _MSC_VER
				  wsaerror(WSAGetLastError())
#else
				  strerror(errno)
#endif
				);
		}
		listen(sock, maxusers);
	}
#ifdef HAVE_SYS_UN_H
	if (usockfile) {
		usock = socket(AF_UNIX, SOCK_STREAM, 0);
		if (usock == INVALID_SOCKET ) {
			GDKfree(psock);
			GDKfree(usockfile);
			throw(IO, "mal_mapi.listen",
				  OPERATION_FAILED ": creation of UNIX socket failed: %s",
#ifdef _MSC_VER
				  wsaerror(WSAGetLastError())
#else
				  strerror(errno)
#endif
				);
		}

		/* prevent silent truncation, sun_path is typically around 108
		 * chars long :/ */
		if (strlen(usockfile) >= sizeof(userver.sun_path)) {
			char *e;
			closesocket(usock);
			GDKfree(psock);
			e = createException(MAL, "mal_mapi.listen",
					OPERATION_FAILED ": UNIX socket path too long: %s",
					usockfile);
			GDKfree(usockfile);
			return e;
		}

		userver.sun_family = AF_UNIX;
		strncpy(userver.sun_path, usockfile, sizeof(userver.sun_path));
		userver.sun_path[sizeof(userver.sun_path) - 1] = 0;

		length = (SOCKLEN) sizeof(userver);
		unlink(usockfile);
		if (bind(usock, (SOCKPTR) &userver, length) == SOCKET_ERROR) {
			char *e;
			closesocket(usock);
			unlink(usockfile);
			GDKfree(psock);
			e = createException(IO, "mal_mapi.listen",
								OPERATION_FAILED
								": binding to UNIX socket file %s failed: %s",
								usockfile,
#ifdef _MSC_VER
								wsaerror(WSAGetLastError())
#else
								strerror(errno)
#endif
				);
			GDKfree(usockfile);
			return e;
		}
		listen(usock, maxusers);
	}
#endif

#ifdef DEBUG_SERVER
	mnstr_printf(cntxt->fdout, "#SERVERlisten:Network started at %d\n", port);
#endif

	psock[0] = sock;
#ifdef HAVE_SYS_UN_H
	psock[1] = usock;
#else
	psock[1] = INVALID_SOCKET;
#endif
	psock[2] = INVALID_SOCKET;
	if (MT_create_thread(&pid, (void (*)(void *)) SERVERlistenThread, psock, MT_THR_JOINABLE) != 0) {
		GDKfree(psock);
		if (usockfile)
			GDKfree(usockfile);
		throw(MAL, "mal_mapi.listen", OPERATION_FAILED ": starting thread failed");
	}
	GDKregister(pid);
#ifdef DEBUG_SERVER
	gethostname(host, (int) 512);
	snprintf(msg, (int) 512, "#Ready to accept connections on %s:%d\n", host, port);
	mnstr_printf(cntxt->fdout, "%s", msg);
#endif

	/* seed the randomiser such that our challenges aren't
	 * predictable... */
	srand((unsigned int) GDKusec());

	SERVERannounce(server.sin_addr, port, usockfile);
	if (usockfile)
		GDKfree(usockfile);
	return MAL_SUCCEED;
}

/*
 * @- Wrappers
 * The MonetDB Version 5 wrappers are collected here
 * The latest port known to gain access is stored
 * in the database, so that others can more easily
 * be notified.
 */
str
SERVERlisten_default(int *ret)
{
	int port = SERVERPORT;
	str p;
	int maxusers = SERVERMAXUSERS;

	(void) ret;
	p = GDKgetenv("mapi_port");
	if (p)
		port = (int) strtol(p, NULL, 10);
	p = GDKgetenv("mapi_usock");
	return SERVERlisten(&port, &p, &maxusers);
}

str
SERVERlisten_usock(int *ret, str *usock)
{
	int maxusers = SERVERMAXUSERS;
	(void) ret;
	return SERVERlisten(0, usock, &maxusers);
}

str
SERVERlisten_port(int *ret, int *pid)
{
	int port = *pid;
	int maxusers = SERVERMAXUSERS;

	(void) ret;
	return SERVERlisten(&port, 0, &maxusers);
}
/*
 * The internet connection listener may be terminated from the server console,
 * or temporarily suspended to enable system maintenance.
 * It is advisable to trace the interactions of clients on the server
 * side. At least as far as it concerns requests received.
 * The kernel supports this 'spying' behavior with a file descriptor
 * field in the client record.
 */

str
SERVERstop(void *ret)
{
fprintf(stderr, "SERVERstop\n");
	ATOMIC_SET(serverexiting, 1, atomicLock);
	/* wait until they all exited, but skip the wait if the whole
	 * system is going down */
	while (ATOMIC_GET(nlistener, atomicLock) > 0 && !GDKexiting())
		MT_sleep_ms(100);
	(void) ret;		/* fool compiler */
	return MAL_SUCCEED;
}


str
SERVERsuspend(void *res)
{
	(void) res;
	ATOMIC_SET(serveractive, 0, atomicLock);
	return MAL_SUCCEED;
}

str
SERVERresume(void *res)
{
	ATOMIC_SET(serveractive, 1, atomicLock);
	(void) res;
	return MAL_SUCCEED;
}

str
SERVERclient(void *res, const Stream *In, const Stream *Out)
{
	struct challengedata *data;
	MT_Id tid;

	(void) res;
	/* in embedded mode we allow just one client */
	data = GDKmalloc(sizeof(*data));
	data->in = *In;
	data->out = *Out;
	if (MT_create_thread(&tid, doChallenge, data, MT_THR_JOINABLE)) {
		mnstr_printf(data->out, "!internal server error (cannot fork new "
					 "client thread), please try again later\n");
		mnstr_flush(data->out);
		showException(GDKstdout, MAL, "mapi.SERVERclient",
					  "cannot fork new client thread");
		free(data);
	}
	GDKregister(tid);
	return MAL_SUCCEED;
}

/*
 * @+ Remote Processing
 * The remainder of the file contains the wrappers around
 * the Mapi library used by application programmers.
 * Details on the functions can be found there.
 *
 * Sessions have a lifetime different from dynamic scopes.
 * This means the  user should use a session identifier
 * to select the correct handle.
 * For the time being we use the index in the global
 * session table. The client pointer is retained to
 * perform access control.
 *
 * We use a single result set handle. All data should be
 * consumed before continueing.
 *
 * A few extra routines should be defined to
 * dump and inspect the sessions table.
 *
 * The remote site may return a single error
 * with a series of error lines. These contain
 * then a starting !. They are all stripped here.
 */
#define catchErrors(fcn)												\
	do {																\
		int rn = mapi_error(mid);										\
		if ((rn == -4 && hdl && mapi_result_error(hdl)) || rn) {		\
			str err, newerr;											\
			str ret;													\
			size_t l;													\
			char *e, *f;												\
																		\
			if (hdl && mapi_result_error(hdl))							\
				err = mapi_result_error(hdl);							\
			else														\
				err = mapi_result_error(SERVERsessions[i].hdl);			\
																		\
			if (err == NULL)											\
				err = "(no additional error message)";					\
																		\
			l = 2 * strlen(err) + 8192;									\
			newerr = (str) GDKmalloc(l);								\
			if(newerr == NULL) { err = MAL_MALLOC_FAIL; break;}			\
																		\
			f = newerr;													\
			/* I think this code tries to deal with multiple errors, this \
			 * will fail this way if it does, since no ! is in the error \
			 * string, only newlines to separate them */				\
			for (e = err; *e && l > 1; e++) {							\
				if (*e == '!' && *(e - 1) == '\n') {					\
					snprintf(f, l, "MALException:" fcn ":remote error:"); \
					l -= strlen(f);										\
					while (*f)											\
						f++;											\
				} else {												\
					*f++ = *e;											\
					l--;												\
				}														\
			}															\
																		\
			*f = 0;														\
			ret = createException(MAL, fcn,								\
								  OPERATION_FAILED ": remote error: %s", \
								  newerr);								\
			GDKfree(newerr);											\
			return ret;													\
		}																\
	} while (0)

#define MAXSESSIONS 32
struct{
	int key;
	str dbalias;	/* logical name of the session */
	Client c;
	Mapi mid;		/* communication channel */
	MapiHdl hdl;	/* result set handle */
} SERVERsessions[MAXSESSIONS];

static int sessionkey=0;

/* #define MAPI_TEST*/

static str
SERVERconnectAll(Client cntxt, int *key, str *host, int *port, str *username, str *password, str *lang)
{
	Mapi mid;
	int i;

	MT_lock_set(&mal_contextLock);
	for(i=1; i< MAXSESSIONS; i++)
	if( SERVERsessions[i].c ==0 ) break;

	if( i==MAXSESSIONS){
		MT_lock_unset(&mal_contextLock);
		throw(IO, "mapi.connect", OPERATION_FAILED ": too many sessions");
	}
	SERVERsessions[i].c= cntxt;
	SERVERsessions[i].key= ++sessionkey;
	MT_lock_unset(&mal_contextLock);

	mid = mapi_connect(*host, *port, *username, *password, *lang, NULL);

	if (mapi_error(mid)) {
		str err = mapi_error_str(mid);
		str ex;
		if (err == NULL)
			err = "(no reason given)";
		if (err[0] == '!')
			err = err + 1;
		SERVERsessions[i].c = NULL;
		ex = createException(IO, "mapi.connect", "Could not connect: %s", err);
		mapi_destroy(mid);
		return(ex);
	}

#ifdef MAPI_TEST
	mnstr_printf(SERVERsessions[i].c->fdout,"Succeeded to establish session\n");
#endif
	SERVERsessions[i].mid= mid;
	*key = SERVERsessions[i].key;
	return MAL_SUCCEED;
}

str
SERVERdisconnectALL(int *key){
	int i;

	MT_lock_set(&mal_contextLock);

	for(i=1; i< MAXSESSIONS; i++)
		if( SERVERsessions[i].c != 0 ) {
#ifdef MAPI_TEST
	mnstr_printf(SERVERsessions[i].c->fdout,"Close session %d\n",i);
#endif
			SERVERsessions[i].c = 0;
			if( SERVERsessions[i].dbalias)
				GDKfree(SERVERsessions[i].dbalias);
			SERVERsessions[i].dbalias = NULL;
			*key = SERVERsessions[i].key;
			mapi_disconnect(SERVERsessions[i].mid);
		}

	MT_lock_unset(&mal_contextLock);

	return MAL_SUCCEED;
}

str
SERVERdisconnectWithAlias(int *key, str *dbalias){
	int i;

	MT_lock_set(&mal_contextLock);

	for(i=0; i<MAXSESSIONS; i++)
		 if( SERVERsessions[i].dbalias &&
			 strcmp(SERVERsessions[i].dbalias, *dbalias)==0){
				SERVERsessions[i].c = 0;
				if( SERVERsessions[i].dbalias)
					GDKfree(SERVERsessions[i].dbalias);
				SERVERsessions[i].dbalias = NULL;
				*key = SERVERsessions[i].key;
				mapi_disconnect(SERVERsessions[i].mid);
				break;
		}

	if( i==MAXSESSIONS){
		MT_lock_unset(&mal_contextLock);
		throw(IO, "mapi.disconnect", "Impossible to close session for db_alias: '%s'", *dbalias);
	}

	MT_lock_unset(&mal_contextLock);
	return MAL_SUCCEED;
}

str
SERVERconnect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	int *key =getArgReference_int(stk,pci,0);
	str *host = getArgReference_str(stk,pci,1);
	int *port = getArgReference_int(stk,pci,2);
	str *username = getArgReference_str(stk,pci,3);
	str *password= getArgReference_str(stk,pci,4);
	str *lang = getArgReference_str(stk,pci,5);

	(void) mb;
	return SERVERconnectAll(cntxt, key,host,port,username,password,lang);
}


str
SERVERreconnectAlias(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *key =getArgReference_int(stk,pci,0);
	str *host = getArgReference_str(stk,pci,1);
	int *port = getArgReference_int(stk,pci,2);
	str *dbalias = getArgReference_str(stk,pci,3);
	str *username = getArgReference_str(stk,pci,4);
	str *password= getArgReference_str(stk,pci,5);
	str *lang = getArgReference_str(stk,pci,6);
	int i;
	str msg=MAL_SUCCEED;

	(void) mb;

	for(i=0; i<MAXSESSIONS; i++)
	 if( SERVERsessions[i].key &&
		 SERVERsessions[i].dbalias &&
		 strcmp(SERVERsessions[i].dbalias, *dbalias)==0){
			*key = SERVERsessions[i].key;
			return msg;
	}

	msg= SERVERconnectAll(cntxt, key, host, port, username, password, lang);
	if( msg == MAL_SUCCEED)
		msg = SERVERsetAlias(NULL, key, dbalias);
	return msg;
}

str
SERVERreconnectWithoutAlias(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	int *key =getArgReference_int(stk,pci,0);
	str *host = getArgReference_str(stk,pci,1);
	int *port = getArgReference_int(stk,pci,2);
	str *username = getArgReference_str(stk,pci,3);
	str *password= getArgReference_str(stk,pci,4);
	str *lang = getArgReference_str(stk,pci,5);
	int i;
	str msg=MAL_SUCCEED, nme= "anonymous";

	(void) mb;

	for(i=0; i<MAXSESSIONS; i++)
	 if( SERVERsessions[i].key ){
			*key = SERVERsessions[i].key;
			return msg;
	}

	msg= SERVERconnectAll(cntxt, key, host, port, username, password, lang);
	if( msg == MAL_SUCCEED)
		msg = SERVERsetAlias(NULL, key, &nme);
	return msg;
}

#define accessTest(val, fcn)										\
	do {															\
		for(i=0; i< MAXSESSIONS; i++)								\
			if( SERVERsessions[i].c &&								\
				SERVERsessions[i].key== (val)) break;				\
		if( i== MAXSESSIONS)										\
			throw(MAL, "mapi." fcn, "Access violation,"				\
				  " could not find matching session descriptor");	\
		mid= SERVERsessions[i].mid;									\
		(void) mid; /* silence compilers */							\
	} while (0)

str
SERVERsetAlias(void *ret, int *key, str *dbalias){
	int i;
	Mapi mid;
	accessTest(*key, "setAlias");
    SERVERsessions[i].dbalias= GDKstrdup(*dbalias);
	(void) ret;
	return MAL_SUCCEED;
}

str
SERVERlookup(int *ret, str *dbalias)
{
	int i;
	for(i=0; i< MAXSESSIONS; i++)
	if( SERVERsessions[i].dbalias &&
		strcmp(SERVERsessions[i].dbalias, *dbalias)==0){
		*ret= SERVERsessions[i].key;
		return MAL_SUCCEED;
	}
	throw(MAL, "mapi.lookup", "Could not find database connection");
}

str
SERVERtrace(void *ret, int *key, int *flag){
	(void )ret;
	mapi_trace(SERVERsessions[*key].mid,*flag);
	return MAL_SUCCEED;
}

str
SERVERdisconnect(void *ret, int *key){
	int i;
	Mapi mid;
	(void) ret;
	accessTest(*key, "disconnect");
	mapi_disconnect(mid);
	if( SERVERsessions[i].dbalias)
		GDKfree(SERVERsessions[i].dbalias);
	SERVERsessions[i].c= 0;
	SERVERsessions[i].dbalias= 0;
	return MAL_SUCCEED;
}

str
SERVERdestroy(void *ret, int *key){
	int i;
	Mapi mid;
	(void) ret;
	accessTest(*key, "destroy");
	mapi_destroy(mid);
	SERVERsessions[i].c= 0;
	if( SERVERsessions[i].dbalias)
		GDKfree(SERVERsessions[i].dbalias);
	SERVERsessions[i].dbalias= 0;
	return MAL_SUCCEED;
}

str
SERVERreconnect(void *ret, int *key){
	int i;
	Mapi mid;
	(void) ret;
	accessTest(*key, "destroy");
	mapi_reconnect(mid);
	return MAL_SUCCEED;
}

str
SERVERping(int *ret, int *key){
	int i;
	Mapi mid;
	accessTest(*key, "destroy");
	*ret= mapi_ping(mid);
	return MAL_SUCCEED;
}

str
SERVERquery(int *ret, int *key, str *qry){
	Mapi mid;
	MapiHdl hdl=0;
	int i;
	accessTest(*key, "query");
	if( SERVERsessions[i].hdl)
		mapi_close_handle(SERVERsessions[i].hdl);
	SERVERsessions[i].hdl = mapi_query(mid, *qry);
	catchErrors("mapi.query");
	*ret = *key;
	return MAL_SUCCEED;
}

str
SERVERquery_handle(int *ret, int *key, str *qry){
	Mapi mid;
	MapiHdl hdl=0;
	int i;
	accessTest(*key, "query_handle");
	mapi_query_handle(SERVERsessions[i].hdl, *qry);
	catchErrors("mapi.query_handle");
	*ret = *key;
	return MAL_SUCCEED;
}

str
SERVERquery_array(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc){
	(void)cntxt, (void) mb; (void) stk; (void) pc;
	throw(MAL, "mapi.query_array","not yet implemented");
}

str
SERVERprepare(int *ret, int *key, str *qry){
	Mapi mid;
	int i;
	accessTest(*key, "prepare");
	if( SERVERsessions[i].hdl)
		mapi_close_handle(SERVERsessions[i].hdl);
	SERVERsessions[i].hdl= mapi_prepare(mid, *qry);
	if( mapi_error(mid) )
		throw(MAL, "mapi.prepare", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	*ret = *key;
	return MAL_SUCCEED;
}

str
SERVERfinish(int *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "finish");
	mapi_finish(SERVERsessions[i].hdl);
	if( mapi_error(mid) )
		throw(MAL, "mapi.finish", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	*ret = *key;
	return MAL_SUCCEED;
}

str
SERVERget_row_count(lng *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "get_row_count");
	*ret= mapi_get_row_count(SERVERsessions[i].hdl);
	if( mapi_error(mid) )
		throw(MAL, "mapi.get_row_count", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	return MAL_SUCCEED;
}

str
SERVERget_field_count(int *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "get_field_count");
	*ret= mapi_get_field_count(SERVERsessions[i].hdl);
	if( mapi_error(mid) )
		throw(MAL, "mapi.get_field_count", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	return MAL_SUCCEED;
}

str
SERVERrows_affected(lng *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "rows_affected");
	*ret= mapi_rows_affected(SERVERsessions[i].hdl);
	return MAL_SUCCEED;
}

str
SERVERfetch_row(int *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "fetch_row");
	*ret= mapi_fetch_row(SERVERsessions[i].hdl);
	return MAL_SUCCEED;
}

str
SERVERfetch_all_rows(lng *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "fetch_all_rows");
	*ret= mapi_fetch_all_rows(SERVERsessions[i].hdl);
	return MAL_SUCCEED;
}

str
SERVERfetch_field_str(str *ret, int *key, int *fnr){
	Mapi mid;
	int i;
	str fld;
	accessTest(*key, "fetch_field");
	fld= mapi_fetch_field(SERVERsessions[i].hdl,*fnr);
	*ret= GDKstrdup(fld? fld: str_nil);
	if( mapi_error(mid) )
		throw(MAL, "mapi.fetch_field_str", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	return MAL_SUCCEED;
}

str
SERVERfetch_field_int(int *ret, int *key, int *fnr){
	Mapi mid;
	int i;
	str fld;
	accessTest(*key, "fetch_field");
	fld= mapi_fetch_field(SERVERsessions[i].hdl,*fnr);
	*ret= fld? (int) atol(fld): int_nil;
	if( mapi_error(mid) )
		throw(MAL, "mapi.fetch_field_int", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	return MAL_SUCCEED;
}

str
SERVERfetch_field_lng(lng *ret, int *key, int *fnr){
	Mapi mid;
	int i;
	str fld;
	accessTest(*key, "fetch_field");
	fld= mapi_fetch_field(SERVERsessions[i].hdl,*fnr);
	*ret= fld? atol(fld): lng_nil;
	if( mapi_error(mid) )
		throw(MAL, "mapi.fetch_field_lng", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	return MAL_SUCCEED;
}

#ifdef HAVE_HGE
str
SERVERfetch_field_hge(hge *ret, int *key, int *fnr){
	Mapi mid;
	int i;
	str fld;
	accessTest(*key, "fetch_field");
	fld= mapi_fetch_field(SERVERsessions[i].hdl,*fnr);
	*ret= fld? atol(fld): hge_nil;
	if( mapi_error(mid) )
		throw(MAL, "mapi.fetch_field_hge", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	return MAL_SUCCEED;
}
#endif

str
SERVERfetch_field_sht(sht *ret, int *key, int *fnr){
	Mapi mid;
	int i;
	str fld;
	accessTest(*key, "fetch_field");
	fld= mapi_fetch_field(SERVERsessions[i].hdl,*fnr);
	*ret= fld? (sht) atol(fld): sht_nil;
	if( mapi_error(mid) )
		throw(MAL, "mapi.fetch_field", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	return MAL_SUCCEED;
}

str
SERVERfetch_field_void(void *ret, int *key, int *fnr){
	Mapi mid;
	int i;
	(void) ret;
	(void) fnr;
	accessTest(*key, "fetch_field");
	throw(MAL, "mapi.fetch_field_void","defaults to nil");
}

str
SERVERfetch_field_oid(oid *ret, int *key, int *fnr){
	Mapi mid;
	int i;
	str fld;
	accessTest(*key, "fetch_field");
	fld= mapi_fetch_field(SERVERsessions[i].hdl,*fnr);
	if( mapi_error(mid) )
		throw(MAL, "mapi.fetch_field_oid", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	if(fld==0 || strcmp(fld,"nil")==0)
		*(oid*) ret= void_nil;
	else *(oid*) ret = (oid) atol(fld);
	return MAL_SUCCEED;
}

str
SERVERfetch_field_bte(bte *ret, int *key, int *fnr){
	Mapi mid;
	int i;
	str fld;
	accessTest(*key, "fetch_field");
	fld= mapi_fetch_field(SERVERsessions[i].hdl,*fnr);
	if( mapi_error(mid) )
		throw(MAL, "mapi.fetch_field_bte", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	if(fld==0 || strcmp(fld,"nil")==0)
		*(bte*) ret= bte_nil;
	else *(bte*) ret = *fld;
	return MAL_SUCCEED;
}

str
SERVERfetch_line(str *ret, int *key){
	Mapi mid;
	int i;
	str fld;
	accessTest(*key, "fetch_line");
	fld= mapi_fetch_line(SERVERsessions[i].hdl);
	if( mapi_error(mid) )
		throw(MAL, "mapi.fetch_line", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	*ret= GDKstrdup(fld? fld:str_nil);
	return MAL_SUCCEED;
}

str
SERVERnext_result(int *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "next_result");
	mapi_next_result(SERVERsessions[i].hdl);
	if( mapi_error(mid) )
		throw(MAL, "mapi.next_result", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	*ret= *key;
	return MAL_SUCCEED;
}

str
SERVERfetch_reset(int *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "fetch_reset");
	mapi_fetch_reset(SERVERsessions[i].hdl);
	if( mapi_error(mid) )
		throw(MAL, "mapi.fetch_reset", "%s",
			mapi_result_error(SERVERsessions[i].hdl));
	*ret= *key;
	return MAL_SUCCEED;
}

str
SERVERfetch_field_bat(bat *bid, int *key){
	int i,j,cnt;
	Mapi mid;
	char *fld;
	BAT *b;

	accessTest(*key, "rpc");
	b= COLnew(0,TYPE_str,256, TRANSIENT);
	if( b == NULL)
		throw(MAL,"mapi.fetch",MAL_MALLOC_FAIL);
	cnt= mapi_get_field_count(SERVERsessions[i].hdl);
	for(j=0; j< cnt; j++){
		fld= mapi_fetch_field(SERVERsessions[i].hdl,j);
		if( mapi_error(mid) ) {
			*bid = b->batCacheid;
			BBPkeepref(*bid);
			throw(MAL, "mapi.fetch_field_bat", "%s",
				mapi_result_error(SERVERsessions[i].hdl));
		}
		BUNappend(b,fld, FALSE);
	}
	if (!(b->batDirty&2)) BATsetaccess(b, BAT_READ);
	*bid = b->batCacheid;
	BBPkeepref(*bid);
	return MAL_SUCCEED;
}

str
SERVERerror(int *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "error");
	*ret= mapi_error(mid);
	return MAL_SUCCEED;
}

str
SERVERgetError(str *ret, int *key){
	Mapi mid;
	int i;
	accessTest(*key, "getError");
	*ret= GDKstrdup(mapi_error_str(mid));
	return MAL_SUCCEED;
}

str
SERVERexplain(str *ret, int *key){
	Mapi mid;
	int i;

	accessTest(*key, "explain");
	*ret= GDKstrdup(mapi_error_str(mid));
	return MAL_SUCCEED;
}
/*
 * The remainder should contain the wrapping of
 * relevant SERVER functions. Furthermore, we
 * should analyse the return value and update
 * the stack trace.
 *
 * Two routines should be
 * mapi.rpc(key,"query")
 *
 * The generic scheme for handling a remote MAL
 * procedure call with a single row answer.
 */
static void SERVERfieldAnalysis(str fld, int tpe, ValPtr v){
	v->vtype= tpe;
	switch(tpe){
	case TYPE_void:
		v->val.oval = void_nil;
		break;
	case TYPE_oid:
		if(fld==0 || strcmp(fld,"nil")==0)
			v->val.oval= void_nil;
		else v->val.oval = (oid) atol(fld);
		break;
	case TYPE_bit:
		if(fld== 0 || strcmp(fld,"nil")==0)
			v->val.btval= bit_nil;
		else
		if(strcmp(fld,"true")==0)
			v->val.btval= TRUE;
		else
		if(strcmp(fld,"false")==0)
			v->val.btval= FALSE;
		break;
	case TYPE_bte:
		if(fld==0 || strcmp(fld,"nil")==0)
			v->val.btval= bte_nil;
		else
			v->val.btval= *fld;
		break;
	case TYPE_sht:
		if(fld==0 || strcmp(fld,"nil")==0)
			v->val.shval = sht_nil;
		else v->val.shval= (sht)  atol(fld);
		break;
	case TYPE_int:
		if(fld==0 || strcmp(fld,"nil")==0)
			v->val.ival = int_nil;
		else v->val.ival= (int)  atol(fld);
		break;
	case TYPE_lng:
		if(fld==0 || strcmp(fld,"nil")==0)
			v->val.lval= lng_nil;
		else v->val.lval= (lng)  atol(fld);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if(fld==0 || strcmp(fld,"nil")==0)
			v->val.hval= hge_nil;
		else v->val.hval= (hge)  atol(fld);
		break;
#endif
	case TYPE_flt:
		if(fld==0 || strcmp(fld,"nil")==0)
			v->val.fval= flt_nil;
		else v->val.fval= (flt)  atof(fld);
		break;
	case TYPE_dbl:
		if(fld==0 || strcmp(fld,"nil")==0)
			v->val.dval= dbl_nil;
		else v->val.dval= (dbl)  atof(fld);
		break;
	case TYPE_str:
		if(fld==0 || strcmp(fld,"nil")==0){
			v->val.sval= GDKstrdup(str_nil);
			v->len= (int) strlen(v->val.sval);
		} else {
			v->val.sval= GDKstrdup(fld);
			v->len= (int) strlen(fld);
		}
		break;
	}
}

str
SERVERmapi_rpc_single_row(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int key,i,j;
	Mapi mid;
	MapiHdl hdl;
	char *s,*fld, *qry=0;

	(void) cntxt;
	key= * getArgReference_int(stk,pci,pci->retc);
	accessTest(key, "rpc");
#ifdef MAPI_TEST
	mnstr_printf(cntxt->fdout,"about to send: %s\n",qry);
#endif
	/* glue all strings together */
	for(i= pci->retc+1; i<pci->argc; i++){
		fld= * getArgReference_str(stk,pci,i);
		if( qry == 0)
			qry= GDKstrdup(fld);
		else {
			s= (char*) GDKmalloc(strlen(qry)+strlen(fld)+1);
			if ( s == NULL) {
				GDKfree(qry);
				throw(MAL, "mapi.rpc",MAL_MALLOC_FAIL);
			}
			strcpy(s,qry);
			strcat(s,fld);
			GDKfree(qry);
			qry= s;
		}
	}
	hdl= mapi_query(mid, qry);
	GDKfree(qry);
	catchErrors("mapi.rpc");

	i= 0;
	while( mapi_fetch_row(hdl)){
		for(j=0; j<pci->retc; j++){
			fld= mapi_fetch_field(hdl,j);
#ifdef MAPI_TEST
			mnstr_printf(cntxt->fdout,"Got: %s\n",fld);
#endif
			switch(getVarType(mb,getArg(pci,j)) ){
			case TYPE_void:
			case TYPE_oid:
			case TYPE_bit:
			case TYPE_bte:
			case TYPE_sht:
			case TYPE_int:
			case TYPE_lng:
#ifdef HAVE_HGE
			case TYPE_hge:
#endif
			case TYPE_flt:
			case TYPE_dbl:
			case TYPE_str:
				SERVERfieldAnalysis(fld,
					getVarType(mb,getArg(pci,j)),
					&stk->stk[pci->argv[j]]);
				break;
			default:
				throw(MAL, "mapi.rpc",
						"Missing type implementation ");
			/* all the other basic types come here */
			}
		}
		i++;
	}
	if( i>1)
		throw(MAL, "mapi.rpc","Too many answers");
	return MAL_SUCCEED;
}
/*
 * Transport of the BATs is only slightly more complicated.
 * The generic implementation based on a pattern is the next
 * step.
 */
str
SERVERmapi_rpc_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	bat *ret;
	int *key;
	str *qry,err= MAL_SUCCEED;
	Mapi mid;
	MapiHdl hdl;
	char *fld2;
	BAT *b;
	ValRecord tval;
	int i=0, tt;

	(void) cntxt;
	ret= getArgReference_bat(stk,pci,0);
	key= getArgReference_int(stk,pci,pci->retc);
	qry= getArgReference_str(stk,pci,pci->retc+1);
	accessTest(*key, "rpc");
	tt= getBatType(getVarType(mb,getArg(pci,0)));

	hdl= mapi_query(mid, *qry);
	catchErrors("mapi.rpc");

	b= COLnew(0,tt,256, TRANSIENT);
	if ( b == NULL)
		throw(MAL,"mapi.rpc",MAL_MALLOC_FAIL);
	while( mapi_fetch_row(hdl)){
		fld2= mapi_fetch_field(hdl,1);
		SERVERfieldAnalysis(fld2, tt, &tval);
		BUNappend(b,VALptr(&tval), FALSE);
	}
	if (!(b->batDirty&2)) BATsetaccess(b, BAT_READ);
	*ret = b->batCacheid;
	BBPkeepref(*ret);

	return err;
}

str
SERVERput(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	int *key;
	str *nme;
	ptr val;
	int i,tpe;
	Mapi mid;
	MapiHdl hdl=0;
	char *w=0, buf[BUFSIZ];

	(void) cntxt;
	key= getArgReference_int(stk,pci,pci->retc);
	nme= getArgReference_str(stk,pci,pci->retc+1);
	val= getArgReference(stk,pci,pci->retc+2);
	accessTest(*key, "put");
	switch( (tpe=getArgType(mb,pci, pci->retc+2)) ){
	case TYPE_bat:{
		/* generate a tuple batch */
		/* and reload it into the proper format */
		str ht,tt;
		BAT *b= BATdescriptor(BBPindex(*nme));
		int len;

		if( b== NULL){
			throw(MAL,"mapi.put","Can not access BAT");
		}

		/* reconstruct the object */
		ht = getTypeName(TYPE_oid);
		tt = getTypeName(getBatType(tpe));
		snprintf(buf,BUFSIZ,"%s:= bat.new(:%s,%s);", *nme, ht,tt );
		len = (int) strlen(buf);
		snprintf(buf+len,BUFSIZ-len,"%s:= io.import(%s,tuples);", *nme, *nme);

		/* and execute the request */
		if( SERVERsessions[i].hdl)
			mapi_close_handle(SERVERsessions[i].hdl);
		SERVERsessions[i].hdl= mapi_query(mid, buf);

		GDKfree(ht); GDKfree(tt);
		BBPdecref(b->batCacheid,TRUE);
		break;
		}
	case TYPE_str:
		snprintf(buf,BUFSIZ,"%s:=%s;",*nme,*(char**)val);
		if( SERVERsessions[i].hdl)
			mapi_close_handle(SERVERsessions[i].hdl);
		SERVERsessions[i].hdl= mapi_query(mid, buf);
		break;
	default:
		ATOMformat(tpe,val,&w);
		snprintf(buf,BUFSIZ,"%s:=%s;",*nme,w);
		GDKfree(w);
		if( SERVERsessions[i].hdl)
			mapi_close_handle(SERVERsessions[i].hdl);
		SERVERsessions[i].hdl= mapi_query(mid, buf);
		break;
	}
	catchErrors("mapi.put");
	return MAL_SUCCEED;
}

str
SERVERputLocal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	str *ret, *nme;
	ptr val;
	int tpe;
	char *w=0, buf[BUFSIZ];

	(void) cntxt;
	ret= getArgReference_str(stk,pci,0);
	nme= getArgReference_str(stk,pci,pci->retc);
	val= getArgReference(stk,pci,pci->retc+1);
	switch( (tpe=getArgType(mb,pci, pci->retc+1)) ){
	case TYPE_bat:
	case TYPE_ptr:
		throw(MAL, "mapi.glue","Unsupported type");
	case TYPE_str:
		snprintf(buf,BUFSIZ,"%s:=%s;",*nme,*(char**)val);
		break;
	default:
		ATOMformat(tpe,val,&w);
		snprintf(buf,BUFSIZ,"%s:=%s;",*nme,w);
		GDKfree(w);
		break;
	}
	*ret= GDKstrdup(buf);
	return MAL_SUCCEED;
}

str
SERVERbindBAT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	int *key;
	str *nme,*tab,*col;
	int i;
	Mapi mid;
	MapiHdl hdl=0;
	char buf[BUFSIZ];

	(void) cntxt;
	key= getArgReference_int(stk,pci,pci->retc);
	nme= getArgReference_str(stk,pci,pci->retc+1);
	accessTest(*key, "bind");
	if( pci->argc == 6) {
		char *tn;
		tab= getArgReference_str(stk,pci,pci->retc+2);
		col= getArgReference_str(stk,pci,pci->retc+3);
		i= *getArgReference_int(stk,pci,pci->retc+4);
		tn = getTypeName(getBatType(getVarType(mb,getDestVar(pci))));
		snprintf(buf,BUFSIZ,"%s:bat[:%s]:=sql.bind(\"%s\",\"%s\",\"%s\",%d);",
			getVarName(mb,getDestVar(pci)),
			tn,
			*nme, *tab,*col,i);
		GDKfree(tn);
	} else if( pci->argc == 5) {
		tab= getArgReference_str(stk,pci,pci->retc+2);
		i= *getArgReference_int(stk,pci,pci->retc+3);
		snprintf(buf,BUFSIZ,"%s:bat[:oid]:=sql.bind(\"%s\",\"%s\",0,%d);",
			getVarName(mb,getDestVar(pci)),*nme, *tab,i);
	} else {
		str hn,tn;
		int target= getArgType(mb,pci,0);
		hn= getTypeName(TYPE_oid);
		tn= getTypeName(getBatType(target));
		snprintf(buf,BUFSIZ,"%s:bat[:%s]:=bbp.bind(\"%s\");",
			getVarName(mb,getDestVar(pci)), tn, *nme);
		GDKfree(hn);
		GDKfree(tn);
	}
	if( SERVERsessions[i].hdl)
		mapi_close_handle(SERVERsessions[i].hdl);
	SERVERsessions[i].hdl= mapi_query(mid, buf);
	catchErrors("mapi.bind");
	return MAL_SUCCEED;
}
#else
// this avoids a compiler warning w.r.t. empty compilation units.
int SERVERdummy = 42;
#endif // HAVE_MAPI
