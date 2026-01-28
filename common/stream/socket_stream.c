/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/* Generic stream handling code such as init and close */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_IOCTL_H
#include <sys/ioctl.h>
#endif


/* ------------------------------------------------------------------ */
/* streams working on a socket */

static int
socket_getoob(stream *s)
{
	SOCKET fd = s->stream_data.s;
#ifdef HAVE_POLL
	struct pollfd pfd = {
		.fd = fd,
		.events = POLLPRI,
	};
	if (poll(&pfd, 1, 0) > 0)
#else
	fd_set xfds;
	struct timeval t = {
		.tv_sec = 0,
		.tv_usec = 0,
	};
#ifndef _MSC_VER
#ifdef FD_SETSIZE
	if (fd >= FD_SETSIZE)
		return 0;
#endif
#endif
	FD_ZERO(&xfds);
	FD_SET(fd, &xfds);
	if (select(
#ifdef _MSC_VER
			0,	/* ignored on Windows */
#else
			fd + 1,
#endif
			NULL, NULL, &xfds, &t) > 0)
#endif
	{
#ifdef HAVE_POLL
		if (pfd.revents & (POLLHUP | POLLNVAL))
			return -1;
		if ((pfd.revents & POLLPRI) == 0)
			return -1;
#else
		if (!FD_ISSET(fd, &xfds))
			return 0;
#endif
		/* discard regular data until OOB mark */
#ifndef _MSC_VER				/* Windows has to be different... */
		for (;;) {
			int atmark = 0;
			char flush[100];
#ifdef HAVE_SOCKATMARK
			if ((atmark = sockatmark(fd)) < 0) {
				perror("sockatmark");
				break;
			}
#else
			if (ioctlsocket(fd, SIOCATMARK, &atmark) < 0) {
				perror("ioctl");
				break;
			}
#endif
			if (atmark)
				break;
			if (recv(fd, flush, sizeof(flush), 0) < 0) {
				perror("recv");
				break;
			}
		}
#endif
		unsigned char b = 0;
		switch (recv(fd, &b, 1, MSG_OOB)) {
		case 0:
			/* unexpectedly didn't receive a byte */
			break;
		case 1:
			return b;
		case -1:
			perror("recv OOB");
			return -1;
		}
	}
	return 0;
}

static int
socket_putoob(stream *s, char val)
{
	SOCKET fd = s->stream_data.s;
	if (send(fd, &val, 1, MSG_OOB) == -1) {
		perror("send OOB");
		return -1;
	}
	return 0;
}

#ifdef HAVE_SYS_UN_H
/* UNIX domain sockets do not support OOB messages, so we need to do
 * something different */
#define OOBMSG0	'\377'			/* the two special bytes we send as "OOB" */
#define OOBMSG1	'\377'

static int
socket_getoob_unix(stream *s)
{
	SOCKET fd = s->stream_data.s;
#ifdef HAVE_POLL
	struct pollfd pfd = {
		.fd = fd,
		.events = POLLIN,
	};
	if (poll(&pfd, 1, 0) > 0)
#else
	fd_set fds;
	struct timeval t = {
		.tv_sec = 0,
		.tv_usec = 0,
	};
#ifndef _MSC_VER
#ifdef FD_SETSIZE
	if (fd >= FD_SETSIZE)
		return 0;
#endif
#endif
	FD_ZERO(&fds);
	FD_SET(fd, &fds);
	if (select(
#ifdef _MSC_VER
			0,	/* ignored on Windows */
#else
			fd + 1,
#endif
			&fds, NULL, NULL, &t) > 0)
#endif
		{
			char buf[3];
			ssize_t nr;
			nr = recv(fd, buf, 2, MSG_PEEK);
			if (nr == 2 && buf[0] == OOBMSG0 && buf[1] == OOBMSG1) {
				nr = recv(fd, buf, 3, 0);
				if (nr == 3)
					return (unsigned char) buf[2];
			}
		}
	return 0;
}

static int
socket_putoob_unix(stream *s, char val)
{
	const char buf[3] = {
		OOBMSG0,
		OOBMSG1,
		val,
	};
	if (send(s->stream_data.s, buf, 3, 0) == -1) {
		perror("send");
		return -1;
	}
	return 0;
}
#endif

static ssize_t
socket_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt, res = 0;
#ifdef _MSC_VER
	int nr = 0;
#else
	ssize_t nr = 0;
#endif

	if (s->errkind != MNSTR_NO__ERROR)
		return -1;

	if (size == 0 || elmsize == 0)
		return (ssize_t) cnt;

	errno = 0;
	while (res < size &&
	       (
		       /* Windows send works on int, make sure the argument fits */
		       ((nr = send(s->stream_data.s, (const char *) buf + res,
#ifdef _MSC_VER
						   (int) min(size - res, 1 << 16)
#else
						   size - res
#endif
						   , 0)) > 0)
		       || (nr < 0 &&	/* syscall failed */
			   s->timeout > 0 &&	/* potentially timeout */
#ifdef _MSC_VER
			   WSAGetLastError() == WSAEWOULDBLOCK &&
#else
			   (errno == EAGAIN
#if EAGAIN != EWOULDBLOCK
			    || errno == EWOULDBLOCK
#endif
				   ) &&	/* it was! */
#endif
			   s->timeout_func != NULL &&	/* callback function exists */
			   !s->timeout_func(s->timeout_data))	/* callback says don't stop */
		       || (nr < 0 &&
#ifdef _MSC_VER
			  WSAGetLastError() == WSAEINTR
#else
			  errno == EINTR
#endif
			       ))	/* interrupted */
		) {
		errno = 0;
#ifdef _MSC_VER
		WSASetLastError(0);
#endif
		if (nr > 0)
			res += (size_t) nr;
	}
	if (res >= elmsize)
		return (ssize_t) (res / elmsize);
	if (nr < 0) {
		if (s->timeout > 0 &&
#ifdef _MSC_VER
		    WSAGetLastError() == WSAEWOULDBLOCK
#else
		    (errno == EAGAIN
#if EAGAIN != EWOULDBLOCK
		     || errno == EWOULDBLOCK
#endif
			    )
#endif
			)
			mnstr_set_error(s, MNSTR_TIMEOUT, NULL);
		else
			mnstr_set_error_errno(s, MNSTR_WRITE_ERROR, "socket write");
		return -1;
	}
	return 0;
}

static ssize_t
socket_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
#ifdef _MSC_VER
	int nr = 0;
	int size;
	if (elmsize * cnt > INT_MAX)
		size = (int) (elmsize * (INT_MAX / elmsize));
	else
		size = (int) (elmsize * cnt);
#else
	ssize_t nr = 0;
	size_t size = elmsize * cnt;
#endif

	if (s->errkind != MNSTR_NO__ERROR)
		return -1;
	if (size == 0)
		return 0;

	for (;;) {
		if (s->timeout) {
			int ret;
#ifdef HAVE_POLL
			struct pollfd pfd = {
				.fd = s->stream_data.s,
				.events = POLLIN
			};
#ifdef HAVE_SYS_UN_H
			if (s->putoob != socket_putoob_unix)
				pfd.events |= POLLPRI;
#endif

			ret = poll(&pfd, 1, (int) s->timeout);
			if (ret == -1) {
				if (errno == EINTR)
					continue;
				mnstr_set_error_errno(s, MNSTR_READ_ERROR, "poll error");
				return -1;
			}
			if (ret == 1) {
				if (pfd.revents & POLLHUP) {
					/* hung up, return EOF */
					s->eof = true;
					return 0;
				}
				if (pfd.revents & POLLPRI) {
					/* discard regular data until OOB mark */
					for (;;) {
						int atmark = 0;
						char flush[100];
#ifdef HAVE_SOCKATMARK
						if ((atmark = sockatmark(s->stream_data.s)) < 0) {
							perror("sockatmark");
							break;
						}
#else
						if (ioctlsocket(s->stream_data.s, SIOCATMARK, &atmark) < 0) {
							perror("ioctl");
							break;
						}
#endif
						if (atmark)
							break;
						if (recv(s->stream_data.s, flush, sizeof(flush), 0) < 0) {
							perror("recv");
							break;
						}
					}
					char b = 0;
					switch (recv(s->stream_data.s, &b, 1, MSG_OOB)) {
					case 0:
						/* unexpectedly didn't receive a byte */
						continue;
					case 1:
						mnstr_set_error(s, MNSTR_INTERRUPT, "query abort from client");
						return -1;
					case -1:
						mnstr_set_error_errno(s, MNSTR_READ_ERROR, "recv error");
						return -1;
					}
				}
			}
#else
			struct timeval tv;
			fd_set fds, xfds;

			errno = 0;
#ifdef _MSC_VER
			WSASetLastError(0);
#endif
			FD_ZERO(&fds);
			FD_SET(s->stream_data.s, &fds);
			FD_ZERO(&xfds);
			FD_SET(s->stream_data.s, &xfds);
			tv.tv_sec = s->timeout / 1000;
			tv.tv_usec = (s->timeout % 1000) * 1000;
			ret = select(
#ifdef _MSC_VER
				0,	/* ignored on Windows */
#else
				s->stream_data.s + 1,
#endif
				&fds, NULL, &xfds, &tv);
			if (ret == SOCKET_ERROR) {
				mnstr_set_error_errno(s, MNSTR_READ_ERROR, "select");
				return -1;
			}
			if (ret > 0 && FD_ISSET(s->stream_data.s, &xfds)) {
				/* discard regular data until OOB mark */
#ifndef _MSC_VER				/* Windows has to be different... */
				for (;;) {
					int atmark = 0;
					char flush[100];
#ifdef HAVE_SOCKATMARK
					if ((atmark = sockatmark(s->stream_data.s)) < 0) {
						perror("sockatmark");
						break;
					}
#else
					if (ioctlsocket(s->stream_data.s, SIOCATMARK, &atmark) < 0) {
						perror("ioctl");
						break;
					}
#endif
					if (atmark)
						break;
					if (recv(s->stream_data.s, flush, sizeof(flush), 0) < 0) {
						perror("recv");
						break;
					}
				}
#endif
				char b = 0;
				switch (recv(s->stream_data.s, &b, 1, MSG_OOB)) {
				case 0:
					/* unexpectedly didn't receive a byte */
					continue;
				case 1:
					mnstr_set_error(s, MNSTR_INTERRUPT, "query abort from client");
					return -1;
				case -1:
					mnstr_set_error_errno(s, MNSTR_READ_ERROR, "recv error");
					return -1;
				}
				continue;		/* try again */
			}
#endif
			if (ret == 0) {
				if (s->timeout_func == NULL || s->timeout_func(s->timeout_data)) {
					mnstr_set_error(s, MNSTR_TIMEOUT, NULL);
					return -1;
				}
				continue;
			}
#ifdef HAVE_POLL
			assert(pfd.revents & (POLLIN|POLLHUP));
#else
			assert(FD_ISSET(s->stream_data.s, &fds) || FD_ISSET(s->stream_data.s, &xfds));
#endif
		}
		nr = recv(s->stream_data.s, buf, size, 0);
		if (nr == SOCKET_ERROR) {
			mnstr_set_error_errno(s, errno == EINTR ? MNSTR_INTERRUPT : MNSTR_READ_ERROR, NULL);
			return -1;
		}
		break;
	}
	if (nr == 0) {
		s->eof = true;
		return 0;	/* end of file */
	}
	if (elmsize > 1) {
		while ((size_t) nr % elmsize != 0) {
			/* if elmsize > 1, we really expect that "the
			 * other side" wrote complete items in a
			 * single system call, so we expect to at
			 * least receive complete items, and hence we
			 * continue reading until we did in fact
			 * receive an integral number of complete
			 * items, ignoring any timeouts (but not real
			 * errors) (note that recursion is limited
			 * since we don't propagate the element size
			 * to the recursive call) */
			ssize_t n;
			n = socket_read(s, (char *) buf + nr, 1, size - (size_t) nr);
			if (n < 0) {
				if (s->errkind == MNSTR_NO__ERROR)
					mnstr_set_error(s, MNSTR_READ_ERROR, "socket_read failed");
				return -1;
			}
			if (n == 0)	/* unexpected end of file */
				break;
			nr +=
#ifdef _MSC_VER
				(int)
#endif
				n;
		}
	}
	return nr / (ssize_t) elmsize;
}

static void
socket_close(stream *s)
{
	SOCKET fd = s->stream_data.s;

	if (fd != INVALID_SOCKET) {
		/* Related read/write (in/out, from/to) streams
		 * share a single socket which is not dup'ed (anymore)
		 * as Windows' dup doesn't work on sockets;
		 * hence, only one of the streams must/may close that
		 * socket; we choose to let the read socket do the
		 * job, since in mapi.c it may happen that the read
		 * stream is closed before the write stream was even
		 * created.
		 */
		if (s->readonly) {
#ifdef HAVE_SHUTDOWN
			shutdown(fd, SHUT_RDWR);
#endif
			closesocket(fd);
		}
	}
	s->stream_data.s = INVALID_SOCKET;
}

static void
socket_update_timeout(stream *s)
{
	SOCKET fd = s->stream_data.s;
	struct timeval tv;

	if (fd == INVALID_SOCKET)
		return;
	tv.tv_sec = s->timeout / 1000;
	tv.tv_usec = (s->timeout % 1000) * 1000;
	/* cast to char * for Windows, no harm on "normal" systems */
	if (s->readonly)
		(void) setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char *) &tv, (socklen_t) sizeof(tv));
	else
		(void) setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char *) &tv, (socklen_t) sizeof(tv));
}

#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif

static int
socket_isalive(const stream *s)
{
	SOCKET fd = s->stream_data.s;
#ifdef HAVE_POLL
	struct pollfd pfd = {.fd = fd};
	int ret;
	if ((ret = poll(&pfd, 1, 0)) == 0)
		return 1;
	if (ret == -1 && errno == EINTR)
		return socket_isalive(s);
	if (ret < 0 || pfd.revents & (POLLERR | POLLHUP))
		return 0;
	assert(0);		/* unexpected revents value */
	return 0;
#else
	fd_set fds;
	struct timeval t;
	char buffer[32];

	t.tv_sec = 0;
	t.tv_usec = 0;
	FD_ZERO(&fds);
	FD_SET(fd, &fds);
	return select(
#ifdef _MSC_VER
		0,	/* ignored on Windows */
#else
		fd + 1,
#endif
		&fds, NULL, NULL, &t) <= 0 ||
		recv(fd, buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT) != 0;
#endif
}

static stream *
socket_open(SOCKET sock, const char *name)
{
	stream *s;
	int domain = 0;

	if (sock == INVALID_SOCKET) {
		mnstr_set_open_error(name, 0, "invalid socket");
		return NULL;
	}
	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->read = socket_read;
	s->write = socket_write;
	s->close = socket_close;
	s->stream_data.s = sock;
	s->update_timeout = socket_update_timeout;
	s->isalive = socket_isalive;
	s->getoob = socket_getoob;
	s->putoob = socket_putoob;

	errno = 0;
#ifdef _MSC_VER
	WSASetLastError(0);
#endif
#if defined(SO_DOMAIN)
	{
		socklen_t len = (socklen_t) sizeof(domain);
		if (getsockopt(sock, SOL_SOCKET, SO_DOMAIN, (void *) &domain, &len) == SOCKET_ERROR)
			domain = AF_INET;	/* give it a value if call fails */
	}
#else
	{
		struct sockaddr_storage a;
		socklen_t l = (socklen_t) sizeof(a);
		if (getpeername(sock, (struct sockaddr *) &a, &l) == 0) {
			domain = a.ss_family;
		}
	}
#endif
#ifdef HAVE_SYS_UN_H
	if (domain == AF_UNIX) {
		s->getoob = socket_getoob_unix;
		s->putoob = socket_putoob_unix;
	}
#endif
#if defined(SO_KEEPALIVE) && !defined(WIN32)
	if (domain != AF_UNIX) {	/* not on UNIX sockets */
		int opt = 1;
		(void) setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void *) &opt, sizeof(opt));
	}
#endif
#if defined(IPTOS_THROUGHPUT) && !defined(WIN32)
	if (domain != AF_UNIX) {	/* not on UNIX sockets */
		int tos = IPTOS_THROUGHPUT;

		(void) setsockopt(sock, IPPROTO_IP, IP_TOS, (void *) &tos, sizeof(tos));
	}
#endif
#ifdef TCP_NODELAY
	if (domain != AF_UNIX) {	/* not on UNIX sockets */
		int nodelay = 1;

		(void) setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (void *) &nodelay, sizeof(nodelay));
	}
#endif
#ifdef HAVE_FCNTL
	{
		int fl = fcntl(sock, F_GETFL);

		fl &= ~O_NONBLOCK;
		if (fcntl(sock, F_SETFL, fl) < 0) {
			mnstr_set_error_errno(s, MNSTR_OPEN_ERROR, "fcntl unset O_NONBLOCK failed");
			return s;
		}
	}
#endif

	return s;
}

stream *
socket_rstream(SOCKET sock, const char *name)
{
	stream *s = NULL;

#ifdef STREAM_DEBUG
	fprintf(stderr, "socket_rstream %zd %s\n", (ssize_t) sock, name);
#endif
	if ((s = socket_open(sock, name)) != NULL)
		s->binary = true;
	return s;
}

stream *
socket_wstream(SOCKET sock, const char *name)
{
	stream *s;

#ifdef STREAM_DEBUG
	fprintf(stderr, "socket_wstream %zd %s\n", (ssize_t) sock, name);
#endif
	if ((s = socket_open(sock, name)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = true;
	return s;
}
