/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* Generic stream handling code such as init and close */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif


/* ------------------------------------------------------------------ */
/* streams working on a socket */

static ssize_t
socket_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt, res = 0;
#ifdef NATIVE_WIN32
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
#ifdef NATIVE_WIN32
		       /* send works on int, make sure the argument fits */
		       ((nr = send(s->stream_data.s, (const char *) buf + res, (int) min(size - res, 1 << 16), 0)) > 0)
#else
		       ((nr = write(s->stream_data.s, (const char *) buf + res, size - res)) > 0)
#endif
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
		       ||(nr < 0 &&
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
#else
	ssize_t nr = 0;
#endif
	size_t size = elmsize * cnt;

	if (s->errkind != MNSTR_NO__ERROR)
		return -1;
	if (size == 0)
		return 0;

#ifdef _MSC_VER
	/* recv only takes an int parameter, and read does not accept
	 * sockets */
	if (size > INT_MAX)
		size = elmsize * (INT_MAX / elmsize);
#endif
	for (;;) {
		if (s->timeout) {
			int ret;
#ifdef HAVE_POLL
			struct pollfd pfd;

			pfd = (struct pollfd) {.fd = s->stream_data.s,
					       .events = POLLIN};

			ret = poll(&pfd, 1, (int) s->timeout);
			if (ret == -1 && errno == EINTR)
				continue;
			if (ret == -1 || (pfd.revents & POLLERR)) {
				mnstr_set_error_errno(s, MNSTR_READ_ERROR, "poll error");
				return -1;
			}
#else
			struct timeval tv;
			fd_set fds;

			errno = 0;
#ifdef _MSC_VER
			WSASetLastError(0);
#endif
			FD_ZERO(&fds);
			FD_SET(s->stream_data.s, &fds);
			tv.tv_sec = s->timeout / 1000;
			tv.tv_usec = (s->timeout % 1000) * 1000;
			ret = select(
#ifdef _MSC_VER
				0,	/* ignored on Windows */
#else
				s->stream_data.s + 1,
#endif
				&fds, NULL, NULL, &tv);
			if (ret == SOCKET_ERROR) {
				mnstr_set_error_errno(s, MNSTR_READ_ERROR, "select");
				return -1;
			}
#endif
			if (ret == 0) {
				if (s->timeout_func == NULL || s->timeout_func(s->timeout_data)) {
					mnstr_set_error(s, MNSTR_TIMEOUT, NULL);
					return -1;
				}
				continue;
			}
			assert(ret == 1);
#ifdef HAVE_POLL
			assert(pfd.revents & (POLLIN|POLLHUP));
#else
			assert(FD_ISSET(s->stream_data.s, &fds));
#endif
		}
#ifdef _MSC_VER
		nr = recv(s->stream_data.s, buf, (int) size, 0);
		if (nr == SOCKET_ERROR) {
			mnstr_set_error_errno(s, MNSTR_READ_ERROR, "recv");
			return -1;
		}
#else
		nr = read(s->stream_data.s, buf, size);
		if (nr == -1 && errno == EINTR)
			continue;
		if (nr == -1) {
			mnstr_set_error_errno(s, MNSTR_READ_ERROR, NULL);
			return -1;
		}
#endif
		break;
	}
	if (nr == 0)
		return 0;	/* end of file */
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
	if (!s->readonly)
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
	struct pollfd pfd;
	int ret;
	pfd = (struct pollfd){.fd = fd};
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
#endif
#if defined(SO_KEEPALIVE) && !defined(WIN32)
	if (domain != PF_UNIX) {	/* not on UNIX sockets */
		int opt = 1;
		(void) setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void *) &opt, sizeof(opt));
	}
#endif
#if defined(IPTOS_THROUGHPUT) && !defined(WIN32)
	if (domain != PF_UNIX) {	/* not on UNIX sockets */
		int tos = IPTOS_THROUGHPUT;

		(void) setsockopt(sock, IPPROTO_IP, IP_TOS, (void *) &tos, sizeof(tos));
	}
#endif
#ifdef TCP_NODELAY
	if (domain != PF_UNIX) {	/* not on UNIX sockets */
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
