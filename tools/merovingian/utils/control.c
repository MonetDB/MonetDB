/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "control.h"
#include <unistd.h> /* close */
#include <string.h> /* strerror */
#include <sys/socket.h> /* socket */
#ifdef HAVE_SYS_UN_H
#include <sys/un.h> /* sockaddr_un */
#endif
#include <netdb.h>
#include <netinet/in.h>
#include <fcntl.h>

#include "stream.h"
#include "stream_socket.h"
#include "mcrypt.h"
#include "mstring.h"

struct control_state {
	int sock;
	stream *fdin;
	stream *fdout;
	char sbuf[8096];
	char rbuf[8096];
};

static char *
control_setup(
	struct control_state *control,
	const char *host,
	int port,
	const char *database,
	const char *command,
	const char *pass
)
{
	ssize_t len;
	char *buf;

	if (port == -1) {
		struct sockaddr_un server;
		/* UNIX socket connect */
		if ((control->sock = socket(PF_UNIX, SOCK_STREAM
#ifdef SOCK_CLOEXEC
						   | SOCK_CLOEXEC
#endif
						   , 0)) == -1) {
			snprintf(control->sbuf, sizeof(control->sbuf), "cannot open connection: %s",
					strerror(errno));
			return(strdup(control->sbuf));
		}
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
		(void) fcntl(control->sock, F_SETFD, FD_CLOEXEC);
#endif
		server = (struct sockaddr_un) {
			.sun_family = AF_UNIX,
		};
		strcpy_len(server.sun_path, host, sizeof(server.sun_path));
		if (connect(control->sock, (struct sockaddr *) &server, sizeof(struct sockaddr_un)) == -1) {
			switch (errno) {
			case ENOENT:
				snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: control socket does not exist");
				break;
			case EACCES:
				snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: no permission to access control socket");
				break;
			default:
				snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: %s", strerror(errno));
				break;
			}
			closesocket(control->sock);
			return(strdup(control->sbuf));
		}
	} else {
		int check;
		char ver = 0, *p;
		char sport[16];
		struct addrinfo *res, *rp, hints = (struct addrinfo) {
			.ai_family = AF_UNSPEC,
			.ai_socktype = SOCK_STREAM,
			.ai_protocol = IPPROTO_TCP,
		};

		snprintf(sport, sizeof(sport), "%d", port & 0xFFFF);
		check = getaddrinfo(host, sport, &hints, &res);
		if (check) {
			snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: %s", gai_strerror(check));
			return(strdup(control->sbuf));
		}
		for (rp = res; rp; rp = rp->ai_next) {
			control->sock = socket(rp->ai_family, rp->ai_socktype
#ifdef SOCK_CLOEXEC
						 | SOCK_CLOEXEC
#endif
						 , rp->ai_protocol);
			if (control->sock == INVALID_SOCKET)
				continue;
			if (connect(control->sock, rp->ai_addr, (socklen_t) rp->ai_addrlen) != SOCKET_ERROR)
				break;  /* success */
			closesocket(control->sock);
		}
		freeaddrinfo(res);
		if (rp == NULL) {
			snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect to %s:%s: %s", host, sport,
#ifdef _MSC_VER
					 wsaerror(WSAGetLastError())
#else
					 strerror(errno)
#endif
			);
			return(strdup(control->sbuf));
		}
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
		(void) fcntl(control->sock, F_SETFD, FD_CLOEXEC);
#endif

		/* try reading length */
		len = recv(control->sock, control->rbuf, 2, 0);
		if (len == 2)
			len += recv(control->sock, control->rbuf + len, sizeof(control->rbuf) - len - 1, 0);
		/* perform login ritual */
		if (len <= 2) {
			snprintf(control->sbuf, sizeof(control->sbuf), "no response from monetdbd");
			closesocket(control->sock);
			return(strdup(control->sbuf));
		}
		control->rbuf[len] = 0;
		/* we only understand merovingian:1 and :2 (backwards compat
		 * <=Aug2011) and mapi v9 on merovingian */
		if (strncmp(control->rbuf, "merovingian:1:", strlen("merovingian:1:")) == 0) {
			buf = control->rbuf + strlen("merovingian:1:");
			ver = 1;
		} else if (strncmp(control->rbuf, "merovingian:2:", strlen("merovingian:2:")) == 0) {
			buf = control->rbuf + strlen("merovingian:2:");
			ver = 2;
		} else if (strstr(control->rbuf + 2, ":merovingian:9:") != NULL) {
			buf = control->rbuf + 2;
			ver = 9;

			control->fdin = block_stream(socket_rstream(control->sock, "client in"));
			if (control->fdin == NULL) {
				snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: %s", mnstr_peek_error(NULL));
				return strdup(control->sbuf);
			}
			control->fdout = block_stream(socket_wstream(control->sock, "client out"));
			if (control->fdout == NULL) {
				close_stream(control->fdin);
				snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: %s", mnstr_peek_error(NULL));
				return strdup(control->sbuf);
			}
		} else {
			if (strstr(control->rbuf + 2, ":BIG:") != NULL ||
				strstr(control->rbuf + 2, ":LIT:") != NULL)
			{
				snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
						"server looks like a mapi server, "
						"are you connecting to an mserver directly "
						"instead of monetdbd?");
			} else {
				snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
						"unsupported monetdbd server");
			}
			closesocket(control->sock);
			return(strdup(control->sbuf));
		}

		switch (ver) {
			case 1:
			case 2:  /* we never really used the mode specifier of v2 */
				p = strchr(buf, ':');
				if (p != NULL)
					*p = '\0';
				p = control_hash(pass, buf);
				len = snprintf(control->sbuf, sizeof(control->sbuf), "%s%s\n",
						p, ver == 2 ? ":control" : "");
				len = send(control->sock, control->sbuf, len, 0);
				free(p);
				if (len == -1) {
					closesocket(control->sock);
					return(strdup("cannot send challenge response to server"));
				}
				break;
			case 9:
			{
				char *chal = NULL;
				char *algos = NULL;
				char *shash = NULL;
				char *phash = NULL;
				char *algsv[] = {
#ifdef HAVE_RIPEMD160_UPDATE
					"RIPEMD160",
#endif
#ifdef HAVE_SHA512_UPDATE
					"SHA512",
#endif
#ifdef HAVE_SHA384_UPDATE
					"SHA384",
#endif
#ifdef HAVE_SHA256_UPDATE
					"SHA256",
#endif
#ifdef HAVE_SHA224_UPDATE
					"SHA224",
#endif
#ifdef HAVE_SHA1_UPDATE
					"SHA1",
#endif
					NULL
				};
				(void)algsv;

				/* buf at this point looks like
				 * "challenge:servertype:protover:algos:endian:hash:" */
				chal = buf; /* chal */
				p = strchr(chal, ':');
				if (p == NULL) {
					snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(control->fdout);
					close_stream(control->fdin);
					return(strdup(control->sbuf));
				}
				*p++ = '\0'; /* servertype */
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(control->fdout);
					close_stream(control->fdin);
					return(strdup(control->sbuf));
				}
				*p++ = '\0'; /* protover */
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(control->fdout);
					close_stream(control->fdin);
					return(strdup(control->sbuf));
				}
				*p++ = '\0'; /* algos */
				algos = p;
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(control->fdout);
					close_stream(control->fdin);
					return(strdup(control->sbuf));
				}
				*p++ = '\0'; /* endian */
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(control->fdout);
					close_stream(control->fdin);
					return(strdup(control->sbuf));
				}
				*p++ = '\0'; /* hash */
				shash = p;
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(control->fdout);
					close_stream(control->fdin);
					return(strdup(control->sbuf));
				}
				*p = '\0';

				/* we first need to hash our password in the form the
				 * server stores it too */
#ifdef HAVE_RIPEMD160_UPDATE
				if (strcmp(shash, "RIPEMD160") == 0) {
					phash = mcrypt_RIPEMD160Sum(pass, strlen(pass));
				} else
#endif
#ifdef HAVE_SHA512_UPDATE
				if (strcmp(shash, "SHA512") == 0) {
					phash = mcrypt_SHA512Sum(pass, strlen(pass));
				} else
#endif
#ifdef HAVE_SHA384_UPDATE
				if (strcmp(shash, "SHA384") == 0) {
					phash = mcrypt_SHA384Sum(pass, strlen(pass));
				} else
#endif
#ifdef HAVE_SHA256_UPDATE
				if (strcmp(shash, "SHA256") == 0) {
					phash = mcrypt_SHA256Sum(pass, strlen(pass));
				} else
#endif
#ifdef HAVE_SHA224_UPDATE
				if (strcmp(shash, "SHA224") == 0) {
					phash = mcrypt_SHA224Sum(pass, strlen(pass));
				} else
#endif
#ifdef HAVE_SHA1_UPDATE
				if (strcmp(shash, "SHA1") == 0) {
					phash = mcrypt_SHA1Sum(pass, strlen(pass));
				} else
#endif
				{
					(void)phash; (void)algos;
					snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
							"monetdbd server requires unknown hash: %s", shash);
					close_stream(control->fdout);
					close_stream(control->fdin);
					return(strdup(control->sbuf));
				}
#if defined(HAVE_RIPEMD160_UPDATE) || defined(HAVE_SHA512_UPDATE) || defined(HAVE_SHA384_UPDATE) || defined(HAVE_SHA256_UPDATE) || defined(HAVE_SHA224_UPDATE) || defined(HAVE_SHA1_UPDATE)
				if (!phash) {
					snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
							"allocation failure while establishing connection");
					close_stream(control->fdout);
					close_stream(control->fdin);
					return(strdup(control->sbuf));
				}

				/* now hash the password hash with the provided
				 * challenge */
				char **algs = algsv;
				for (; *algs != NULL; algs++) {
					/* TODO: make this actually obey the separation by
					 * commas, and only allow full matches */
					if (strstr(algos, *algs) != NULL) {
						p = mcrypt_hashPassword(*algs, phash, chal);
						if (p == NULL)
							continue;
						mnstr_printf(control->fdout,
								"BIG:monetdb:{%s}%s:control:merovingian:\n",
								*algs, p);
						mnstr_flush(control->fdout, MNSTR_FLUSH_DATA);
						free(p);
						break;
					}
				}
				free(phash);
				if (p == NULL) {
					/* the server doesn't support what we can */
					snprintf(control->sbuf, sizeof(control->sbuf), "cannot connect: "
							"unsupported hash algoritms: %s", algos);
					close_stream(control->fdout);
					close_stream(control->fdin);
					return(strdup(control->sbuf));
				}
#endif
			}
		}

		if (control->fdin != NULL) {
			/* stream.h is sooo broken :( */
			memset(control->rbuf, '\0', sizeof(control->rbuf));
			if ((len = mnstr_read_block(control->fdin, control->rbuf, sizeof(control->rbuf) - 1, 1)) < 0) {
				close_stream(control->fdout);
				close_stream(control->fdin);
				return(strdup("no response from monetdbd after login"));
			}
			control->rbuf[len - 1] = '\0';
		} else {
			if ((len = recv(control->sock, control->rbuf, sizeof(control->rbuf), 0)) <= 0) {
				closesocket(control->sock);
				return(strdup("no response from monetdbd after login"));
			}
			control->rbuf[len - 1] = '\0';
		}

		if (strncmp(control->rbuf, "=OK", 3) != 0 && strncmp(control->rbuf, "OK", 2) != 0) {
			buf = control->rbuf;
			if (*buf == '!')
				buf++;
			if (control->fdin != NULL) {
				close_stream(control->fdout);
				close_stream(control->fdin);
			} else {
				closesocket(control->sock);
			}
			return(strdup(buf));
		}
	}

	if (control->fdout != NULL) {
		mnstr_printf(control->fdout, "%s %s\n", database, command);
		mnstr_flush(control->fdout, MNSTR_FLUSH_DATA);
	} else {
		len = snprintf(control->sbuf, sizeof(control->sbuf), "%s %s\n", database, command);
		if (send(control->sock, control->sbuf, len, 0) == -1) {
			closesocket(control->sock);
			return(strdup("failed to send control command to server"));
		}
	}

	return NULL;
}

static char *
control_receive_wait(char **ret, struct control_state *control)
{
	ssize_t len;
	size_t buflen = sizeof(control->sbuf);
	size_t bufpos = 0;
	char *buf, *bufp;
	bufp = buf = malloc(buflen);
	if (buf == NULL) {
		if (control->fdin != NULL) {
			close_stream(control->fdin);
			close_stream(control->fdout);
		} else {
			closesocket(control->sock);
		}
		return(strdup("failed to allocate memory"));
	}
	while (1) {
		if (control->fdin != NULL) {
			/* stream.h is sooo broken :( */
			memset(buf + bufpos, '\0', buflen - bufpos);
			len = mnstr_read_block(control->fdin, buf + bufpos, buflen - bufpos - 1, 1);
			if (len >= 0)
				len = strlen(buf + bufpos);
		} else {
			len = recv(control->sock, buf + bufpos, buflen - bufpos, 0);
		}
		if (len <= 0)
			break;
		if ((size_t)len == buflen - bufpos) {
			buflen *= 2;
			bufp = realloc(buf, sizeof(char) * buflen);
			if (bufp == NULL) {
				free(buf);
				if (control->fdin != NULL) {
					close_stream(control->fdin);
					close_stream(control->fdout);
				} else {
					closesocket(control->sock);
				}
				return(strdup("failed to allocate more memory"));
			}
			buf = bufp;
		}
		bufpos += (size_t)len;
	}
	if (bufpos == 0) {
		if (control->fdin != NULL) {
			close_stream(control->fdin);
			close_stream(control->fdout);
		} else {
			closesocket(control->sock);
		}
		free(buf);
		return(strdup("incomplete response from monetdbd"));
	}
	buf[bufpos - 1] = '\0';

	if (control->fdin) {
		/* strip out protocol = */
		memmove(bufp, bufp + 1, strlen(bufp + 1) + 1);
		while ((bufp = strstr(bufp, "\n=")) != NULL)
			memmove(bufp + 1, bufp + 2, strlen(bufp + 2) + 1);
	}
	*ret = buf;

	return NULL;
}

static char *
control_receive_nowait(char **ret, struct control_state *control)
{
	ssize_t len;
	if (control->fdin != NULL) {
		if (mnstr_read_block(control->fdin, control->rbuf, sizeof(control->rbuf) - 1, 1) < 0) {
			close_stream(control->fdin);
			close_stream(control->fdout);
			return(strdup("incomplete response from monetdbd"));
		}
		control->rbuf[strlen(control->rbuf) - 1] = '\0';
		*ret = strdup(control->rbuf + 1);
	} else {
		if ((len = recv(control->sock, control->rbuf, sizeof(control->rbuf), 0)) <= 0) {
			closesocket(control->sock);
			return(strdup("incomplete response from monetdbd"));
		}
		control->rbuf[len - 1] = '\0';
		*ret = strdup(control->rbuf);
	}

	return NULL;
}

/* Sends command for database to merovingian listening at host and port.
 * If host is a path, and port is -1, a UNIX socket connection for host
 * is opened.  The response of merovingian is returned as a malloced
 * string.  If wait is set to a non-zero value, this function will only
 * return after it has seen an EOF from the server.  This is useful with
 * multi-line responses, but can lock up for single line responses where
 * the server allows pipelining (and hence doesn't close the
 * connection).
 */
char* control_send(
		char** ret,
		const char* host,
		int port,
		const char *database,
		const char *command,
		char wait,
		const char *pass)
{
	char *msg;
	struct control_state control_state = {0};
	struct control_state *control = &control_state;

	*ret = NULL;		/* gets overwritten in case of success */

	msg = control_setup(control, host, port, database, command, pass);
	if (msg != NULL)
		return msg;

	if (wait)
		msg = control_receive_wait(ret, control);
	else
		msg = control_receive_nowait(ret, control);

	if (msg)
		return msg;

	if (control->fdin != NULL) {
		close_stream(control->fdin);
		close_stream(control->fdout);
	} else {
		closesocket(control->sock);
	}

	return(NULL);
}


/* Sends command for database to merovingian listening at host and port.
 * If host is a path, and port is -1, a UNIX socket connection for host
 * is opened.  The response of merovingian is returned as a malloced
 * string.  If wait is set to a non-zero value, this function will only
 * return after it has seen an EOF from the server.  This is useful with
 * multi-line responses, but can lock up for single line responses where
 * the server allows pipelining (and hence doesn't close the
 * connection).
 */
char* control_send_callback(
		char** ret,
		const char *host,
		int port,
		const char *database,
		const char *command,
		void (*callback)(const void *data, size_t size, void *cb_private),
		void *cb_private,
		const char *pass)
{
	char *msg = NULL;
	struct control_state control_state = {0};
	struct control_state *control = &control_state;
	char buf[8192];
	ssize_t nread;

	*ret = NULL;		/* gets overwritten in case of success */

	msg = control_setup(control, host, port, database, command, pass);
	if (msg != NULL)
		return msg;

	stream *wrapper = NULL;
	stream *bs = NULL;
	stream *s = NULL;  // aliases either bs or control->fdin

	do {
		if (control->fdin) {
			assert(isa_block_stream(control->fdin));
			s = control->fdin;
		} else {
			wrapper = socket_rstream(control->sock, "sockwrapper");
			if (!wrapper) {
				msg = strdup("could not wrap socket");
				break;
			}
			bs = block_stream(wrapper);
			if (!bs) {
				msg = strdup("could not wrap block stream");
				break;
			}
			wrapper = NULL; // it will be cleaned up when bs is cleaned up
			s = bs;
		}
		while ((nread = mnstr_read(s, buf, 1, sizeof(buf))) > 0) {
			callback(buf, (size_t)nread, cb_private);
		}
		if (mnstr_errnr(s))
			msg = mnstr_error(s);
	} while (0);
	if (bs)
		mnstr_destroy(bs);
	if (wrapper)
		mnstr_destroy(wrapper);
	if (msg)
		return msg;

	msg = control_receive_wait(ret, control);

	/* control_receive_wait closes control in case of error */
	if (msg == NULL) {
		if (control->fdin != NULL) {
			close_stream(control->fdin);
			close_stream(control->fdout);
		} else {
			closesocket(control->sock);
		}
	}

	return msg;
}

/**
 * Returns a hash for pass and salt, to use when logging in on a remote
 * merovingian.  The result is a malloced string.
 * DEPRECATED
 * This function is deprecated, and only used for authorisation to v1
 * and v2 protocol merovingians (<=Aug2011).
 */
char *
control_hash(const char *pass, const char *salt) {
	unsigned int ho;
	unsigned int h = 0;
	char buf[32];

	/* use a very simple hash function designed for a single int val
	 * (hash buckets), we can make this more interesting if necessary in
	 * the future.
	 * http://www.cs.hmc.edu/~geoff/classes/hmc.cs070.200101/homework10/hashfuncs.html */

	while (*pass != '\0') {
		ho = h & 0xf8000000;
		h <<= 5;
		h ^= ho >> 27;
		h ^= (unsigned int)(*pass);
		pass++;
	}

	while (*salt != '\0') {
		ho = h & 0xf8000000;
		h <<= 5;
		h ^= ho >> 27;
		h ^= (unsigned int)(*salt);
		salt++;
	}

	snprintf(buf, sizeof(buf), "%u", h);
	return(strdup(buf));
}

/**
 * Returns if the merovingian server at host, port using pass is alive
 * or not.  If alive, 0 is returned.  The same rules for host, port and
 * pass hold as for control_send regarding the use of a UNIX vs TCP
 * socket.
 */
char *
control_ping(const char *host, int port, const char *pass) {
	char *res;
	char *err;
	if ((err = control_send(&res, host, port, "", "ping", 0, pass)) == NULL) {
		if (res != NULL)
			free(res);
	}
	return err;
}
