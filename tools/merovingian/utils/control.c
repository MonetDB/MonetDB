/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

#define SOCKPTR struct sockaddr *

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
		char* host,
		int port,
		char* database,
		char* command,
		char wait,
		char* pass)
{
	char sbuf[8096];
	char rbuf[8096];
	char *buf;
	int sock = -1;
	ssize_t len;
	stream *fdin = NULL;
	stream *fdout = NULL;

	*ret = NULL;		/* gets overwritten in case of success */
	if (port == -1) {
		struct sockaddr_un server;
		/* UNIX socket connect */
		if ((sock = socket(PF_UNIX, SOCK_STREAM
#ifdef SOCK_CLOEXEC
						   | SOCK_CLOEXEC
#endif
						   , 0)) == -1) {
			snprintf(sbuf, sizeof(sbuf), "cannot open connection: %s",
					strerror(errno));
			return(strdup(sbuf));
		}
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
		(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif
		server = (struct sockaddr_un) {
			.sun_family = AF_UNIX,
		};
		strcpy_len(server.sun_path, host, sizeof(server.sun_path));
		if (connect(sock, (SOCKPTR) &server, sizeof(struct sockaddr_un)) == -1) {
			switch (errno) {
			case ENOENT:
				snprintf(sbuf, sizeof(sbuf), "cannot connect: control socket does not exist");
				break;
			case EACCES:
				snprintf(sbuf, sizeof(sbuf), "cannot connect: no permission to access control socket");
				break;
			default:
				snprintf(sbuf, sizeof(sbuf), "cannot connect: %s", strerror(errno));
				break;
			}
			closesocket(sock);
			return(strdup(sbuf));
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
			snprintf(sbuf, sizeof(sbuf), "cannot connect: %s", gai_strerror(check));
			return(strdup(sbuf));
		}
		for (rp = res; rp; rp = rp->ai_next) {
			sock = socket(rp->ai_family, rp->ai_socktype
#ifdef SOCK_CLOEXEC
						 | SOCK_CLOEXEC
#endif
						 , rp->ai_protocol);
			if (sock == INVALID_SOCKET)
				continue;
			if (connect(sock, rp->ai_addr, (socklen_t) rp->ai_addrlen) != SOCKET_ERROR)
				break;  /* success */
			closesocket(sock);
		}
		freeaddrinfo(res);
		if (rp == NULL) {
			snprintf(sbuf, sizeof(sbuf), "cannot connect to %s:%s: %s", host, sport,
#ifdef _MSC_VER
					 wsaerror(WSAGetLastError())
#else
					 strerror(errno)
#endif
			);
			return(strdup(sbuf));
		}
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
		(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif

		/* try reading length */
		len = recv(sock, rbuf, 2, 0);
		if (len == 2)
			len += recv(sock, rbuf + len, sizeof(rbuf) - len - 1, 0);
		/* perform login ritual */
		if (len <= 2) {
			snprintf(sbuf, sizeof(sbuf), "no response from monetdbd");
			closesocket(sock);
			return(strdup(sbuf));
		}
		rbuf[len] = 0;
		/* we only understand merovingian:1 and :2 (backwards compat
		 * <=Aug2011) and mapi v9 on merovingian */
		if (strncmp(rbuf, "merovingian:1:", strlen("merovingian:1:")) == 0) {
			buf = rbuf + strlen("merovingian:1:");
			ver = 1;
		} else if (strncmp(rbuf, "merovingian:2:", strlen("merovingian:2:")) == 0) {
			buf = rbuf + strlen("merovingian:2:");
			ver = 2;
		} else if (strstr(rbuf + 2, ":merovingian:9:") != NULL) {
			buf = rbuf + 2;
			ver = 9;

			fdin = block_stream(socket_rstream(sock, "client in"));
			fdout = block_stream(socket_wstream(sock, "client out"));
		} else {
			if (strstr(rbuf + 2, ":BIG:") != NULL ||
				strstr(rbuf + 2, ":LIT:") != NULL)
			{
				snprintf(sbuf, sizeof(sbuf), "cannot connect: "
						"server looks like a mapi server, "
						"are you connecting to an mserver directly "
						"instead of monetdbd?");
			} else {
				snprintf(sbuf, sizeof(sbuf), "cannot connect: "
						"unsupported monetdbd server");
			}
			closesocket(sock);
			return(strdup(sbuf));
		}

		switch (ver) {
			case 1:
			case 2:  /* we never really used the mode specifier of v2 */
				p = strchr(buf, ':');
				if (p != NULL)
					*p = '\0';
				p = control_hash(pass, buf);
				len = snprintf(sbuf, sizeof(sbuf), "%s%s\n",
						p, ver == 2 ? ":control" : "");
				len = send(sock, sbuf, len, 0);
				free(p);
				if (len == -1) {
					closesocket(sock);
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
				char **algs = algsv;

				/* buf at this point looks like
				 * "challenge:servertype:protover:algos:endian:hash:" */
				chal = buf; /* chal */
				p = strchr(chal, ':');
				if (p == NULL) {
					snprintf(sbuf, sizeof(sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(fdout);
					close_stream(fdin);
					return(strdup(sbuf));
				}
				*p++ = '\0'; /* servertype */
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(sbuf, sizeof(sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(fdout);
					close_stream(fdin);
					return(strdup(sbuf));
				}
				*p++ = '\0'; /* protover */
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(sbuf, sizeof(sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(fdout);
					close_stream(fdin);
					return(strdup(sbuf));
				}
				*p++ = '\0'; /* algos */
				algos = p;
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(sbuf, sizeof(sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(fdout);
					close_stream(fdin);
					return(strdup(sbuf));
				}
				*p++ = '\0'; /* endian */
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(sbuf, sizeof(sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(fdout);
					close_stream(fdin);
					return(strdup(sbuf));
				}
				*p++ = '\0'; /* hash */
				shash = p;
				p = strchr(p, ':');
				if (p == NULL) {
					snprintf(sbuf, sizeof(sbuf), "cannot connect: "
							"invalid challenge from monetdbd server");
					close_stream(fdout);
					close_stream(fdin);
					return(strdup(sbuf));
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
					snprintf(sbuf, sizeof(sbuf), "cannot connect: "
							"monetdbd server requires unknown hash: %s", shash);
					close_stream(fdout);
					close_stream(fdin);
					return(strdup(sbuf));
				}

				if (!phash) {
					snprintf(sbuf, sizeof(sbuf), "cannot connect: "
							"allocation failure while establishing connection");
					close_stream(fdout);
					close_stream(fdin);
					return(strdup(sbuf));
				}

				/* now hash the password hash with the provided
				 * challenge */
				for (; *algs != NULL; algs++) {
					/* TODO: make this actually obey the separation by
					 * commas, and only allow full matches */
					if (strstr(algos, *algs) != NULL) {
						p = mcrypt_hashPassword(*algs, phash, chal);
						if (p == NULL)
							continue;
						mnstr_printf(fdout,
								"BIG:monetdb:{%s}%s:control:merovingian:\n",
								*algs, p);
						mnstr_flush(fdout);
						free(p);
						break;
					}
				}
				free(phash);
				if (p == NULL) {
					/* the server doesn't support what we can */
					snprintf(sbuf, sizeof(sbuf), "cannot connect: "
							"unsupported hash algoritms: %s", algos);
					close_stream(fdout);
					close_stream(fdin);
					return(strdup(sbuf));
				}
			}
		}

		if (fdin != NULL) {
			/* stream.h is sooo broken :( */
			memset(rbuf, '\0', sizeof(rbuf));
			if ((len = mnstr_read_block(fdin, rbuf, sizeof(rbuf) - 1, 1)) < 0) {
				close_stream(fdout);
				close_stream(fdin);
				return(strdup("no response from monetdbd after login"));
			}
			rbuf[len - 1] = '\0';
		} else {
			if ((len = recv(sock, rbuf, sizeof(rbuf), 0)) <= 0) {
				closesocket(sock);
				return(strdup("no response from monetdbd after login"));
			}
			rbuf[len - 1] = '\0';
		}

		if (strncmp(rbuf, "=OK", 3) != 0 && strncmp(rbuf, "OK", 2) != 0) {
			buf = rbuf;
			if (*buf == '!')
				buf++;
			if (fdin != NULL) {
				close_stream(fdout);
				close_stream(fdin);
			} else {
				closesocket(sock);
			}
			return(strdup(buf));
		}
	}

	if (fdout != NULL) {
		mnstr_printf(fdout, "%s %s\n", database, command);
		mnstr_flush(fdout);
	} else {
		len = snprintf(sbuf, sizeof(sbuf), "%s %s\n", database, command);
		if (send(sock, sbuf, len, 0) == -1) {
			closesocket(sock);
			return(strdup("failed to send control command to server"));
		}
	}
	if (wait != 0) {
		size_t buflen = sizeof(sbuf);
		size_t bufpos = 0;
		char *bufp;
		bufp = buf = malloc(sizeof(char) * buflen);
		if (buf == NULL) {
			if (fdin != NULL) {
				close_stream(fdin);
				close_stream(fdout);
			} else {
				closesocket(sock);
			}
			return(strdup("failed to allocate memory"));
		}
		while (1) {
			if (fdin != NULL) {
				/* stream.h is sooo broken :( */
				memset(buf + bufpos, '\0', buflen - bufpos);
				len = mnstr_read_block(fdin, buf + bufpos, buflen - bufpos - 1, 1);
				if (len >= 0)
					len = strlen(buf + bufpos);
			} else {
				len = recv(sock, buf + bufpos, buflen - bufpos, 0);
			}
			if (len <= 0)
				break;
			if ((size_t)len == buflen - bufpos) {
				buflen *= 2;
				bufp = realloc(buf, sizeof(char) * buflen);
				if (bufp == NULL) {
					free(buf);
					if (fdin != NULL) {
						close_stream(fdin);
						close_stream(fdout);
					} else {
						closesocket(sock);
					}
					return(strdup("failed to allocate more memory"));
				}
				buf = bufp;
			}
			bufpos += (size_t)len;
		}
		if (bufpos == 0) {
			if (fdin != NULL) {
				close_stream(fdin);
				close_stream(fdout);
			} else {
				closesocket(sock);
			}
			free(buf);
			return(strdup("incomplete response from monetdbd"));
		}
		buf[bufpos - 1] = '\0';

		if (fdin) {
			/* strip out protocol = */
			memmove(bufp, bufp + 1, strlen(bufp + 1) + 1);
			while ((bufp = strstr(bufp, "\n=")) != NULL)
				memmove(bufp + 1, bufp + 2, strlen(bufp + 2) + 1);
		}
		*ret = buf;
	} else {
		if (fdin != NULL) {
			if (mnstr_read_block(fdin, rbuf, sizeof(rbuf) - 1, 1) < 0) {
				close_stream(fdin);
				close_stream(fdout);
				return(strdup("incomplete response from monetdbd"));
			}
			rbuf[strlen(rbuf) - 1] = '\0';
			*ret = strdup(rbuf + 1);
		} else {
			if ((len = recv(sock, rbuf, sizeof(rbuf), 0)) <= 0) {
				closesocket(sock);
				return(strdup("incomplete response from monetdbd"));
			}
			rbuf[len - 1] = '\0';
			*ret = strdup(rbuf);
		}
	}

	if (fdin != NULL) {
		close_stream(fdin);
		close_stream(fdout);
	} else {
		closesocket(sock);
	}

	return(NULL);
}

/**
 * Returns a hash for pass and salt, to use when logging in on a remote
 * merovingian.  The result is a malloced string.
 * DEPRECATED
 * This function is deprecated, and only used for authorisation to v1
 * and v2 protocol merovingians (<=Aug2011).
 */
char *
control_hash(char *pass, char *salt) {
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
control_ping(char *host, int port, char *pass) {
	char *res;
	char *err;
	if ((err = control_send(&res, host, port, "", "ping", 0, pass)) == NULL) {
		if (res != NULL)
			free(res);
	}
	return err;
}
