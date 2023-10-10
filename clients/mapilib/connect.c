

#include "monetdb_config.h"
#include "stream.h"		/* include before mapi.h */
#include "stream_socket.h"
#include "mapi.h"
#include "mapi_prompt.h"
#include "mcrypt.h"
#include "matomic.h"
#include "mstring.h"

#include "mapi_intern.h"

#ifdef HAVE_SYS_UN_H
#define DO_UNIX_DOMAIN (1)
#else
#define DO_UNIX_DOMAIN (0)
#endif

static MapiMsg establish_connection(Mapi mid);
static MapiMsg scan_unix_sockets(Mapi mid);
static MapiMsg connect_socket(Mapi mid);
static MapiMsg mapi_handshake(Mapi mid);

/* (Re-)establish a connection with the server. */
MapiMsg
mapi_reconnectx(Mapi mid)
{
	// If neither host nor port are given, scan the Unix domain sockets in
	// /tmp and see if any of them serve this database.
	// Otherwise, just try to connect to what was given.
	if (mid->hostname == NULL && mid->port == 0)
		return scan_unix_sockets(mid);
	else
		return establish_connection(mid);
}

#define MAX_SCAN (24)

static MapiMsg
scan_unix_sockets(Mapi mid)
{
	MapiMsg msg;
	Mapi backup = NULL;
	struct {
		int port;
		int priority;
	} candidates[MAX_SCAN];
	int ncandidates = 0;
	DIR *dir = NULL;
	struct dirent *entry;

	assert(mid->hostname == NULL && mid->port == 0);

	// We need to preserve the original state because when we attempt to
	// connect to the sockets we may get redirected, for example to a
	// different database name. When we move on to the next socket, we
	// should use the original database name.
	backup = mapi_new();
	if (!backup || mapi_copymapi(backup, mid) != MOK) {
		msg = mapi_setError(mid, "malloc failed", __func__, MERROR);
		goto wrap_up;
	}

	uid_t me = getuid();
	if (DO_UNIX_DOMAIN && (dir = opendir("/tmp"))) {
		while (ncandidates < MAX_SCAN && (entry = readdir(dir)) != NULL) {
			const char *basename = entry->d_name;
			if (strncmp(basename, ".s.monetdb.", 11) != 0 || basename[11] == '\0' || strlen(basename) > 20)
				continue;

			char *end;
			long port = strtol(basename + 11, &end, 10);
			if (port < 1 || port > 65535 || *end)
				continue;
			char name[80]; // enough, see checks above
			sprintf(name, "/tmp/.s.monetdb.%ld", port);

			struct stat st;
			if (stat(name, &st) < 0 || !S_ISSOCK(st.st_mode))
				continue;

			candidates[ncandidates].port = port;
			candidates[ncandidates++].priority = st.st_uid == me ? 0 : 1;
		}
	}

	// those owned by us first, then all others
	for (int round = 0; round < 2; round++) {
		for (int i = 0; i < ncandidates; i++) {
			if (candidates[i].priority != round)
				continue;
			// mid->hostname = candidates[i].sock_name;
			// free(mid->hostname);
			assert(mid->hostname == NULL);
			mid->port = candidates[i].port;
			if (establish_connection(mid) == MOK) {
				msg = MOK;
				goto wrap_up;
			} else if (mapi_copymapi(mid, backup) != MOK) {
				msg = mapi_setError(mid, "malloc failed", __func__, MERROR);
				goto wrap_up;
			}
		}
	}

	// last-ditch attempt
	free(mid->hostname);
	mid->hostname = strdup("localhost");
	if (!mid->hostname) {
		msg = mapi_setError(mid, "malloc failed", __func__, MERROR);
		goto wrap_up;
	}
	mid->port = MAPI_PORT;
	msg = establish_connection(mid);

wrap_up:
	if (msg != MOK && backup) {
		mapi_movemapi(mid, backup);
		mapi_destroy(backup);
	}
	if (dir)
		closedir(dir);
	return msg;
}

/* (Re-)establish a connection with the server. */
static MapiMsg
establish_connection(Mapi mid)
{
	if (mid->connected)
		close_connection(mid);

	MapiMsg msg = MREDIRECT;
	while (msg == MREDIRECT) {
		// Generally at this point we need to set up a new TCP or Unix
		// domain connection.
		//
		// The only exception is if mapi_handshake() below has decided
		// that the handshake must be restarted on the existing
		// connection.
		if (!mid->connected) {
			msg = connect_socket(mid);
		}
		if (msg != MOK)
			return msg;
		msg = mapi_handshake(mid);
	}

	return msg;
}

static MapiMsg
connect_socket(Mapi mid)
{
	SOCKET s = INVALID_SOCKET;
	char errbuf[8096];
	char buf[BLOCK];

	if (mid->uri == NULL) {
		/* continue work started by mapi_mapi */

		/* connection searching strategy:
		 * 0) if host and port are given, resort to those
		 * 1) if no dbname given, make TCP connection
		 *    (merovingian will complain regardless, so it is
		 *    more likely an mserver is meant to be directly
		 *    addressed)
		 *    a) resort to default (hardwired) port 50000,
		 *       unless port given, then
		 *    b) resort to port given
		 * 2) a dbname is given
		 *    a) if a port is given, open unix socket for that
		 *       port, resort to TCP connection if not found
		 *    b) no port given, start looking for a matching
		 *       merovingian, by searching through socket
		 *       files, attempting connect to given dbname
		 *       I) try available sockets that have a matching
		 *          owner with the current user
		 *       II) try other sockets
		 *       III) resort to TCP connection on hardwired
		 *            port (localhost:50000)
		 */

		char *host;
		int port;

		host = mid->hostname;
		port = mid->port;

		if (host != NULL && port != 0) {
			/* case 0), just do what the user told us */
#ifdef HAVE_SYS_UN_H
			if (*host == '/') {
				/* don't stat or anything, the
				 * mapi_reconnect will return the
				 * error if it doesn't exist, falling
				 * back to TCP with a hostname like
				 * '/var/sockets' won't work anyway */
				snprintf(buf, sizeof(buf),
					 "%s/.s.monetdb.%d", host, port);
				host = buf;
			}
#endif
		} else if (mid->database == NULL) {
			/* case 1) */
			if (port == 0)
				port = MAPI_PORT;	/* case 1a), hardwired default */
			if (host == NULL)
				host = "localhost";
		} else {
			/* case 2), database name is given */
			if (port != 0) {
				/* case 2a), if unix socket found, use
				 * it, otherwise TCP */
#ifdef HAVE_SYS_UN_H
				struct stat st;
				snprintf(buf, sizeof(buf),
					 "/tmp/.s.monetdb.%d", port);
				if (stat(buf, &st) != -1 &&
				    S_ISSOCK(st.st_mode))
					host = buf;
				else
#endif
					host = "localhost";
			} else if (host != NULL) {
#ifdef HAVE_SYS_UN_H
				if (*host == '/') {
					/* see comment above for why
					 * we don't stat */
					snprintf(buf, sizeof(buf),
						 "%s/.s.monetdb.%d", host, MAPI_PORT);
					host = buf;
				}
#endif
				port = MAPI_PORT;
			} else {
				return mapi_setError(mid, "internal error", __func__, MERROR);
			}
		}
		if (host != mid->hostname) {
			if (mid->hostname)
				free(mid->hostname);
			mid->hostname = strdup(host);
		}
		mid->port = port;
		set_uri(mid);
	}

#ifdef HAVE_SYS_UN_H
	if (mid->hostname && mid->hostname[0] == '/') {
		struct msghdr msg;
		struct iovec vec;
		struct sockaddr_un userver;

		if (strlen(mid->hostname) >= sizeof(userver.sun_path)) {
			return mapi_setError(mid, "path name too long", __func__, MERROR);
		}

		if ((s = socket(PF_UNIX, SOCK_STREAM
#ifdef SOCK_CLOEXEC
				| SOCK_CLOEXEC
#endif
				, 0)) == INVALID_SOCKET) {
			snprintf(errbuf, sizeof(errbuf),
				 "opening socket failed: %s",
#ifdef _MSC_VER
				 wsaerror(WSAGetLastError())
#else
				 strerror(errno)
#endif
				);
			return mapi_setError(mid, errbuf, __func__, MERROR);
		}
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
		(void) fcntl(s, F_SETFD, FD_CLOEXEC);
#endif
		userver = (struct sockaddr_un) {
			.sun_family = AF_UNIX,
		};
		strcpy_len(userver.sun_path, mid->hostname, sizeof(userver.sun_path));

		if (connect(s, (struct sockaddr *) &userver, sizeof(struct sockaddr_un)) == SOCKET_ERROR) {
			snprintf(errbuf, sizeof(errbuf),
				 "initiating connection on socket failed: %s",
#ifdef _MSC_VER
				 wsaerror(WSAGetLastError())
#else
				 strerror(errno)
#endif
				);
			closesocket(s);
			return mapi_setError(mid, errbuf, __func__, MERROR);
		}

		/* send first byte, nothing special to happen */
		msg.msg_name = NULL;
		msg.msg_namelen = 0;
		buf[0] = '0';	/* normal */
		vec.iov_base = buf;
		vec.iov_len = 1;
		msg.msg_iov = &vec;
		msg.msg_iovlen = 1;
		msg.msg_control = NULL;
		msg.msg_controllen = 0;
		msg.msg_flags = 0;

		if (sendmsg(s, &msg, 0) < 0) {
			snprintf(errbuf, sizeof(errbuf), "could not send initial byte: %s",
#ifdef _MSC_VER
				 wsaerror(WSAGetLastError())
#else
				 strerror(errno)
#endif
				);
			closesocket(s);
			return mapi_setError(mid, errbuf, __func__, MERROR);
		}
	} else
#endif
	{
		struct addrinfo hints, *res, *rp;
		char port[32];
		int ret;

		if (mid->hostname == NULL)
			mid->hostname = strdup("localhost");
		snprintf(port, sizeof(port), "%d", mid->port & 0xFFFF);

		hints = (struct addrinfo) {
			.ai_family = AF_UNSPEC,
			.ai_socktype = SOCK_STREAM,
			.ai_protocol = IPPROTO_TCP,
		};
		ret = getaddrinfo(mid->hostname, port, &hints, &res);
		if (ret) {
			snprintf(errbuf, sizeof(errbuf), "getaddrinfo failed: %s", gai_strerror(ret));
			return mapi_setError(mid, errbuf, __func__, MERROR);
		}
		errbuf[0] = 0;
		for (rp = res; rp; rp = rp->ai_next) {
			s = socket(rp->ai_family, rp->ai_socktype
#ifdef SOCK_CLOEXEC
				   | SOCK_CLOEXEC
#endif
				   , rp->ai_protocol);
			if (s != INVALID_SOCKET) {
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
				(void) fcntl(s, F_SETFD, FD_CLOEXEC);
#endif
				if (connect(s, rp->ai_addr, (socklen_t) rp->ai_addrlen) != SOCKET_ERROR)
					break;  /* success */
				closesocket(s);
			}
			snprintf(errbuf, sizeof(errbuf),
				 "could not connect to %s:%s: %s",
				 mid->hostname, port,
#ifdef _MSC_VER
				 wsaerror(WSAGetLastError())
#else
				 strerror(errno)
#endif
				);
		}
		freeaddrinfo(res);
		if (rp == NULL) {
			if (errbuf[0] == 0) {
				/* should not happen */
				snprintf(errbuf, sizeof(errbuf),
					 "getaddrinfo succeeded but did not return a result");
			}
			return mapi_setError(mid, errbuf, __func__, MERROR);
		}
		/* compare our own address with that of our peer and
		 * if they are the same, we were connected to our own
		 * socket, so then we can't use this connection */
		union {
			struct sockaddr_storage ss;
			struct sockaddr_in i4;
			struct sockaddr_in6 i6;
		} myaddr, praddr;
		socklen_t myaddrlen, praddrlen;
		myaddrlen = (socklen_t) sizeof(myaddr.ss);
		praddrlen = (socklen_t) sizeof(praddr.ss);
		if (getsockname(s, (struct sockaddr *) &myaddr.ss, &myaddrlen) == 0 &&
		    getpeername(s, (struct sockaddr *) &praddr.ss, &praddrlen) == 0 &&
		    myaddr.ss.ss_family == praddr.ss.ss_family &&
		    (myaddr.ss.ss_family == AF_INET
		     ? myaddr.i4.sin_port == praddr.i4.sin_port
		     : myaddr.i6.sin6_port == praddr.i6.sin6_port) &&
		    (myaddr.ss.ss_family == AF_INET
		     ? myaddr.i4.sin_addr.s_addr == praddr.i4.sin_addr.s_addr
		     : memcmp(myaddr.i6.sin6_addr.s6_addr,
			      praddr.i6.sin6_addr.s6_addr,
			      sizeof(praddr.i6.sin6_addr.s6_addr)) == 0)) {
			closesocket(s);
			return mapi_setError(mid, "connected to self",
					     __func__, MERROR);
		}
	}

	mid->to = socket_wstream(s, "Mapi client write");
	mapi_log_record(mid, "Mapi client write");
	mid->from = socket_rstream(s, "Mapi client read");
	mapi_log_record(mid, "Mapi client read");
	check_stream(mid, mid->to, "Cannot open socket for writing", mid->error);
	check_stream(mid, mid->from, "Cannot open socket for reading", mid->error);

	// Send an even number of NUL '\0' bytes to the server.
	// This forces an error message when accidentally connecting to a TLS server.
	// Also, surprisingly it seems to make connection setup slightly faster!
	static const char zeroes[8] = { 0 };
	for (ssize_t nleft = sizeof(zeroes); nleft > 0; ) {
		ssize_t nwritten = mnstr_write(mid->to, zeroes, 1, nleft);
		if (nwritten < 0)
			return mapi_setError(mid, "could not send leader block", __func__, MERROR);
		nleft -= nwritten;
	}

	mid->connected = true;

	if (!isa_block_stream(mid->to)) {
		mid->to = block_stream(mid->to);
		check_stream(mid, mid->to, "not a block stream", mid->error);

		mid->from = block_stream(mid->from);
		check_stream(mid, mid->from, "not a block stream", mid->error);
	}

	return MOK;
}


static MapiMsg
mapi_handshake(Mapi mid)
{
	char buf[BLOCK];
	size_t len;
	MapiHdl hdl;

	  try_again_after_redirect:

	/* consume server challenge */
	len = mnstr_read_block(mid->from, buf, 1, sizeof(buf));

	check_stream(mid, mid->from, "Connection terminated while starting", (mid->blk.eos = true, mid->error));

	assert(len < sizeof(buf));
	buf[len] = 0;

	if (len == 0) {
		mapi_setError(mid, "Challenge string is not valid, it is empty", __func__, MERROR);
		return mid->error;
	}
	/* buf at this point looks like "challenge:servertype:protover[:.*]" */

	char *strtok_state = NULL;
	char *chal = strtok_r(buf, ":", &strtok_state);
	if (chal == NULL) {
		mapi_setError(mid, "Challenge string is not valid, challenge not found", __func__, MERROR);
		close_connection(mid);
		return mid->error;
	}

	char *server = strtok_r(NULL, ":", &strtok_state);
	if (server == NULL) {
		mapi_setError(mid, "Challenge string is not valid, server not found", __func__, MERROR);
		close_connection(mid);
		return mid->error;
	}

	char *protover = strtok_r(NULL, ":", &strtok_state);
	if (protover == NULL) {
		mapi_setError(mid, "Challenge string is not valid, protocol not found", __func__, MERROR);
		close_connection(mid);
		return mid->error;
	}
	int pversion = atoi(protover);
	if (pversion != 9) {
		/* because the headers changed, and because it makes no sense to
		 * try and be backwards (or forwards) compatible, we bail out
		 * with a friendly message saying so */
		snprintf(buf, sizeof(buf), "unsupported protocol version: %d, "
			 "this client only supports version 9", pversion);
		mapi_setError(mid, buf, __func__, MERROR);
		close_connection(mid);
		return mid->error;
	}

	char *hashes = strtok_r(NULL, ":", &strtok_state);
	if (hashes == NULL) {
		/* protocol violation, not enough fields */
		mapi_setError(mid, "Not enough fields in challenge string", __func__, MERROR);
		close_connection(mid);
		return mid->error;
	}
	char *algsv[] = {
		"RIPEMD160",
		"SHA512",
		"SHA384",
		"SHA256",
		"SHA224",
		"SHA1",
		NULL
	};
	char **algs = algsv;

	/* rBuCQ9WTn3:mserver:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA1: */

	if (mid->username == NULL || mid->password == NULL) {
		mapi_setError(mid, "username and password must be set",
				__func__, MERROR);
		close_connection(mid);
		return mid->error;
	}

	/* the database has sent a list of supported hashes to us, it's
		* in the form of a comma separated list and in the variable
		* rest.  We try to use the strongest algorithm. */


	/* in rest now should be the byte order of the server */
	char *byteo = strtok_r(NULL, ":", &strtok_state);

	/* Proto v9 is like v8, but mandates that the password is a
		* hash, that is salted like in v8.  The hash algorithm is
		* specified in the 6th field.  If we don't support it, we
		* can't login. */
	char *serverhash = strtok_r(NULL, ":", &strtok_state);

	char *handshake_options = strtok_r(NULL, ":", &strtok_state);
	if (handshake_options) {
		if (sscanf(handshake_options, "sql=%d", &mid->handshake_options) != 1) {
			mapi_setError(mid, "invalid handshake options",
					__func__, MERROR);
			close_connection(mid);
			return mid->error;
		}
	}

	/* hash password, if not already */
	if (mid->password[0] != '\1') {
		char *pwdhash = NULL;
		if (strcmp(serverhash, "RIPEMD160") == 0) {
			pwdhash = mcrypt_RIPEMD160Sum(mid->password,
							strlen(mid->password));
		} else if (strcmp(serverhash, "SHA512") == 0) {
			pwdhash = mcrypt_SHA512Sum(mid->password,
							strlen(mid->password));
		} else if (strcmp(serverhash, "SHA384") == 0) {
			pwdhash = mcrypt_SHA384Sum(mid->password,
							strlen(mid->password));
		} else if (strcmp(serverhash, "SHA256") == 0) {
			pwdhash = mcrypt_SHA256Sum(mid->password,
							strlen(mid->password));
		} else if (strcmp(serverhash, "SHA224") == 0) {
			pwdhash = mcrypt_SHA224Sum(mid->password,
							strlen(mid->password));
		} else if (strcmp(serverhash, "SHA1") == 0) {
			pwdhash = mcrypt_SHA1Sum(mid->password,
							strlen(mid->password));
		} else {
			(void)pwdhash;
			snprintf(buf, sizeof(buf), "server requires unknown hash '%.100s'",
					serverhash);
			close_connection(mid);
			return mapi_setError(mid, buf, __func__, MERROR);
		}

		if (pwdhash == NULL) {
			snprintf(buf, sizeof(buf), "allocation failure or unknown hash '%.100s'",
					serverhash);
			close_connection(mid);
			return mapi_setError(mid, buf, __func__, MERROR);
		}

		free(mid->password);
		mid->password = malloc(1 + strlen(pwdhash) + 1);
		sprintf(mid->password, "\1%s", pwdhash);
		free(pwdhash);
	}


	char *pw = mid->password + 1;

	char *hash = NULL;
	for (; *algs != NULL; algs++) {
		/* TODO: make this actually obey the separation by
			* commas, and only allow full matches */
		if (strstr(hashes, *algs) != NULL) {
			char *pwh = mcrypt_hashPassword(*algs, pw, chal);
			size_t len;
			if (pwh == NULL)
				continue;
			len = strlen(pwh) + strlen(*algs) + 3 /* {}\0 */;
			hash = malloc(len);
			if (hash == NULL) {
				close_connection(mid);
				free(pwh);
				return mapi_setError(mid, "malloc failure", __func__, MERROR);
			}
			snprintf(hash, len, "{%s}%s", *algs, pwh);
			free(pwh);
			break;
		}
	}
	if (hash == NULL) {
		/* the server doesn't support what we can */
		snprintf(buf, sizeof(buf), "unsupported hash algorithms: %.100s", hashes);
		close_connection(mid);
		return mapi_setError(mid, buf, __func__, MERROR);
	}

	mnstr_set_bigendian(mid->from, strcmp(byteo, "BIG") == 0);

	char *p = buf;
	int remaining = sizeof(buf);
	int n;
#define CHECK_SNPRINTF(...) \
	do { \
		n = snprintf(p, remaining, __VA_ARGS__); \
		if (n < remaining) { \
			remaining -= n; \
			p += n; \
		} else { \
			mapi_setError(mid, "combination of database name and user name too long", __func__, MERROR); \
			free(hash); \
			close_connection(mid); \
			return mid->error; \
		} \
	} while (0)

#ifdef WORDS_BIGENDIAN
	char *our_endian = "BIG";
#else
	char *our_endian = "LIT";
#endif
	/* note: if we make the database field an empty string, it
		* means we want the default.  However, it *should* be there. */
	CHECK_SNPRINTF("%s:%s:%s:%s:%s:FILETRANS:",
			our_endian,
			mid->username, hash, mid->language,
			mid->database == NULL ? "" : mid->database);

	if (mid->handshake_options > MAPI_HANDSHAKE_AUTOCOMMIT) {
		CHECK_SNPRINTF("auto_commit=%d", mid->auto_commit);
	}
	if (mid->handshake_options > MAPI_HANDSHAKE_REPLY_SIZE) {
		CHECK_SNPRINTF(",reply_size=%d", mid->cachelimit);
	}
	if (mid->handshake_options > MAPI_HANDSHAKE_SIZE_HEADER) {
		CHECK_SNPRINTF(",size_header=%d", mid->sizeheader); // with underscore, despite X command without
	}
	if (mid->handshake_options > MAPI_HANDSHAKE_COLUMNAR_PROTOCOL) {
		CHECK_SNPRINTF(",columnar_protocol=%d", mid->columnar_protocol);
	}
	if (mid->handshake_options > MAPI_HANDSHAKE_TIME_ZONE) {
		CHECK_SNPRINTF(",time_zone=%d", mid->time_zone);
	}
	if (mid->handshake_options > 0) {
		CHECK_SNPRINTF(":");
	}
	CHECK_SNPRINTF("\n");

	free(hash);

	if (mid->trace) {
		printf("sending first request [%zu]:%s", sizeof(buf), buf);
		fflush(stdout);
	}
	len = strlen(buf);
	mnstr_write(mid->to, buf, 1, len);
	mapi_log_record(mid, buf);
	check_stream(mid, mid->to, "Could not send initial byte sequence", mid->error);
	mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
	check_stream(mid, mid->to, "Could not send initial byte sequence", mid->error);

	/* consume the welcome message from the server */
	hdl = mapi_new_handle(mid);
	if (hdl == NULL) {
		close_connection(mid);
		return MERROR;
	}
	mid->active = hdl;
	read_into_cache(hdl, 0);
	if (mid->error) {
		char *errorstr = NULL;
		MapiMsg error;
		struct MapiResultSet *result;
		/* propagate error from result to mid, the error probably is in
		 * the last produced result, not the first
		 * mapi_close_handle clears the errors, so save them first */
		for (result = hdl->result; result; result = result->next) {
			errorstr = result->errorstr;
			result->errorstr = NULL;	/* clear these so errorstr doesn't get freed */
		}
		if (!errorstr)
			errorstr = mid->errorstr;
		error = mid->error;

		if (hdl->result)
			hdl->result->errorstr = NULL;	/* clear these so errorstr doesn't get freed */
		mid->errorstr = NULL;
		mapi_close_handle(hdl);
		mapi_setError(mid, errorstr, __func__, error);
		if (errorstr != mapi_nomem)
			free(errorstr);	/* now free it after a copy has been made */
		close_connection(mid);
		return mid->error;
	}
	if (hdl->result && hdl->result->cache.line) {
		int i;
		size_t motdlen = 0;
		struct MapiResultSet *result = hdl->result;

		for (i = 0; i < result->cache.writer; i++) {
			if (result->cache.line[i].rows) {
				char **r;
				int m;
				switch (result->cache.line[i].rows[0]) {
				case '#':
					motdlen += strlen(result->cache.line[i].rows) + 1;
					break;
				case '^':
					r = mid->redirects;
					m = NELEM(mid->redirects) - 1;
					while (*r != NULL && m > 0) {
						m--;
						r++;
					}
					if (m == 0)
						break;
					*r++ = strdup(result->cache.line[i].rows + 1);
					*r = NULL;
					break;
				}
			}
		}
		if (motdlen > 0) {
			mid->motd = malloc(motdlen + 1);
			*mid->motd = 0;
			for (i = 0; i < result->cache.writer; i++)
				if (result->cache.line[i].rows && result->cache.line[i].rows[0] == '#') {
					strcat(mid->motd, result->cache.line[i].rows);
					strcat(mid->motd, "\n");
				}
		}

		if (*mid->redirects != NULL) {
			char *red;
			char *p, *q;
			char **fr;

			/* redirect, looks like:
			 * ^mapi:monetdb://localhost:50001/test?lang=sql&user=monetdb
			 * or
			 * ^mapi:merovingian://proxy?database=test */

			/* first see if we reached our redirection limit */
			if (mid->redircnt >= mid->redirmax) {
				mapi_close_handle(hdl);
				mapi_setError(mid, "too many redirects", __func__, MERROR);
				close_connection(mid);
				return mid->error;
			}
			/* we only implement following the first */
			red = mid->redirects[0];

			/* see if we can possibly handle the redirect */
			if (strncmp("mapi:monetdb://", red, 15) == 0) {
				char *db = NULL;
				/* parse components (we store the args
				 * immediately in the mid... ok,
				 * that's dirty) */
				red += 15; /* "mapi:monetdb://" */
				p = red;
				q = NULL;
				if (*red == '[') {
					if ((red = strchr(red, ']')) == NULL) {
						mapi_close_handle(hdl);
						mapi_setError(mid, "invalid IPv6 hostname", __func__, MERROR);
						close_connection(mid);
						return mid->error;
					}
				}
				if ((red = strchr(red, ':')) != NULL) {
					*red++ = '\0';
					q = red;
				} else {
					red = p;
				}
				if ((red = strchr(red, '/')) != NULL) {
					*red++ = '\0';
					if (q != NULL) {
						mid->port = atoi(q);
						if (mid->port == 0)
							mid->port = MAPI_PORT;	/* hardwired default */
					}
					db = red;
				} else {
					red = p;
					db = NULL;
				}
				if (mid->hostname)
					free(mid->hostname);
				mid->hostname = strdup(p);
				if (mid->database)
					free(mid->database);
				mid->database = db != NULL ? strdup(db) : NULL;

				parse_uri_query(mid, red);

				mid->redircnt++;
				mapi_close_handle(hdl);
				/* free all redirects */
				fr = mid->redirects;
				while (*fr != NULL) {
					free(*fr);
					*fr = NULL;
					fr++;
				}
				/* reconnect using the new values */
				close_connection(mid);
				return MREDIRECT;
			} else if (strncmp("mapi:merovingian", red, 16) == 0) {
				/* this is a proxy "offer", it means we should
				 * restart the login ritual, without
				 * disconnecting */
				parse_uri_query(mid, red + 16);
				mid->redircnt++;
				/* free all redirects */
				fr = mid->redirects;
				while (*fr != NULL) {
					free(*fr);
					*fr = NULL;
					fr++;
				}
				goto try_again_after_redirect;
			} else {
				char re[BUFSIZ];
				snprintf(re, sizeof(re),
					 "error while parsing redirect: %.100s\n", red);
				mapi_close_handle(hdl);
				mapi_setError(mid, re, __func__, MERROR);
				close_connection(mid);
				return mid->error;
			}
		}
	}
	mapi_close_handle(hdl);

	if (mid->trace)
		printf("connection established\n");
	if (mid->languageId != LANG_SQL)
		return mid->error;

	if (mid->error != MOK)
		return mid->error;

	/* use X commands to send options that couldn't be sent in the handshake */
	/* tell server about auto_complete and cache limit if handshake options weren't used */
	if (mid->handshake_options <= MAPI_HANDSHAKE_AUTOCOMMIT && mid->auto_commit != MapiStructDefaults.auto_commit) {
		char buf[2];
		sprintf(buf, "%d", !!mid->auto_commit);
		MapiMsg result = mapi_Xcommand(mid, "auto_commit", buf);
		if (result != MOK)
			return mid->error;
	}
	if (mid->handshake_options <= MAPI_HANDSHAKE_REPLY_SIZE && mid->cachelimit != MapiStructDefaults.cachelimit) {
		char buf[50];
		sprintf(buf, "%d", mid->cachelimit);
		MapiMsg result = mapi_Xcommand(mid, "reply_size", buf);
		if (result != MOK)
			return mid->error;
	}
	if (mid->handshake_options <= MAPI_HANDSHAKE_SIZE_HEADER && mid->sizeheader != MapiStructDefaults.sizeheader) {
		char buf[50];
		sprintf(buf, "%d", !!mid->sizeheader);
		MapiMsg result = mapi_Xcommand(mid, "sizeheader", buf); // no underscore!
		if (result != MOK)
			return mid->error;
	}
	// There is no if  (mid->handshake_options <= MAPI_HANDSHAKE_COLUMNAR_PROTOCOL && mid->columnar_protocol != MapiStructDefaults.columnar_protocol)
	// The reason is that columnar_protocol is very new. If it isn't supported in the handshake it isn't supported at
	// all so sending the Xcommand would just give an error.
	if (mid->handshake_options <= MAPI_HANDSHAKE_TIME_ZONE) {
		mapi_set_time_zone(mid, mid->time_zone);
	}

	return mid->error;

}
