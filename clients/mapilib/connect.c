

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

#ifdef _MSC_VER
#define SOCKET_STRERROR()	wsaerror(WSAGetLastError())
#else
#define SOCKET_STRERROR()	strerror(errno)
#endif


static MapiMsg establish_connection(Mapi mid);
static MapiMsg scan_unix_sockets(Mapi mid);
static MapiMsg connect_socket(Mapi mid);
static SOCKET connect_socket_unix(Mapi mid, const char *sockname);
static SOCKET connect_socket_tcp(Mapi mid, const char *host, int port);
static SOCKET connect_socket_tcp_addr(Mapi mid, struct addrinfo *addr);
static MapiMsg mapi_handshake(Mapi mid);

/* (Re-)establish a connection with the server. */
MapiMsg
mapi_reconnectx(Mapi mid)
{
	msettings_error err = msettings_validate(mid->settings);
	if (err)
		return mapi_setError(mid, err, __func__, MERROR);

	// If neither host nor port are given, scan the Unix domain sockets in
	// /tmp and see if any of them serve this database.
	// Otherwise, just try to connect to what was given.
	if (msettings_connect_scan(mid->settings))
		return scan_unix_sockets(mid);
	else
		return establish_connection(mid);
}

#define MAX_SCAN (24)

static MapiMsg
scan_unix_sockets(Mapi mid)
{
	struct {
		int port;
		int priority;
	} candidates[MAX_SCAN];
	int ncandidates = 0;
	DIR *dir = NULL;
	struct dirent *entry;

	msettings *original = mid->settings;
	mid->settings = NULL;  // invalid state, will fix it before use and on return

	// Make a list of Unix domain sockets in /tmp
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

	// Try those owned by us first, then all others
	for (int round = 0; round < 2; round++) {
		for (int i = 0; i < ncandidates; i++) {
			if (candidates[i].priority != round)
				continue;

			assert(!mid->connected);
			assert(mid->settings == NULL);
			mid->settings = msettings_clone(original);
			if (!mid->settings) {
				mid->settings = original;
				return mapi_setError(mid, "malloc failed", __func__, MERROR);
			}
			msettings_error errmsg = msetting_set_long(mid->settings, MP_PORT, candidates[i].port);
			if (errmsg) {
				mapi_setError(mid, errmsg, __func__, MERROR);
				msettings_destroy(mid->settings);
				mid->settings = original;
				return MERROR;
			}
			MapiMsg msg = establish_connection(mid);
			if (msg == MOK) {
				// do not restore original
				msettings_destroy(original);
				return MOK;
			} else {
				msettings_destroy(mid->settings);
				mid->settings = NULL;
				// now we're ready to try another one
			}
		}
	}

	// Last-ditch attempt.
	// We can now freely modify original
	assert(mid->settings == NULL);
	mid->settings = original;
	msettings_error errmsg = msetting_set_string(mid->settings, MP_HOST, "localhost");
	if (errmsg) {
		return mapi_setError(mid, errmsg, __func__, MERROR);
	}
	return establish_connection(mid);
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
			if (msg != MOK)
				return msg;
		}
		msg = mapi_handshake(mid);
	}

	return msg;
}

static MapiMsg
connect_socket(Mapi mid)
{
	SOCKET s = INVALID_SOCKET;

	assert(!mid->connected);
	const char *sockname = msettings_connect_unix(mid->settings);
	const char *tcp_host = msettings_connect_tcp(mid->settings);
	int tcp_port = msettings_connect_port(mid->settings);

	assert(*sockname || *tcp_host);
	if (*sockname) {
		s = connect_socket_unix(mid, sockname);
	}
	if (s == INVALID_SOCKET && *tcp_host) {
		s = connect_socket_tcp(mid, tcp_host, tcp_port);
	}
	if (s == INVALID_SOCKET) {
		assert(mid->error == MERROR);
		mid->error = MERROR;
		return mid->error;
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

#ifndef HAVE_SYS_UN_H
static SOCKET
connect_socket_unix(Mapi mid, const char *sockname)
{
	(void)sockname;
	mapi_setError(mid, "Unix domain sockets not supported", __func__, MERROR);
	return INVALID_SOCKET;
}
#endif

#ifdef HAVE_SYS_UN_H

static SOCKET
connect_socket_unix(Mapi mid, const char *sockname)
{
	struct sockaddr_un userver;
	if (strlen(sockname) >= sizeof(userver.sun_path)) {
		mapi_setError(mid, "path name too long", __func__, MERROR);
		return INVALID_SOCKET;
	}

	// Create the socket, taking care of CLOEXEC

#ifdef SOCK_CLOEXEC
	int s = socket(PF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
#else
	int s = socket(PF_UNIX, SOCK_STREAM, 0);
#endif
	if (s == INVALID_SOCKET) {
		mapi_PrintError(
			mid, __func__, MERROR,
			"could not create Unix domain socket: %s", strerror(errno));
		return INVALID_SOCKET;
	}
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
	(void) fcntl(s, F_SETFD, FD_CLOEXEC);
#endif

	// Attempt to connect

	userver = (struct sockaddr_un) {
		.sun_family = AF_UNIX,
	};
	strcpy_len(userver.sun_path, sockname, sizeof(userver.sun_path));

	if (connect(s, (struct sockaddr *) &userver, sizeof(struct sockaddr_un)) == SOCKET_ERROR) {
		closesocket(s);
		mapi_PrintError(
			mid, __func__, MERROR,
			"connect to Unix domain socket failed: %s", strerror(errno));
		return INVALID_SOCKET;
	}

	// Send an initial zero (not NUL) to let the server know we're not passing a file
	// descriptor.

	ssize_t n = send(s, "0", 1, 0);
	if (n < 1) {
		// used to be if n < 0 but this makes more sense
		closesocket(s);
		mapi_PrintError(
			mid, __func__, MERROR,
			"could not send initial '0' on Unix domain socket: %s", strerror(errno));
		return INVALID_SOCKET;
	}

	return MOK;
}

#endif  // end of ifdef HAVE_SYS_UN_H

static SOCKET
connect_socket_tcp(Mapi mid, const char *host, int port)
{
	int ret;
	char portbuf[10];
	snprintf(portbuf, sizeof(portbuf), "%d", port);

	struct addrinfo hints = (struct addrinfo) {
		.ai_family = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM,
		.ai_protocol = IPPROTO_TCP,
	};
	struct addrinfo *addresses;
	ret = getaddrinfo(host, portbuf, &hints, &addresses);
	if (ret != 0) {
		mapi_PrintError(
			mid, __func__, MERROR,
			"getaddrinfo failed: %s", gai_strerror(ret));
		return INVALID_SOCKET;
	}
	if (addresses == NULL) {
		mapi_PrintError(
			mid, __func__, MERROR,
			"getaddrinfo return 0 addresses");
		return INVALID_SOCKET;
	}

	assert(addresses);
	SOCKET s;
	for (struct addrinfo *addr = addresses; addr; addr = addr->ai_next) {
		s = connect_socket_tcp_addr(mid, addr);
		if (s)
			break;
	}
	freeaddrinfo(addresses);
	if (s == INVALID_SOCKET) {
		return INVALID_SOCKET;
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
		mapi_setError(mid, "connected to self",
					__func__, MERROR);
		return INVALID_SOCKET;
	}

	return s;
}

static SOCKET
connect_socket_tcp_addr(Mapi mid, struct addrinfo *addr)
{
	int socktype = addr->ai_socktype;
#ifdef SOCK_CLOEXEC
	socktype |= SOCK_CLOEXEC;
#endif

	SOCKET s =  socket(addr->ai_family, socktype, addr->ai_protocol);
	if (s == INVALID_SOCKET) {
		mapi_PrintError(
			mid, __func__, MERROR,
			"could not create TCP socket: %s", SOCKET_STRERROR());
		return INVALID_SOCKET;
	}

#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
	(void) fcntl(s, F_SETFD, FD_CLOEXEC);
#endif

	if (connect(s, addr->ai_addr, addr->ai_addrlen) == SOCKET_ERROR) {
		mapi_PrintError(
			mid, __func__, MERROR,
			"could not connect: %s", SOCKET_STRERROR());
		closesocket(s);
		return INVALID_SOCKET;
	}

	return s;
}

static MapiMsg
mapi_handshake(Mapi mid)
{
	char buf[BLOCK];
	size_t len;
	MapiHdl hdl;

	const char *username = msetting_string(mid->settings, MP_USER);
	const char *password = msetting_string(mid->settings, MP_PASSWORD);

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

	if (!*username || !*password) {
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
	if (password[0] != '\1') {
		char *pwdhash = NULL;
		if (strcmp(serverhash, "RIPEMD160") == 0) {
			pwdhash = mcrypt_RIPEMD160Sum(password,
							strlen(password));
		} else if (strcmp(serverhash, "SHA512") == 0) {
			pwdhash = mcrypt_SHA512Sum(password,
							strlen(password));
		} else if (strcmp(serverhash, "SHA384") == 0) {
			pwdhash = mcrypt_SHA384Sum(password,
							strlen(password));
		} else if (strcmp(serverhash, "SHA256") == 0) {
			pwdhash = mcrypt_SHA256Sum(password,
							strlen(password));
		} else if (strcmp(serverhash, "SHA224") == 0) {
			pwdhash = mcrypt_SHA224Sum(password,
							strlen(password));
		} else if (strcmp(serverhash, "SHA1") == 0) {
			pwdhash = mcrypt_SHA1Sum(password,
							strlen(password));
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

		char *replacement_password = malloc(1 + strlen(pwdhash) + 1);
		if (replacement_password == NULL) {
			close_connection(mid);
			return mapi_setError(mid, "malloc failed", __func__, MERROR);
		}
		sprintf(replacement_password, "\1%s", pwdhash);
		free(pwdhash);
		msettings_error errmsg = msetting_set_string(mid->settings, MP_PASSWORD, replacement_password);
		if (errmsg != NULL) {
			close_connection(mid);
			return mapi_setError(mid, "could not stow hashed password", __func__, MERROR);
		}
	}


	const char *pw = msetting_string(mid->settings, MP_PASSWORD);
	assert(*pw == '\1');
	pw++;

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
	const char *language = msetting_string(mid->settings, MP_LANGUAGE);
	const char *database = msetting_string(mid->settings, MP_DATABASE);
	CHECK_SNPRINTF("%s:%s:%s:%s:%s:FILETRANS:",
			our_endian,
			username, hash,
			language, database);

	if (mid->handshake_options > MAPI_HANDSHAKE_AUTOCOMMIT) {
		CHECK_SNPRINTF("auto_commit=%d", msetting_bool(mid->settings, MP_AUTOCOMMIT));
	}
	if (mid->handshake_options > MAPI_HANDSHAKE_REPLY_SIZE) {
		CHECK_SNPRINTF(",reply_size=%ld", msetting_long(mid->settings, MP_REPLYSIZE));
	}
	if (mid->handshake_options > MAPI_HANDSHAKE_SIZE_HEADER) {
		CHECK_SNPRINTF(",size_header=%d", mid->sizeheader); // with underscore, despite X command without
	}
	if (mid->handshake_options > MAPI_HANDSHAKE_COLUMNAR_PROTOCOL) {
		CHECK_SNPRINTF(",columnar_protocol=%d", mid->columnar_protocol);
	}
	if (mid->handshake_options > MAPI_HANDSHAKE_TIME_ZONE) {
		CHECK_SNPRINTF(",time_zone=%ld", msetting_long(mid->settings, MP_TIMEZONE));
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
			mid->redircnt++;

			/* we only implement following the first */
			char *red = mid->redirects[0];

			char *error_message = NULL;
			if (!msettings_parse_url(mid->settings, red, &error_message)) {
				mapi_close_handle(hdl);
				close_connection(mid);
				return mapi_PrintError(
					mid, __func__, MERROR,
					"%s: %s",
					error_message ? error_message : "invalid redirect",
					red);
			}

			if (strncmp("mapi:merovingian", red, 16) == 0) {
				// do not close the connection so caller knows to restart handshake
				assert(mid->connected);
			} else {
				close_connection(mid);
			}
			return MREDIRECT;
		}
	}
	mapi_close_handle(hdl);

	if (mid->trace)
		printf("connection established\n");

	// I don't understand this assert.
	if (!msettings_lang_is_sql(mid->settings))
		return mid->error;

	if (mid->error != MOK)
		return mid->error;

	/* use X commands to send options that couldn't be sent in the handshake */
	/* tell server about auto_complete and cache limit if handshake options weren't used */
	bool autocommit = msetting_bool(mid->settings, MP_AUTOCOMMIT);
	if (mid->handshake_options <= MAPI_HANDSHAKE_AUTOCOMMIT && autocommit != msetting_bool(msettings_default, MP_AUTOCOMMIT)) {
		char buf[2];
		sprintf(buf, "%d", !!autocommit);
		MapiMsg result = mapi_Xcommand(mid, "auto_commit", buf);
		if (result != MOK)
			return mid->error;
	}
	long replysize = msetting_long(mid->settings, MP_REPLYSIZE);
	if (mid->handshake_options <= MAPI_HANDSHAKE_REPLY_SIZE && replysize != msetting_long(msettings_default, MP_REPLYSIZE)) {
		char buf[50];
		sprintf(buf, "%ld", replysize);
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
		mapi_set_time_zone(mid, msetting_long(mid->settings, MP_TIMEZONE));
	}

	return mid->error;

}
