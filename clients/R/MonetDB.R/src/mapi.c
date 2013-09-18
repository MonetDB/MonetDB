#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#ifdef __WIN32__
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#endif
#include <assert.h>
#include <fcntl.h>

// R headers
#include <R.h>
#include <Rdefines.h>

#define BLOCKSIZE 8190
#define BUFSIZE BLOCKSIZE+1
#define SOCKET int
#define SOCK_ATTR "mapi_conn_do_not_touch"
#define CONN_CLASS "monetdb_mapi_conn"
#define TRUE 1
#define FALSE 0
#define ALLOCSIZE 1048576 // 1 MB
#define DEBUG FALSE

SEXP mapiConnect(SEXP host, SEXP port, SEXP timeout) {
	// be a bit paranoid about the parameters
	assert(IS_CHARACTER(host));
	assert(GET_LENGTH(host) == 1);
	assert(IS_INTEGER(port));
	assert(GET_LENGTH(port) == 1);
	assert(IS_INTEGER(timeout));
	assert(GET_LENGTH(timeout) == 1);

	const char *hostval = CHAR(STRING_ELT(host, 0));
	const int portval = INTEGER_POINTER(AS_INTEGER(port))[0];
	const int timeoutval = INTEGER_POINTER(AS_INTEGER(port))[0];

	assert(strlen(hostval) > 0);
	assert(portval > 0 && portval < 65535);
	assert(timeoutval > 0);

	SEXP connobj, class, attr;
	SOCKET sock;

	struct addrinfo hints;
	struct addrinfo *result, *rp;
#ifdef __WIN32__
	// I will not even TRY to understand why this is required
	WSADATA wsaData;
	int iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
	if (iResult != 0) {
		error("WSAStartup failed: %d", iResult);
	}
#endif

	//  send/receive timeouts for socket
	struct timeval sto;
	sto.tv_sec = timeoutval;
	sto.tv_usec = 0;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	char portvalstr[15];
	sprintf(portvalstr, "%d", portval);

	int s = getaddrinfo(hostval, portvalstr, &hints, &result);
	if (s != 0) {
		error("ERROR, failed to resolve host %s", hostval);
	}

	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sock == -1)
			continue;

		if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &sto,
				sizeof(sto)) < 0) {
			error("setsockopt failed");
		}
		if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (char *) &sto,
				sizeof(sto)) < 0) {
			error("setsockopt failed\n");
		}
		// lets have a 1M buffer on this socket, ok?
		int recvbuf_size = ALLOCSIZE;

		if (setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &recvbuf_size,
				sizeof(recvbuf_size))) {
			error("setsockopt failed");
		}
		if (connect(sock, rp->ai_addr, rp->ai_addrlen) != -1) {
			if (DEBUG) {
				printf("II: Connected to %s:%s\n", hostval, portvalstr);
			}
			break; /* Success */
		}
		close(sock);
	}

	if (rp == NULL) { /* No address succeeded */
		error("Could not connect to %s:%i", hostval, portval);
	}
	freeaddrinfo(result);

	if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &sto, sizeof(sto))
			< 0)
		error("setsockopt failed");
	if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (char *) &sto, sizeof(sto))
			< 0)
		error("setsockopt failed");

	// construct a r object of class monetdb_mapi_conn with an attribute holding the connection id
	PROTECT(connobj = ScalarInteger(1));
	PROTECT(attr = ScalarInteger(1));
	PROTECT(class = allocVector(STRSXP, 1));
	SET_STRING_ELT(class, 0, mkChar(CONN_CLASS));
	classgets(connobj, class);
	INTEGER_POINTER(attr)[0] = sock;
	setAttrib(connobj, install(SOCK_ATTR), attr);
	UNPROTECT(3);
	return connobj;
}

SEXP mapiDisconnect(SEXP conn) {
	SOCKET sock = INTEGER_POINTER(
			AS_INTEGER(getAttrib(conn, install(SOCK_ATTR))))[0];
	shutdown(sock, 2);
	return R_NilValue;
}

SEXP mapiRead(SEXP conn) {
	SOCKET sock = INTEGER_POINTER(
			AS_INTEGER(getAttrib(conn, install(SOCK_ATTR))))[0];

	SEXP lines;
	char read_buf[BUFSIZE];
	int n, block_final, block_length;
	short header;
	size_t response_buf_len = ALLOCSIZE;
	size_t response_buf_offset = 0;

	char* response_buf = malloc(response_buf_len);
	if (response_buf == NULL) {
		error("ERROR allocating memory");
	}

	block_final = FALSE;
	while (!block_final) {
		//  read block header and extract block length and final bit from header
		// this assumes little-endianness (so sue me)

		n = recv(sock, (void *) &header, 2, MSG_WAITALL);
		if (n != 2) {
			error("ERROR reading MAPI block header (%d)", n);
		}
		block_length = header >> 1;
		if (block_length < 0 || block_length > BLOCKSIZE) {
			error("Invalid block size %i\n", block_length);
		}
		block_final = header & 1;

		if (block_length > 0) {
			n = recv(sock, read_buf, block_length, MSG_WAITALL);
			if (n != block_length) {
				error(
						"ERROR reading block of %u bytes (final=%s) from socket (%d)",
						block_length, block_final ? "true" : "false", n);
			}
		}
		if (DEBUG) {
			printf("II: Received block of %u bytes, final=%s\n", block_length,
					block_final ? "true" : "false");
		}
		read_buf[block_length] = '\0';
		// lets see whether we need moar memory for the response
		while (response_buf_offset + block_length > response_buf_len) {
			response_buf_len += ALLOCSIZE;
			if (DEBUG) {
				printf("II: Reallocating memory, new size %lu\n",
						response_buf_len);
			}
			response_buf = realloc(response_buf, response_buf_len);
			if (response_buf == NULL) {
				error("ERROR allocating memory");
			}
		}
		// now that we know to have enough space in the buffer, let's copy
		memcpy(response_buf + response_buf_offset, read_buf, block_length);
		response_buf_offset += block_length;
	}

	response_buf[response_buf_offset] = '\0';
	PROTECT(lines = NEW_STRING(1));
	SET_STRING_ELT(lines, 0, mkChar(response_buf));
	free(response_buf);
	unprotect(1);
	return lines;
}

SEXP mapiWrite(SEXP conn, SEXP message) {
	assert(IS_CHARACTER(message));
	assert(GET_LENGTH(message) == 1);

	SOCKET sock = INTEGER_POINTER(
			AS_INTEGER(getAttrib(conn, install(SOCK_ATTR))))[0];
	const char *messageval = CHAR(STRING_ELT(message, 0));
	assert(strlen(messageval) > 0);

	size_t message_len = strlen(messageval);
	int n, block_final, block_length;

	size_t request_offset = 0;

	while (request_offset < message_len) {
		if (message_len - request_offset > BLOCKSIZE) {
			block_length = BLOCKSIZE;
			block_final = FALSE;
		} else {
			block_length = message_len - request_offset;
			block_final = TRUE;
		}
		if (DEBUG) {
			printf("II: Writing block of %u bytes, final=%s\n", block_length,
					block_final ? "true" : "false");
		}
		short header = (short) (block_length << 1);
		if (block_final) {
			header |= 1;
		}
		n = send(sock, (void *) &header, 2, 0);
		if (n != 2) {
			error("ERROR writing MAPI block header");
		}
		n = send(sock, messageval + request_offset, block_length, 0);
		if (n != block_length) {
			error("ERROR writing block of %u bytes (final=%s) to socket",
					block_length, block_final ? "true" : "false");
		}
		request_offset += block_length;
	}
	return R_NilValue;
}
