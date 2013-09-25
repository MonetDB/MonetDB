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
#include <errno.h>

// R headers
#include <R.h>
#include <Rdefines.h>

#define BLOCKSIZE 8190
#define BUFSIZE BLOCKSIZE+1
#define SOCKET int
#define TRUE 1
#define FALSE 0
#define ALLOCSIZE 1048576 // 1 MB
#define DEBUG FALSE

// reference tricks taken from http://homepage.stat.uiowa.edu/~luke/R/simpleref.html#NWarqU3-KrSQa-1
static SEXP MAPI_type_tag;

#define CHECK_MAPI_SOCK(s) do { \
    if (TYPEOF(s) != EXTPTRSXP || \
        R_ExternalPtrTag(s) != MAPI_type_tag || \
        EXTPTR_PTR(s) == NULL) \
        error("Socket has already been closed and cannot be used."); \
} while (0)

SEXP mapiInit(void) {
	MAPI_type_tag = install("MAPI_TYPE_TAG");
	return R_NilValue;
}

SEXP mapiDisconnect(SEXP conn) {
	CHECK_MAPI_SOCK(conn);
	SOCKET *sock = R_ExternalPtrAddr(conn);
	if (sock != NULL) {
		shutdown(*sock, 2);
		R_ClearExternalPtr(conn);
		free(sock);
	}
	return R_NilValue;
}

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

	SEXP connobj;
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
#ifdef __WIN32__
	int sto;
	sto = timeoutval * 1000;
#else
	struct timeval sto;
	sto.tv_sec = timeoutval;
	sto.tv_usec = 0;
#endif

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	// resolve dns name
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

		if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char *) &sto,
				sizeof(sto)) < 0) {
			error("setsockopt failed");
		}
		if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char *) &sto,
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
			break; // Profit
		}
		close(sock);
	}

	if (rp == NULL) { /* No address succeeded */
		error("Could not connect to %s:%i", hostval, portval);
	}
	freeaddrinfo(result);

	// get the socket number off the stack so that we can use R external pointers for passing it around
	// external pointers are very useful as they can have finalizers, in our case, mapiDisconnect()
	void * sockaddr = malloc(sizeof(int));
	if (sockaddr == NULL) {
		error("Error in malloc() for a single integer, srsly?");
	}
	memcpy(sockaddr, &sock, sizeof(int));

	PROTECT(connobj = R_MakeExternalPtr(sockaddr, MAPI_type_tag, R_NilValue));
	R_RegisterCFinalizerEx(connobj, (R_CFinalizer_t) mapiDisconnect, 0);
	CHECK_MAPI_SOCK(connobj);
	UNPROTECT(1);
	return connobj;
}

size_t sockRead(int fd, void *buf, size_t size) {
	ssize_t retval = -1;
	do {
		retval = recv(fd, buf, size, MSG_WAITALL);
	} while (retval == -1 && errno == EINTR);
	if (retval == -1) {
#ifdef __WIN32__
		errno = WSAGetLastError();
#endif
		error("error reading from socket (%d)", errno);
	} else {
		return retval;
	}
}

size_t sockWrite(int fd, const void *buf, size_t size) {
	ssize_t retval = -1;
	do {
		retval = send(fd, buf, size, 0);
	} while (retval == -1 && errno == EINTR);
	if (retval == -1) {
#ifdef __WIN32__
		errno = WSAGetLastError();
#endif
		error("error writing to socket (%d)", errno);
	} else {
		return retval;
	}
}

SEXP mapiRead(SEXP conn) {
	CHECK_MAPI_SOCK(conn);
	SOCKET sock = *((SOCKET*) R_ExternalPtrAddr(conn));

	SEXP lines;
	char read_buf[BUFSIZE];
	int block_final, block_length;
	short header;
	size_t response_buf_len = ALLOCSIZE;
	size_t response_buf_offset = 0;
	size_t n = 0;

	char* response_buf = malloc(response_buf_len);
	if (response_buf == NULL) {
		error("ERROR allocating memory");
	}

	block_final = FALSE;
	while (!block_final) {
		//  read block header and extract block length and final bit from header
		// this assumes little-endianness (so sue me)
		n = sockRead(sock, (void*) &header, 2);
		if (n != 2) {
			error("ERROR reading MAPI block header (%d)", n);
		}
		block_length = header >> 1;
		if (block_length < 0 || block_length > BLOCKSIZE) {
			error("Invalid block size %i\n", block_length);
		}
		block_final = header & 1;

		if (block_length > 0) {
			n = sockRead(sock, (void*) read_buf, block_length);
			if (n != block_length) {
				error(
						"ERROR reading block of %u bytes (final=%s) from socket (%d)",
						block_length, block_final ? "true" : "false", n);
			}
		}
		read_buf[block_length] = '\0';
		if (DEBUG) {
			printf("II: Received block of %u bytes, final=%s\n", block_length,
					block_final ? "true" : "false");
		}
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

	// We have this issue that on Windows sometimes a \0 appears in the middle of the response...
	// since it always appears instead of a \t, let's replace it for now and pray
	// MonetDB Bug #3369 http://bugs.monetdb.org/show_bug.cgi?id=3369
#ifdef __WIN32__
	size_t i;
	for (i = 0; i < response_buf_offset; i++) {
		if (response_buf[i] == '\0') {
			warning("Removed a NULL character from response at offset %lu of %lu",i,response_buf_offset);
			response_buf[i] = '\t';
		}
	}
#endif

	PROTECT(lines = NEW_STRING(1));
	SET_STRING_ELT(lines, 0, mkChar(response_buf));
	free(response_buf);
	unprotect(1);
	return lines;
}

SEXP mapiWrite(SEXP conn, SEXP message) {
	CHECK_MAPI_SOCK(conn);
	SOCKET sock = *((SOCKET*) R_ExternalPtrAddr(conn));

	assert(IS_CHARACTER(message));
	assert(GET_LENGTH(message) == 1);

	const char *messageval = CHAR(STRING_ELT(message, 0));
	assert(strlen(messageval) > 0);

	size_t message_len = strlen(messageval);
	int block_final, block_length;
	size_t n;

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
		n = sockWrite(sock, (void *) &header, 2);
		if (n != 2) {
			error("ERROR writing MAPI block header");
		}
		n = sockWrite(sock, messageval + request_offset, block_length);
		if (n != block_length) {
			error("ERROR writing block of %u bytes (final=%s) to socket",
					block_length, block_final ? "true" : "false");
		}
		request_offset += block_length;
	}
	return R_NilValue;
}

SEXP mapiRequest(SEXP conn, SEXP message) {
	mapiWrite(conn, message);
	return (mapiRead(conn));
}
