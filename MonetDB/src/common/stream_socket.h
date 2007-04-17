/* This file contains the prototype declarations of the stream
 * functions that need special include files (sockets and SSL) */

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef NATIVE_WIN32
# include <winsock.h>
#endif

#ifndef INVALID_SOCKET
#define INVALID_SOCKET (-1)
#endif

stream_export stream *socket_rstream(SOCKET socket, const char *name);
stream_export stream *socket_wstream(SOCKET socket, const char *name);
stream_export stream *socket_rastream(SOCKET socket, const char *name);
stream_export stream *socket_wastream(SOCKET socket, const char *name);

#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>

stream_export stream *ssl_rstream(SSL * ssl, const char *name);
stream_export stream *ssl_wstream(SSL * ssl, const char *name);
stream_export stream *ssl_rastream(SSL * ssl, const char *name);
stream_export stream *ssl_wastream(SSL * ssl, const char *name);
#endif
