
#include "monetdb_config.h"


#include "stream.h"		/* include before mapi.h */
#include "stream_socket.h"
#include "mapi.h"
#include "mapi_intern.h"

#include <stdarg.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

typedef struct ssl_state {
	SOCKET socket;
	SSL_CTX *ctx;
	SSL *ssl;
} ssl_state;

static ssl_state*
create_ssl_state(void)
{
	ssl_state *st = malloc(sizeof(*st));
	if (st == NULL)
		return NULL;
	*st = (ssl_state) {
		.socket = INVALID_SOCKET,
	};
	return st;
}

static void
destroy_ssl_state(struct ssl_state *st)
{
	if (st) {
		if (st->socket != INVALID_SOCKET)
			closesocket(st->socket);
		if (st->ctx != NULL)
			SSL_CTX_free(st->ctx);
	}
	free(st);
}

static MapiMsg croak(Mapi mid, const char *action, const char *fmt, ...)
	__attribute__(( __format__(__printf__, 3, 4) ));
static MapiMsg
croak(Mapi mid, const char *action, const char *fmt, ...)
{
	va_list ap;
	char buffer[800];
	va_start(ap, fmt);
	vsnprintf(buffer, sizeof(buffer), fmt, ap);
	va_end(ap);

	unsigned long err = ERR_get_error();
	const char *errmsg =
		ERR_SYSTEM_ERROR(err)
		? strerror(ERR_GET_REASON(err))
		: ERR_reason_error_string(err);
	if (errmsg)
		return mapi_printError(mid, action, MERROR, "TLS error: %s: %s", buffer, errmsg);
	else
		return mapi_printError(mid, action, MERROR, "TLS error: %s: failed with error %lu (0x%lx)", buffer, err, err);
}

static MapiMsg
perform_handshake(Mapi mid, ssl_state *state, SOCKET sock)
{
	// Based on the example on the OpenSSL wiki:
	// https://wiki.openssl.org/index.php/SSL/TLS_Client
	const msettings *settings = mid->settings;

	// Clear any earlier errrors
	do {} while (ERR_get_error() != 0);

	/////////////////////////////////////////////////////////////////////
	// Set up the context

	const SSL_METHOD *method = TLS_method();
	if (!method)
		return croak(mid, __func__, "TLS_method");
	SSL_CTX *ctx = state->ctx = SSL_CTX_new(method);
	if (!ctx)
		return croak(mid, __func__, "SSL_CTX_new");

	SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
	// SSL_CTX_set_verify_depth: why change the default?
	SSL_CTX_set_options(ctx, SSL_OP_NO_COMPRESSION);
	SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
	// Because we use at least TLSv1.3 we don't need to mess with
	// SSL_CTX_set_cipher_list() and SSL_CTX_set_ciphersuites().

	switch (msettings_connect_tls_verify(settings)) {
		case verify_none:
		case verify_hash:
			SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
			break;
		case verify_cert:
			const char *cert = msetting_string(settings, MP_CERT);
			if (1 != SSL_CTX_load_verify_file(ctx, cert))
				return croak(mid, __func__, "SSL_CTX_load_verify_file: %s", cert);
			break;
		case verify_system:
			if (1 != SSL_CTX_set_default_verify_paths(ctx))
				return croak(mid, __func__, "SSL_CTX_set_default_verify_paths");
			break;
	}

	/////////////////////////////////////////////////////////////////////
	// Create the SSL connection

	SSL *ssl = state->ssl = SSL_new(ctx);
	if (ssl == NULL) {
		return croak(mid, __func__, "SSL_new");
	}

	BIO *bio = BIO_new_socket(sock, BIO_NOCLOSE);
	if (bio == NULL)
		return croak(mid, __func__, "BIO_new_socket");
	if (!BIO_up_ref(bio)) {
		BIO_free_all(bio);
		return croak(mid, __func__, "BIO_up_ref");
	}
	SSL_set0_rbio(ssl, bio); // consumes first ref
	SSL_set0_wbio(ssl, bio); // consumes second ref
	// from here on 'bio' will be freed through 'ssl'.

	const char *hostname = msettings_connect_tcp(settings);
	if (!SSL_set_tlsext_host_name(ssl, hostname))
		return croak(mid, __func__, "SSL_set_tlsext_host_name");

	if (1 != SSL_connect(ssl))
		return croak(mid, __func__, "SSL_connect");


	return mapi_setError(mid, "that's how far we get", __func__, MERROR);
}


MapiMsg
wrap_tls(Mapi mid, SOCKET sock)
{
	ssl_state *state = create_ssl_state();
	if (!state) {
		mapi_setError(mid, "malloc failed", __func__, MERROR);
		goto bailout;
	}

	if (perform_handshake(mid, state, sock) != MOK)
		goto bailout;


	return mapi_setError(mid, "it's still a work in progress", __func__, MERROR);

bailout:
	assert(mid->error != MOK);
	destroy_ssl_state(state);
	return mid->error;
}
