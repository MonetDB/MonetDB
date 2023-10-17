
#include "monetdb_config.h"


#include "stream.h"		/* include before mapi.h */
#include "stream_socket.h"
#include "mapi.h"
#include "mapi_intern.h"

#include <stdarg.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

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
	else if (err != 0)
		return mapi_printError(mid, action, MERROR, "TLS error: %s: failed with error %lu (0x%lx)", buffer, err, err);
	else
		return mapi_printError(mid, action, MERROR, "TLS error: %s", buffer);
}

static MapiMsg
make_ssl_context(Mapi mid, SSL_CTX **ctx_out)
{
	// Today we just create a new one but if we load the system trust store
	// the result could be cached for a while.
	// (What's a reasonable amount of time for a process to pick up changes to
	// the system trust store?)

	*ctx_out = NULL;

	const SSL_METHOD *method = TLS_method();
	if (!method)
		return croak(mid, __func__, "TLS_method");
	SSL_CTX *ctx = SSL_CTX_new(method);
	if (!ctx)
		return croak(mid, __func__, "SSL_CTX_new");
	// From here on we need to free 'ctx' on failure

	SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
	SSL_CTX_set_options(ctx, SSL_OP_NO_COMPRESSION);
	SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
	// Because we use at least TLSv1.3 we don't need to mess with
	// SSL_CTX_set_cipher_list() and SSL_CTX_set_ciphersuites().

	switch (msettings_connect_tls_verify(mid->settings)) {
		case verify_none:
		case verify_hash:
			SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
			break;
		case verify_cert:
			const char *cert = msetting_string(mid->settings, MP_CERT);
			if (1 != SSL_CTX_load_verify_file(ctx, cert)) {
				SSL_CTX_free(ctx);
				return croak(mid, __func__, "SSL_CTX_load_verify_file: %s", cert);
			}
			break;
		case verify_system:
			if (1 != SSL_CTX_set_default_verify_paths(ctx)) {
				SSL_CTX_free(ctx);
				return croak(mid, __func__, "SSL_CTX_set_default_verify_paths");
			}
			break;
	}

	*ctx_out = ctx;
	return MOK;
}

MapiMsg
wrap_tls(Mapi mid, SOCKET sock)
{
	// Based on the example on the OpenSSL wiki:
	// https://wiki.openssl.org/index.php/SSL/TLS_Client

	// On error: close 'sock'.

	MapiMsg msg = MOK;
	const msettings *settings = mid->settings;

	// Clear any earlier errrors
	do {} while (ERR_get_error() != 0);

	SSL_CTX *ctx = NULL;
	msg = make_ssl_context(mid, &ctx);
	if (msg != MOK) {
		closesocket(sock);
		return msg;
	}
	// On error: close 'sock' and free 'ctx'.

	/////////////////////////////////////////////////////////////////////
	// Create the SSL connection

	SSL *ssl = SSL_new(ctx);
	if (ssl == NULL) {
		closesocket(sock);
		SSL_CTX_free(ctx);
		return croak(mid, __func__, "SSL_new");
	}
	// SSL_new has inc'd the refcount of ctx. We can now drop our ref
	// so we don't have to call SSL_CTX_free all the time
	SSL_CTX_free(ctx);
	// On error: close 'sock' and free 'ssl'.

	BIO *bio = BIO_new_socket(sock, BIO_CLOSE);
	if (bio == NULL) {
		closesocket(sock);
		SSL_free(ssl);
		return croak(mid, __func__, "BIO_new_socket");
	}
	// From here on, 'sock' will be free'd by 'bio'.
	// On error: free 'bio' and free 'ssl'.

	if (!BIO_up_ref(bio)) {
		BIO_free_all(bio);
		SSL_free(ssl);
		return croak(mid, __func__, "BIO_up_ref");
	}
	SSL_set0_rbio(ssl, bio); // consumes first ref
	SSL_set0_wbio(ssl, bio); // consumes second ref
	// from here on 'bio' will be freed through 'ssl'.
	// On error: free 'ssl'.

	const char *hostname = msettings_connect_tcp(settings);
	if (!SSL_set_tlsext_host_name(ssl, hostname)) {
		SSL_free(ssl);
		return croak(mid, __func__, "SSL_set_tlsext_host_name");
	}

	// handshake
	if (1 != SSL_connect(ssl)) {
		SSL_free(ssl);
		return croak(mid, __func__, "SSL_connect");
	}

	/////////////////////////////////////////////////////////////////////
	// Attach the connection to 'mid'

	if (!SSL_up_ref(ssl)) {
		SSL_free(ssl);
		return croak(mid, __func__, "SSL_up_ref");
	}
	// On error: free 'ssl' twice
	stream *rstream = openssl_stream(ssl);
	if (rstream == NULL || mnstr_errnr(rstream) != MNSTR_NO__ERROR) {
		SSL_free(ssl); // drops first ref
		SSL_free(ssl); // drops second ref
		return croak(mid, __func__, "openssl_stream: %s", mnstr_peek_error(rstream));
	}
	// On error: free 'ssl' and close 'rstream'.
	stream *wstream = openssl_stream(ssl);
	if (wstream == NULL || mnstr_errnr(wstream) != MNSTR_NO__ERROR) {
		mnstr_close(rstream);
		SSL_free(ssl);
		return croak(mid, __func__, "openssl_stream: %s", mnstr_peek_error(wstream));
	}
	// On error: free 'rstream' and 'wstream'.
	msg = mapi_set_streams(mid, rstream, wstream);
	if (msg != MOK) {
		mnstr_close(rstream);
		mnstr_close(wstream);
		return msg;
	}
	// 'rstream' and 'wstream' are part of 'mid' now.

	return MOK;
}
