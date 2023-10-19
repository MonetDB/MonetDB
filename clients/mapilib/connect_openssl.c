
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

	const char *cert;
	switch (msettings_connect_tls_verify(mid->settings)) {
		case verify_none:
		case verify_hash:
			SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
			break;
		case verify_cert:
			cert = msetting_string(mid->settings, MP_CERT);
			if (1 != SSL_CTX_load_verify_locations(ctx, cert, NULL)) {
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
	const char *host = msettings_connect_tcp(settings);
	int port = msettings_connect_port(settings);
	size_t hostlen = strlen(host);
	size_t hostportlen = hostlen + 1 + 20;
	char *hostcolonport = malloc(hostportlen);
	if (hostcolonport == NULL)
		return mapi_setError(mid, "malloc failed", __func__, MERROR);
	snprintf(hostcolonport, hostportlen, "%s:%d", host, port);

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

	// BIO 'bio' represents the whole SSL connection.
	// We will read and write plaintext from it.
	BIO *bio = BIO_new_ssl(ctx, 1);
	if (bio == NULL) {
		closesocket(sock);
		SSL_CTX_free(ctx);
		return croak(mid, __func__, "BIO_new_ssl");
	}
	// BIO_new_ssl() inc'd the reference count of ctx so we can drop our
	// reference here.
	SSL_CTX_free(ctx);
	// On error: close 'sock' and free 'bio'

	SSL *ssl = NULL;
	if (1 != BIO_get_ssl(bio, &ssl)) {
		closesocket(sock);
		BIO_free(bio);
		return croak(mid, __func__, "BIO_get_ssl");
	}
	// As far as I know the SSL returned by BIO_get_ssl has not had
	// its refcount inc'd so we don't need to free it.
	// On error: close 'sock' and free 'bio'.
	assert(ssl != NULL);

	// BIO 'sockbio' wraps the socket. OpenSSL will read and write
	// ciphertext from it.
	BIO *sockbio = BIO_new_socket(sock, BIO_CLOSE);
	if (sockbio == NULL) {
		closesocket(sock);
		BIO_free_all(bio);
		return croak(mid, __func__, "BIO_new_socket");
	}
	// From here on, 'sock' will be free'd by 'sockbio'.
	// On error: free 'sockbio' and free 'bio'.

	if (!BIO_up_ref(sockbio)) {
		BIO_free_all(sockbio);
		BIO_free_all(bio);
		return croak(mid, __func__, "BIO_up_ref sockbio");
	}
	SSL_set0_rbio(ssl, sockbio); // consumes first ref
	SSL_set0_wbio(ssl, sockbio); // consumes second ref
	// from here on 'sockbio' will be freed through 'ssl' which is freed through 'bio'.
	// On error: free 'bio'.

	if (!SSL_set_tlsext_host_name(ssl, host)) {
		BIO_free_all(bio);
		return croak(mid, __func__, "SSL_set_tlsext_host_name");
	}

	// handshake
	if (1 != SSL_connect(ssl)) {
		BIO_free_all(bio);
		return croak(mid, __func__, "SSL_connect");
	}

	/////////////////////////////////////////////////////////////////////
	// Attach the connection to 'mid'

	if (!BIO_up_ref(bio)) {
		BIO_free_all(bio);
		return croak(mid, __func__, "BIO_up_ref bio");
	}
	// On error: free 'bio' twice
	stream *rstream = openssl_rstream(hostcolonport, bio);
	if (rstream == NULL || mnstr_errnr(rstream) != MNSTR_NO__ERROR) {
		BIO_free_all(bio); // drops first ref
		BIO_free_all(bio); // drops second ref
		return croak(mid, __func__, "openssl_rstream: %s", mnstr_peek_error(rstream));
	}
	// On error: free 'bio' and close 'rstream'.
	stream *wstream = openssl_wstream(hostcolonport, bio);
	if (wstream == NULL || mnstr_errnr(wstream) != MNSTR_NO__ERROR) {
		BIO_free_all(bio);
		mnstr_close(rstream);
		return croak(mid, __func__, "openssl_wstream: %s", mnstr_peek_error(wstream));
	}
	// On error: free 'rstream' and 'wstream'.
	msg = mapi_set_streams(mid, rstream, wstream);
	if (msg != MOK) {
		mnstr_close(rstream);
		mnstr_close(wstream);
		return msg;
	}
	// 'rstream' and 'wstream' are part of 'mid' now.

	mapi_log_record(mid, "CONN", "TLS handshake succeeded");
	return MOK;
}
