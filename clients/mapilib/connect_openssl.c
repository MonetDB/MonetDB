/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

// Request compatibility with OpenSSL 1.1.1.
// We need this for the hashing API.
#define OPENSSL_API_COMPAT 0x10100000L

#include "monetdb_config.h"


#include "stream.h"		/* include before mapi.h */
#include "stream_socket.h"
#include "mapi.h"
#include "mapi_intern.h"

#include <stdarg.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>

MapiMsg
croak_openssl(Mapi mid, const char *action, const char *fmt, ...)
{
	va_list ap;
	char buffer[800];
	va_start(ap, fmt);
	vsnprintf(buffer, sizeof(buffer), fmt, ap);
	va_end(ap);

	unsigned long err = ERR_get_error();
	const char *errmsg =
#ifdef ERR_SYSTEM_ERROR
		ERR_SYSTEM_ERROR(err)
		? strerror(ERR_GET_REASON(err))
		:
#endif
		ERR_reason_error_string(err);
	if (errmsg)
		return mapi_printError(mid, action, MERROR, "TLS error: %s: %s", buffer, errmsg);
	else if (err != 0)
		return mapi_printError(mid, action, MERROR, "TLS error: %s: failed with error %lu (0x%lx)", buffer, err, err);
	else
		return mapi_printError(mid, action, MERROR, "TLS error: %s", buffer);
}

#ifndef NATIVE_WIN32
MapiMsg
add_system_certificates(Mapi mid, SSL_CTX *ctx)
{
	// On Linux, the Linux distribution has arranged for the system root certificates
	// to be found in OpenSSL's default locations.
	// On MacOS, MonetDB is generally installed using Homebrew and then Homebrew
	// takes care of it.
	// On Windows we use another implementation of add_system_certificates(),
	// found in openssl_windows.c.

	if (1 != SSL_CTX_set_default_verify_paths(ctx)) {
		SSL_CTX_free(ctx);
		return croak_openssl(mid, __func__, "SSL_CTX_set_default_verify_paths");
	}
	return MOK;
}
#endif

static MapiMsg
make_ssl_context(Mapi mid, SSL_CTX **ctx_out)
{
	// Today we just create a new one but if we load the system trust store
	// the result could be cached for a while.
	// (What's a reasonable amount of time for a process to pick up changes to
	// the system trust store?)

	MapiMsg msg;
	*ctx_out = NULL;

	const SSL_METHOD *method = TLS_method();
	if (!method)
		return croak_openssl(mid, __func__, "TLS_method");
	SSL_CTX *ctx = SSL_CTX_new(method);
	if (!ctx)
		return croak_openssl(mid, __func__, "SSL_CTX_new");
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
				return croak_openssl(mid, __func__, "SSL_CTX_load_verify_file: %s", cert);
			}
			break;
		case verify_system:
			msg = add_system_certificates(mid, ctx);
			if (msg != MOK) {
				SSL_CTX_free(ctx);
				return msg;
			}
			break;
	}

	*ctx_out = ctx;
	return MOK;
}

static MapiMsg
verify_server_certificate_hash(Mapi mid, X509 *x509, const char *required_prefix)
{
	mapi_log_record(mid, "CONN", "verifying certificate hash against prefix '%s'", required_prefix);

	size_t prefix_len = strlen(required_prefix);
	if (prefix_len > 2 * SHA256_DIGEST_LENGTH)
		return mapi_setError(mid, "value of certhash= is longer than a sha256 digest", __func__, MERROR);

	// Convert to DER
	unsigned char *buf = NULL;
	int buflen = i2d_X509(x509, &buf);
	if (buflen <= 0) {
		return croak_openssl(mid, __func__, "could not convert server certificate to DER");
	}
	assert(buf);

	// Compute the has of the DER using the deprecated API so we stay
	// compatible with OpenSSL 1.1.1.
	SHA256_CTX sha256;
	if (1 != SHA256_Init(&sha256)) {
		OPENSSL_free(buf);
		return mapi_setError(mid, "SHA256_Init", __func__, MERROR);
	}
	if (1 != SHA256_Update(&sha256, buf, buflen)) {
		OPENSSL_free(buf);
		return mapi_setError(mid, "SHA256_Update", __func__, MERROR);
	}
	unsigned char digest[SHA256_DIGEST_LENGTH];
	if (1 != SHA256_Final(digest, &sha256)) {
		OPENSSL_free(buf);
		return mapi_setError(mid, "SHA256_Final", __func__, MERROR);
	}
	OPENSSL_free(buf);

	// Make hexadecimal;
	char hex[2 * SHA256_DIGEST_LENGTH + 1];
	for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
		snprintf(&hex[2 * i], 3, "%02x", digest[i]);
	}
	assert(hex[2 * SHA256_DIGEST_LENGTH] == '\0');

	// Compare the digits
	if (strncmp(required_prefix, hex, prefix_len) != 0)
		return mapi_setError(mid, "server certificate does not match certhash= prefix", __func__, MERROR);

	mapi_log_record(mid, "CONN", "server certificate matches certhash");
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

	const char *clientkey = msettings_connect_clientkey(settings);
	const char *clientcert = msettings_connect_clientcert(settings);
	enum msetting_tls_verify verify_method = msettings_connect_tls_verify(settings);

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
		return croak_openssl(mid, __func__, "BIO_new_ssl");
	}
	// BIO_new_ssl() inc'd the reference count of ctx so we can drop our
	// reference here.
	SSL_CTX_free(ctx);
	// On error: close 'sock' and free 'bio'

	SSL *ssl = NULL;
	if (1 != BIO_get_ssl(bio, &ssl)) {
		closesocket(sock);
		BIO_free(bio);
		return croak_openssl(mid, __func__, "BIO_get_ssl");
	}
	// As far as I know the SSL returned by BIO_get_ssl has not had
	// its refcount inc'd so we don't need to free it.
	// On error: close 'sock' and free 'bio'.
	assert(ssl != NULL);

	// BIO 'sockbio' wraps the socket. OpenSSL will read and write
	// ciphertext from it.
	//
	// The OpenSSL developers apparently believe that it's safe to cast
	// SOCKET to int here, see https://github.com/openssl/openssl/blob/openssl-3.1.3/include/internal/sockets.h#L54-L58
	BIO *sockbio = BIO_new_socket((int)sock, BIO_CLOSE);
	if (sockbio == NULL) {
		closesocket(sock);
		BIO_free_all(bio);
		return croak_openssl(mid, __func__, "BIO_new_socket");
	}
	// From here on, 'sock' will be free'd by 'sockbio'.
	// On error: free 'sockbio' and free 'bio'.

	if (!BIO_up_ref(sockbio)) {
		BIO_free_all(sockbio);
		BIO_free_all(bio);
		return croak_openssl(mid, __func__, "BIO_up_ref sockbio");
	}
	SSL_set0_rbio(ssl, sockbio); // consumes first ref
	SSL_set0_wbio(ssl, sockbio); // consumes second ref
	// from here on 'sockbio' will be freed through 'ssl' which is freed through 'bio'.
	// On error: free 'bio'.

	if (!SSL_set_tlsext_host_name(ssl, host)) {
		BIO_free_all(bio);
		return croak_openssl(mid, __func__, "SSL_set_tlsext_host_name");
	}

	X509_VERIFY_PARAM *param = SSL_get0_param(ssl);
	if (param == NULL) {
		BIO_free_all(bio);
		return croak_openssl(mid, __func__, "SSL_get0_param");
	}
	X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
	if (1 != X509_VERIFY_PARAM_set1_host(param, host, strlen(host))) {
		BIO_free_all(bio);
		return croak_openssl(mid, __func__, "X509_VERIFY_PARAM_set1_host");
	}

	// Temporarily disable the ALPN header.
	// TODO re-enable it when test systemcertificates.py no longer relies
	// on connecting to an HTTPS server. (Which is an ugly hack in the first place!)
#if 0
	unsigned char alpn_vector[] = { 6, 'm', 'a', 'p', 'i', '/', '9' };
	// NOTE: these functions return 0 on success, not 1!
	if (0 != SSL_set_alpn_protos(ssl, alpn_vector, sizeof(alpn_vector))) {
		BIO_free_all(bio);
		return croak_openssl(mid, __func__, "SSL_set_alpn_protos");
	}
#endif

	assert(clientkey);
	assert(clientcert);
	if (clientkey[0]) {
		if (1 != SSL_use_PrivateKey_file(ssl, clientkey, SSL_FILETYPE_PEM)) {
			BIO_free_all(bio);
			return croak_openssl(mid, __func__, "SSL_use_PrivateKey_file");
		}
		if (1 != SSL_use_certificate_chain_file(ssl, clientcert)) {
			BIO_free_all(bio);
			return croak_openssl(mid, __func__, "SSL_use_certificate_chain_file");
		}
	}

	// Handshake.
	if (1 != SSL_connect(ssl)) {
		BIO_free_all(bio);
		return croak_openssl(mid, __func__, "SSL_connect handshake");
	}

	// Verify the server certificate
	X509 *server_cert = SSL_get_peer_certificate(ssl);
	if (server_cert == NULL) {
		BIO_free_all(bio);
		return croak_openssl(mid, __func__, "Server did not send a certificate");
	}
	// be careful when to free server_cert
	if (verify_method == verify_hash) {
		const char *required_prefix = msettings_connect_certhash_digits(settings);
		msg = verify_server_certificate_hash(mid, server_cert, required_prefix);
		X509_free(server_cert);
		if (msg != MOK) {
			BIO_free_all(bio);
			return msg;
		}
	} else {
		X509_free(server_cert);
		long verify_result = SSL_get_verify_result(ssl);
		if (verify_result != X509_V_OK) {
			BIO_free_all(bio);
			const char *error_message = X509_verify_cert_error_string(verify_result);
			return croak_openssl(mid, __func__, "Invalid server certificate: %s", error_message);
		}
	}

	/////////////////////////////////////////////////////////////////////
	// Attach the connection to 'mid'

	if (!BIO_up_ref(bio)) {
		BIO_free_all(bio);
		return croak_openssl(mid, __func__, "BIO_up_ref bio");
	}
	// On error: free 'bio' twice

	char *hostcolonport = malloc(hostportlen);
	if (hostcolonport != NULL)
		snprintf(hostcolonport, hostportlen, "%s:%d", host, port);

	stream *rstream = openssl_rstream(hostcolonport ? hostcolonport : "ssl rstream", bio);
	if (rstream == NULL || mnstr_errnr(rstream) != MNSTR_NO__ERROR) {
		BIO_free_all(bio); // drops first ref
		BIO_free_all(bio); // drops second ref
		free(hostcolonport);
		msg = croak_openssl(mid, __func__, "openssl_rstream: %s", mnstr_peek_error(rstream));
		close_stream(rstream);
		return msg;
	}
	// On error: free 'bio' and close 'rstream'.
	stream *wstream = openssl_wstream(hostcolonport ? hostcolonport : "ssl wstream", bio);
	free(hostcolonport);
	if (wstream == NULL || mnstr_errnr(wstream) != MNSTR_NO__ERROR) {
		BIO_free_all(bio);
		close_stream(rstream);
		msg = croak_openssl(mid, __func__, "openssl_wstream: %s", mnstr_peek_error(wstream));
		close_stream(wstream);
		return msg;
	}
	// On error: free 'rstream' and 'wstream'.
	msg = mapi_wrap_streams(mid, rstream, wstream);
	if (msg != MOK) {
		close_stream(rstream);
		close_stream(wstream);
		return msg;
	}
	// 'rstream' and 'wstream' are part of 'mid' now.


	mapi_log_record(mid, "CONN", "TLS handshake succeeded");
	return MOK;
}
