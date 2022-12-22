/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"

/* ---------------------------------------------- */
/* streams working over TLS */

#ifdef HAVE_OPENSSL
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

typedef struct ssl_wrapper {
	SSL_CTX *ctx;
	SSL *cSSL;
} ssl_wrapper;

/*
static ssize_t
tls_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	(void)s;
	(void)buf;
	(void)elmsize;
	(void)cnt;

	return 0;
}

static ssize_t
tls_read(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	(void)s;
	(void)buf;
	(void)elmsize;
	(void)cnt;

	return 0;
}
*/
stream *
open_tls_serv_stream(int fd)
{
	int err = 1;

	ssl_wrapper *w = (ssl_wrapper *)malloc(sizeof(ssl_wrapper));
	if (w == NULL) {
		/* TODO handle */
		return NULL;
	}

	w->ctx = SSL_CTX_new(TLS_server_method());
	if (w->ctx == NULL) {
		/* TODO handle */
		return NULL;
	}


	/* TODO parametrize */
	const char *server_keypair_fname = "/home/kutsurak/src/monetdb/mercurial-repos/public/smapi/smapi-dev-certificates/leaf_keypair.pem";
	err = SSL_CTX_use_PrivateKey_file(w->ctx, server_keypair_fname, SSL_FILETYPE_PEM);
	if (err <= 0) {
		/* TODO handle */
		return NULL;
	}

	/* TODO parametrize */
	const char *server_cert_chain_fname = "/home/kutsurak/src/monetdb/mercurial-repos/public/smapi/smapi-dev-certificates/leaf_cert.pem";
	err = SSL_CTX_use_certificate_chain_file(w->ctx, server_cert_chain_fname);
	if (err <= 0) {
		/* TODO handle */
		return NULL;
	}

	err = SSL_CTX_check_private_key(w->ctx);
	if (err <= 0) {
		/* TODO handle */
		return NULL;
	}

	SSL_CTX_set_mode(w->ctx, SSL_MODE_AUTO_RETRY);
	w->cSSL = SSL_new(w->ctx);

	SSL_set_fd(w->cSSL, fd);

	/* TODO: Accept connection and construct stream.
	 *
	 * NOTE: Accepting the connection will probably need to happen at the point where the TCP connection is
	 * accepted. The handshake also happens there and the open_tlsstream is given a fully constructed ssl wrapper.
	 */

	return NULL;
}

#else

stream *
open_tls_serv_stream(int fd)
{
	(void) fd;
	return NULL;
}

#endif	/* HAVE_OPENSSL */
