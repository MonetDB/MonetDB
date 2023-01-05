/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"
#include <stdio.h>

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


static ssize_t
tls_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	/* Is it safe to cast a void pointer into an ssl_wrapper pointer? */
	ssl_wrapper *w = (ssl_wrapper *)s->stream_data.p;

	return SSL_write(w->cSSL, buf, elmsize*cnt);
}

static ssize_t
tls_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	ssl_wrapper *w = (ssl_wrapper *)s->stream_data.p;

	return SSL_read(w->cSSL, buf, elmsize*cnt);
}

static void
tls_close(stream *s) {
	ssl_wrapper *w = (ssl_wrapper *)s->stream_data.p;
	SSL_shutdown(w->cSSL);
}

stream *
open_tls_serv_stream(int fd, const char *name, bool readonly)
{
	int ssl_err = 1;
	stream *ret;

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
	const char *server_keypair_fname = "/home/kutsurak/src/monetdb/mercurial-repos/public/smapi/smapi-dev-certificates/new/server_keypair.pem";
	ssl_err = SSL_CTX_use_PrivateKey_file(w->ctx, server_keypair_fname, SSL_FILETYPE_PEM);
	if (ssl_err <= 0) {
		/* TODO handle */
		return NULL;
	}

	/* TODO parametrize */
	const char *server_cert_chain_fname = "/home/kutsurak/src/monetdb/mercurial-repos/public/smapi/smapi-dev-certificates/server_cert.pem";
	ssl_err = SSL_CTX_use_certificate_chain_file(w->ctx, server_cert_chain_fname);
	if (ssl_err <= 0) {
		/* TODO handle */
		ERR_print_errors_fp(stdout);
		return NULL;
	}

	ssl_err = SSL_CTX_check_private_key(w->ctx);
	if (ssl_err <= 0) {
		/* TODO handle */
		return NULL;
	}

	SSL_CTX_set_mode(w->ctx, SSL_MODE_AUTO_RETRY);
	w->cSSL = SSL_new(w->ctx);

	SSL_set_fd(w->cSSL, fd);

	ssl_err = SSL_accept(w->cSSL);
	if (ssl_err <= 0) {
		/* TODO handle */
		return NULL;
	}


	if ((ret = create_stream(name)) == NULL) {
		return NULL;
	}


	ret->stream_data.p = w;
	ret->readonly = readonly;
	ret->read = tls_read;
	ret->write = tls_write;
	ret->close = tls_close;

	return ret;
}

#else

stream *
open_tls_serv_stream(int fd)
{
	(void) fd;
	return NULL;
}

#endif	/* HAVE_OPENSSL */
