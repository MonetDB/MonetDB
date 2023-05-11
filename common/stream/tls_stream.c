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
#include <sys/types.h>

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
	ssl_wrapper *w = (ssl_wrapper *)s->stream_data.p;
	ssize_t ret;
	size_t retries = 5;
	int ssl_err = SSL_ERROR_NONE;

	fprintf(stderr, "SSL stream write %zu bytes\n", elmsize*cnt);

	ERR_clear_error();

	while (retries > 0) {
		ret = SSL_write(w->cSSL, buf, elmsize*cnt);
		if (ret < 0 ) {
			ssl_err = SSL_get_error(w->cSSL, ret);

			if (ssl_err == SSL_ERROR_ZERO_RETURN) {
				retries -= 1;
			} else {
				break;
			}
		} else {
			break;
		}
	}

	switch (ssl_err) {
		case SSL_ERROR_NONE:
			/* no error */
			s->errkind = MNSTR_NO__ERROR;
			break;
		default:
			s->errkind = MNSTR_WRITE_ERROR;
			ERR_error_string_n(ERR_get_error() , s->errmsg, 1024);
			fprintf(stderr, "SSL write error: %s\n", s->errmsg);
			if (errno) {
				perror(0);
			}
			return -1;
	}
	fprintf(stderr, "SSL stream write wrote %zd bytes\n", ret);

	return ret/elmsize;
}

static ssize_t
tls_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	ssl_wrapper *w = (ssl_wrapper *)s->stream_data.p;
	ssize_t ret;

	fprintf(stderr, "SSL stream read\n");

	ret = SSL_read(w->cSSL, buf, elmsize*cnt);
	if (ret < 0) {
		s->errkind = MNSTR_READ_ERROR;
		ERR_error_string(ERR_get_error(), s->errmsg);
		return -1;
	}

	fprintf(stderr, "SSL stream read got %zd bytes\n", ret);

	if (ret == 0) {
		s->eof = true;
	}
	return ret/elmsize;
}

static void
tls_close(stream *s)
{
	/* TODO properly shutdown */
	ssl_wrapper *w = (ssl_wrapper *)s->stream_data.p;
	SSL_shutdown(w->cSSL);
}

static stream *
new_tls_server_stream(int fd, const char *name, const char *kp_fname, const char *ct_fname)
{
	int ssl_err = 1;
	stream *ret;

	ERR_clear_error();

	ssl_wrapper *w = (ssl_wrapper *)malloc(sizeof(ssl_wrapper));
	if (w == NULL) {
		/* TODO handle */
		fprintf(stderr, "ssl wrapper allocation\n");
		return NULL;
	}

	w->ctx = SSL_CTX_new(TLS_server_method());
	if (w->ctx == NULL) {
		/* TODO handle */
		fprintf(stderr, "SSL_CTX_new\n");
		ERR_print_errors_fp(stderr);
		return NULL;
	}

	ssl_err = SSL_CTX_use_PrivateKey_file(w->ctx, kp_fname, SSL_FILETYPE_PEM);
	if (ssl_err <= 0) {
		/* TODO handle */
		fprintf(stderr, "SSL_CTX_use_PrivateKey_file\n");
		ERR_print_errors_fp(stderr);
		return NULL;
	}

	ssl_err = SSL_CTX_use_certificate_chain_file(w->ctx, ct_fname);
	if (ssl_err <= 0) {
		/* TODO handle */
		fprintf(stderr, "SSL_CTX_use_certificate_chain_file\n");
		ERR_print_errors_fp(stderr);
		return NULL;
	}

	ssl_err = SSL_CTX_check_private_key(w->ctx);
	if (ssl_err <= 0) {
		/* TODO handle */
		fprintf(stderr, "SSL_CTX_check_private_key\n");
		ERR_print_errors_fp(stderr);
		return NULL;
	}

	SSL_CTX_set_mode(w->ctx, SSL_MODE_AUTO_RETRY);
	w->cSSL = SSL_new(w->ctx);

	SSL_set_fd(w->cSSL, fd);

	ssl_err = SSL_accept(w->cSSL);
	if (ssl_err <= 0) {
		/* TODO handle */
		fprintf(stderr, "SSL_accept\n");
		ERR_print_errors_fp(stderr);
		return NULL;
	}


	if ((ret = create_stream(name)) == NULL) {
		fprintf(stderr, "Could not create read stream\n");
		return NULL;
	}


	ret->stream_data.p = w;
	ret->readonly = true;
	ret->read = tls_read;
	ret->write = tls_write;
	ret->close = tls_close;
	ret->binary = true;

	return ret;
}

stream *
open_tls_server_stream(int fd, const char *name, stream *s, const char *kp_fname, const char *ct_fname)
{
	stream *ret;

	/* This assumes that the read stream is created before the write stream. This probably */
	/* needs to change. */
	if (s == NULL) {
		ret = new_tls_server_stream(fd, name, kp_fname, ct_fname);
	}
	else {
		if ((ret = create_stream(name)) == NULL) {
			fprintf(stderr, "Could not create write stream\n");
			return NULL;
		}

		if (s->inner != NULL) {
			ret->stream_data.p = s->inner->stream_data.p;
		}
		else {
			ret->stream_data.p = s->stream_data.p;
		}

		ret->readonly = false;
		ret->read = tls_read;
		ret->write = tls_write;
		ret->close = tls_close;
		ret->binary = true;
	}

	return ret;
}

#else

stream *
open_tls_server_stream(int fd, const char *name, stream *s)
{
	(void) fd;
	(void) name;
	(void) stream;
	return NULL;
}

#endif	/* HAVE_OPENSSL */
