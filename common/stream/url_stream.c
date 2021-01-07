/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* streams working on a gzip-compressed disk file */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"



/* ------------------------------------------------------------------ */
/* streams working on a remote file using cURL */

#ifdef HAVE_CURL
#include <curl/curl.h>

struct curl_data {
	CURL *handle;
	char *buffer;		/* buffer to store incoming data */
	size_t maxsize;		/* size of allocated buffer */
	size_t usesize;		/* end of used data */
	size_t offset;		/* start of unread data */
	int running;		/* whether still transferring */
	char errbuf[CURL_ERROR_SIZE]; /* a place for error messages */
};

#define BLOCK_CURL	((size_t) 1 << 16)

/* this function is called by libcurl when there is data for us */
static size_t
write_callback(char *buffer, size_t size, size_t nitems, void *userp)
{
	stream *s = (stream *) userp;
	struct curl_data *c = (struct curl_data *) s->stream_data.p;

	size *= nitems;
	if (size == 0)		/* unlikely */
		return 0;
	/* allocate a buffer if we don't have one yet */
	if (c->buffer == NULL) {
		/* BLOCK_CURL had better be a power of 2! */
		c->maxsize = (size + BLOCK_CURL - 1) & ~(BLOCK_CURL - 1);
		if ((c->buffer = malloc(c->maxsize)) == NULL)
			return 0;
		c->usesize = 0;
		c->offset = 0;
	}
	/* move data if we don't have enough space */
	if (c->maxsize - c->usesize < size && c->offset > 0) {
		memmove(c->buffer, c->buffer + c->offset, c->usesize - c->offset);
		c->usesize -= c->offset;
		c->offset = 0;
	}
	/* allocate more buffer space if we still don't have enough space */
	if (c->maxsize - c->usesize < size) {
		char *b;
		size_t maxsize;

		maxsize = (c->usesize + size + BLOCK_CURL - 1) & ~(BLOCK_CURL - 1);
		b = realloc(c->buffer, maxsize);
		if (b == NULL)
			return 0;	/* indicate failure to library */
		c->buffer = b;
		c->maxsize = maxsize;
	}
	/* finally, store the data we received */
	memcpy(c->buffer + c->usesize, buffer, size);
	c->usesize += size;
	return size;
}

static void
curl_destroy(stream *s)
{
	struct curl_data *c;

	if ((c = (struct curl_data *) s->stream_data.p) != NULL) {
		s->stream_data.p = NULL;
		if (c->handle) {
			curl_easy_cleanup(c->handle);
		}
		if (c->buffer)
			free(c->buffer);
		free(c);
	}
	destroy_stream(s);
}

static ssize_t
curl_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	struct curl_data *c = (struct curl_data *) s->stream_data.p;
	size_t size = cnt * elmsize;

	if (c == NULL) {
		mnstr_set_error(s, MNSTR_READ_ERROR, "stream already ended");
		return -1;
	}

	if (size == 0)
		return 0;
	if (c->usesize - c->offset >= elmsize || !c->running) {
		/* there is at least one element's worth of data
		 * available, or we have reached the end: return as
		 * much as we have, but no more than requested */
		if (size > c->usesize - c->offset) {
			cnt = (c->usesize - c->offset) / elmsize;
			size = cnt * elmsize;
		}
		memcpy(buf, c->buffer + c->offset, size);
		c->offset += size;
		if (c->offset == c->usesize)
			c->usesize = c->offset = 0;
		return (ssize_t) cnt;
	}
	/* not enough data, we must wait until we get some */
	return 0;
}

static ssize_t
curl_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	(void) s;
	(void) buf;
	(void) elmsize;
	(void) cnt;
	assert(0);
	return -1;
}

static void
curl_close(stream *s)
{
	(void) s;
}

stream *
open_urlstream(const char *url)
{
	stream *s;
	struct curl_data *c;

	if ((c = malloc(sizeof(*c))) == NULL) {
		mnstr_set_open_error(url, errno, NULL);
		return NULL;
	}
	*c = (struct curl_data) {
		.running = 1,
		.errbuf = {0},
	};
	if ((s = create_stream(url)) == NULL) {
		free(c);
		return NULL;
	}
	s->read = curl_read;
	s->write = curl_write;
	s->close = curl_close;
	s->destroy = curl_destroy;
	if ((c->handle = curl_easy_init()) == NULL) {
		free(c);
		destroy_stream(s);
		mnstr_set_open_error(url, 0, "curl_easy_init failed");
		return NULL;
	}
	s->stream_data.p = (void *) c;
	curl_easy_setopt(c->handle, CURLOPT_URL, s->name);
	curl_easy_setopt(c->handle, CURLOPT_WRITEDATA, s);
	curl_easy_setopt(c->handle, CURLOPT_VERBOSE, 0);
	curl_easy_setopt(c->handle, CURLOPT_NOSIGNAL, 1);
	curl_easy_setopt(c->handle, CURLOPT_FAILONERROR, 1);
	curl_easy_setopt(c->handle, CURLOPT_ERRORBUFFER, c->errbuf);
	curl_easy_setopt(c->handle, CURLOPT_WRITEFUNCTION, write_callback);
	CURLcode ret = curl_easy_perform(c->handle);
	if (ret != CURLE_OK) {
		if (strlen(c->errbuf) > 0)
			mnstr_set_open_error(url, 0, "%s", c->errbuf);
		else
			mnstr_set_open_error(url, 0, "curl_easy_perform: %s", curl_easy_strerror(ret));
		curl_destroy(s);
		return NULL;
	}
	curl_easy_cleanup(c->handle);
	c->handle = NULL;
	c->running = 0;
	return s;
}

#else
stream *
open_urlstream(const char *url)
{
	if (url != NULL &&
	    strncmp(url, "file://", sizeof("file://") - 1) == 0) {
		url +=sizeof("file://") - 1;
#ifdef _MSC_VER
		/* file:///C:/... -- remove third / as well */
		if (url[0] == '/' && url[2] == ':')
			url++;
#endif
		return open_rastream(url);
	}
	mnstr_set_open_error(url, 0, "Remote URL support has been left out of this MonetDB");
	return NULL;
}
#endif /* HAVE_CURL */
