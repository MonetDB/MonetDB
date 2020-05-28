/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
#ifdef USE_CURL_MULTI
	CURLMcode result;	/* result of transfer (if !running) */
	struct curl_data *next;	/* linked list (curl_handles) */
#endif
};
#ifdef USE_CURL_MULTI
static CURLM *multi_handle;
static struct curl_data *curl_handles;
#endif

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
#ifndef USE_CURL_MULTI
	/* move data if we don't have enough space */
	if (c->maxsize - c->usesize < size && c->offset > 0) {
		memmove(c->buffer, c->buffer + c->offset, c->usesize - c->offset);
		c->usesize -= c->offset;
		c->offset = 0;
	}
#endif
	/* allocate more buffer space if we still don't have enough space */
	if (c->maxsize - c->usesize < size) {
		char *b;
		size_t maxsize;

		maxsize = (c->usesize + size + BLOCK_CURL - 1) & ~(BLOCK_CURL - 1);
		b = realloc(c->buffer, c->maxsize);
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
#ifdef USE_CURL_MULTI
	struct curl_data **cp;
#endif

	if ((c = (struct curl_data *) s->stream_data.p) != NULL) {
		s->stream_data.p = NULL;
#ifdef USE_CURL_MULTI
		/* lock access to curl_handles */
		cp = &curl_handles;
		while (*cp && *cp != c)
			cp = &(*cp)->next;
		if (*cp)
			*cp = c->next;
		/* unlock access to curl_handles */
#endif
		if (c->handle) {
#ifdef USE_CURL_MULTI
			curl_multi_remove_handle(mult_handle, c->handle);
#endif
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
#ifndef USE_CURL_MULTI
	return 0;
#endif
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
#ifdef USE_CURL_MULTI
	CURLMsg *msg;
#endif

	if ((c = malloc(sizeof(*c))) == NULL) {
		mnstr_set_open_error(url, errno, NULL);
		return NULL;
	}
	*c = (struct curl_data) {
		.running = 1,
	};
	if ((s = create_stream(url)) == NULL) {
		free(c);
		return NULL;
	}
#ifdef USE_CURL_MULTI
	/* lock access to curl_handles */
	c->next = curl_handles;
	curl_handles = c;
	/* unlock access to curl_handles */
#endif
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
	curl_easy_setopt(c->handle, CURLOPT_WRITEFUNCTION, write_callback);
#ifdef USE_CURL_MULTI
	if (multi_handle == NULL)
		multi_handle = curl_multi_init();
	curl_multi_add_handle(multi_handle, c->handle);
	while (curl_multi_perform(multi_handle, NULL) == CURLM_CALL_MULTI_PERFORM)
		;
	while ((msg = curl_multi_info_read(multi_handle, NULL)) != NULL) {
		struct curl_data *p;
		/* lock access to curl_handles */
		for (p = curl_handles; p; p = p->next) {
			if (p->handle == msg->easy_handle) {
				switch (msg->msg) {
				case CURLMSG_DONE:
					p->running = 0;
					p->result = msg->data.result;
					curl_multi_remove_handle(multi_handle, p->handle);
					curl_easy_cleanup(p->handle);
					p->handle = NULL;
					break;
				default:
					break;
				}
				break;
			}
		}
		/* unlock access to curl_handles */
	}
#else
	CURLcode ret = curl_easy_perform(c->handle);
	if (ret != CURLE_OK) {
		curl_destroy(s);
		mnstr_set_open_error(url, 0, "curl_easy_perform: %s", curl_easy_strerror(ret));
		return NULL;
	}
	curl_easy_cleanup(c->handle);
	c->handle = NULL;
	c->running = 0;
#endif
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
