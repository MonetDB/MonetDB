#include <curl/curl.h>

#ifndef CURL_WRITEFUNC_ERROR
#define CURL_WRITEFUNC_ERROR 0
#endif

static size_t
write_callback(char *buffer, size_t size, size_t nitems, void *userp)
{
	stream *s = userp;

	/* size is expected to always be 1 */

	ssize_t sz = mnstr_write(s, buffer, size, nitems);
	if (sz < 0)
		return CURL_WRITEFUNC_ERROR; /* indicate failure to library */
	return (size_t) sz * size;
}

static stream *
open_urlstream(const char *url, char *errbuf)
{
	CURL *handle;
	stream *s;
	CURLcode ret;

	s = buffer_wastream(NULL, url);
	if (s == NULL) {
		snprintf(errbuf, CURL_ERROR_SIZE, "could not allocate memory");
		return NULL;
	}

	if ((handle = curl_easy_init()) == NULL) {
		mnstr_destroy(s);
		snprintf(errbuf, CURL_ERROR_SIZE, "could not create CURL handle");
		return NULL;
	}

	errbuf[0] = 0;

	if ((ret = curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, errbuf)) != CURLE_OK ||
	    (ret = curl_easy_setopt(handle, CURLOPT_URL, url)) != CURLE_OK ||
	    (ret = curl_easy_setopt(handle, CURLOPT_WRITEDATA, s)) != CURLE_OK ||
	    (ret = curl_easy_setopt(handle, CURLOPT_VERBOSE, 0)) != CURLE_OK ||
	    (ret = curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1)) != CURLE_OK ||
	    (ret = curl_easy_setopt(handle, CURLOPT_FAILONERROR, 1)) != CURLE_OK ||
	    (ret = curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, write_callback)) != CURLE_OK ||
	    (ret = curl_easy_perform(handle)) != CURLE_OK) {
		curl_easy_cleanup(handle);
		mnstr_destroy(s);
		if (errbuf[0] == 0)
			snprintf(errbuf, CURL_ERROR_SIZE, "%s", curl_easy_strerror(ret));
		return NULL;
	}
	curl_easy_cleanup(handle);
	(void) mnstr_get_buffer(s);	/* switch to read-only */
	return s;
}
