/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 *  A. de Rijke
 * The cURL module
 * The cURL module contains a wrapper for all function in
 * libcurl.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"

#include <unistd.h>
#include <string.h>

struct MemoryStruct {
	char *memory;
	size_t size;
};

#ifdef HAVE_CURL
#include <curl/curl.h>

static size_t
WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t realsize = size * nmemb;
	struct MemoryStruct *mem = (struct MemoryStruct *)userp;
	char *nmem;

	nmem = realloc(mem->memory, mem->size + realsize + 1);
	if(nmem == NULL) {
		/* out of memory! */
		free(mem->memory);
		mem->memory = NULL;
		fprintf(stderr, "mcurl module: not enough memory (realloc returned NULL)\n");
		return 0;
	}
	mem->memory = nmem;

	memcpy(&(mem->memory[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->memory[mem->size] = 0;

	return realsize;
}

static str
handle_get_request(str *retval, str *url)
{
	str d = NULL;
	str msg = MAL_SUCCEED;

	CURL *curl_handle;
	CURLcode res = CURLE_OK;

	struct MemoryStruct chunk;

	chunk.memory = malloc(1);  /* will be grown as needed by the realloc above */
	if (chunk.memory == NULL)
		throw(MAL, "mcurl.getrequest", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	chunk.size = 0;    /* no data at this point */

	curl_global_init(CURL_GLOBAL_ALL);
	/* init the curl session */
	curl_handle = curl_easy_init();
	curl_easy_setopt(curl_handle, CURLOPT_HTTPGET, 1);
	/* set URL to get */

	curl_easy_setopt(curl_handle, CURLOPT_URL, *url);

	/* no progress meter please */
	curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);

	/* send all data to this function  */
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);

	/* we want the body be written to this file handle instead of stdout */
	/* coverity[bad_sizeof] */
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);

	/* get it! */
	res = curl_easy_perform(curl_handle);

	/* check for errors */
	if(res != CURLE_OK) {
		msg = createException(MAL, "mcurl.getrequest",
							  "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
	} else {
		/*
		 * Now, our chunk.memory points to a memory block that is
		 * chunk.size bytes big and contains the remote file.
		 *
		 * Do something nice with it!
		 *
		 * You should be aware of the fact that at this point we might
		 * have an allocated data block, and nothing has yet
		 * deallocated that data. So when you're done with it, you
		 * should free() it as a nice application.
		 */

		//printf("%zu bytes retrieved\n", chunk.size);
	}
	if (chunk.size) {
		d = GDKstrdup(chunk.memory);
		if(chunk.memory)
			free(chunk.memory);
	}
	/* cleanup curl stuff */
	curl_easy_cleanup(curl_handle);

	*retval = d;
	return msg;
}

static str
handle_put_request(str *retval, str *url)
{
	str d = NULL;
	str msg = MAL_SUCCEED;

	CURL *curl_handle;
	CURLcode res = CURLE_OK;

	struct MemoryStruct chunk;

	chunk.memory = malloc(1);  /* will be grown as needed by the realloc above */
	if (chunk.memory == NULL)
		throw(MAL, "mcurl.putrequest", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	chunk.size = 0;    /* no data at this point */

	curl_global_init(CURL_GLOBAL_ALL);
	/* init the curl session */
	curl_handle = curl_easy_init();
	curl_easy_setopt(curl_handle, CURLOPT_PUT, 1);
	/* set URL to get */

	curl_easy_setopt(curl_handle, CURLOPT_URL, *url);

	/* no progress meter please */
	curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);

	/* send all data to this function  */
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);

	/* we want the body be written to this file handle instead of stdout */
	/* coverity[bad_sizeof] */
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);

	/* get it! */
	res = curl_easy_perform(curl_handle);

	/* check for errors */
	if(res != CURLE_OK) {
		msg = createException(MAL, "mcurl.putrequest",
							  "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
	} else {
		/*
		 * Now, our chunk.memory points to a memory block that is
		 * chunk.size bytes big and contains the remote file.
		 *
		 * Do something nice with it!
		 *
		 * You should be aware of the fact that at this point we might
		 * have an allocated data block, and nothing has yet
		 * deallocated that data. So when you're done with it, you
		 * should free() it as a nice application.
		 */

		//printf("%zu bytes retrieved\n", chunk.size);
	}
	if (chunk.size) {
		d = GDKstrdup(chunk.memory);
		if(chunk.memory)
			free(chunk.memory);
	}
	/* cleanup curl stuff */
	curl_easy_cleanup(curl_handle);

	*retval = d;
	return msg;
}

static str
handle_post_request(str *retval, str *url)
{
	str d = NULL;
	str msg = MAL_SUCCEED;

	CURL *curl_handle;
	CURLcode res = CURLE_OK;

	struct MemoryStruct chunk;

	chunk.memory = malloc(1);  /* will be grown as needed by the realloc above */
	if (chunk.memory == NULL)
		throw(MAL, "mcurl.postrequest", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	chunk.size = 0;    /* no data at this point */

	curl_global_init(CURL_GLOBAL_ALL);
	/* init the curl session */
	curl_handle = curl_easy_init();
	curl_easy_setopt(curl_handle, CURLOPT_POST, 1);
	/* set URL to get */

	curl_easy_setopt(curl_handle, CURLOPT_URL, *url);

	/* no progress meter please */
	curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);

	/* send all data to this function  */
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);

	/* we want the body be written to this file handle instead of stdout */
	/* coverity[bad_sizeof] */
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);

	/* get it! */
	res = curl_easy_perform(curl_handle);

	/* check for errors */
	if(res != CURLE_OK) {
		msg = createException(MAL, "mcurl.postrequest",
							  "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
	} else {
		/*
		 * Now, our chunk.memory points to a memory block that is
		 * chunk.size bytes big and contains the remote file.
		 *
		 * Do something nice with it!
		 *
		 * You should be aware of the fact that at this point we might
		 * have an allocated data block, and nothing has yet
		 * deallocated that data. So when you're done with it, you
		 * should free() it as a nice application.
		 */

		//printf("%zu bytes retrieved\n", chunk.size);
	}
	if (chunk.size) {
		d = GDKstrdup(chunk.memory);
		if(chunk.memory)
			free(chunk.memory);
	}
	/* cleanup curl stuff */
	curl_easy_cleanup(curl_handle);

	*retval = d;
	return msg;
}

static str
handle_delete_request(str *retval, str *url)
{
	str d = NULL;
	str msg = MAL_SUCCEED;

	CURL *curl_handle;
	CURLcode res = CURLE_OK;
	char * delete_request = "DELETE";

	struct MemoryStruct chunk;

	chunk.memory = malloc(1);  /* will be grown as needed by the realloc above */
	if (chunk.memory == NULL)
		throw(MAL, "mcurl.deleterequest", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	chunk.size = 0;    /* no data at this point */

	curl_global_init(CURL_GLOBAL_ALL);
	/* init the curl session */
	curl_handle = curl_easy_init();
	curl_easy_setopt(curl_handle, CURLOPT_CUSTOMREQUEST, delete_request);
	/* set URL to get */

	curl_easy_setopt(curl_handle, CURLOPT_URL, *url);

	/* no progress meter please */
	curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);

	/* send all data to this function  */
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);

	/* we want the body be written to this file handle instead of stdout */
	/* coverity[bad_sizeof] */
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);

	/* get it! */
	res = curl_easy_perform(curl_handle);

	/* check for errors */
	if(res != CURLE_OK) {
		msg = createException(MAL, "mcurl.deleterequest",
							  "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
	} else {
		/*
		 * Now, our chunk.memory points to a memory block that is
		 * chunk.size bytes big and contains the remote file.
		 *
		 * Do something nice with it!
		 *
		 * You should be aware of the fact that at this point we might
		 * have an allocated data block, and nothing has yet
		 * deallocated that data. So when you're done with it, you
		 * should free() it as a nice application.
		 */

		//printf("%zu bytes retrieved\n", chunk.size);
	}
	if (chunk.size) {
		d = GDKstrdup(chunk.memory);
		if(chunk.memory)
			free(chunk.memory);
	}
	/* cleanup curl stuff */
	curl_easy_cleanup(curl_handle);

	*retval = d;
	return msg;
}

#endif

mal_export str CURLgetRequest(str *retval, str *url);
mal_export str CURLputRequest(str *retval, str *url);
mal_export str CURLpostRequest(str *retval, str *url);
mal_export str CURLdeleteRequest(str *retval, str *url);

str
CURLgetRequest(str *retval, str *url)
{
#ifdef HAVE_CURL
	return handle_get_request(retval, url);
#else
	(void)retval;
	(void)url;
	return MAL_SUCCEED;
#endif
}

str
CURLputRequest(str *retval, str *url)
{
#ifdef HAVE_CURL
	return handle_put_request(retval, url);
#else
	(void)retval;
	(void)url;
	return MAL_SUCCEED;
#endif
}

str
CURLpostRequest(str *retval, str *url)
{
#ifdef HAVE_CURL
	return handle_post_request(retval, url);
#else
	(void)retval;
	(void)url;
	return MAL_SUCCEED;
#endif
}

str
CURLdeleteRequest(str *retval, str *url)
{
#ifdef HAVE_CURL
	return handle_delete_request(retval, url);
#else
	(void)retval;
	(void)url;
	return MAL_SUCCEED;
#endif
}
