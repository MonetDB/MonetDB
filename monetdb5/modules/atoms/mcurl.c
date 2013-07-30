/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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

#include <stdio.h>
#include <stdlib.h>
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

	mem->memory = realloc(mem->memory, mem->size + realsize + 1);
	if(mem->memory == NULL) {
		/* out of memory! */
		printf("not enough memory (realloc returned NULL)\n");
		return 0;
	}

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
	CURLcode res = 0;

	struct MemoryStruct chunk;

	chunk.memory = malloc(1);  /* will be grown as needed by the realloc above */
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

		//printf(SZFMT " bytes retrieved\n", chunk.size);
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

#ifdef WIN32
#define mcurl_export extern __declspec(dllexport)
#else
#define mcurl_export extern
#endif

mcurl_export str CURLgetRequest(str *retval, str *url);

str
CURLgetRequest(str *retval, str *url)
{
	(void)retval;
	(void)url;
#ifdef HAVE_CURL
	return handle_get_request(retval, url);
#endif
	return MAL_SUCCEED;
}
