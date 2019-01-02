/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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

mal_export str CURLgetRequest(str *retval, str *url);
mal_export str CURLputRequest(str *retval, str *url);
mal_export str CURLpostRequest(str *retval, str *url);
mal_export str CURLdeleteRequest(str *retval, str *url);

str
CURLgetRequest(str *retval, str *url)
{
	(void)retval;
	(void)url;
	return MAL_SUCCEED;
}

str
CURLputRequest(str *retval, str *url)
{
	(void)retval;
	(void)url;
	return MAL_SUCCEED;
}

str
CURLpostRequest(str *retval, str *url)
{
	(void)retval;
	(void)url;
	return MAL_SUCCEED;
}

str
CURLdeleteRequest(str *retval, str *url)
{
	(void)retval;
	(void)url;
	return MAL_SUCCEED;
}
