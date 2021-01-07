/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


stream *
compressed_stream(stream *inner, int level)
{
	if (inner == NULL)
		return NULL;

	char *filename = mnstr_name(inner);
	if (filename == NULL)
		return inner;

	const char *ext = strrchr(filename, '.');
	if (ext == NULL)
		return inner;
	if (strcmp(ext, ".gz") == 0)
		return gz_stream(inner, level);
	if (strcmp(ext, ".bz2") == 0)
		return bz2_stream(inner, level);
	if (strcmp(ext, ".xz") == 0)
		return xz_stream(inner, level);
	if (strcmp(ext, ".lz4") == 0)
		return lz4_stream(inner, level);

	return inner;
}


