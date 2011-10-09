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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/* NOTE: for this file to work correctly, the random number generator
 * must have been seeded (srand) with something like the current time */

#include "monetdb_config.h"
#include "muuid.h"
#include <stdlib.h> /* rand */
#include <string.h> /* strdup */
#ifdef HAVE_UUID_UUID_H
# include <uuid/uuid.h>
#endif

/**
 * Shallow wrapper around uuid, that comes up with some random pseudo
 * uuid if uuid is not available
 */
char *
generateUUID(void)
{
#ifdef HAVE_UUID_UUID_H
# ifdef UUID_PRINTABLE_STRING_LENGTH
	/* Solaris */
	char out[UUID_PRINTABLE_STRING_LENGTH];
# else
	char out[37];
# endif
	uuid_t uuid;
	uuid_generate(uuid);
	uuid_unparse(uuid, out);
#else
	/* try to do some pseudo interesting stuff, and stash it in the
	 * format of an UUID to at least return some uniform answer */
	char out[37];
	char *p = out;

	/* generate something like this:
	 * cefa7a9c-1dd2-11b2-8350-880020adbeef ("%08x-%04x-%04x-%04x-%012x") */
	p += snprintf(p, 5, "%04x", rand() % 65536);
	p += snprintf(p, 6, "%04x-", rand() % 65536);
	p += snprintf(p, 6, "%04x-", rand() % 65536);
	p += snprintf(p, 6, "%04x-", rand() % 65536);
	p += snprintf(p, 6, "%04x-", rand() % 65536);
	p += snprintf(p, 5, "%04x", rand() % 65536);
	p += snprintf(p, 5, "%04x", rand() % 65536);
	p += snprintf(p, 5, "%04x", rand() % 65536);
#endif
	return(strdup(out));
}

/* vim:set ts=4 sw=4 noexpandtab: */
