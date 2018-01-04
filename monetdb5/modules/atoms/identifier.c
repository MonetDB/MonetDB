/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * @f identifier
 * @a Fabian Groffen, Martin Kersten
 * @+ Identifier Wrapper
 * The identifier atom is a shallow wrapper that contains an object's id.
 * Due to it being wrapped by this atom, methods can distinguish
 * it from a normal string.
 * The variable of this time can be further extended with properties
 * to further qualify the identifier referenced.
 *
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"

typedef str identifier;

mal_export int TYPE_identifier;
mal_export str IDprelude(void *ret);
mal_export ssize_t IDfromString(const char *src, size_t *len, identifier *retval);
mal_export ssize_t IDtoString(str *retval, size_t *len, const char *handle);
mal_export str IDentifier(identifier *retval, str *in);

int TYPE_identifier;

str IDprelude(void *ret)
{
	(void) ret;
	TYPE_identifier = ATOMindex("identifier");
	return MAL_SUCCEED;
}

/**
 * Creates a new identifier from the given string (stupid string copy).
 * Warning: GDK function, does NOT pass a string by reference, and wants
 * a pointer to a pointer for the retval!
 * Returns the number of chars read
 */
ssize_t
IDfromString(const char *src, size_t *len, identifier *retval)
{
	size_t l = strlen(src) + 1;
	if (*retval == NULL || *len < l) {
		GDKfree(*retval);
		*retval = GDKmalloc(l);
		if (*retval == NULL)
			return -1;
		*len = l;
	}
	memcpy(*retval, src, l);
	return (ssize_t) l - 1;
}

/**
 * Returns the string representation of the given identifier.
 * Warning: GDK function
 * Returns the length of the string
 */
ssize_t
IDtoString(str *retval, size_t *len, const char *handle)
{
	size_t hl = strlen(handle) + 1;
	if (*len < hl || *retval == NULL) {
		GDKfree(*retval);
		*retval = GDKmalloc(hl);
		if (*retval == NULL)
			return -1;
		*len = hl;
	}
	memcpy(*retval, handle, hl);
	return (ssize_t) hl - 1;
}
/**
 * Returns an identifier, parsed from a string.  The fromStr function is used
 * to parse the string.
 */
str
IDentifier(identifier *retval, str *in)
{
	size_t len = 0;

	if (IDfromString(*in, &len, retval) < 0)
		throw(PARSE, "identifier.identifier", "Error while parsing %s", *in);

	return (MAL_SUCCEED);
}
