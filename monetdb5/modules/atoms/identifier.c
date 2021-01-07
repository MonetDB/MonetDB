/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

static int TYPE_identifier;

static str IDprelude(void *ret)
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
static ssize_t
IDfromString(const char *src, size_t *len, void **RETVAL, bool external)
{
	identifier *retval = (identifier *) RETVAL;
	size_t l = strlen(src) + 1;
	if (*retval == NULL || *len < l) {
		GDKfree(*retval);
		*retval = GDKmalloc(l);
		if (*retval == NULL)
			return -1;
		*len = l;
	}
	if (external && strncmp(src, "nil", 3) == 0) {
		memcpy(*retval, str_nil, 2);
		return 3;
	}
	memcpy(*retval, src, l);
	return (ssize_t) l - 1;
}

/**
 * Returns the string representation of the given identifier.
 * Warning: GDK function
 * Returns the length of the string
 */
static ssize_t
IDtoString(char **retval, size_t *len, const void *HANDLE, bool external)
{
	const char *handle = HANDLE;
	size_t hl = strlen(handle) + 1;
	if (external && strNil(handle))
		hl = 4;
	if (*len < hl || *retval == NULL) {
		GDKfree(*retval);
		*retval = GDKmalloc(hl);
		if (*retval == NULL)
			return -1;
		*len = hl;
	}
	if (external && strNil(handle))
		strcpy(*retval, "nil");
	else
		memcpy(*retval, handle, hl);
	return (ssize_t) hl - 1;
}
/**
 * Returns an identifier, parsed from a string.  The fromStr function is used
 * to parse the string.
 */
static str
IDentifier(identifier *retval, str *in)
{
	size_t len = 0;

	if (IDfromString(*in, &len, (void **) retval, false) < 0)
		throw(PARSE, "identifier.identifier", "Error while parsing %s", *in);

	return (MAL_SUCCEED);
}

#include "mel.h"
mel_atom identifier_init_atoms[] = {
 { .name="identifier", .basetype="str", .fromstr=IDfromString, .tostr=IDtoString, },  { .cmp=NULL }
};
mel_func identifier_init_funcs[] = {
 command("identifier", "identifier", IDentifier, false, "Cast a string to an identifer ", args(1,2, arg("",identifier),arg("s",str))),
 command("identifier", "prelude", IDprelude, false, "Initialize the module", args(1,1, arg("",void))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_identifier_mal)
{ mal_module("identifier", identifier_init_atoms, identifier_init_funcs); }
