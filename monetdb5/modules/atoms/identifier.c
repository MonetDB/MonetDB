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
#include "identifier.h"	/* for the implementation of the functions */

int TYPE_identifier;

str IDprelude(void)
{
	TYPE_identifier = ATOMindex("identifier");
	return MAL_SUCCEED;
}

/**
 * Creates a new identifier from the given string (stupid string copy).
 * Warning: GDK function, does NOT pass a string by reference, and wants
 * a pointer to a pointer for the retval!
 * Returns the number of chars read
 */
int
IDfromString(str src, int *len, str *retval)
{
	if (src == NULL) {
		*len = 0;
		*retval = GDKstrdup(str_nil);
	} else {
		*retval = GDKstrdup(src);
		*len = (int)strlen(src);
	}

	return(*len);
}

/**
 * Returns the string representation of the given identifier.
 * Warning: GDK function
 * Returns the length of the string
 */
int
IDtoString(str *retval, int *len, str handle)
{
	int hl = (int)strlen(handle) + 1;
	if (*len < hl) {
		if (*retval != NULL)
			GDKfree(*retval);
		*retval = GDKmalloc(sizeof(char) * hl);
	}
	*len = hl;
	memcpy(*retval, handle, hl);

	return(*len);
}
/**
 * Returns an identifier, parsed from a string.  The fromStr function is used
 * to parse the string.
 */
str
IDentifier(str *retval, str *in)
{
	int len = 0;

	(void)IDfromString(*in, &len, retval);
	if (len == 0)
		throw(PARSE, "identifier.identifier", "Error while parsing %s", *in);

	return (MAL_SUCCEED);
}
