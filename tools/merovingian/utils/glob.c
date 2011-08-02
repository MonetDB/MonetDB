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

/**
 * glob
 * Fabian Groffen
 * Limited globbing within merovingian's tags.
 * The rules are kept simple for the time being:
 * - * expands to an arbitrary string
 */

#include "monetdb_config.h"
#include <stdlib.h>  /* NULL */
#include "glob.h"

/**
 * Returns if haystack matches expr, using tag globbing.
 */
char
glob(const char *expr, const char *haystack)
{
	const char *haymem = NULL;
	const char *exprmem = NULL;
	char escape = 0;

	/* probably need to implement this using libpcre once we get
	 * advanced users, doing even more advanced things */

	while (*expr != '\0') {
		if (*expr == '\\') {
			escape = !escape;
			if (escape) {
				expr++;
				continue; /* skip over escape */
			}
		}
		switch (*expr) {
			case '*':
				if (!escape) {
					/* store expression position for retry lateron */
					exprmem = expr;
					/* skip over haystack till the next char from expr */
					do {
						expr++;
						if (*expr == '\0') {
							/* this will always match the rest */
							return(1);
						} else if (!escape && *expr == '*') {
							continue;
						} else if (*expr == '\\') {
							escape = !escape;
							if (!escape)
								break;
						} else {
							break;
						}
					} while(1);
					while (*haystack != '\0' && *haystack != *expr)
						haystack++;
					/* store match position, for retry lateron */
					haymem = haystack + 1;
					if (*haystack == '\0')
						/* couldn't find it, so no match  */
						return(0);
					break;
				}
				/* do asterisk match if escaped */
			default:
				if (*expr != *haystack) {
					if (haymem != NULL) {
						return(glob(exprmem, haymem));
					} else {
						return(0);
					}
				}
			break;
		}
		expr++;
		haystack++;
		escape = 0;
	}
	return(*haystack == '\0');
}
