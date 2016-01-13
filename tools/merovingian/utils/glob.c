/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
