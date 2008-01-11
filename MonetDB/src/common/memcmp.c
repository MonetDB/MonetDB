/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

/* memcmp -- compare two memory regions.
   This function is in the public domain.  */

/*
NAME
	memcmp -- compare two memory regions

SYNOPSIS
	int memcmp (const void *from, const void *to, size_t count)

DESCRIPTION
	Compare two memory regions and return less than,
	equal to, or greater than zero, according to lexicographical
	ordering of the compared regions.
*/

#ifdef __STDC__
#include <stddef.h>
#else
#define size_t unsigned long
#endif

int
memcmp(const char *str1, const char *str2, size_t count)
{
	register unsigned char *s1 = (unsigned char *) str1;
	register unsigned char *s2 = (unsigned char *) str2;

	while (count-- > 0) {
		if (*s1++ != *s2++)
			return s1[-1] < s2[-1] ? -1 : 1;
	}
	return 0;
}
