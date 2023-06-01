/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */


void
FUNCNAME(FILE *f, bool byteswap, long nrecs, char *arg)
{
	if (!arg)
		croak(2, "this generator needs a scale argument");

	char *end = NULL;
	int scale = (int)strtol(arg, &end, 10);
	if (*arg == '\0' || *end != '\0')
		croak(2, "invalid scale argument");
	STYP hi = 1;
	while (scale-- > 0)
		hi *= 10;
	hi -= 1;

	STYP n = 0;
	for (long i = 0; i < nrecs; i++) {
		STYP svalue = n / 2;
		if (i % 2 != 0)
			svalue = -svalue;
		UTYP uvalue = (UTYP) svalue;
#ifdef CONVERT
		if (byteswap)
			CONVERT(&uvalue);
#else
		(void)byteswap;
#endif
		fwrite(&uvalue, sizeof(uvalue), 1, f);

		if (n == 2 * hi + 1)
			n = 0;
		else
			n++;
	}
}

#undef FUNCNAME
#undef STYP
#undef UTYP
#undef CONVERT
