/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @a Lefteris Sidirourgos
 * @d 30/08/2011
 * @+ The sampling facilities
 */

#ifndef _SAMPLE_H_
#define _SAMPLE_H_

/* #define _DEBUG_SAMPLE_ */

mal_export str
SAMPLEuniform(bat *r, bat *b, lng *s);

mal_export str
SAMPLEuniform_dbl(bat *r, bat *b, dbl *p);

#endif
