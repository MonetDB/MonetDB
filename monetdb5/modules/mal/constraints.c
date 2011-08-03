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
 * @f constraints
 * @a Niels Nes
 * @* constraints
 * The constraints defined here are runtime support for SQL.
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "constraints.h"


str
CemptySet(int *k, int *bid)
{
	BAT *b = BATdescriptor(*bid);
	BUN cnt = 0;

	(void)k;
	if (b) {
		cnt = BATcount(b);
		BBPunfix(b->batCacheid);
	}
	if (cnt)
		throw(OPTIMIZER, "mal.assert", "emptySet");
	return MAL_SUCCEED;
}
