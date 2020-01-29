/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * @f alarm
 * @a M.L. Kersten, P. Boncz
 *
 * @+ Timers and Timed Interrupts
 * This module handles various signaling/timer functionalities.
 * The Monet interface supports two timer commands: @emph{ alarm} and @emph{ sleep}.
 * Their argument is the number of seconds to wait before the timer goes off.
 * The @emph{ sleep} command blocks till the alarm goes off.
 * The @emph{ alarm} command continues directly, executes off a 
 * string when it goes off.
 * The parameterless routines @emph{ time} and @emph{ ctime} provide access to
 * the cpu clock.They return an integer and string, respectively.
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include <time.h>

mal_export str ALARMusec(lng *ret);
mal_export str ALARMsleep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str ALARMctime(str *res);
mal_export str ALARMepoch(int *res);
mal_export str ALARMtime(int *res);

str
ALARMusec(lng *ret)
{
	*ret = GDKusec();
	return MAL_SUCCEED;
}

str
ALARMsleep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r, *b;
	int *restrict rb, *restrict bb;
	BUN i, j;

	(void) cntxt;
	if (getArgType(mb, pci, 0) != TYPE_void && isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		bat *bid = getArgReference_bat(stk, pci, 1);
		
		if (!(b = BATdescriptor(*bid)))
			throw(MAL, "alarm.sleepr", SQLSTATE(HY005) "Cannot access column descriptor");

		j = BATcount(b);
		bb = Tloc(b, 0);
		for (i = 0; i < j ; i++) {
			if (is_int_nil(bb[i])) {
				BBPunfix(b->batCacheid);
				throw(MAL, "alarm.sleepr", "NULL values not allowed for the sleeping time");
			} else if (bb[i]) {
				BBPunfix(b->batCacheid);
				throw(MAL, "alarm.sleepr", "Cannot sleep for a negative time");
			}
		}

		r = COLnew(0, TYPE_int, j, TRANSIENT);
		if (r == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "alarm.sleepr", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		rb = Tloc(r, 0);
		for (i = 0; i < j ; i++) {
			MT_sleep_ms(bb[i]);
			rb[i] = bb[i];
		}

		BBPunfix(b->batCacheid);
		BBPkeepref(*res = r->batCacheid);
	} else {
		int *res = (int*) getArgReference(stk, pci, 0), *msecs = (int*) getArgReference(stk,pci,1);

		if (is_int_nil(*msecs))
			throw(MAL, "alarm.sleepr", "NULL values not allowed for the sleeping time");
		else if (*msecs < 0)
			throw(MAL, "alarm.sleepr", "Cannot sleep for a negative time");

		MT_sleep_ms(*msecs);
		*res = *msecs;
	}
	return MAL_SUCCEED;
}

str
ALARMctime(str *res)
{
	time_t t = time(0);
	char *base;

#ifdef HAVE_CTIME_R3
	char buf[26];

	base = ctime_r(&t, buf, sizeof(buf));
#else
#ifdef HAVE_CTIME_R
	char buf[26];

	base = ctime_r(&t, buf);
#else
	base = ctime(&t);
#endif
#endif
	if (base == NULL)
		/* very unlikely to happen... */
		throw(MAL, "alarm.ctime", "failed to format time");

	base[24] = 0;				/* squash final newline */
	*res = GDKstrdup(base);
	if (*res == NULL)
		throw(MAL, "alarm.ctime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
ALARMepoch(int *res)  /* XXX should be lng */
{
	*res = (int) time(0);
	return MAL_SUCCEED;
}

str
ALARMtime(int *res)
{
	*res = GDKms();
	return MAL_SUCCEED;
}

