/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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
#include <signal.h>
#include <time.h>

mal_export str ALARMusec(lng *ret);
mal_export str ALARMsleep(void *res, int *secs);
mal_export str ALARMctime(str *res);
mal_export str ALARMepoch(int *res);
mal_export str ALARMtime(int *res);

#include "mal.h"
#include "mal_exception.h"


str
ALARMusec(lng *ret)
{
	*ret = GDKusec();
	return MAL_SUCCEED;
}

str
ALARMsleep(void *res, int *secs)
{
	(void) res;		/* fool compilers */
	if (*secs < 0)
		throw(MAL, "alarm.sleep", "negative delay");

	MT_sleep_ms(*secs * 1000);
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
		throw(MAL, "alarm.ctime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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

