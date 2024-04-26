/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
#include "mal_exception.h"

static str
ALARMusec(lng *ret)
{
	*ret = GDKusec();
	return MAL_SUCCEED;
}

#define SLEEP_SINGLE(TPE)												\
	do {																\
		TPE *res = getArgReference_##TPE(stk, pci, 0);					\
		const TPE *msecs = getArgReference_##TPE(stk,pci,1);			\
		if (is_##TPE##_nil(*msecs))										\
			throw(MAL, "alarm.sleep", "NULL values not allowed for sleeping time"); \
		if (*msecs < 0)													\
			throw(MAL, "alarm.sleep", "Cannot sleep for a negative time"); \
		MT_sleep_ms((unsigned int) *msecs);								\
		*res = *msecs;													\
	} while (0)

static str
ALARMsleep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	switch (getArgType(mb, pci, 1)) {
	case TYPE_bte:
		SLEEP_SINGLE(bte);
		break;
	case TYPE_sht:
		SLEEP_SINGLE(sht);
		break;
	case TYPE_int:
		SLEEP_SINGLE(int);
		break;
	default:
		throw(MAL, "alarm.sleep",
			  SQLSTATE(42000) "Sleep function not available for type %s",
			  ATOMname(getArgType(mb, pci, 1)));
	}
	return MAL_SUCCEED;
}

static str
ALARMctime(str *res)
{
	time_t t = time(0);
	char *base;
	char buf[26];

#ifdef HAVE_CTIME_R3
	base = ctime_r(&t, buf, sizeof(buf));
#else
	base = ctime_r(&t, buf);
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

static str
ALARMepoch(int *res)
{								/* XXX should be lng */
	*res = (int) time(0);
	return MAL_SUCCEED;
}

static str
ALARMtime(int *res)
{
	*res = GDKms();
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func alarm_init_funcs[] = {
 pattern("alarm", "sleep", ALARMsleep, true, "Sleep a few milliseconds", args(1,2, arg("",void),argany("msecs",1))),
 pattern("alarm", "sleep", ALARMsleep, true, "Sleep a few milliseconds and return the slept value", args(1,2, argany("",1),argany("msecs",1))),
 command("alarm", "usec", ALARMusec, true, "Return time since Jan 1, 1970 in microseconds.", args(1,1, arg("",lng))),
 command("alarm", "time", ALARMtime, true, "Return time since program start in milliseconds.", args(1,1, arg("",int))),
 command("alarm", "epoch", ALARMepoch, true, "Return time since Jan 1, 1970 in seconds.", args(1,1, arg("",int))),
 command("alarm", "ctime", ALARMctime, true, "Return the current time as a C-time string.", args(1,1, arg("",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_alarm_mal)
{ mal_module("alarm", NULL, alarm_init_funcs); }
