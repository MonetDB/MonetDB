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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
 * The @emph{ alarm} command continues directly, executes off a MIL
 * string when it goes off.
 * The parameterless routines @emph{ time} and @emph{ ctime} provide access to
 * the cpu clock.They return an integer and string, respectively.
 */
#include "monetdb_config.h"
#include "alarm.h"
#include <time.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define alarm_export extern __declspec(dllimport)
#else
#define alarm_export extern __declspec(dllexport)
#endif
#else
#define alarm_export extern
#endif

alarm_export str ALARMprelude(void);
alarm_export str ALARMepilogue(void);
alarm_export str ALARMusec(lng *ret);
alarm_export str ALARMsleep(int *res, int *secs);
alarm_export str ALARMsetalarm(int *res, int *secs, str *action);
alarm_export str ALARMtimers(int *res);
alarm_export str ALARMctime(str *res);
alarm_export str ALARMepoch(int *res);
alarm_export str ALARMtime(int *res);

static monet_timer_t timer[MAXtimer];
static int timerTop = 0;

/*
 * Once a timer interrupt occurs, we should inspect the timer queue and
 * emit a notify signal.
 */
#ifdef SIGALRM
/* HACK to pacify compiler */
#if (defined(__INTEL_COMPILER) && (SIZEOF_VOID_P > SIZEOF_INT))
#undef  SIG_ERR			/*((__sighandler_t)-1 ) */
#define SIG_ERR   ((__sighandler_t)-1L)
#endif
static void
CLKsignal(int nr)
{
	/* int restype; */
	int k = timerTop;
	int t;

	(void) nr;

	if (signal(SIGALRM, CLKsignal) == SIG_ERR) {
		GDKsyserror("CLKsignal: call failed\n");
	}

	if (timerTop == 0) {
		return;
	}
	t = time(0);
	while (k-- && t >= timer[k].alarm_time) {
		if (timer[k].action) {
			/* monet_eval(timer[k].action, &restype); */
			GDKfree(timer[k].action);
		} else {
			MT_sema_up(&timer[k].sema, "CLKsignal");
		}
		timerTop--;
	}
	if (timerTop > 0) {
		alarm(timer[timerTop - 1].alarm_time - time(0));
	}
}
#endif

#include "mal.h"
#include "mal_exception.h"


str
ALARMprelude(void)
{
#ifdef SIGALRM
	(void) signal(SIGALRM, (void (*)()) CLKsignal);
#endif
	return MAL_SUCCEED;
}

str
ALARMepilogue(void)
{
	int k;

#if (defined(SIGALRM) && defined(SIG_IGN))
/* HACK to pacify compiler */
#if (defined(__INTEL_COMPILER) && (SIZEOF_VOID_P > SIZEOF_INT))
#undef  SIG_IGN			/*((__sighandler_t)1 ) */
#define SIG_IGN   ((__sighandler_t)1L)
#endif
	(void) signal(SIGALRM, SIG_IGN);
#endif
	for (k = 0; k < timerTop; k++) {
		if (timer[k].action)
			GDKfree(timer[k].action);
	}
	return MAL_SUCCEED;
}

str
ALARMusec(lng *ret)
{
	*ret = GDKusec();
	return MAL_SUCCEED;
}

str
ALARMsleep(int *res, int *secs)
{
	(void) res;		/* fool compilers */
	if (*secs < 0)
		throw(MAL, "alarm.sleep", "negative delay");

#ifdef __CYGWIN__
	/* CYGWIN cannot handle SIGALRM with sleep */
	{
		lng t = GDKusec() + (*secs)*1000000;

		while (GDKusec() < t)
			;
	}
#else
	MT_sleep_ms(*secs * 1000);
#endif
	return MAL_SUCCEED;
}

str
ALARMsetalarm(int *res, int *secs, str *action)
{
	(void) res;
	(void) secs;
	(void) action;		/* foolc compiler */
	throw(MAL, "alarm.setalarm", PROGRAM_NYI);
}

str
ALARMtimers(int *res)
{
	(void) res;		/* fool compiler */
	throw(MAL, "alarm.timers", PROGRAM_NYI);
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

