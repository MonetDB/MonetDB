/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

mal_export str ALARMprelude(void *ret);
mal_export str ALARMepilogue(void *ret);
mal_export str ALARMusec(lng *ret);
mal_export str ALARMsleep(void *res, int *secs);
mal_export str ALARMsetalarm(void *res, int *secs, str *action);
mal_export str ALARMtimers(bat *res, bat *actions);
mal_export str ALARMctime(str *res);
mal_export str ALARMepoch(int *res);
mal_export str ALARMtime(int *res);

#define MAXtimer                200

typedef struct {
	str action;		/* action (as a string) */
	MT_Sema sema;		/* barrier */
	time_t alarm_time;	/* time when the alarm goes off */
} monet_timer_t;

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

	if (signal(SIGALRM, CLKsignal) == SIG_ERR) 
		GDKsyserror("CLKsignal: call failed\n");

	if (timerTop == 0) {
		return;
	}
	t = time(0);
	while (k-- && t >= timer[k].alarm_time) {
		if (timer[k].action) {
			/* monet_eval(timer[k].action, &restype); */
			GDKfree(timer[k].action);
		} else {
			MT_sema_up(&timer[k].sema);
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
ALARMprelude(void *ret)
{
	(void) ret;
#ifdef SIGALRM
	if(signal(SIGALRM, (void (*)()) CLKsignal) == SIG_ERR)
		throw(MAL, "alarm.prelude", SQLSTATE(HY001) "Signal call failed");
#endif
	return MAL_SUCCEED;
}

str
ALARMepilogue(void *ret)
{
	int k;

	(void) ret;
#if (defined(SIGALRM) && defined(SIG_IGN))
/* HACK to pacify compiler */
#if (defined(__INTEL_COMPILER) && (SIZEOF_VOID_P > SIZEOF_INT))
#undef  SIG_IGN			/*((__sighandler_t)1 ) */
#define SIG_IGN   ((__sighandler_t)1L)
#endif
	if(signal(SIGALRM, SIG_IGN) == SIG_ERR)
		throw(MAL, "alarm.epilogue", SQLSTATE(HY001) "Signal call failed");
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
ALARMsleep(void *res, int *secs)
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
ALARMsetalarm(void *res, int *secs, str *action)
{
	(void) res;
	(void) secs;
	(void) action;		/* foolc compiler */
	throw(MAL, "alarm.setalarm", PROGRAM_NYI);
}

str
ALARMtimers(bat *res, bat *actions)
{
	(void) res;		/* fool compiler */
	(void) actions;		/* fool compiler */
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

