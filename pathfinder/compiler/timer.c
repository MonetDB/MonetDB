/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder timer services (resolution microseconds): start/stop
 * timer, convert timer value into formatted string representation.
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* PFinfo() */
#include "pathfinder.h"

/* time functions() */
#ifdef HAVE_FTIME
#include <sys/timeb.h>
#endif

#if TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# if HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif


/* sprintf() */
#include <stdio.h>
/* make abs() available */
#include <stdlib.h>

#ifdef NATIVE_WIN32
#include <windows.h>
#endif

#include "mem.h"

static long
PFgettime (void)
{
#ifdef NATIVE_WIN32
    SYSTEMTIME st;

    GetSystemTime(&st);
    return (((st.wDay * 24 * 60 + st.wMinute) * 60 + st.wSecond) * 1000 +
            (long) st.wMilliseconds) * 1000;
#else
#ifdef HAVE_GETTIMEOFDAY
        struct timeval tp;

        gettimeofday(&tp, NULL);
        return (long) tp.tv_sec * 1000000 + (long) tp.tv_usec;
#else
#ifdef HAVE_FTIME
        struct timeb tb;

        ftime(&tb);
        return (long) tb.time * 1000000 + (long) tb.millitm * 1000;
#endif
#endif
#endif

}

/**
 * Start a new timer.  Later, when you stop and want to read off the
 * timer's elapsed time, you need to pass the return value of this
 * function to #PFtimer_stop ().  This allows for several concurrent
 * timers running ``in parallel''.
 *
 * @return timer value to be passed to PFtimer_stop ()
 */
long
PFtimer_start (void)
{
    return PFgettime();
}

/**
 * Stop an already running timer and read off its current elapsed time
 * (the returned value indicates elpased microseconds).  You can
 * ``stop'' the same timer more than once.
 *
 * @param start the timer's start time returned by #PFtimer_start ()
 * @return the timer's elapsed time (microseconds)
 */
long
PFtimer_stop (long start)
{
    long stop;

    stop = PFgettime();

    return abs (stop - start);
}

/**
 * Convert a timer's elapsed microseconds value into a formatted string
 * representation suitable to write to log files etc.
 *
 * @param elapsed the timer's elapsed microseconds value (as returned
 *        by #PFtimer_stop ())
 * @return formatted string representation of timer's value
 */
char *
PFtimer_str (long elapsed)
{
    char *tm = PFmalloc (sizeof (char) * (sizeof ("000h 00m 00s 000ms") + 1));
    char *str;

    tm[0] = '\0';
    str = tm;

    if (elapsed / 3600000000UL) {
        str += sprintf (str, "%03luh ", elapsed / 3600000000UL);
        elapsed %= 3600000000UL;
    }

    if (elapsed / 60000000UL) {
        str += sprintf (str, "%02lum ", elapsed / 60000000UL);
        elapsed %= 60000000UL;
    }

    if (elapsed / 1000000UL) {
        str += sprintf (str, "%02lus ", elapsed / 1000000UL);
        elapsed %= 1000000UL;
    }

    str += sprintf (str, "%03lums", elapsed / 1000UL);

    return tm;
}

/* vim:set shiftwidth=4 expandtab: */
