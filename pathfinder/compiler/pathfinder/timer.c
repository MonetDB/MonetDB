/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder timer services (resolution microseconds): start/stop
 * timer, convert timer value into formatted string representation.
 *
 * $Id$
 */

/* PFinfo() */
#include "pathfinder.h"

/* gettimeofday() */
#include <sys/time.h>
/* sprintf() */
#include <stdio.h>
/* make abs() available */
#include <stdlib.h>

/**
 * Start a new timer.  Later, when you stop and want to read off the
 * timer's elapsed time, you need to pass the return value of this
 * function to #PFtimer_stop ().  This allows for several concurrent
 * timers running ``in parallel''.
 *
 * @return timer value to be passed to PFtimer_stop ()
 */
long 
PFtimer_start () { 
    struct timeval now; 
    long start;

    (void) gettimeofday (&now, 0);
    start = now.tv_sec * 1000000 + now.tv_usec;

    return start;
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
    struct timeval now;
    long stop;

    (void) gettimeofday (&now, 0);
    stop = now.tv_sec * 1000000 + now.tv_usec;

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
    static char tm[sizeof ("000h 00m 00s 000ms")] = "";
    char *str;

    str = tm;

    if (elapsed / 3600000000UL) {
        str += sprintf (str, "%03ldh ", elapsed / 3600000000UL);
        elapsed %= 3600000000UL;
    }
  
    if (elapsed / 60000000UL) {
        str += sprintf (str, "%02ldm ", elapsed / 60000000UL);
        elapsed %= 60000000UL;
    }
  
    if (elapsed / 1000000UL) {
        str += sprintf (str, "%02lds ", elapsed / 1000000UL);
        elapsed %= 1000000UL;
    }

    str += sprintf (str, "%03ldms", elapsed / 1000UL);

    return tm;
}

/* vim:set shiftwidth=4 expandtab: */
