/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder timer services (resolution microseconds): start/stop
 * timer, convert timer value into formatted string representation.
 *
 * $Id$
 */

#ifndef TIMER_H
#define TIMER_H

/**
 * Start a new timer.  Later, when you stop and want to read off the
 * timer's elapsed time, you need to pass the return value of this
 * function to #PFtimer_stop ().  This allows for several concurrent
 * timers running ``in parallel''.
 *
 * @return timer value to be passed to PFtimer_stop ()
 */
long
PFtimer_start ();

/**
 * Stop an already running timer and read off its current elapsed time
 * (the returned value indicates elapsed microseconds).  You can
 * ``stop'' the same timer more than once.
 *
 * @param start the timer's start time returned by #PFtimer_start ()
 * @return the timer's elapsed time (microseconds)
 */
long
PFtimer_stop (long);

/**
 * Convert a timer's elapsed microseconds value into a formatted string
 * representation suitable to write to log files etc.
 *
 * @param elapsed the timer's elapsed microseconds value (as returned
 *        by #PFtimer_stop ())
 * @return formatted string representation of timer's value
 */
char *
PFtimer_str (long);

#endif

/* vim:set shiftwidth=4 expandtab: */
