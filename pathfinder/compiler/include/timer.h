/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder timer services (resolution microseconds): start/stop
 * timer, convert timer value into formatted string representation.
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2005 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
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
