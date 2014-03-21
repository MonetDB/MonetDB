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
 * (author) R Cijvat
 * This file contains some global definitions, used by multiple bam library files
 */

#ifndef _BAM_LOG_H
#define _BAM_LOG_H

#ifndef NDEBUG
#define BAM_DEBUG /* We are in 'debug-mode' if --enable-assert was set during configuration, since in that case NDEBUG will not be set. */
#endif

/* Macro that enables writing to a log. If the debug flag is not set, it does not do anything */
#ifdef BAM_DEBUG
#define TO_LOG(...) fprintf(stderr, __VA_ARGS__)
#else
#define TO_LOG(...) (void)0
#endif

/* Macro to create an exception that uses a previous allocated exception as format parameter. Makes sure that the old one is freed and that
 * msg points to the new exception afterwards.
 * I had a look at rethrow but this does not achieve the right thing.
 */
#define REUSE_EXCEPTION(msg, type, fnc, ...) { \
    str msg_tmp = createException(type, fnc, __VA_ARGS__); \
    GDKfree(msg); \
    msg = msg_tmp; \
}

#endif
