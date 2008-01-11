/**
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

#ifndef FMT_H
#define FMT_H

/* define printf formats for printing size_t and ssize_t variables */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901 && !defined(__svr4__) && !defined(WIN32) && !defined(__sgi) && (!defined(__APPLE_CC__) || __GNUC__ > 3)
#define SZFMT "%zu"
#define SSZFMT "%zd"
#elif defined(__MINGW32__)
#define SZFMT "%u"
#define SSZFMT "%ld"
#elif defined(__APPLE_CC__) /* && __GNUC__ <= 3 */
#define SZFMT "%zu"
        #if SIZEOF_SIZE_T == SIZEOF_INT
        #define SSZFMT "%d"
        #else
        #define SSZFMT "%ld"
        #endif
#elif SIZEOF_SIZE_T == SIZEOF_INT
#define SZFMT "%u"
#define SSZFMT "%d"
#elif SIZEOF_SIZE_T == SIZEOF_LONG
#define SZFMT "%lu"
#define SSZFMT "%ld"
#elif SIZEOF_SIZE_T == SIZEOF_LONG_LONG || SIZEOF_SIZE_T == SIZEOF___INT64
#define SZFMT ULLFMT
#define SSZFMT LLFMT
#else
#error no definition for SZFMT/SSZFMT
#endif

#endif /* FMT_H */

/* vim:set shiftwidth=4 expandtab: */
