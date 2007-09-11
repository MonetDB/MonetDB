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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef SHRED_HELPER_H__
#define SHRED_HELPER_H__

#include "pf_config.h"
#include <stdlib.h>
#include <stdio.h>

#define STACK_MAX 100

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

/* boolean type `bool' and constants `true', `false' */
#ifdef HAVE_STDBOOL_H
#include <stdbool.h>
#elif !defined(HAVE_BOOL)
#define bool    char
#define true    (char)1
#define false   (char)0
#endif

typedef long int nat;

/**
 * Alternative definition of strdup. It just duplicates a string
 * given by @a s.
 *
 * @param s  String to duplicate.
 */
char * strdup (const char * s);

/**
 * Alternative definition of strdup. It just duplicates a string
 * given by @a s.
 * If the length exceeds n duplicate only the first @a n characters.
 *
 * @param s  String to duplicate.
 * @param n  Copy only the first n characters.
 */
char * strndup (const char * s, size_t n);

/* global state of the shredder */
struct shred_state_t {
    bool quiet;               /** < shredder gives no addtional information. */
	bool suppress_attributes; /** < suppress the attributes                  */
	bool outfile_given;       /** < we have a file to place the generated 
                                    stuff                                    */
    bool infile_given;        /** < we have a file we can parse              */
	bool sql;                 /** < format supported by sql generation       */
	char *outfile;            /** < path of the out-file                     */
	char *infile;             /** < path of file to parse                    */
	char *format;             /** < format string                            */
};
typedef struct shred_state_t shred_state_t;

/**
 * Test if we have the right to
 * read the given @a filename.
 */
bool SHreadable (const char *path);

/**
 * Test if the given @a filename
 * exists.
 */
bool SHexists (const char *path);

/**
 * Open file @a path for writing.
 */
FILE * SHopen_write (const char *path);

#endif /* SHRED_H__ */
/* vim:set shiftwidth=4 expandtab: */

