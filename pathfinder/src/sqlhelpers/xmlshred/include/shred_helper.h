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

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#define STACK_MAX 100

#include "fmt.h"

/* boolean type `bool' and constants `true', `false' */
#ifdef HAVE_STDBOOL_H
#include <stdbool.h>
#elif !defined(HAVE_BOOL)
#define bool    char
#define true    (char)1
#define false   (char)0
#endif

typedef ssize_t nat;

#ifdef HAVE_STRING_H
#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* enable strndup prototype (on Linux, only?) */
#endif
#include <string.h>
#endif

#ifndef HAVE_STRDUP
/**
 * Alternative definition of strdup. It just duplicates a string
 * given by @a s.
 *
 * @param s  String to duplicate.
 */
char * strdup (const char * s);
#else
#if !HAVE_DECL_STRDUP
extern char *strdup(const char *);
#endif
#endif

#ifndef HAVE_STRNDUP
/**
 * Alternative definition of strdup. It just duplicates a string
 * given by @a s.
 * If the length exceeds n duplicate only the first @a n characters.
 *
 * @param s  String to duplicate.
 * @param n  Copy only the first n characters.
 */
char * strndup (const char * s, size_t n);
#else
#if !HAVE_DECL_STRNDUP
extern char *strndup(const char *, size_t);
#endif
#endif

/* global state of the shredder */
struct shred_state_t {
    char *infile;              /** < path of file to parse                    */
    char *outfile;             /** < path of the out-file                     */
    char *doc_name;            /** < name of the document                     */
    bool  quiet;               /** < shredder gives no addtional information. */
    bool  attributes_separate; /** < print attributes into a separate file    */
    bool  names_separate;      /** < print names into a separate file         */
    bool  statistics;          /** < print guides into a separate file        */
    bool  fastformat;          /** < flag to use static formatting            */
    char *format;              /** < format string                            */
    unsigned int strip_values; /** < only store strip_values characters in the
                                     value column                             */
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
