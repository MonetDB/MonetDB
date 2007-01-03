/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Compiler Driver interface defs for external usage
 *
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 */

#ifndef COMPILEINT_H
#define COMPILEINT_H

#define COMPILE_OPTION_STANDOFF 1 /* use bit 0 of the 'options'
                                     param to enable StandOff */

/* main compiler call from the Monet runtime environment */
char* PFcompile_MonetDB (char* xquery, char** prologue, char** query, char** epilogue, int options);

/* get a document by URL (if not in cache, fetch it) */
char* PFurlcache(char *url, int keep);

/* flush the url cache */
void PFurlcache_flush(void);

const char* PFinitMIL(void);   /* MIL pattern for module init */
const char* PFvarMIL(void);    /* MIL pattern for global variable definitions */
const char* PFstartMIL(int);   /* MIL pattern for starting query execution */
const char* PFstopMIL(int);    /* MIL pattern for stopping query execution (and print) */
const char* PFdocbatMIL(void); /* MIL pattern for adding a shredded document to the ws */
const char* PFudfMIL(void);    /* MIL pattern for calling a UDF */

#endif

/* vim:set shiftwidth=4 expandtab: */
