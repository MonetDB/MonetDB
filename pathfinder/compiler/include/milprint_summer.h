/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Convert the internal representation of a MIL programm into a
 * string. Declarations for mil/milprint.c
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef MILPRINT_SUMMER_H
#define MILPRINT_SUMMER_H

#include "core.h"

static char* PFloadMIL();  /* MIL pattern for loading modules (and global variable init) */
static char* PFstartMIL(); /* MIL pattern for starting query execution */
static char* PFudfMIL();   /* MIL pattern for calling a UDF */
static char* PFstopMIL();  /* MIL pattern for stopping query execution (and print) */
static char* PFclearMIL(); /* MIL pattern for clearing re-usable variables */
static char* PFdropMIL();  /* MIL pattern for dropping modules */

int PFprintMILtemp (PFcnode_t *, int mode, char* genType, long tm, char** prologue, char** query, char** epilogue);

#endif    /* MILPRINT_H */

/* vim:set shiftwidth=4 expandtab: */
