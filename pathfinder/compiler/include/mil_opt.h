/**
 * @file
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
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Peter Boncz <boncz@cwi.nl>
 *
 * $Id$
 */

#include <stdio.h>

/* ----------------------------------------------------------------------------
 * dead MIL code eliminator (one-day hack by Peter Boncz) 
 *
 * accepts chunks of MIL at a time
 * echoes optimized MIL to a file pointer
 *
 * limitations: 
 * - don't declare more than one variable in a line "var x := 1, y;", if any but the last is assigned to. 
 * - don't generate MIL statements with multiple (i.e. inline) assignments "x := (y := 1) + 1;"
 * - for better pruning: use assignment notation for update statements "x := x.insert(y);"
 * ----------------------------------------------------------------------------
 */

#define OPT_STMTS 32767 /* <65535 if first-use further out than this amount of stamements, dead codes survises anyhow */
#define OPT_VARS 8192 /* don't try dead code elimintation above this amount of live variables */
#define OPT_REFS 12 /* keep track of usage dependencies; may omit some, which results in surviving dead code */
#define OPT_COND 255 

typedef struct {
        char *mil; /* buffered line of MIL */
        unsigned int stmt_nr; /* absolute statement number in MIL input */
        unsigned int used:16,inactive:1,delchar:7,nilassign:1,refs:7; 
        /* used:      becomes true if this variable was used */ 
        /* inactive:  set if we have tried to eliminate this statement already */
	/* delchar:   we separate statements by substituting a ';' char (or \n for comments) */
        /* nilassign: special treatment: nil assignments for early memory reduction are never pruned */
        /* refs:      number of variable references found */
        unsigned short refstmt[OPT_REFS]; /* variable references found on this stmt */
} opt_stmt_t;

typedef struct {
        long long prefix[2]; /* first 12 chars, last integer actually stores suffix length */
        char* suffix;
} opt_name_t; /* string representation designed for fast equality check */

#define OPT_NAME_SUFFIXLEN(x) ((unsigned int*) (x))[3] 
#define OPT_NAME_EQ(x,y) (((((x)->prefix[0] == (y)->prefix[0]) & ((x)->prefix[1] == (y)->prefix[1])) & \
			    (OPT_NAME_SUFFIXLEN(x) == 0)) | \
			 (((((x)->prefix[0] == (y)->prefix[0]) & ((x)->prefix[1] == (y)->prefix[1])) & \
			    (OPT_NAME_SUFFIXLEN(x) != 0)) && \
			   (strncmp((x)->suffix, (y)->suffix, OPT_NAME_SUFFIXLEN(x)) == 0)))
typedef struct {
        opt_name_t name; /* variable name  */
        unsigned short scope; /* scope in which var was defined */
        unsigned short lastset; /* point to a statement where var was assigned.  stmt_nr must match there as well */
        unsigned int stmt_nr; 
} opt_var_t;

typedef struct {
        unsigned int curstmt; /* number of detected MIL statements so far */
        unsigned int scope; /* current scope depth */
        unsigned int curvar; /* length of variable stack */
        unsigned int iflevel; /* how many conditionals have we passed? */
        unsigned int optimize; 

        unsigned int condlevel; /* number of nested conditional blocks recorded on stack (<OPT_COND) */
        unsigned short condscopes[OPT_COND]; /* scopes where each conditional block starts */

        opt_stmt_t stmts[OPT_STMTS]; /* line buffer */
        opt_var_t vars[OPT_VARS]; /* variable stack */
        FILE *fp; 
} opt_t; /* should take ~1.5MB of RAM resources */

void opt_open(opt_t *o, FILE *fp, int optimize);
void opt_close(opt_t *o);
void opt_mil(opt_t *o, char* milbuf);
