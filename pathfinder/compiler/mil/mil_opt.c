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
#include "mil_opt.h"
#include <string.h>

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
opt_name_t name_if, name_else;

/* opt_findvar(): find a variable on the stack 
 */ 
static int opt_findvar(opt_t* o, opt_name_t *name) {
	int i = o->curvar;
	/* FIXME: make a hash table instead of this stupid linear search */
	for(i--; i>=0; i--) {
		if (OPT_NAME_EQ(name, &o->vars[i].name)) return i;
	}
	return -1;
}

/* opt_setname(): helper function that fills a opt_name_t with string data 
 */ 
static int opt_setname(char *p, opt_name_t *name) {
	char *dst = (char*) name->prefix;
	int i;

	name->prefix[0] = 0;
	name->prefix[1] = 0;

	for(i=0; (p[i] == '_') | 
		 ((p[i] >= 'a') & (p[i] <= 'z')) | 
		 ((p[i] >= 'A') & (p[i] <= 'Z')) | 
		 ((p[i] >= '0') & (p[i] <= '9')); i++) 
	{
		if (i < 12) dst[i] = p[i];
	}
	if (i > 12) {
		OPT_NAME_SUFFIXLEN(name) = i-12;
		name->suffix = p+12;
	} else {
		name->suffix = NULL;
	}
	return i;
}

/* opt_elim(): can a MIL statement be pruned? If so; do by commenting out 
 */
static void opt_elim(opt_t* o, unsigned int stmt) {
	if (o->stmts[stmt].used == 0 && o->stmts[stmt].inactive && o->curvar+1 < OPT_VARS) { 
		char *p = o->stmts[stmt].mil;

		if (!o->stmts[stmt].nilassign) {
			/* eliminate dead code (comment out ) */
			if (p[0] == ':' && p[1] == '=') {
				p[0] = ';'; p[1] = '#'; /* special case: "var x := y" =>  "var x ;# y" */
			} else if (p[0] != '\n') {
				p[0] = '#'; 
			}
			while(p[0]) { /* handle multi-stmt statements */
				if (p[0] == '\n' && p[1] != '\n') p[1] = '#';
				p++;
			}
		}
		/* decrement the references (if any) and try to eliminate more */ 
		while(o->stmts[stmt].refs > 0) {
			int i = --(o->stmts[stmt].refs);
			if (o->stmts[o->stmts[stmt].refstmt[i]].stmt_nr <= stmt) {
				if (o->stmts[o->stmts[stmt].refstmt[i]].used < 65535)
					o->stmts[o->stmts[stmt].refstmt[i]].used--;
				opt_elim(o, o->stmts[stmt].refstmt[i]);
			}
		}
	}
}

/* opt_endscope(): when exiting a scope; destroy all varables defined in it 
 */
static void opt_endscope(opt_t* o, unsigned int scope) {
	while(o->curvar > 0 && o->vars[o->curvar-1].scope >= scope) {
		unsigned int lastset = o->vars[--(o->curvar)].lastset;
		if (lastset < OPT_STMTS && o->vars[o->curvar].stmt_nr == o->stmts[lastset].stmt_nr) {
			o->stmts[lastset].inactive = 1;
			opt_elim(o, lastset);
		}
	}
}

/* opt_purgestmt(): push a stmt out of the buffer; no chance to prune it further 
 */
static void opt_purgestmt(opt_t* o, unsigned int stmt) {
	if (o->stmts[stmt].mil) { 
		char *p = o->stmts[stmt].mil;
		if (p) {
			if (*p && *p != ':' && *p != ';') fputs(p, o->fp);
			if (o->stmts[stmt].delchar) fputc(o->stmts[stmt].delchar, o->fp);
		}
		o->stmts[stmt].mil = NULL;
	}
}

/* opt_assign(): record an assigment statement into variable 'name'. 
 * This triggers a attempt of the previous statement that set that variable (if any).
 */
static void opt_assign(opt_t *o, opt_name_t *name, unsigned int curstmt) {
	int i = opt_findvar(o, name);
	/* we may only prune if the variable being overwritten comes from a unconditional scope */
	if (i >= 0 && (o->condlevel == 0 || o->vars[i].scope >= o->condscopes[o->condlevel-1])) {
		unsigned int lastset = o->vars[i].lastset;
		if (lastset < OPT_STMTS && o->vars[i].stmt_nr == o->stmts[lastset].stmt_nr) {
			o->stmts[lastset].inactive = 1;
			opt_elim(o, lastset); /* variable is overwritten; try to eliminate previous assignment */
		}
		o->vars[i].lastset = curstmt;
		o->vars[i].stmt_nr = o->stmts[curstmt].stmt_nr;
	}
}

/* opt_open(): set up our administration.
 */
void opt_open(opt_t* o, FILE *fp, int optimize) {
	memset(o, 0, sizeof(opt_t));
	o->fp = fp;
	o->optimize = optimize;
	opt_setname("if", &name_if);
	opt_setname("else", &name_else);
}

/* opt_close(): flush all output stmts; and clean up
 */
void opt_close(opt_t *o) {
	int i; 
	opt_endscope(o, 0); /* destroy all variables (and elim dead code) */

	/* push all stmts out of the buffer */
	for(i=0; i<OPT_STMTS; i++) {
		opt_purgestmt(o, (i + o->curstmt) % OPT_STMTS);
	}
	fflush(o->fp);
}

/* opt_mil(): accept a chunk of unoptimized MIL.
 */
void opt_mil(opt_t *o, char* milbuf) {
	opt_name_t name, assign;
	char *p = milbuf;

	if (o->optimize == 0) {
		fputs(milbuf, o->fp);
		return;
	} 
	name.prefix[0] = 0;
	assign.prefix[0] = 0;
	while(*p) {
		/* add a new yet unused stmt (MIL statement) */
		unsigned int inc, curstmt = o->curstmt % OPT_STMTS;
		unsigned int var_statement = 0, cond_statement = 0;
		opt_purgestmt(o, curstmt); /* make room if necessary */
		o->stmts[curstmt].mil = p;
		o->stmts[curstmt].used = 0;
		o->stmts[curstmt].inactive = 0;
		o->stmts[curstmt].refs = 0;
		o->stmts[curstmt].delchar = 0;
		o->stmts[curstmt].stmt_nr = o->curstmt;
		o->curstmt++;

		/* extract the next statement from the MIL block, 
		 * .. detecting var decls, assignments, open/close blocks and var usage 
		 */
		for(; *p && *p != ';'; p += inc) {
			inc = 1;
			if (p[0]  == '#') { /* ignore comment stmts */
				while(p[inc] && p[inc] != '\n') inc++;
			} else if (p[0] == '"' || p[0] == '\'') { /* skip strings as they may contain ';' chars */
				int quote = p[0], escape = 0;
				while(p[inc]) {
					if (escape) {
					    escape = 0;
					} else if (p[inc] == '\\') {
					    escape = 1;
					}
					if (p[inc++] == quote && !escape) break;
				}
			} else if (p[0] == ':' && p[1] == '=') { /* a mil assignment; delay registration to statement end */
				if (assign.prefix[0] == 0) {
					assign = name; 
					if (var_statement) break; /* break up var x := y; so we can cut off := y */
				}
				inc = 2;
				while((p[inc] == ' ') | (p[inc] == '\t') | (p[inc] == '\n')) inc++;
				if ((p[inc] == 'n') && (p[inc+1] == 'i') && (p[inc+2] == 'l')) {
					o->stmts[curstmt].nilassign = 1; /* nil assignments should never be pruned! */
				}
			} else if (p[0]  == '{' && ((p[1] == ' ') | (p[1] == '\t') | (p[1] == '\n'))) {
				o->scope++;
				if (cond_statement && o->condlevel+1 < OPT_COND) {
				       o->condscopes[o->condlevel++] = o->scope;
				       cond_statement = 0;
				}
				break; /* blocks are separate statements */
			} else if (p[0]  == '}' && ((p[1] == ' ') | (p[1] == '\t') | (p[1] == '\n'))) {
				opt_endscope(o, o->scope); /* this will attempt to eliminate dead code */
				if (o->condlevel > 0 && o->condscopes[o->condlevel-1] == o->scope) {
					o->condlevel--;
				}
				o->scope--;
				break; /* blocks are separate statements */
			} else if (((p[0] == 'v') | (p[0] == 'V')) && 
				   ((p[1] == 'a') | (p[1] == 'A')) && 
				   ((p[2] == 'r') | (p[2] == 'R')) && 
				   ((p[3] == ' ') | (p[3] == '\t') | (p[3] == '\n'))) 
			{
				var_statement = 1;
				inc = 4;
				while((p[inc] == ' ') | (p[inc] == '\t') | (p[inc] == '\n')) inc++;
				inc += opt_setname(p+inc, &name);

				if (o->curvar+1 < OPT_VARS) {
					/* put a new variable on the stack */
					o->vars[o->curvar].name = name;
					o->vars[o->curvar].scope = o->scope;  
					o->vars[o->curvar].lastset = OPT_STMTS;  
					o->vars[o->curvar].stmt_nr = 0;  
					o->curvar++;
				}
			} else if ((p[0] == '_') | ((p[0] >= 'a') & (p[0] <= 'z')) | ((p[0] >= 'A') & (p[0] <= 'Z'))) { 
				inc = opt_setname(p, &name);
				cond_statement |= (name.prefix[0] == name_if.prefix[0]) | 
					          (name.prefix[0] == name_else.prefix[0]);
				while((p[inc] == ' ') | (p[inc] == '\t') | (p[inc] == '\n')) inc++;
				if ((p[inc] != '(') & (p[inc] != ':')) {
					/* detect use of a mil variable */
					int i = opt_findvar(o, &name); 
					if (i >= 0 && o->vars[i].lastset < OPT_STMTS && 
					    o->vars[i].stmt_nr == o->stmts[o->vars[i].lastset].stmt_nr)
					{
						int ref_nr = o->stmts[curstmt].refs;
						if (o->stmts[o->vars[i].lastset].used < 65535) 
							o->stmts[o->vars[i].lastset].used++;
						if (ref_nr+1 < OPT_REFS) {
							o->stmts[curstmt].refstmt[ref_nr] = o->vars[i].lastset;
							o->stmts[curstmt].refs++;
						}
					}
				}
			}
		}
		if (!var_statement) {
			if (assign.prefix[0]) opt_assign(o, &assign, curstmt); /* it was an assigment statement */
			assign.prefix[0] = 0;
		}
		/* separate MIL statements by replacing last char with 0 */
		if (*p != ':') {
			o->stmts[curstmt].delchar = *p;
			*p++ = 0; 
		}
	}
}
