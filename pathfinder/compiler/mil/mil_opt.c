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
 *  created by U Konstanz are Copyright (C) 2000-2005 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Peter Boncz <boncz@cwi.nl>
 *
 * $Id$
 */

/* always include "pathfinder.h", first! */
#include "pathfinder.h"

#include "mil_opt.h"
#include "mem.h"
#include <string.h>

/* ----------------------------------------------------------------------------
 * dead MIL code eliminator (two-day hack by Peter Boncz)
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

/* opt_append(): append stmt either to preamble (proc,module) or normal (MIL query) buf 
 */
static void opt_append(opt_t* o, char* stmt, int sec) {
	int len = strlen(stmt);
	if (o->off[sec] + len >= o->len[sec]) {
		o->len[sec] += o->len[sec] + len+1;
		o->buf[sec] = PFrealloc(o->len[sec], o->buf[sec]);
	}
	memcpy(o->buf[sec] + o->off[sec], stmt, len+1);
	o->off[sec] += len;
}

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
				*p++ = ';'; /* special case: "var x := y" =>  "var x ;" */
			} 
			*p = 0;
			o->stmts[stmt].delchar = 0;
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

/* opt_elimvar(): set all last assignments to this var to inactive; and try to eliminate them (if not used in between)
 */
static void opt_elimvar(opt_t *o, unsigned int varnr) {
	unsigned int i, cond = OPT_COND(o);
	for(i=o->vars[varnr].setlo[cond]; i < o->vars[varnr].sethi[cond]; i++) {
		unsigned int lastset = o->vars[varnr].lastset[i];
		if (lastset < OPT_STMTS && o->vars[varnr].stmt_nr[i] == o->stmts[lastset].stmt_nr) {
			o->stmts[lastset].inactive = 1;
			opt_elim(o, lastset);
		}
		o->vars[varnr].lastset[i] = OPT_STMTS;
	}
}

/* opt_endscope(): when exiting a scope; destroy all varables defined in it 
 */
static void opt_endscope(opt_t* o, unsigned int scope) {
	while(o->curvar > 0 && o->vars[o->curvar-1].scope >= scope) {
		opt_elimvar(o, --(o->curvar));
	}
}

/* opt_purgestmt(): push a stmt out of the buffer; no chance to prune it further 
 */
static void opt_purgestmt(opt_t* o, unsigned int stmt) {
	if (o->stmts[stmt].mil) { 
		char *p = o->stmts[stmt].mil;
		if (p) {
			if (*p && *p != ':' && *p != ';') {
				opt_append(o, p, o->stmts[stmt].sec);
			}
			if (o->stmts[stmt].delchar) {
				char buf[2]; 
				buf[0] = o->stmts[stmt].delchar; 
				buf[1] = 0;
				opt_append(o, buf, o->stmts[stmt].sec);
			}
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
	if (i >= 0) {
		unsigned int cond = OPT_COND(o);
		opt_elimvar(o, i); /* variable is overwritten; try to eliminate previous assignment */

		/* in this conditional scope, make curstmt the only valid assignment */
		if (o->vars[i].setlo[cond] == 0) {
			o->vars[i].setlo[cond] = o->vars[i].setmax;
		} else {
			o->vars[i].setmax = o->vars[i].setlo[cond];
		}
		if (o->vars[i].setmax == OPT_REFS) {
			/* overflow; keep this statement no matter what */
			o->stmts[curstmt].used = 65535;
			o->vars[i].setlo[cond] = o->vars[i].sethi[cond] = 0;
		} else {
			o->vars[i].lastset[o->vars[i].setmax] = curstmt;
			o->vars[i].stmt_nr[o->vars[i].setmax] = o->stmts[curstmt].stmt_nr;
			o->vars[i].sethi[cond] = ++(o->vars[i].setmax);
		}
		o->vars[i].always |= (1 << cond);
	}
}

/* opt_usevar(): record the fact that in a certain MIL statement a certain variable was used
 * This entails incrementing the 'used' count of all assignment statements that *potentially*
 * assign a value seen at this point.
 */
static void opt_usevar(opt_t *o, unsigned int var_nr, unsigned int stmt_nr) {
	unsigned int i, cond, level = o->condlevel;
	do {
		/* compute cond from level, just like OPT_COND() macro does for o->condlevel */
		cond = level + level + o->condifelse[level]; 

		for(i=o->vars[var_nr].setlo[cond]; i<o->vars[var_nr].sethi[cond]; i++) {
	 		if (o->vars[var_nr].lastset[i] < OPT_STMTS && 
			    o->vars[var_nr].stmt_nr[i] == o->stmts[o->vars[var_nr].lastset[i]].stmt_nr)
			{
				int ref_nr = o->stmts[stmt_nr].refs;
				if (o->stmts[o->vars[var_nr].lastset[i]].used < 65535) 
					o->stmts[o->vars[var_nr].lastset[i]].used++;
				if (ref_nr+1 < OPT_REFS) {
					o->stmts[stmt_nr].refstmt[ref_nr] = o->vars[var_nr].lastset[i];
					o->stmts[stmt_nr].refs++;
				}
			}
		}
	/* descent to parent cond; unless we know that current cond overwrites it always */
	} while ((o->vars[var_nr].always & (1<<cond)) == 0 && level-- > 0);
}

/* opt_start_cond(): open conditional block (do conditional variable assignment bookkeeping)
 */
static void opt_start_cond(opt_t *o, unsigned int cond) {
	unsigned int i;
	for(i=0; i<o->curvar; i++) {
		o->vars[i].always &= ~(1<<cond);
		o->vars[i].setlo[cond] = o->vars[i].sethi[cond] = 0;
	}
}
	
/* opt_end_if(): close an if-then-block (do conditional variable assignment bookkeeping)
 */
static void opt_end_if(opt_t *o) {
	unsigned int i, cond = OPT_COND(o), cond_if = (cond+2)&(~1);
	for(i=0; i<o->curvar; i++) {
		if (o->vars[i].sethi[cond_if]) {
			/* live range of last assigments are union of parent and if-branch */
			if (o->vars[i].sethi[cond] == 0) {
				o->vars[i].setlo[cond] = o->vars[i].setlo[cond_if];
			}
			o->vars[i].sethi[cond] = o->vars[i].sethi[cond_if];
		}
	}
}

/* opt_end_else(): close an if-then-else-block (do conditional variable assignment bookkeeping)
 */
static void opt_end_else(opt_t *o) {
	unsigned int i, cond = OPT_COND(o), cond_if = (cond+2)&(~1), cond_else = cond_if+1;
	for(i=0; i<o->curvar; i++) {
		if (o->vars[i].sethi[cond_else]) {
			if ((o->vars[i].always & (1<<cond_if)) && (o->vars[i].always & (1<<cond_else))) {
				/* undo effects of opt_end_if first */
				o->vars[i].sethi[cond] = o->vars[i].setlo[cond_if];

				/* variable was always overwritten in both child branches => elim */
				o->vars[i].always |= (1<< cond);
				opt_elimvar(o, i); 

				/* live range of last assigments are union of if- and else-branch */
				o->vars[i].setlo[cond] = o->vars[i].setlo[cond_if];
				o->vars[i].sethi[cond] = o->vars[i].sethi[cond_else];
			} else {
				/* live range of last assigments are union of parent and else-branch */
				if (o->vars[i].sethi[cond] == 0) {
					o->vars[i].setlo[cond] = o->vars[i].setlo[cond_else];
				}
				o->vars[i].sethi[cond] = o->vars[i].sethi[cond_else];
			}
		}
	}
}

/* opt_open(): set up our administration.
 */
void opt_open(opt_t* o, int optimize) {
	memset(o, 0, sizeof(opt_t));
	o->optimize = optimize;
	opt_setname("if", &name_if);
	opt_setname("else", &name_else);
	o->buf[0] = (char*) PFmalloc(o->len[0] = 1024*1024);
	o->buf[1] = (char*) PFmalloc(o->len[1] = 1024*1024);
	o->buf[2] = (char*) PFmalloc(o->len[2] = 1024);
}

/* opt_close(): flush all output stmts; and clean up
 */
char* opt_close(opt_t *o) {
	int i; 
	opt_endscope(o, 0); /* destroy all variables (and elim dead code) */

	/* push all stmts out of the buffer */
	for(i=0; i<OPT_STMTS; i++) {
		opt_purgestmt(o, (i + o->curstmt) % OPT_STMTS);
	}
	/* append the query section to the preamble into the result */ 
	opt_append(o, o->buf[OPT_SEC_MAIN], OPT_SEC_PROLOGUE);
	opt_append(o, o->buf[OPT_SEC_EPILOGUE], OPT_SEC_PROLOGUE);
	return o->buf[OPT_SEC_PROLOGUE];
}

/* opt_mil(): accept a chunk of unoptimized MIL.
 */
void opt_mil(opt_t *o, char* milbuf) {
	opt_name_t name, assign;
	char *p = milbuf;

	if (o->optimize == 0) {
		opt_append(o, milbuf, o->sec);
		return;
	} 
	name.prefix[0] = 0;
	assign.prefix[0] = 0;
	while(*p) {
		/* add a new yet unused stmt (MIL statement) */
		unsigned int inc, curstmt = o->curstmt % OPT_STMTS;
		unsigned int var_statement = 0;
		opt_purgestmt(o, curstmt); /* make room if necessary */
		o->stmts[curstmt].mil = p;
		o->stmts[curstmt].used = 0;
		o->stmts[curstmt].inactive = 0;
		o->stmts[curstmt].refs = 0;
		o->stmts[curstmt].delchar = 0;
		o->stmts[curstmt].stmt_nr = o->curstmt;
		o->stmts[curstmt].sec = o->sec;
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
			} else if (p[0]  == '{') {
				int j = 1;
				while((p[j] >= 'a' && p[j] <= 'z') || (p[j] >= 'A' && p[j] <= 'Z') || p[j] == '_') j++; 
				if (p[j] == '}') {
					inc = j+1; continue; /* detect aggregates */
				}
				o->scope++;
				if ((o->if_statement || o->else_statement) && o->condlevel+1 < OPT_CONDS) {
					o->condscopes[o->condlevel++] = o->scope;
					o->condifelse[o->condlevel] = o->else_statement;
					opt_start_cond(o, OPT_COND(o));
					o->else_statement = o->if_statement = 0;
				}
				break; /* blocks are separate statements */
			} else if (p[0]  == '}') {
				opt_endscope(o, o->scope); /* destroy local variables */
				o->scope--;
				if (o->condlevel > 0 && o->condscopes[o->condlevel-1] == o->scope+1) {
					if (o->condifelse[o->condlevel--]) {
						opt_end_else(o); /* if-then-else block was closed */
					} else {
						opt_end_if(o); /* if-then block was closed */
					}
				}
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
					memset(o->vars+o->curvar, 0, sizeof(opt_var_t));
					o->vars[o->curvar].name = name;
					o->vars[o->curvar].scope = o->scope;  
					o->curvar++;
				}
			} else if ((p[0] == '_') | ((p[0] >= 'a') & (p[0] <= 'z')) | ((p[0] >= 'A') & (p[0] <= 'Z'))) { 
				inc = opt_setname(p + (p[0] == '{'), &name);
				o->if_statement |= (name.prefix[0] == name_if.prefix[0]);
				o->else_statement |= (name.prefix[0] == name_else.prefix[0]);
				while((p[inc] == ' ') | (p[inc] == '\t') | (p[inc] == '\n')) inc++;
				if ((p[inc] != '(') & (p[inc] != ':')) {
					/* detect use of a mil variable */
					int i = opt_findvar(o, &name); 
					if (i >= 0) opt_usevar(o, i, curstmt);
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
