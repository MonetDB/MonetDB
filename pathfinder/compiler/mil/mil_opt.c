/**
 * @file
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

/* always include "pathfinder.h", first! */
#include "pathfinder.h"

#include "mil_opt.h"
#include "mem.h"

#include <string.h>
#include <stdlib.h>

/* malloc to return result buffers with */
#define EXTERN_MALLOC(n)	malloc(n)
#define EXTERN_REALLOC(p, n)	realloc(p, n)
#define EXTERN_FREE(p)		free(p)

/* malloc for internal use in milopt structures */
#define INTERN_MALLOC(n)	PFmalloc(n)
#define INTERN_REALLOC(p, n)	PFrealloc(p, n)
#define INTERN_FREE(p)		

opt_name_t name_if, name_else;

/* opt_setname(): helper function that fills a opt_name_t with string data 
 */ 
static int opt_setname(char *p, opt_name_t *name) {
	char *dst = (char*) name->prefix;
	int i;
	name->prefix[0] = name->prefix[1] = 0;
	name->suffix = NULL;
	for(i=0; ((p[i] >= 'a') & (p[i] <= 'z')) | ((p[i] >= 'A') & (p[i] <= 'Z')) | 
	         ((p[i] >= '0') & (p[i] <= '9')) | (p[i] == '_'); i++) 
	{
		if (i < 12) dst[i] = p[i];
	}
	if (i > 12) {
		OPT_NAME_SUFFIXLEN(name) = i-12;
		name->suffix = p+12;
	}
	return i;
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


/* ----------------------------------------------------------------------------
 * string matching defines and helpers
 * - opt_skip():        skip over whitespace and (optionally) MIL comments
 * - opt_match_space(): check if next character is whitespace 
 * - opt_match_var():   efficiently check if next token is "var"
 * - opt_match_else():  efficiently check if next token is "else"
 * ----------------------------------------------------------------------------
 */
#define opt_match_space(c) 	(((c) == ' ') | ((c) == '\t') | ((c) == '\n') | ((c) == '#'))
#define opt_match_var(p)	(((p[0] == 'v') | (p[0] == 'V')) &&\
				 ((p[1] == 'a') | (p[1] == 'A')) &&\
				 ((p[2] == 'r') | (p[2] == 'R')) &&\
				 ((p[3] == 0) | opt_match_space(p[3])))
#define opt_match_else(p)	(((p[0] == 'e') | (p[0] == 'E')) &&\
				 ((p[1] == 'l') | (p[1] == 'L')) &&\
				 ((p[2] == 's') | (p[2] == 'S')) &&\
				 ((p[3] == 'e') | (p[3] == 'E')) &&\
				 ((p[4] == 0) | (p[4] == '{') | opt_match_space(p[4])))

/* opt_skip(): skip over whitespace and comments
 */
char* opt_skip(char* p, int skip_comment) {
	int skip = -1;
	do {
    		if (p[++skip] == 0) break;
		if (p[skip] == '#')  { 
    			if (skip_comment == 0) return p+skip;
			/*skip comment until end of line */
			while(p[++skip] != '\n') if (p[skip] == 0) return p+skip;
		}
	} while (opt_match_space(p[skip]));
	return p+skip;
}



/* ----------------------------------------------------------------------------
 * code motion: move statements into if- and else-blocks to eliminate more
 *
 * Inside each conditional MIL scope we keep an active list of "moveables"
 * i.e. assignment statements without direct dependencies/overwrites, that may be moved
 * until the current point without semantic changes.
 * - opt_move_add():     add a statement to the moveables list 
 *
 * If another statement overwrites the target or one of the dependencies of  
 * the assigment statement, it must be removed from the moveables list:
 * - opt_move_kill():    eliminate a statement from the moveables list
 * - opt_move_killref(): eliminate all statements refered to by this statement from the moveables list
 *
 * When a new conditional (if-statement) is opened we *inject* all moveables both in the 
 * if- and in the else- block (if the latter does not exist, it is created artificially!)
 * - opt_move_inject():  re-emit all alive movable statements 
 * 
 * After closing the else, the moveables list is cleared. Thus, we only try to move 
 * statements into the *directly* following if- (and else-)
 * - opt_move_clear():   clear the list of movable statements
 *
 * Finally, *after* the whole dead-code elimination has been performed, we can check whether moving 
 * a statement actually delivered result. If not, we undo the move.
 * - opt_move_undo():    undo a move of a statment into a nested if- and else- block (recursively) 
 * - opt_move_useful():  a move was useful if in at least one conditional branch, the moved statement 
 *                         could be dead-code-eliminated.
 * ----------------------------------------------------------------------------
 */

/* opt_move_clear: clear the list of moveable statements for the current context 
 */ 
static void opt_move_clear(opt_t *o) {
	unsigned int cond = OPT_COND(o);
	o->movmax = o->movhi[cond] = o->movlo[cond];
	o->movnr[cond] = 0;
}

/* opt_move_add: register this statement as moveable 
 */
static void opt_move_add(opt_t *o, unsigned int stmt) {
	unsigned int cond = OPT_COND(o);

	if ((o->stmts[stmt].mil[0] != ':') &
	    (o->stmts[stmt].mil[0] != '#') &
	    (o->stmts[stmt].scope > 1) & 
	    (o->stmts[stmt].nilassign == 0) &
	    (o->movhi[cond] < OPT_STMTS))
	 {
		if (o->movhi[cond] == 0) o->movlo[cond] = o->movmax;
		o->movstmt[o->movmax] = stmt;
		o->movstmt_nr[o->movmax] = o->stmts[stmt].stmt_nr;
		o->movhi[cond] = ++o->movmax;
		o->movnr[cond]++;
	}
}

/* opt_move_kill(): register that the 'kill' statement itself is not moveable 
 */
static void opt_move_kill(opt_t *o, unsigned int kill) {
	unsigned int i, cond = OPT_COND(o);
	for(i=o->movlo[cond]; i < o->movhi[cond]; i++)  {
		if ((o->movstmt[i] < OPT_STMTS) & (o->movstmt[i] == kill)) {
			o->movstmt[i] = OPT_STMTS;
			o->movnr[cond]--;
		}
	}
}

/* opt_move_killref(): register that the 'kill' statement cannot refer to any moveable statement
 */
static void opt_move_killref(opt_t *o, unsigned int kill) {
	unsigned int i, cond = OPT_COND(o);
	for(i=o->movlo[cond]; i < o->movhi[cond]; i++) {
		unsigned int j, stmt = o->movstmt[i];
		for(j=0; j<o->stmts[stmt].refs; j++) {
			if (o->stmts[stmt].refstmt[j] == kill) {
				o->movstmt[i] = OPT_STMTS;
				o->movnr[cond]--;
				break;
			}
		}
	}
}

/* opt_move_inject(): re-emit all alive movable statements inside the directly enclosed
 *                    if-block or else-block (if no else existed, it is created artificially). 
 */
static void opt_move_inject(opt_t *o, unsigned int cond_base) {
	unsigned int i, cond = OPT_COND(o);

	/* in the if-branch (first run), we determine which statements are moved in */
	if ((cond&1) == 0) {
		for(i=o->movlo[cond_base]; i < o->movhi[cond_base]; i++) {
			unsigned int stmt = o->movstmt[i]; 
			if ((stmt == OPT_STMTS) || (o->stmts[stmt].deleted  |
			    (o->stmts[stmt].stmt_nr != o->movstmt_nr[i])))
			{ 
				o->movstmt[i] = OPT_STMTS; /* not moveable after all */
			}
		}
	}

	/* enter new conditional scope: create a new empty stack of moveable statements */
	o->movlo[cond] = o->movhi[cond] = o->movmax;
	o->movnr[cond] = 0;

	/* inject moveable MIL statements inside the nested if- and else- blocks */
	for(i=o->movlo[cond_base]; i < o->movhi[cond_base]; i++) {
		unsigned int stmt = o->movstmt[i];
		if (stmt != OPT_STMTS && o->movstmt_nr[i] == o->stmts[stmt].stmt_nr) {
			char bak, *q = o->stmts[stmt].mil;
			while(*q) q++;
			bak = q[1]; 
			q[0] = ';'; q[1] = 0;
			o->stmts[stmt].moved[cond&1] = o->curstmt % OPT_STMTS;
			milprintf(o, o->stmts[stmt].mil);
			q[0] = 0; q[1] = bak;
		}
	}
}

/* opt_move_undo(): undo a move of a statment into a nested if- and else- block (recursively) 
 */
static void opt_move_undo(opt_t *o, unsigned int stmt) {
	o->stmts[stmt].deleted = 1;
	if (o->stmts[stmt].moved[0] | o->stmts[stmt].moved[1]) {
		opt_move_undo(o, o->stmts[stmt].moved[0]);
		opt_move_undo(o, o->stmts[stmt].moved[1]);
		o->stmts[stmt].moved[0] = o->stmts[stmt].moved[1] = 0;
	}
}

/* opt_move_useful(): a move was useful if in at least one conditional branch, the moved statement 
 *                    could be dead-code-eliminated.
 */
static int opt_move_useful(opt_t *o, unsigned int stmt) {
	if ((o->stmts[stmt].moved[0] | o->stmts[stmt].moved[1]) == 0)
		return o->stmts[stmt].deleted; /* not moved; just check status */ 

        /* statement was moved sensibly iff in some conditional this caused its elimination */
	if (opt_move_useful(o, o->stmts[stmt].moved[0]) || 
	    opt_move_useful(o, o->stmts[stmt].moved[1])) return 1;

	/* moving did not eliminate any work (only makes MIL script longer): undo */
	opt_move_undo(o, stmt);
	return o->stmts[stmt].deleted = 0;
} 


/* ----------------------------------------------------------------------------
 * output printing: here we do pretty printing (indentation)
 * - opt_printmil(): append stmt (with proper indenting) to the active section (prologue,query,epilogue) 
 * - APPEND_INIT():  initialize dst,end pointers and check for malloc failure
 * - APPEND_PUTC():  add one character to the output buffer. re-alloc if full
 * ---------------------------------------------------------------------------
 */
#define APPEND_INIT(o,sec,dst,end)						\
	{ if (o->buf[sec] == NULL) return;					\
	  dst = o->buf[sec] + o->off[sec]; 					\
	  end = o->buf[sec] + o->len[sec] - 2; }
#define APPEND_PUTC(o,sec,c,dst,end)						\
        if (((scope<0) | (c!='\n')) || (dst > o->buf[sec] && dst[-1]!='\n')) {	\
	  if (dst >= end) { 							\
		o->off[sec] = dst - o->buf[sec];				\
		o->len[sec] += (o->len[sec]<1024)?1024:o->len[sec];		\
		o->buf[sec] = (char*) EXTERN_REALLOC(o->buf[sec], o->len[sec]);	\
		APPEND_INIT(o,sec,dst,end);					\
	  } *dst++ = c; }

/* opt_printstmt(): append stmt (with proper indenting) to the active section (prologue,query,epilogue) 
 */
static void opt_printmil(opt_t* o, char* src, int sec, int scope) {
	char *dst, *end;
	APPEND_INIT(o,sec,dst,end);
	if (scope >= 0) /* skip whitespace at start of line */
		while((*src == ' ') | (*src == '\t') | (*src =='\n')) src++;
	while(*src) {
		int i, c = 0, nocomment = (scope >= 0);
		if (scope >= 0) { /* indent */
			if (*src == '}') scope--;
			if (*src == '{') APPEND_PUTC(o,sec,'\n',dst,end); 
			for(i=0; i<scope; i++) 
				APPEND_PUTC(o,sec,' ',dst,end); 
			if (*src == '{') scope++;
		} 
		while(*src && c != '\n') { /* write out one line */
			c = *src++;
			nocomment &= (c != '#'); 
			APPEND_PUTC(o,sec,c,dst,end);
			if (nocomment && src[0] == '{' && src[1] == 0) break;
		}
		if (nocomment & ((c == '{') | (c == '}') | (c == ';')))
			APPEND_PUTC(o,sec,'\n',dst,end);
	}
	o->off[sec] = dst - o->buf[sec]; *dst = 0;
}


/* ----------------------------------------------------------------------------
 * emit code: additional post-dead-code analysis to further prune useless statements
 * - opt_emit():               emit a statement if useful (call pretty-printing)
 * - opt_emit_check_vardecl(): remove useless vardefs
 * - opt_emit_killempty():     remove sequential blocks that have become empty.
 * - we also check if code motion is actually useful, and undo it if not (opt_move_useful)
 * ----------------------------------------------------------------------------
 */

/* opt_emit_check_vardecl(): after dead-code elimination, remove useless vardefs
 */
static int opt_emit_check_vardecl(opt_t *o, unsigned int stmt) {
	/* usage check: kill vardefs that were never used (and their nil-assigns) */
	unsigned int assigns_to = o->stmts[stmt].assigns_to&32767;
	if ((o->stmts[stmt].assigns_nr > 0) &
	    (o->stmts[assigns_to].stmt_nr == o->stmts[stmt].assigns_nr) &
             (o->stmts[assigns_to].used == 0)) 
	{
		o->stmts[stmt].deleted = 1;
	}
	return o->stmts[stmt].deleted;
}

/* opt_emit_killempty(): after dead-code elimination, we remove sequential blocks that have become empty.
 */
static int opt_emit_check_emptyblock(opt_t *o, unsigned int stmt) {
	unsigned int stmt_nr = o->stmts[stmt].stmt_nr;
	unsigned int until = stmt;

	/* check if we have an ampty block in front of us */
	if (o->stmts[stmt].delchar != '{') return 0;
	while(1) {
		if (++until == OPT_STMTS) until = 0;
		if (o->stmts[until].stmt_nr != ++stmt_nr) return 0;
		if (o->stmts[until].delchar == '}') break;
		if (!opt_emit_check_vardecl(o, until)) return 0;
	}
	/* advance to the statement after the to-be-deleted block */
	if (++until == OPT_STMTS) until = 0;
	if (o->stmts[until].stmt_nr != ++stmt_nr) return 0;

	/* nothing goes if it is an else branch (that cannot be deleted itself) */
	if (opt_match_else(o->stmts[until].mil) && opt_emit_check_emptyblock(o,until) == 0) 
		return 0;

	/* delete the statements */
	do {
		o->stmts[stmt].deleted = 1; 
		if (++stmt == OPT_STMTS) stmt = 0;
	} while (stmt < until);
	return 1;
}


/* opt_emit(): push a stmt out of the buffer; no chance to prune it further 
 */
static void opt_emit(opt_t* o, unsigned int stmt) {
	char *p = o->stmts[stmt].mil;
	if (	/* statements cut off from variable declarations were already printed */
		((p != NULL) && ((*p != ':') & (*p != ';'))) 
	&&
		/* check if moves were actually useful; if not, undo them */
		(opt_move_useful(o, stmt) == 0) 
	&&
		/* kill variable declerations that are now unused */
		(opt_emit_check_vardecl(o, stmt) == 0)
	&&
		/* post-optimization: delete empty blocks */
		(o->stmts[stmt].scope == 0 || opt_emit_check_emptyblock(o, stmt) == 0))
	{
		/* emit the statement (temporarily put delchar back) */
		char bak, *p = o->stmts[stmt].mil;
		while(*p) p++;
		p[0] = o->stmts[stmt].delchar;
		if (p[0]) { bak = p[1]; p[1] = 0; }
		opt_printmil(o, o->stmts[stmt].mil, o->stmts[stmt].sec, o->stmts[stmt].scope);
		if (p[0]) { p[0] = 0; p[1] = bak; }
	}
	if (o->stmts[stmt].ptr) { /* garbage collect milprintf() buffer */
		INTERN_FREE(o->stmts[stmt].ptr);
		o->stmts[stmt].ptr = NULL;
	}
}



/* ----------------------------------------------------------------------------
 * main dead-code eliminator
 * - opt_mil():      recognize statements in a chunk of MIL
 * - opt_assign():   record an assigment statement into variable 'name'. 
 * - opt_usevar():   record the fact that in a certain MIL statement a certain variable was used
 * - opt_elim():     try to delete a MIL statement 
 * - opt_elimvar():  set all last assignments to this var to inactive
 *                       and try to eliminate them (if not used in between)
 * - opt_endscope(): when exiting a scope; destroy all varables defined in it 
 *
 * Special care is taken for the analysis of if-then-else blocks. The rationale is 
 * that we really want to know whether a variable is always set in *both*
 * conditional branches. Only allows us to try to dead-code eliminate the previous
 * assignments (before the branch).
 *
 * - opt_start_cond(): open conditional block (do conditional variable assignment bookkeeping)
 * - opt_end_if():     close an if-then-block (do conditional variable assignment bookkeeping)
 * - opt_end_else():   close an if-then-else-block (do conditional variable assignment bookkeeping)
 *
 * We need a *stack* data structure with a two entries (even=if, odd=else) for each level
 * of depth of conditional statements. The current stack position is given by OPT_COND(o). 
 * Such a stack is used both for the variable "last-set" administration in each variable record, 
 * as well as for the "moveable statements" list. 
 * ----------------------------------------------------------------------------
 */

/* opt_elim(): can a MIL statement be pruned? If so; set 'deleted' field
 */
static void opt_elim(opt_t* o, unsigned int stmt, int kill_nilassign) {
	unsigned int assigns_to = o->stmts[stmt].assigns_to;
	if ((assigns_to < 32768) & (o->stmts[stmt].used == 0) & o->stmts[stmt].inactive & (o->curvar+1 < OPT_VARS)) { 
		char *p = o->stmts[stmt].mil;

		o->stmts[stmt].assigns_to |= 32768; /* this ensures we kill the statement only once */

		/* decrease the use count of the var declaration statement */
		if ((o->stmts[stmt].assigns_nr > 0) & 
	 	    (o->stmts[assigns_to].stmt_nr == o->stmts[stmt].assigns_nr))
		{
			o->stmts[assigns_to].used--;
		}

		if (kill_nilassign || !o->stmts[stmt].nilassign) {
			/* eliminate dead code (comment out ) */
			if (p[0] == ':' && p[1] == '=') {
				*p++ = 0; /* special case: "var x := y" =>  "var x ;" */
			} 
			o->stmts[stmt].deleted = 1;
		}
		/* decrement the references (if any) and try to eliminate more */ 
		while(o->stmts[stmt].refs > 0) {
			int i = --(o->stmts[stmt].refs);
			if (o->stmts[o->stmts[stmt].refstmt[i]].stmt_nr <= stmt) {
				o->stmts[o->stmts[stmt].refstmt[i]].used--;
				opt_elim(o, o->stmts[stmt].refstmt[i], 1);
			}
		}
	}
}

/* opt_elimvar(): set all last assignments to this var to inactive; and try to eliminate them (if not used in between)
 */
static void opt_elimvar(opt_t *o, unsigned int varnr, int kill_nilassign) {
	unsigned int i, cond = OPT_COND(o);
	for(i=o->vars[varnr].setlo[cond]; i < o->vars[varnr].sethi[cond]; i++) {
		unsigned int lastset = o->vars[varnr].lastset[i];
		if (lastset < OPT_STMTS && o->vars[varnr].stmt_nr[i] == o->stmts[lastset].stmt_nr) {
			o->stmts[lastset].inactive = 1;
			opt_elim(o, lastset, kill_nilassign);
		}
		o->vars[varnr].lastset[i] = OPT_STMTS;
	}
	/* try to shrink the lastset list and setlo/sethi referring to it */
	for(i=o->vars[varnr].setmax; i>0; i--)
		if (o->vars[varnr].lastset[i-1] != OPT_STMTS) break;
	if (i < o->vars[varnr].setmax) {
		o->vars[varnr].setmax = i;
		for(i=0; i<=cond; i++) {
			if (o->vars[varnr].setlo[i] >= o->vars[varnr].setmax) {
				o->vars[varnr].setlo[i] = o->vars[varnr].sethi[i] = 0;
			} else if (o->vars[varnr].sethi[i] > o->vars[varnr].setmax) {
				o->vars[varnr].sethi[i] = o->vars[varnr].setmax;
			}
		}
	}
}

/* opt_endscope(): when exiting a scope; destroy all varables defined in it 
 */
static void opt_endscope(opt_t* o, unsigned int scope) {
	unsigned int i = o->curvar;
	while(i-- > 0 && o->vars[i].scope >= scope) {
		unsigned int j, cond=OPT_COND(o);
		for(j=o->vars[i].setlo[cond]; j<o->vars[i].sethi[cond]; j++) {
			opt_move_kill(o, o->vars[i].lastset[j]);
			opt_move_killref(o, o->vars[i].lastset[j]);
		}
		opt_elimvar(o, o->curvar = i, 0);
	}
}

/* opt_assign(): record an assigment statement into variable 'name'. 
 */
static void opt_assign(opt_t *o, opt_name_t *name, unsigned int stmt) {
	int j,i = opt_findvar(o, name);
	/* we may only prune if the variable being overwritten comes from a unconditional scope */
	if (i >= 0) {
		unsigned int def_stmt = o->vars[i].def_stmt;
		unsigned int cond = OPT_COND(o);

		if (o->vars[i].sethi[cond] > o->vars[i].setlo[cond]) {
			/* all statements that depend on lastassign cannot be moveable */
			for(j=o->vars[i].setlo[cond]; j<o->vars[i].sethi[cond]; j++) 
				if (o->vars[i].lastset[j] < OPT_STMTS)
					opt_move_killref(o, o->vars[i].lastset[j]);
		}
		/* variable is overwritten; try to eliminate previous assignment */
		opt_elimvar(o, i, 1); 

		/* in this conditional scope, make stmt the only valid assignment */
		if (o->vars[i].sethi[cond] <= o->vars[i].setlo[cond]) {
			o->vars[i].setlo[cond] = o->vars[i].setmax;
		}
		if (o->vars[i].setmax == OPT_REFS) {
			/* overflow; keep this statement no matter what */
			o->stmts[stmt].used = 1<<31;
			o->vars[i].setlo[cond] = o->vars[i].sethi[cond] = 0;
		} else {
			o->vars[i].lastset[o->vars[i].setmax] = stmt;
			o->vars[i].stmt_nr[o->vars[i].setmax] = o->stmts[stmt].stmt_nr;
			o->vars[i].sethi[cond] = ++(o->vars[i].setmax);
		}
		o->vars[i].always |= (1 << cond);

		/* increase the use count of the var declaration statement */	
		if (o->stmts[def_stmt].stmt_nr == o->vars[i].def_stmt_nr) {
			o->stmts[stmt].assigns_to = def_stmt;
			o->stmts[stmt].assigns_nr = o->stmts[def_stmt].stmt_nr;
			o->stmts[def_stmt].used++;
			opt_move_add(o, stmt); /* register this statement as moveable */
		}
	} else {
		/* delete this statement */
		o->stmts[stmt].deleted = 1;
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
				if (ref_nr+1 < OPT_REFS) {
					o->stmts[stmt_nr].refstmt[ref_nr] = o->vars[var_nr].lastset[i];
					o->stmts[stmt_nr].refs++;
				}
				o->stmts[o->vars[var_nr].lastset[i]].used++;
				opt_move_kill(o, o->vars[var_nr].lastset[i]);
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
	unsigned int i, j, k, cond = OPT_COND(o), cond_if = (cond+2)&(~1), cond_else = cond_if+1;
	for(i=0; i<o->curvar; i++) {
		if ((o->vars[i].always & (1<<cond_if)) && (o->vars[i].always & (1<<cond_else))) {
			/* variable was always overwritten in both child branches => elim */
			o->vars[i].always |= (1<< cond);
			o->vars[i].setmax = o->vars[i].setlo[cond_if];
			opt_elimvar(o, i, 1); 
			k = o->vars[i].setlo[cond] = o->vars[i].setmax;
			for(j=o->vars[i].setlo[cond_if]; j<o->vars[i].sethi[cond_if]; j++, k++) {
				o->vars[i].lastset[k] = o->vars[i].lastset[j];
				o->vars[i].stmt_nr[k] = o->vars[i].stmt_nr[j];
			}
			for(j=o->vars[i].setlo[cond_else]; j<o->vars[i].sethi[cond_else]; j++, k++) {
				o->vars[i].lastset[k] = o->vars[i].lastset[j];
				o->vars[i].stmt_nr[k] = o->vars[i].stmt_nr[j];
			}
			o->vars[i].sethi[cond] = o->vars[i].setmax = k;
		} else if (o->vars[i].sethi[cond_if] || o->vars[i].sethi[cond_else]) {
			/* live range of last assigments are union of parent, if- and else-branch */
			if (o->vars[i].sethi[cond] == 0)
				o->vars[i].setlo[cond] = o->vars[i].sethi[cond_if]?
						o->vars[i].setlo[cond_if]:o->vars[i].setlo[cond_else];
			o->vars[i].sethi[cond] = o->vars[i].sethi[cond_else]?
					o->vars[i].sethi[cond_else]:o->vars[i].sethi[cond_if];
		}
	}
	opt_move_clear(o); /* deactivate the list of moveable statements for the parent context */ 
}
	
/* opt_mil(): accept a chunk of unoptimized MIL.
 */
static int opt_mil(opt_t *o, char* milbuf) {
	unsigned int stmt, inject_cond, var_statement, new_statement=1, curstmt=o->curstmt;
	opt_name_t name, assign;
	char *p = milbuf;

	if (o == NULL) {
		return -1;
	}
	if (o->optimize == 0) {
		opt_printmil(o, milbuf, o->sec, -1);
		return 0;
	} 
	name.prefix[0] = 0;
	assign.prefix[0] = 0;
  	while((p = opt_skip(p, 0))[0]) {
		if (new_statement) {
			/* add a new yet unused stmt (MIL statement) */
			if  (o->stmts[stmt = o->curstmt % OPT_STMTS].mil)
				opt_emit(o, stmt); /* make room */
			memset(o->stmts+stmt, 0, sizeof(opt_stmt_t));
			o->stmts[stmt].stmt_nr = ++(o->curstmt);
			o->stmts[stmt].scope = o->scope;
			o->stmts[stmt].sec = o->sec;
			inject_cond = 1;  /* impossible cond level */
			new_statement = var_statement = 0;
		}
		o->stmts[stmt].mil = p;

		/* extract the next statement from the MIL block, 
		 * .. detecting var decls, assignments, open/close blocks and var usage 
		 */
		while((p = opt_skip(p,1))[0] && p[0] != ';') {
			if ((p[0] == '"') | (p[0] == '\'')) { /* skip strings as they may contain ';' chars */
				int quote = *p++, escape = 0;
				while(*p) {
					if (escape) {
					    escape = 0;
					} else if (*p == '\\') {
					    escape = 1;
					}
					if ((*p++ == quote) & !escape) break;
				}
			} else if (p[0] == ':' && p[1] == '=') { /* a mil assignment; delay registration to statement end */
				if (assign.prefix[0] == 0) {
					assign = name; 
					if (var_statement) break; /* break up var x := y; so we can cut off := y */
				}
				p = opt_skip(p+2, 1); /* skip whitespace & comment */
				if ((p[0] == 'n') && (p[1] == 'i') && (p[2] == 'l')) {
					o->stmts[stmt].nilassign = 1; /* nil assignments should never be pruned! */
					p += 3;
				}
			} else if (p[0] == '{') {
				int j = 1;
				while(((p[j] >= 'a') & (p[j] <= 'z')) | ((p[j] >= 'A') & (p[j] <= 'Z')) | (p[j] == '_')) j++; 
				if ((j > 1) & (p[j] == '}')) {
					p += j+1; continue; /* detect aggregates */
				}
				o->scope++;
				if ((o->if_statement | o->else_statement) & (o->condlevel+1 < OPT_CONDS)) {
					inject_cond = OPT_COND(o);
					o->condscopes[o->condlevel++] = o->scope;
					o->condifelse[o->condlevel] = o->else_statement;
					opt_start_cond(o, OPT_COND(o));
					o->else_statement = o->if_statement = 0;
				}
				break; /* blocks are separate statements */
			} else if (p[0]  == '}') {
				char *q = opt_skip(p+1, 1); /* peek over whitespace & comment */
				int end_cond = (o->condlevel > 0 && o->condscopes[o->condlevel-1] == o->scope);
				int end_else = (end_cond && o->condifelse[o->condlevel]);
				int else_next = opt_match_else(q);

				if ((end_cond & (end_else == 0) & (else_next == 0)) && 
                                    o->movnr[OPT_COND_LEVEL(o,o->condlevel-1)]) 
				{
					/* ignore '}'; insert artificial else instead */
					*p = ' '; inject_cond = OPT_CONDS+OPT_CONDS; 
					break;
				}
				opt_endscope(o, o->scope); /* destroy local variables */
				o->scope--;
				if (end_cond) {
					o->condlevel--;
					if (end_else) {
						opt_end_else(o); /* if-then-else block was closed */
					} else if (!else_next) {
						opt_end_if(o); /* close if */
					}
				}
				break; /* blocks are separate statements */
			} else if (opt_match_var(p)) {
				var_statement = 1;
				p = opt_skip(p+3, 1); /* skip whitespace & comment */
				p += opt_setname(p, &name);

				if (o->curvar+1 < OPT_VARS) {
					/* put a new variable on the stack */
					memset(o->vars+o->curvar, 0, sizeof(opt_var_t));
					o->vars[o->curvar].name = name;
					o->vars[o->curvar].scope = o->scope;  
					o->vars[o->curvar].def_stmt = stmt;
					o->vars[o->curvar].def_stmt_nr = o->curstmt;
					o->curvar++;

					/* mark the var_statement as assigning to itself (allows usage check later)*/
					o->stmts[stmt].assigns_to = stmt;
					o->stmts[stmt].assigns_nr = o->curstmt;
				}
			} else if ((p[0] == '_') | ((p[0] >= 'a') & (p[0] <= 'z')) | ((p[0] >= 'A') & (p[0] <= 'Z'))) { 
				p += opt_setname(p, &name);
				o->if_statement |= (name.prefix[0] == name_if.prefix[0]);
				o->else_statement |= (name.prefix[0] == name_else.prefix[0]);
				p = opt_skip(p, 1); /* skip comment stmts */
				if ((*p != '(') & (*p != ':')) {
					/* detect use of a mil variable */
					int i = opt_findvar(o, &name); 
					if (i >= 0) opt_usevar(o, i, stmt);
				}
			} else if (p[0]) {
				p++; /* character without special meaning for us */
			}
		}
		if (!var_statement) {
			if (assign.prefix[0]) opt_assign(o, &assign, stmt); /* it was an assigment statement */
			assign.prefix[0] = 0;
		}

		/* separate MIL statements by replacing last char with 0 */
		if (*p && *p != ':') {
			o->stmts[stmt - (o->stmts[stmt].mil[0] == ':')].delchar = *p;
			*p++ = 0; 
			/* inject the moveable statements */
			if (inject_cond == OPT_CONDS+OPT_CONDS) {
				milprintf(o, "}else{}"); /* inject an artificial else */
			} else if (inject_cond != 1) {
				opt_move_inject(o, inject_cond);
			}
		}
		/* check if we actually got some statement now */
		new_statement = o->stmts[stmt].delchar | (o->stmts[stmt].mil && o->stmts[stmt].mil[0]);
	}
	/* make sure milbuf is garbage collected */
	if (new_statement == 0 && --(o->curstmt) == curstmt) {
		INTERN_FREE(milbuf); /* directly (no statements emitted) */
	} else {
		o->stmts[stmt].ptr = milbuf; /* after emit (attach to last statement) */
	}
	return (o->buf[OPT_SEC_PROLOGUE] == NULL) | 
	       (o->buf[OPT_SEC_QUERY] == NULL) |
	       (o->buf[OPT_SEC_EPILOGUE] == NULL);
}




/* ----------------------------------------------------------------------------
 * dead MIL code eliminator API (hack by Peter Boncz)
 * - opt_open():  set up our administration.
 * - milprintf(): print mil in a streaming fashion (call as often as you want)
 * - opt_close(): flush all output stmts; and clean up
 *
 * Depending how o->sec is set during the milprintf() call, optimized MIL is echoed 
 * in one of result buffers ("sections"):
 * - prologue (containing module load statements and MIL proc definitions)
 * - query (the main part)
 * - epilogue (the main part)
 *
 * milprintf() limitations:
 * - don't declare more than one variable in a line "var x := 1, y;", if any but the last is assigned to.
 * - don't generate MIL statements with multiple (i.e. inline) assignments "x := (y := 1) + 1;"
 * - else blocks should be opened in the *same* milprintf() call where the if- statement was closed!
 * - for better pruning: use assignment notation for update statements "x := x.insert(y);"
 * ----------------------------------------------------------------------------
 */

/* opt_open(): set up our administration.
 */
opt_t *opt_open(int optimize) {
	opt_t *o = (opt_t*) EXTERN_MALLOC(sizeof(opt_t));
	if (o) {
		memset(o, 0, sizeof(opt_t));
		o->optimize = optimize;
		opt_setname("if", &name_if);
		opt_setname("else", &name_else);
		o->buf[OPT_SEC_PROLOGUE] = (char*) EXTERN_MALLOC(o->len[OPT_SEC_PROLOGUE] = 1024);
		o->buf[OPT_SEC_QUERY] = (char*) EXTERN_MALLOC(o->len[OPT_SEC_QUERY] = 2048*1024);
		o->buf[OPT_SEC_EPILOGUE] = (char*) EXTERN_MALLOC(o->len[OPT_SEC_EPILOGUE] = 1024);
		if (o->buf[OPT_SEC_PROLOGUE]) o->buf[OPT_SEC_PROLOGUE][0] = 0;
		if (o->buf[OPT_SEC_QUERY]) o->buf[OPT_SEC_QUERY][0] = 0;
		if (o->buf[OPT_SEC_EPILOGUE]) o->buf[OPT_SEC_EPILOGUE][0] = 0;
	}
 	return o;
}

/* milprintf(): print mil in a streaming fashion
 */
int milprintf(opt_t *o, const char *format, ...)
{
        int j, i = strlen(format) + 80;
        char *milbuf = INTERN_MALLOC(i);
        va_list ap;

	if (milbuf == NULL) return -1;

        /* take in a block of MIL statements */
        va_start(ap, format);
        j = vsnprintf(milbuf, i, format, ap);
        va_end (ap);
        while (j < 0 || j > i) {
                if (j > 0)      /* C99 */
                        i = j + 1;
                else            /* old C */
                        i *= 2;

                milbuf = INTERN_REALLOC(i, milbuf);
		if (milbuf == NULL) return -1;

                va_start(ap, format);
                j = vsnprintf(milbuf, i, format, ap);
                va_end (ap);
        }
        return opt_mil(o, milbuf);
}

/* opt_close(): flush all output stmts; and clean up
 */
void opt_close(opt_t *o, char** prologue, char** query, char** epilogue) {
	unsigned int i = 0; 
	opt_endscope(o, 0); /* destroy all variables (and elim dead code) */

	/* push all stmts out of the buffer */
	if (o->stmts[OPT_STMTS-1].stmt_nr) {
		for(i=0; i<OPT_STMTS; i++) { /* buffer full: round-robin */
			opt_emit(o, (i + o->curstmt) % OPT_STMTS);
		}
	} else {
		for(i=0; i<o->curstmt; i++) {
			opt_emit(o, i);
		}
	}
	/* return the three buffers */
	*prologue = o->buf[OPT_SEC_PROLOGUE];
	*query = o->buf[OPT_SEC_QUERY];
	*epilogue = o->buf[OPT_SEC_EPILOGUE];
	EXTERN_FREE(o);
}
