/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

/* ==================================================================== */
/* == Program: idxmx                                                 == */
/* == Author: Peter Boncz                                            == */
/* == Function:                                                      == */
/* == a One Afternoon's Hack that searches the @h and @c parts of Mx == */
/* == files for function definitions, global variables, macros, and  == */
/* == typdefs, and puts them in Mx indexing notation.                == */
/* ==================================================================== */
#include <mx_config.h>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#define ROUTINE		1	/* Mx index table number for routines. */
#define GLOBAL		2	/* Mx index table number for global C variables. */
#define MACRO		3	/* Mx index table number for CPP macros. */
#define TYPE		4	/* Mx index table number for C type definitions. */

/* ==================================================================== */
/* == forward declarations.                                          == */
/* ==================================================================== */

/*#ifdef ANSI_C*/
#define	_(x)	x
/*
#else
#define	_(x)	()
#endif
*/
int do_mx_block _((FILE *fp));	/* Silently skip Mx blocks. */
int do_c_block _((FILE *fp));	/* Parse [ch] block. */
int do_cpp_lines _((FILE *fp));	/* Skip cpp directives. */
char *do_identifier _((FILE *fp));	/* Skip one identifier. */
int do_comment _((FILE *fp));	/* Skip c-comments. */
int do_c_code _((FILE *fp, int init));	/* Skip c-code. */
int do_mx_macro _((FILE *fp));	/* Skip macros. */
int do_whitespace _((FILE *fp));	/* Skip whitespace. */
void resume_parsing _((FILE *fp));	/* Resume after Mx part. */
void substitute _((FILE *fp, long size));	/* Generate indexed Mx file. */
int do_header _((FILE *fp, int init));

/* ==================================================================== */
/* == types && defines.                                              == */
/* ==================================================================== */

#define isMacro(c)	((c == ':') || (c == '=') || (c == '`'))
#define isCcode(c)	((isspace((int)(c))) || (c == 'h') || (c == 'c') ||\
			 (c == '}') || (c == '{') || (isdigit((int)(c))))
#define isidchar(c)	((c == '_') || (isalnum((int)(c))))
#define HSIZE        	100	/* initial size of hits table. */

typedef struct {
	long start, end;	/* the location of a string to be indexed */
	char table;		/* for which index table? */
} hit_t;

/* ==================================================================== */
/* == global variables.                                              == */
/* ==================================================================== */

/*
 * The table with all found index terms:
 */
hit_t *hits;			/* hits table: holds found index strings. */
int s_hits = HSIZE;		/* size of hits table. */
int nhits = 0;			/* number of function names seen. */
char identifier[200];		/* last read identifier. */

/*
 * State info maintained to be able to resume C-parsing after an Mx block:
 */
int commenting = 0;		/* were we interrupted reading a comment? */
int c_coding = 0;		/* were we interrupted reading c blocks? */
int headering = 0;		/* were we interrupted reading a routine header? */


/* ==================================================================== */
/* == functional macros.                                             == */
/* ==================================================================== */

int state;			/* simplistic ad-hoc parsing state. */

#define STATE_TYPEDEF 	(state == -1)	/* parsing a typedef. */
#define STATE_NESTED 	(state == 0)	/* not parsing interesting things. */
#define STATE_GLOBAL 	(state == 1)	/* may parse functions or globals. */
#define STATE_WAIT 	(state == 2)	/* init or array part after global. */
#define SET_TYPEDEF 	{state = -1; }
#define SET_NESTED 	{state = 0; }
#define SET_GLOBAL 	{state = 1; }
#define SET_WAIT 	{state = 2; }

char old_c = 0;

/* Do a getc, but check for Mx block command. */
#define do_fgetc(x, y) {						\
    y = fgetc(x);							\
    if ((old_c == '\n') && (y == '@')) {				\
        ungetc(y, x);							\
 	return 0;							\
    }									\
    old_c = y;								\
}

/* hack for bug (another one..) in SGI stdio */
#define FEOF(fp) (feof(fp)||(old_c=='\377'))

#define DEBUG(name)		/* {                                                \
				   char buf[200];                                                   \
				   long pos = ftell(fp);                                            \
				   \
				   fscanf(fp, "%s\n", buf);                                         \
				   fseek(fp, pos, 0);                                                       \
				   } */

#define add_hit(type) {							\
    hits[nhits++].table = (char) '0'+(type%10);				\
/* printf("HIT %c %d %d %d\n", hits[nhits-1].table,			\
	ftell(fp), hits[nhits-1].start, hits[nhits-1].end); */ 		\
   if (hits[nhits-1].start==0) {					\
        nhits--;  printf("ERROR at %s\n", identifier);  } 		\
    if (nhits >= s_hits) {						\
	hits = (hit_t *) realloc(hits, (s_hits*=2)*sizeof(hit_t));	\
    }									\
}


/* ==================================================================== */
/* == routine implementations.                                       == */
/* ==================================================================== */


int
do_mx_block(FILE *fp /* pointer into Mx file. */ )
{
	int at_seen = 0;	/* was the last char an '@'? */
	char c;			/* current char. */

	DEBUG("do_mx_block")
	    while (!FEOF(fp)) {
		c = getc(fp);
		if (at_seen) {
			if ((c == 'h') || (c == 'c')) {
				ungetc(c, fp);
				ungetc('@', fp);
				return 1;
			}
		}
		at_seen = (c == '@');
	}
	return 0;
}


int
do_c_block(FILE *fp /* pointer into Mx file. */ )
{
	char c;			/* current char. */

	DEBUG("do_c_block")
	    do {
		switch (c = fgetc(fp)) {
		case '@':
			c = fgetc(fp);
			if (isMacro(c)) {
				do_mx_macro(fp);
			} else if (isCcode(c)) {
				/* if some C activity had been interrupted, resume it. */
				resume_parsing(fp);
			} else {
				return 1;	/* An Mx block marker that leaves C mode. */
			}
			SET_GLOBAL;
			break;
		case '(':
			if (STATE_GLOBAL) {
				add_hit(ROUTINE);
				SET_NESTED;
			}
			do_header(fp, 1);
			break;
		case '{':
			do_c_code(fp, 1);
			if (STATE_NESTED) {
				SET_GLOBAL;
			}
			break;
		case '#':
			do_cpp_lines(fp);	/* may find macros here */
			SET_GLOBAL;
			break;
		case ',':
			if (STATE_WAIT) {
				SET_GLOBAL;
			}
		case ';':
			if (STATE_TYPEDEF) {
				add_hit(TYPE);
			}
		case '[':
		case '=':
			if (STATE_GLOBAL) {
				add_hit(GLOBAL);
			}
			if (!STATE_NESTED) {
				if (c == ';') {
					SET_GLOBAL;
				} else if ((c == '=') || (c == '[')) {
					SET_WAIT;
				}
			}
		default:
			if (isidchar(c)) {
				ungetc(c, fp);
				if (strcmp(do_identifier(fp), "typedef") == 0) {
					SET_TYPEDEF;
				}

			}
		}
		do_whitespace(fp);
	} while (!FEOF(fp));
	return -1;

}


int
do_c_code(FILE *fp, int init)
/* fp: pointer into Mx file. */
/* init: init static poarsing info? */
{
	static int pars_seen;	/* number of nested parentheses. */
	char c = (char) 0;	/* current char. */

	DEBUG("do_c_code")
	    if (init) {
		pars_seen = 1;
	}
	c_coding = 1;
	while (pars_seen && do_whitespace(fp)) {
		do_fgetc(fp, c);
		if (old_c != '@') {
			if (c == '{') {
				pars_seen++;
			} else if (c == '}') {
				pars_seen--;
			}
		}
	}
	return ((c_coding = pars_seen) == 0);
}


int
do_cpp_lines(FILE *fp)
{
	int define_seen;	/* is it a define, or other macro? */
	char c1 = 0, c2;	/* current and last char. */

	DEBUG("do_cpp_lines")
	    define_seen = (strcmp(do_identifier(fp), "define")) ? 0 : 1;
	while (!FEOF(fp)) {
		c2 = c1;
		c1 = fgetc(fp);
		if (define_seen) {
			if ((define_seen == 1) && isidchar(c1)) {
				hits[nhits].start = ftell(fp) - 2;
				define_seen++;
			} else if ((define_seen == 2) && !isidchar(c1)) {
				hits[nhits].end = ftell(fp) - 2;
				define_seen++;
				add_hit(MACRO);
			}
		}
		if ((c1 == '\n') && (c2 != '\\')) {
			return 1;
		}
	}
	return 0;
}



int
do_mx_macro(FILE *fp)
{
	char c;			/* current char. */

	DEBUG("do_mx_macro")
	    while (do_whitespace(fp)) {
		c = fgetc(fp);
		if (c == '@') {
			if (!FEOF(fp)) {
				c = fgetc(fp);
				if (isspace((int) (c))) {
					return 1;
				} else if (isMacro(c)) {
					do_mx_macro(fp);	/* recursive macros */
				}
			}
		}
	}
	return 0;
}


int
do_header(FILE *fp, int init)
{
	static int status;	/* static parsing state info. */
	char c;			/* current char. */

	DEBUG("do_header")
	    headering++;
	if (init) {
		status = 0;
	}
	while (do_whitespace(fp) && do_identifier(fp)) {
		do_fgetc(fp, c);

		if ((status == 0) && (c != '(')) {
			status++;
		}
		if ((status == 1) && (c == ')')) {
			status++;
		}
		if ((status == 2) && (c != ')')) {
			/* Close your eyes. Filthy hacks coming up... */
			if (c == '(') {
				/* "...)(" must be a function variable, skip real header. */
				do_header(fp, 1);
			} else if (c == ';') {
				/* "); " means end-of-forward-decl. -- if not typedeffing. */
				if ((headering == 1) && (!(STATE_TYPEDEF || STATE_WAIT))) {
					SET_GLOBAL;
				} else {
					ungetc(c, fp);
				}
			} else {
				ungetc(c, fp);
			}
			headering = 0;
			return 1;
		}
	}
	return 0;
}


char *
do_identifier(FILE *fp)
{
	long pos = ftell(fp);	/* initial file position. */
	char c;			/* current char. */

	DEBUG("do_identifier")
	    /* Clumsy read ahead.. */
	    identifier[0] = 0;
	fscanf(fp, "%[a-zA-Z0-9_]s", identifier);
	fseek(fp, pos, 0);

	/* Real parsing is done here. */
	while (!FEOF(fp)) {
		c = fgetc(fp);
		if (!isidchar(c)) {
			ungetc(c, fp);
			if (ftell(fp) > pos) {
				/* hack: don't allow "_" for the _() macro */
				if (strcmp(identifier, "_")) {
					/* put markers around the identifier. */
					hits[nhits].start = pos - 1;
					hits[nhits].end = ftell(fp) - 1;
				}
			}
			return identifier;
		}
	}
	return 0;
}


int
do_comment(FILE *fp)
{
	int star_seen = 0;	/* was last char a '*'? */
	char c = 0;		/* current char. */

	DEBUG("do_comment")
	    commenting = 1;
	while (!FEOF(fp)) {
		do_fgetc(fp, c);
		if (star_seen) {
			if (c == '/') {
				commenting = 0;
				return 1;
			}
		}
		star_seen = (c == '*');
	}
	return 0;
}


int
do_whitespace(FILE *fp)
{
	char c = 0;		/* current char. */

	DEBUG("do_whitespace")
	    for (;;) {
		do {
			if (FEOF(fp)) {
				return 0;
			}
			do_fgetc(fp, c);
		} while (isspace((int) (c)));

		if (c == '/') {
			char c;

			if (FEOF(fp)) {
				return 0;
			}
			do_fgetc(fp, c);
			if (c == '*') {
				if (!do_comment(fp)) {
					return 0;
				}
				continue;
			} else {
				ungetc(c, fp);
			}
		}
		break;
	}
	ungetc(c, fp);

	return 1;
}


void
resume_parsing(FILE *fp)
{
	DEBUG("resume_parsing")
	    /* were we interrupted last time?? */
	    if (commenting) {
		if (!do_comment(fp)) {
			return;
		}
	}
	if (c_coding) {
		do_c_code(fp, 0);
	} else if (headering) {
		do_header(fp, 0);
	}
}


void
substitute(FILE *fp, long size)
{
	long end = size;	/* size of old file. */
	long tail = size + 4 * nhits;	/* size of new file. */
	int i;			/* scratch variables. */
	long j, s;
	char *buf = (char *) malloc(tail);

	fseek(fp, 0L, 0);
	fread(buf, size, 1, fp);

	/* Substitute in reverse order, so indices stay valid */
	for (i = nhits - 1; i >= 0; i--) {
		/* Copy text. */
		s = end - hits[i].end;
		for (j = 0; j < s; j++) {
			buf[tail - j] = buf[end - j];
		}
		tail -= s;

		/* Insert index number. */
		buf[tail--] = hits[i].table;
		buf[tail--] = '@';

		/* Copy the routine name */
		s = hits[i].end - hits[i].start;
		for (j = 0; j < s; j++) {
			buf[tail - j] = buf[hits[i].end - j];
		}
		tail -= s;

		/* Insert the index code. */
		buf[tail--] = '`';
		buf[tail--] = '@';
		end = hits[i].start;
	}

	/* Write the indexed Mx file on stdout. */
	fwrite(buf, size + 4 * nhits, 1, stdout);
	printf("@%d Table of types\n", TYPE);
	printf("@%d Table of global variables\n", GLOBAL);
	printf("@%d Table of routines\n", ROUTINE);
	printf("@%d Table of macros\n", MACRO);
}

/* ==================================================================== */
/* == main()                                                         == */
/* ==================================================================== */

int
main(argc, argv)
int argc;			/* number of arguments. */
char **argv;			/* table of strings. */
{
	FILE *fp;		/* pointer into Mx file. */

	if ((argc != 2) || ((fp = fopen(argv[1], "r")) == 0)) {
		fprintf(stderr, "usage: %s mx-file\n", argv[0]);
		exit(1);
	}

	/* alloc space for hits table */
	hits = (hit_t *) malloc(s_hits * sizeof(hit_t));

	/* search Mx file for function headers, typedefs, globals and macros. */
	while (do_mx_block(fp) && do_c_block(fp)) ;

	/* put them between Mx index marks, and write to stdout. */
	substitute(fp, ftell(fp));
	exit(0);
	return 1;
}
