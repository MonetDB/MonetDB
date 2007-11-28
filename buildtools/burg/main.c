char rcsid_main[] = "$Id$";

#include "burg_config.h"
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "b.h"
#include "fe.h"

int debugTables = 0;
static int simpleTables = 0;
static int internals = 0;
static int diagnostics = 0;

char *inFileName = NULL;
static char *outFileName;

static char version[] = "BURG, Version 1.0";

extern int main ARGS((int argc, char **argv));

#if HAVE_STRING_H && HAVE_STRDUP
#include <string.h>

#ifdef NATIVE_WIN32
/* The POSIX name for this item is deprecated. Instead, use the ISO
   C++ conformant name: _strdup. See online help for details. */
#define strdup _strdup
#endif
#else
static char *strdup (const char *);
#endif

void doGrammarNts (void);          /* defined in fe.c */
void makeRuleDescArray (void);     /* defined in be.c */
void makeDeltaCostArray (void);    /* defined in be.c */
void makeStateStringArray (void);  /* defined in be.c */

int
main(argc, argv) int argc __attribute__((unused)); char **argv;
{
	int i;

	for (i = 1; argv[i]; i++) {
		char **needStr = 0;
		int *needInt = 0;

		if (argv[i][0] == '-') {
			switch (argv[i][1]) {
			case 'V':
				fprintf(stderr, "%s\n", version);
				break;
			case 'p':
				needStr = &prefix;
				break;
			case 'o':
				needStr = &outFileName;
				break;
			case 'I':
				internals = 1;
				break;
			case 'T':
				simpleTables = 1;
				break;
			case '=':
#ifdef NOLEX
				fprintf(stderr, "'%s' was not compiled to support lexicographic ordering\n", argv[0]);
#else
				lexical = 1;
#endif /* NOLEX */
				break;
			case 'O':
				needInt = &principleCost;
				break;
			case 'c':
				needInt = &prevent_divergence;
				break;
			case 'e':
				needInt = &exceptionTolerance;
				break;
			case 'd':
				diagnostics = 1;
				break;
			case 'S':
				speedflag = 1;
				break;
			case 't':
				trimflag = 1;
				break;
			case 'G':
				grammarflag = 1;
				break;
			default:
				fprintf(stderr, "Bad option (%s)\n", argv[i]);
				exit(1);
			}
		} else {
			if (inFileName) {
				fprintf(stderr, "Unexpected Filename (%s) after (%s)\n", argv[i], inFileName);
				exit(1);
			}
			inFileName = strdup (argv[i]);
		}
		if (needInt || needStr) {
			char *v;
			char *opt = argv[i];

			if (argv[i][2]) {
				v = &argv[i][2];
			} else {
				v = argv[++i];
				if (!v) {
					fprintf(stderr, "Expection argument after %s\n", opt);
					exit(1);
				}
			}
			if (needInt) {
				*needInt = atoi(v);
			} else if (needStr) {
				*needStr = v;
			}
		}
	}

	if (inFileName) {
		int count;
		char *p;
		if(freopen(inFileName, "r", stdin)==NULL) {
			fprintf(stderr, "Failed opening (%s)", inFileName);
			exit(1);
		}
		for (count = 0, p = inFileName; *p; p++)
			if (*p == '\\')
				count++;
		if (count > 0) {
			char *q, *r;
			p = malloc(strlen(inFileName) + count + 1);
			for (q = inFileName, r = p; *q; q++) {
				*r++ = *q;
				if (*q == '\\')
					*r++ = '\\';
			}
			free(inFileName);
			inFileName = p;
		}
	}

	if (outFileName) {
		if ((outfile = fopen(outFileName, "w")) == NULL) {
			fprintf(stderr, "Failed opening (%s)", outFileName);
			exit(1);
		}
	} else {
		outfile = stdout;
	}


	yyparse();

	if (!rules) {
		fprintf(stderr, "ERROR: No rules present\n");
		exit(1);
	}

	findChainRules();
	findAllPairs();
	doGrammarNts();
	build();

	debug(debugTables, foreachList((ListFn) dumpOperator_l, operators_));
	debug(debugTables, printf("---final set of states ---\n"));
	debug(debugTables, dumpMapping(globalMap));


	startBurm();
	makeNts();
	if (simpleTables) {
		makeSimple();
	} else {
		makePlanks();
	}

	startOptional();
	makeLabel();
	makeKids();

	if (internals) {
		makeChild();
		makeOpLabel();
		makeStateLabel();
	}
	endOptional();

	makeOperatorVector();
	makeNonterminals();
	if (internals) {
		makeOperators();
		makeStringArray();
		makeRuleDescArray();
		makeCostArray();
		makeDeltaCostArray();
		makeStateStringArray();
		makeNonterminalArray();
		/*
		makeLHSmap();
		*/
	}
	makeClosureArray();

	if (diagnostics) {
		reportDiagnostics();
	}

	yypurge();
	exit(0);
}

#if !(HAVE_STRING_H && HAVE_STRDUP)
static char *
strdup (const char *s) {

    int   len = strlen (s);
    char *ret = malloc (len + 1);

    if (!ret)
        return NULL;

    for (; len >= 0; len--)
        ret[len] = s[len];

    return ret;
}
#endif
