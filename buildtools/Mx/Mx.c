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
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#include        <monetdb_config.h>
#include	<stdio.h>
#include	<ctype.h>
#ifdef HAVE_UNISTD_H
#include	<unistd.h>
#endif

#include	"Mx.h"
#include	"MxFcnDef.h"
#include	"disclaimer.h"

#include "mx_getopt.h"
#ifndef HAVE_GETOPT
#include "getopt.c"
#endif

unsigned int db_flag = 0x00;
int archived;			/* set for archived portions */
int mode = M_TEXT;
int opt_hide = NO_HIDE;
int textmode = M_TEXT;
int bodymode = 0;		/* all should be shown */
char *opt_code;
char *defHideText = 0;

int mx_err = 0;
char *mx_file = 0;
int mx_line = 0;
int mx_chp = 0, mx_mod = 0, mx_sec = 0;
int condSP = 0;
int codeline = 0;
int noline = 0;
int notouch = 0;
char *texDocStyle = 0;

static void
usage(void)
{
	Message("Usage: Mx <flags> <file>.mx");
	Message("\t-i\t\tProduce texi document (default)");
	Message("\t-c\t\tExtract code");
	Message("\t-R <dir>\tSet target directory to <dir>)");
	Message("\t-H <n>\t\tSet hide level to 'n' (-H0 default)");
	Message("\t-d\t\tProduce a draft document");
	Message("\t-x <extension>\tExtract <extension> labelled code");
	Message("\t-D <id>\t\tDefine macro 'id'");
	Message("\t-T <string>\tDefine default hide text <string>");
	Message("\t-l\t\tNo #line and alike statements");
	Message("\t-n\t\tNon changed files won't be touched");
	Message("\t-+\t\tTreat @c (C code) as @C (C++ code)");
}

int
main(int argc, char **argv)
{
	int i, k;

	if (argc == 1) {
		usage();
		exit(1);
	}
	InitDef();
	OutputDir(".");

/* Preprocess the arguments.
 */
	while ((i = getopt(argc, argv, "icC:x:Bdg:D:R:H:T:ln+")) != EOF) {
		switch (i) {
		case 'i':
			textmode = M_TEXI;
			break;
		case 'c':
			mode = M_CODE;
			break;
		case 'C':
			disclaimer = 1;
			disclaimerfile = optarg;
			break;
		case 'x':	/* code can be extracted selectively */
			mode = M_CODE;
			addextension(optarg);
			break;
		case 'B':
			bodymode = 1;	/* use for inclusion */
			break;
		case 'd':
			mode = M_DRAFT;
			break;
		case 'g':
			sscanf(optarg, "%x", &db_flag);
			break;
		case 'D':{
			Def *d;

			d = NwDef(Mxmacro, 0, 0, 0, mx_file);
			d->d_cmd = StrDup(optarg);
			d->d_blk = NULL;
			break;
		}
		case 'R':
			OutputDir(optarg);
			break;
		case 'H':
			sscanf(optarg, "%d", &opt_hide);
			break;
		case 'T':
			defHideText = optarg;
			break;
		case 'l':
			noline = 1;
			break;
		case 'n':
			notouch = 1;
			break;
		case '+':
			k = 0;
			do {
				if (str2dir[k].code == Csrc)
					str2dir[k].ext = MX_CXX_SUFFIX;
			} while (str2dir[k++].code != Nop);
			break;
		default:
			Error("Unknown flag:%c", i);
			usage();
			exit(1);
		}
	}


	for (i = optind; i < argc; i++)
		MakeDefs(argv[i]);

	if (mode & M_CODE)
		GenCode();
	if (mode & M_DRAFT)
		GenForm();

	exit(mx_err ? 1 : 0);
	return 1;
}

Directive str2dir[] = {
	{"", Continue, "",},
	{"0", Index0, "",},
	{"1", Index1, "",},
	{"2", Index2, "",},
	{"3", Index3, "",},
	{"4", Index4, "",},
	{"5", Index5, "",},
	{"6", Index6, "",},
	{"7", Index7, "",},
	{"8", Index8, "",},
	{"9", Index9, "",},
	{"f", Ofile, "",},
	{"=", Mxmacro, "",},
	{"ifdef", Ifdef, "",},
	{"else", Ifndef, "",},
	{"endif", Endif, "",},
	{"a", Author, "",},
	{"v", Version, "",},
	{"t", Title, "",},
	{"d", Date, "",},
	{"*", Module, "",},
	{"+", Section, "",},
	{"-", Subsection, "",},
	{".", Paragraph, "",},
	{"C", CCsrc, MX_CXX_SUFFIX,},
	{"i", Pimpl, "impl",},
	{"s", Pspec, "spec",},
	{"h", Cdef, "h",},
	{"c", Csrc, "c",},
	{"y", Cyacc, "y",},
	{"l", Clex, "l",},
	{"odl", ODLspec, "odl",},
	{"oql", OQLspec, "oql",},
	{"sql", SQL, "sql",},
	{"p", Prolog, "pl",},
	{"hs", Haskell, "hs",},
	{"m", Monet, "m",},
	{"mal", MALcode, "mal",},
	{"mil", MILcode, "mil",},
	{"w", HTML, "www",},
	{"java", Java, "java",},
	{"Qnap", Qnap, "qnp",},
	{"pc", ProC, "pc",},
	{"sh", Shell, "",},
	{"fgr", fGrammar, "fgr",},
	{"mcr", Macro, "mcr",},
	{"xml", XML, "xml",},
	{"dtd", DTD, "dtd",},
	{"xsl", XSL, "xsl",},
	{"cfg", Config, "cfg",},
	{"swig", Swig, "i",},
	{"Y", CCyacc, "yy",},
	{"L", CClex, "ll",},
	{"{", InHide, "",},
	{"}", OutHide, "",},
	{"/", Comment, "",},
	{NULL, Nop, NULL,},
};

#define NUMEXTENS (sizeof(str2dir)/sizeof(Directive))

int extcnt = 0;
CmdCode extens[NUMEXTENS];
void
addextension(char *ext)
{
	extens[extcnt] = lookup(ext);
	if (extens[extcnt] > 0) {
		extcnt++;
	} else
		fprintf(stderr, "Invalid extension %s\n", ext);
}

int
extract(CmdCode dir)
{
	int i = 0;

	if (extcnt == 0)
		return 1;
	for (; i != extcnt; i++)
		if (extens[i] == dir)
			return 1;
	return 0;
}

struct comments {
	CmdCode code;
	char *comment_pre;	/* start comment block */
	char *comment_start;	/* start comment line (trailing space added) */
	char *comment_post;	/* end comment block */
};

static struct comments comments[] = {
	{ CCsrc, NULL, "//", NULL},
	{ Cdef, "/*", " *", " */"},
	{ Csrc, "/*", " *", " */"},
	{ Cyacc, "/*", " *", " */"},
	{ Clex, "/*", " *", " */"},
	{ SQL, NULL, "--", NULL},
	{ MALcode, NULL, "#", NULL},
	{ MILcode, NULL, "#", NULL},
	{ HTML, "<!--", NULL, "-->"},
	{ Java, NULL, "//", NULL},
	{ Shell, NULL, "#", NULL},
	{ CCyacc, "/*", " *", " */"},
	{ CClex, "/*", " *", " */"},
	{ Nop, NULL, NULL, NULL}, /* sentinel */
};

void
WriteComment(char *fname, char *blk)
{
	int i, j;
	struct comments *c;
	char *s1, *s2;

	/* for each type of file that we're extracting, if we know how
	   comments are written, and if that particular file type is
	   supposed to be generated from the source, write the comment
	   in the appropriate way. */
	for (i = 0; i < extcnt; i++) {
		for (c = comments; c->code != Nop; c++) {
			if (c->code == extens[i]) {
				for (j = 0; j < ndef; j++) {
					if (defs[j].d_dir == extens[i]) {
						s1 = blk;
						IoWriteFile(fname, extens[i]);
						if (c->comment_pre)
							ofile_printf("%s\n", c->comment_pre);
						while ((s2 = strchr(s1, '\n')) != NULL) {
							*s2 = '\0';
							if (c->comment_start) {
								ofile_printf("%s", c->comment_start);
								if (*s1)
									ofile_printf(" ");
							}
							ofile_printf("%s\n", s1);
							*s2++ = '\n';
							s1 = s2;
						}
						if (c->comment_post)
							ofile_printf("%s\n", c->comment_post);
						break;
					}
				}
				break;
			}
		}
	}
}
