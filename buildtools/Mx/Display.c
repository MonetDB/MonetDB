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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

#include <mx_config.h>
#include <stdio.h>
#include <string.h>

#include "Mx.h"
#include "MxFcnDef.h"
#include "disclaimer.h"

extern int pr_env;

#define TEXMODE (textmode==M_TEX)
#define TEXIMODE (textmode==M_TEXI)
#define WWWMODE (textmode==M_WWW )
/* Printing modes
 *	Formatted text	Plain/Math.
 *		Math is used for special symbols.
 *	Formatted code
 */

void
PrFontStr(char *s, char c)
{
	int env = pr_env;

	if (Hide())
		return;

	switch (c) {
	case T_BOLD:
		PrCmd(TEXMODE ? "{\\bf " : TEXIMODE ? "@b{" : WWWMODE ? "<B>" : "\\fB");
		PrText(s);
		PrCmd(TEXMODE ? "}" : TEXIMODE ? "}" : WWWMODE ? "</B>" : "\\fP");
		break;
	case T_ITALIC:
		PrCmd(TEXMODE ? "{\\it " : TEXIMODE ? "@i{" : WWWMODE ? "<I>" : "\\fI");
		PrText(s);
		PrCmd(TEXMODE ? "}" : TEXIMODE ? "}" : WWWMODE ? "</I>" : "\\fP");
		break;
	case T_CODE:
		PrCmd(TEXMODE ? "{\\tt " : TEXIMODE ? "@i{" : WWWMODE ? "<TT>" : "\\fB");
		PrText(s);
		PrCmd(TEXMODE ? "}" : TEXIMODE ? "}" : WWWMODE ? "</TT>" : "\\fP");
		break;
	}
	PrEnv(env);
}

void
PrModeStr(char *s, char c)
{
	int env = pr_env;

	if (Hide())
		return;

	switch (c) {
	case T_CODE:
		PrCode(s);
		break;
	case T_TEX:
		PrCmd(s);
		break;
	}
	/* PrEnv(E_TEXT); */
	PrEnv(env);
}

void
PrCmd(char *s)
{
	extern int pr_pos;

	if (Hide())
		return;
	if (!s)
		return;

	PrEnv(E_CMD);
	ofile_puts(s);
	pr_pos += strlen(s);
}

void
PrText(char *s)
{
	if (Hide())
		return;
	if (!s)
		return;

	PrEnv(E_TEXT);
	PrStr(s);
}

void
PrCode(char *s)
{
	if (Hide())
		return;
	if (!s)
		return;

	PrEnv(E_CODE);
	PrStr(s);
}

char filename_body[200] = { 0 }, filename_index[200] = {
0};
int somethingPrinted = 1;

void
PrRule(char *tail)
{
	if (Hide())
		return;
	if TEXMODE {
		if (tail) {
			strcpy(strchr(filename, '.') + 1, tail);
			ofile_printf("\n\n");
			if (opt_column == 2) {
				if (bodymode == 0)
					ofile_printf("\\noindent\\rule{\\linewidth}{1pt}\\newline\\vspace{-10pt}\n");
			}
			if (bodymode == 0)
				ofile_printf("\\noindent\\makebox[\\linewidth][r]{\\small\\tt ");
			PrTxt(FileName(filename));
			if (bodymode == 0)
				ofile_printf("}\n\\noindent");
		} else if (opt_column == 2) {
			if (bodymode == 0)
				ofile_printf("\\vspace{-1em}\\noindent\\rule{\\linewidth}{1pt}\n");
		}
	} else if TEXIMODE {
		if (tail) {
			ofile_printf("\n\n");
		}
	} else if WWWMODE {
		if (somethingPrinted && bodymode == 0) {
			ofile_printf("\n<hr size=1 noshade>");
		}
		if (tail) {
			strcpy(strchr(filename, '.') + 1, tail);
			ofile_printf("<table width=\"100%%\"><tr>");
			ofile_printf("<td align=right valign=top>");
			ofile_printf("<font size=\"-1\"><i>%s", FileName(filename));
			ofile_printf("</i></font></td></tr></table>\n");
		}
	} else {
		if (somethingPrinted && bodymode == 0) {
			ofile_printf(".br\n.sp -0.5v\n");
			ofile_printf("\\l'\\n(.lu-\\n(.iu'\n");
			ofile_printf(".ps -1\n.vs -1\n.br\n");
			ofile_printf(".nf\n.na\n");
		}
		if (tail) {
			strcpy(strchr(filename, '.') + 1, tail);
			ofile_printf("\\h'\\n(.lu-\\w'\\fI%s\\fP'u'", filename);
			ofile_printf("\\v'-2u'");
			ofile_printf("\\fI%s\\fP", FileName(filename));
			ofile_printf("\\v'+2u'\\h'|0c'");
		}
	}
	somethingPrinted = 0;
}

extern char *bname;

static int preludeDone = 0;

void
PrPrelude(char *file)
{
	extern char *texDocStyle;
	char *s, *t, full[200];

	/* find out the full name in 'full', the basename in 's', end in 't' */
	strncpy(full, file, sizeof(full));
	for (s = full; s[1]; s++) ;
	while (s >= full && *s != DIR_SEP
#ifdef WIN32
	       && *s != '/'
#endif
		)
		s--;
	for (t = ++s; *t; t++)
		if (t[0] == '.' && t[1] == 'm' && t[2] == 'x' && !t[3])
			break;
	*t = 0;


	if (TEXIMODE && bodymode == 0)
		ofile_printf("\\input texinfo\n");
	if (TEXMODE) {
		if (bodymode == 0) {
			if (texDocStyle != NULL)
				ofile_printf("\\documentclass%s\n", texDocStyle);
			else {
				ofile_printf("\\documentclass[twoside,titlepage]{article}\n");
				ofile_printf("\\usepackage{epsf}\n");
				ofile_printf("\\pagestyle{myheadings}\n");
				ofile_printf("\\textheight= 9.5 in\n");
				ofile_printf("\\textwidth= 6.5 in\n");
				ofile_printf("\\topmargin=-0.5 in\n");
				ofile_printf("\\oddsidemargin=-0.25 in\n");
				ofile_printf("\\evensidemargin=-0.25 in\n");
				ofile_printf("\\parindent=0 in\n");
			}


			if (opt_column == 2) {
				if (TEXIMODE)
					ofile_printf("@iftex\n@tex\n\\columnsep=0.5 in\n");
				else
					ofile_printf("\\columnsep=0.5 in\n");
			}
			ofile_printf("\\newcommand{\\codesize}{\n");
			ofile_printf("  %s}\n", opt_column == 1 ? "\\small" : "\\footnotesize");
			ofile_printf("\\newcommand{\\eq}[1]{\n");
			ofile_printf("  ${\\ \\equiv\\ }$}\n");
			if (TEXIMODE)
				ofile_printf("@end tex\n@end iftex\n");
		}

/*
   Starting ...
 */
		if (bodymode == 0) {
			if (TEXIMODE)
				ofile_printf("@iftex\n@tex\n\\begin{document}\n@end tex\n@end iftex\n");
			else
				ofile_printf("\\begin{document}\n");
		}
		if (opt_column == 2) {
			if (TEXIMODE)
				ofile_printf("@iftex\n@tex\n\\twocolumn\n@end tex\n@end iftex\n");
			else
				ofile_printf(",\\twocolumn\n");
		}
	} else if WWWMODE {
		extern char *outputdir;

		/* install the extra HTML filenames */
		snprintf(filename_index, sizeof(filename_index),
			 "%s%c%s", outputdir, DIR_SEP, s);
		snprintf(filename_body, sizeof(filename_body),
			 "%s%c%s", outputdir, DIR_SEP, s);

		strncat(filename_index, ".index.html",
			sizeof(filename_index) - strlen(filename_index) - 1);
		ofile_index = fopen(filename_index, "w+");
		if (disclaimer)
			insertDisclaimer(ofile_index, filename_index);

		strncat(filename_body, ".body.html",
			sizeof(filename_body) - strlen(filename_body) - 1);
		ofile_body = fopen(filename_body, "w+");
		if (disclaimer)
			insertDisclaimer(ofile_body, filename_body);

		mx_out = 7;
		if (bodymode == 0) {
			ofile_printf("<HTML>\n<HEAD>\n");
			ofile_printf("<LINK REL=STYLESHEET HREF=/MonetDB.css>\n");
			mx_out = 4;
			ofile_printf("\n<BASE target=\"_parent\">\n");
			mx_out = 7;
			ofile_printf("<TITLE>Module %s.mx</TITLE>\n</HEAD>\n", s);
			mx_out = 1;
			ofile_printf("<FRAMESET border=1 ROWS=\"85%%,15%%\">\n");
			ofile_printf("\t<FRAME name=\"a\" src=\"%s.body.html\">\n", s);
			ofile_printf("\t<FRAME src=\"%s.index.html\">\n", s);
			ofile_printf("</FRAMESET>\n\n<NOFRAMES>\n");
			mx_out = 7;
			ofile_printf("<BODY bgcolor=\"#FFFFFF\" text=\"" text_color "\" vlink=\"" vlnk_color "\" link=\"" link_color "\">\n");
		}
		mx_out = 5;
	} else {
		if (opt_column == 2)
			ofile_printf(".2C\n");
	}
	PrEnv(E_CMD);
}

void
PrPostlude(void)
{
	PrEnv(E_CMD);
	if (TEXMODE) {
		if (bodymode == 0)
			ofile_printf("\\end{document}\n");
	} else if TEXIMODE {
		if (preludeDone)
			if (bodymode == 0)
				ofile_printf("@bye\n");
	} else if WWWMODE {
		mx_out = 7;
		if (bodymode == 0)
			ofile_printf("</BODY>\n</HTML>\n");
		mx_out = 5;
	}
	if (TEXIMODE && bodymode == 0)
		ofile_printf("@bye\n");
}
