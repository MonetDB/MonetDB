/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * The Original Code is the Monet Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2004 CWI.
 * All Rights Reserved.
 */

#include <monetdb_config.h>       
#include <stdio.h>
#include <string.h>

#include "Mx.h"
#include "MxFcnDef.h"
#include "disclaimer.h"

extern int pr_env;

#define TEXMODE (textmode==M_TEX)
#define WWWMODE (textmode==M_WWW )
/* Printing modes
 *	Formatted text	Plain/Math.
 *		Math is used for special symbols.
 *	Formatted code
 */

void	PrFontStr(char *s, char c)
{
        int env = pr_env;
	if( Hide() ) return;

	switch( c ){
	case T_BOLD:
		PrCmd(TEXMODE? "{\\bf " : WWWMODE? "<B>" : "\\fB");
		PrText(s);
		PrCmd(TEXMODE? "}":WWWMODE? "</B>" : "\\fP" );
		break;
	case T_ITALIC:
		PrCmd(TEXMODE? "{\\it " : WWWMODE? "<I>" : "\\fI");
		PrText(s);
		PrCmd(TEXMODE? "}" : WWWMODE? "</I>" : "\\fP");
		break;
	case T_CODE:
		PrCmd(TEXMODE? "{\\tt " : WWWMODE? "<TT>" : "\\fB");
		PrText(s);
		PrCmd(TEXMODE? "}" : WWWMODE? "</TT>" : "\\fP");
		break;
	}
	PrEnv(env);
}

void	PrModeStr(char *s, char c)
{
        int env = pr_env;
	if( Hide() ) return;

	switch( c ){
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

void	PrCmd(char *s)
{
	extern int pr_pos;
	if( Hide() ) return;
	if( !s ) return;

	PrEnv(E_CMD);
	ofile_puts(s);
        pr_pos += strlen(s);
}

void	PrText(char *s)
{
	if( Hide() ) return;
	if( !s ) return;

	PrEnv(E_TEXT);
	PrStr(s);
}

void	PrCode(char *s)
{
	if( Hide() ) return;
	if( !s ) return;

	PrEnv(E_CODE);
	PrStr(s);
}

char filename_body[200]={0}, filename_index[200]={0};
int somethingPrinted = 1;

void	PrRule(char *tail)
{
	if( Hide()) return;
	if TEXMODE{
	    if (tail){
		strcpy((char*)strchr(filename,'.')+1, tail);
		ofile_printf("\n\n");
		if (opt_column == 2) {
		    ofile_printf("\\noindent\\rule{\\linewidth}{1pt}\\newline\\vspace{-10pt}\n");
		}
		ofile_printf("\\noindent\\makebox[\\linewidth][r]{\\small\\tt ");
		PrTxt(FileName(filename));
		ofile_printf("}\n\\noindent");
	    } else if (opt_column == 2) {
		ofile_printf("\\vspace{-1em}\\noindent\\rule{\\linewidth}{1pt}\n");
	    }
	} else if WWWMODE {
	    if (somethingPrinted && bodymode==0) {
	    	ofile_printf("\n<hr size=1 noshade>");
	    }
	    if (tail) {
		strcpy((char*)strchr(filename,'.')+1, tail);
		ofile_printf("<table width=\"100%%\"><tr>");
		ofile_printf("<td align=right valign=top>");
		ofile_printf("<font size=\"-1\"><i>%s", FileName(filename));
		ofile_printf("</i></font></td></tr></table>\n");
	    }
	} else {
	    if (somethingPrinted && bodymode==0) {
	    	ofile_printf(".br\n.sp -0.5v\n");
	    	ofile_printf("\\l'\\n(.lu-\\n(.iu'\n");
	    	ofile_printf(".ps -1\n.vs -1\n.br\n");
	    	ofile_printf(".nf\n.na\n");
	    }
	    if (tail){
		strcpy((char*)strchr(filename,'.')+1, tail);
		ofile_printf("\\h'\\n(.lu-\\w'\\fI%s\\fP'u'", filename);
		ofile_printf("\\v'-2u'");
		ofile_printf("\\fI%s\\fP", FileName(filename));
		ofile_printf("\\v'+2u'\\h'|0c'");
	    }
	}
	somethingPrinted = 0;
}

extern char *bname;

void	PrPrelude(char *file)
{
    extern char *texDocStyle;
    char *s,*t,full[200];

    /* find out the full name in 'full', the basename in 's', end in 't' */ 
    strcpy(full, file);
    for(s=full; s[1]; s++);
    while(s>=full && *s!=DIR_SEP) s--;
    for(t=++s; *t; t++) 
        if (t[0] == '.' && t[1] == 'm' && t[2] == 'x' && !t[3]) break; 
    *t = 0;


	if ( TEXMODE ){
	   if( bodymode==0){
	    if(texDocStyle != NULL)
		ofile_printf("\\documentclass%s\n",texDocStyle);
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


	    if( opt_column == 2 ) ofile_printf("\\columnsep=0.5 in\n");
	    ofile_printf("\\newcommand{\\codesize}{\n");
	    ofile_printf("  %s}\n", opt_column==1? "\\small": "\\footnotesize");
	    ofile_printf("\\newcommand{\\eq}[1]{\n");
	    ofile_printf("  ${\\ \\equiv\\ }$}\n");
	}

/*
   Starting ...
 */
	if( bodymode==0) ofile_printf("\\begin{document}\n");
	    if( opt_column == 2 ) ofile_printf(",\\twocolumn\n");
    } else if WWWMODE {
	    extern char* outputdir;
    	    /* install the extra HTML filenames */
	    sprintf(filename_index, "%s%c%s", outputdir, DIR_SEP, s);
	    sprintf(filename_body, "%s%c%s", outputdir, DIR_SEP, s);

	    strcat(filename_index, ".index.html");
	    ofile_index = fopen(filename_index, "w+");
            if (disclaimer) insertDisclaimer(ofile_index,filename_index);

	    strcat(filename_body, ".body.html");
	    ofile_body = fopen(filename_body, "w+");
            if (disclaimer) insertDisclaimer(ofile_body,filename_body);

	    mx_out = 7; 
	if( bodymode==0){
	    ofile_printf("<HTML>\n<HEAD>\n");
	    ofile_printf("<LINK REL=STYLESHEET HREF=/MonetDB.css>\n");
	    mx_out = 4;
	    ofile_printf("\n<BASE target=\"_parent\">\n");
	    mx_out = 7; 
	    ofile_printf("<TITLE>Module %s.mx</TITLE>\n</HEAD>\n",s);
	    mx_out = 1;
	    ofile_printf("<FRAMESET border=1 ROWS=\"85%%,15%%\">\n");
	    ofile_printf("\t<FRAME name=\"a\" src=\"%s.body.html\">\n",s);
	    ofile_printf("\t<FRAME src=\"%s.index.html\">\n",s);
	    ofile_printf("</FRAMESET>\n\n<NOFRAMES>\n");
	    mx_out = 7;
	    ofile_printf("<BODY bgcolor=\"#FFFFFF\" text=\"" text_color "\" vlink=\"" vlnk_color "\" link=\"" link_color "\">\n");
	}
	    mx_out = 5;
    } else {
	if( opt_column==2) ofile_printf(".2C\n");
    }
    PrEnv(E_CMD);
}

void	PrPostlude(void)
{
	PrEnv(E_CMD);
	if ( TEXMODE ) {
		if( bodymode==0)ofile_printf("\\end{document}\n");
	} else if WWWMODE {
	    mx_out = 7;
	    if( bodymode==0)ofile_printf("</BODY>\n</HTML>\n");
	    mx_out = 5;
	}
}


