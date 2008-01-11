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

#include	<mx_config.h>
#include	<stdio.h>
/* not available on win32, not needed on linux, test on Sun/Irix/aix
#include 	<pwd.h>
  */
#include 	<ctype.h>
#include	"Mx.h"
#include	"MxFcnDef.h"

#define TEXMODE		(textmode==M_TEX)
#define TEXIMODE (textmode==M_TEXI)
#define WWWMODE		(textmode==M_WWW )
#define Newline		if WWWMODE { PrCmd("<br>"); } else if TEXMODE { PrCmd("\n"); } else if TEXIMODE {PrCmd("\n");} else { PrCmd(".LP\n"); }

extern int pr_env;
extern int opt_hide;
extern char *texDocStyle;
extern int codeline;

void FormSubSec(char *);
void PrCodeDisplay(Def *, char *);


char *mx_title = 0;
char *mx_author = 0;
char *mx_version = 0;
char *mx_date = 0;
char filename[200];

void
GenForm(void)
{
	Def *d;
	CmdCode dirbak = Nop;
	int env;
	char *getlogin();
	char *dstbak = NULL;
	extern char bibtexfile[256];

	for (d = defs; d < defs + ndef && (d->d_dir != Bfile); dirbak = d->d_dir, d++) ;
	for (; d < defs + ndef; dirbak = d->d_dir, d++) {
		codeline = d->d_line;
	      again:switch (d->d_dir) {
		case Title:
		case Author:
		case Version:
		case Date:
			break;
		case Bfile:
			IoWriteFile(d->d_cmd, d->d_dir);
			PrPrelude(d->d_cmd);
			strncpy(filename, d->d_cmd, sizeof(filename));
			FormTitle();
			break;
		case Efile:
			PrIndex();
			PrPostlude();
			if (*bibtexfile) {
				PrCmd("<hr size=5 noshade><br><br>");
				bib_print();
			}
			UpdateFiles();
			break;
		case Ofile:
			snprintf(filename, sizeof(filename), "%s.", d->d_cmd);
			break;
		case Module:
			FormMod(d->d_cmd, d->d_mod);
			PrEnv(E_TEXT);
			FormBlk(d);
			break;
		case Section:
			FormSec(d->d_cmd, d->d_mod, d->d_sec);
			PrEnv(E_TEXT);
			FormBlk(d);
			break;
		case Subsection:
			FormSubSec(d->d_cmd);
			PrEnv(E_TEXT);
			FormBlk(d);
			break;
		case Paragraph:
			if (dirbak >= Qtex)
				PrRule(0);
			FormPar(d->d_cmd);
			PrEnv(E_TEXT);
			FormBlk(d);
			break;
		case Ifdef:
		case Ifndef:
		case Endif:
			if (dirbak >= Qtex)
				PrRule(0);
			env = pr_env;
			PrEnv(E_TEXT);
			FormIf(d);
			Newline;
			PrEnv(env);
			break;
		case Continue:
			if (dirbak >= Qtex)
				PrRule(0);
			FormBlk(d);
			Newline;
			break;
		case Qtexi:
		case Qtex:
			if (dirbak >= Qtex)
				PrRule(0);
			if WWWMODE {
				if (!Hide())
					latex2html(d->d_blk, dirbak != Qtex, d[1].d_dir != Qtex);
			} else {
				/* was: 
				   PrCmd(d->d_blk);
				   Newline;
				   seems incorrect, as @[ @$ and the like still should be processed 
				 */
				env = pr_env;
				PrEnv(E_CMD);
				FormBlk(d);
				PrEnv(env);
			}
			break;
		case Mxmacro:
			PrEnv(E_TEXT);
			PrRule(0);
			if (WWWMODE) {
				int env = pr_env;

				PrCmd("<a name=\"");
				PrCmd(d->d_cmd);
				PrCmd("\"></a><b>");
				PrCmd(d->d_cmd);
				PrCmd(" ::=</b>");
				PrEnv(env);
			} else if (TEXIMODE) {
				PrCode(d->d_cmd);
				PrCode(" ::=\n");
				if (!Hide())
					IndexEntry(IFRAG, d->d_cmd, d->d_mod, d->d_sec);
				PrEnv(E_CODE);
				FormBlk(d);
				PrEnv(E_TEXT);
				break;
			} else {
				PrCode(d->d_cmd);
				PrCode(" ::=\n");
			}
			if (!Hide())
				IndexEntry(IFRAG, d->d_cmd, d->d_mod, d->d_sec);
			PrEnv(E_CODE);
			FormBlk(d);
			PrEnv(E_TEXT);
			PrRule(0);
			break;
		case Index0:
		case Index1:
		case Index2:
		case Index3:
		case Index4:
		case Index5:
		case Index6:
		case Index7:
		case Index8:
		case Index9:
			break;
		case InHide:
			HideOn();
			if (dirbak >= Qtex) {
				d->d_dir = dirbak;	/* dirbak=0; */
				goto again;
			}
			break;
		case OutHide:
			HideOff();
			if (dirbak >= Qtex) {
				d->d_dir = dirbak;	/* dirbak=0; */
				goto again;
			}
			break;
		case Comment:
			break;
		default:
			if (dirbak == d->d_dir && dstbak && (dstbak == d->d_file))
				PrCodeDisplay(d, 0);
			else
				switch (d->d_dir) {
				case Qcode:
					PrCodeDisplay(d, "c");
					break;
				case Cdef:
					PrCodeDisplay(d, "h");
					break;
				case Pspec:
					PrCodeDisplay(d, "spec");
					break;
				case CCsrc:
					PrCodeDisplay(d, "cc");
					break;
				case Csrc:
					PrCodeDisplay(d, "c");
					break;
				case Clex:
					PrCodeDisplay(d, "lex");
					break;
				case Cyacc:
					PrCodeDisplay(d, "yacc");
					break;
				case MALcode:
					PrCodeDisplay(d, "mal");
					break;
				case MILcode:
					PrCodeDisplay(d, "mil");
					break;
				case Monet:
					PrCodeDisplay(d, "m");
					break;
				case OQLspec:
					PrCodeDisplay(d, "oql");
					break;
				case ODLspec:
					PrCodeDisplay(d, "odl");
					break;
				case Prolog:
					PrCodeDisplay(d, "plg");
					break;
				case Haskell:
					PrCodeDisplay(d, "hs");
					break;
				case SQL:
					PrCodeDisplay(d, "sql");
					break;
				case Qnap:
					PrCodeDisplay(d, "qnp");
					break;
				case Tcl:
					PrCodeDisplay(d, "tcl");
					break;
				case Java:
					PrCodeDisplay(d, "java");
					break;
				case Pimpl:
					PrCodeDisplay(d, "impl");
					break;
				case ProC:
					PrCodeDisplay(d, "pc");
					break;
				case Shell:
					PrCodeDisplay(d, "");
					break;
				case fGrammar:
					PrCodeDisplay(d, "fgr");
					break;
				case Macro:
					PrCodeDisplay(d, "mcr");
					break;
				case HTML:
					PrCodeDisplay(d, "html");
					break;
				case XML:
					PrCodeDisplay(d, "xml");
					break;
				case DTD:
					PrCodeDisplay(d, "dtd");
					break;
				case XSL:
					PrCodeDisplay(d, "xsl");
					break;
				case Config:
					PrCodeDisplay(d, "cfg");
					break;
				case Swig:
					PrCodeDisplay(d, "i");
					break;
				case CCyacc:
					PrCodeDisplay(d, "yy");
					break;
				case CClex:
					PrCodeDisplay(d, "ll");
					break;
				case BibTeX:	/* writes out a .bib file;
						   NOTE: this is unrelated to the magic *bibtexfile that seems
						   only of importance in Tex2Html -- CHECK!!!
						 */
					break;
				default:
					Fatal("GenForm", "Non directive:%s [%s:%d]", dir2str(d->d_dir), d->d_file, d->d_line);
				}
		}
		dstbak = d->d_file;
	}
}

void
PrCodeDisplay(Def * d, char *tail)
{
	if (tail)
		PrRule(tail);
	PrEnv(E_CODE);
	FormBlk(d);
	PrEnv(E_TEXT);
}

void
FormIf(Def * d)
{
	if (TEXMODE || TEXIMODE)
		switch (d->d_dir) {
		case Ifdef:
/*
	PrCmd("\\framebox[\\linewidth]{ Variant ");
	PrStr(d->d_cmd);
	PrCmd("}\n");
 */
			break;

		case Ifndef:
			if (TEXMODE)
				PrCmd("\\framebox[\\linewidth]{ Variant not(");
			PrTxt(d->d_cmd);
			if (TEXMODE)
				PrCmd(")}\n");
			break;

		case Endif:
/*
	PrCmd("\\framebox[\\linewidth]{ End variant ");
	PrTxt(d->d_cmd);
	PrCmd("}\n");
 */
			break;
		default:
			/* shut up compiler */
			break;
		}
}

void
FormBlk(Def * d)
{
	Tok *t;
	int i;

	for (t = FstTok(d->d_blk); t; t = NxtTok(t)) {
		if (WWWMODE && (pr_env & E_CODE)) {
			PrCodeline();
		}
		switch (t->t_dir) {
		case T_BOLD:
		case T_ITALIC:
		case T_CODE:
			PrFontStr(t->t_str, t->t_dir);
			break;
		case T_POSCOND:
		case T_TEX:
			PrModeStr(t->t_str, t->t_dir);
			break;
		case T_INDEX:
			i = t->t_ext - '0';
			if (!Hide())
				IndexEntry(i, t->t_str, d->d_mod, d->d_sec);
			PrStr(t->t_str);
			break;
		case T_SGML:
		{
			int instr = 0, sgml = 0;
			char *p;

			/* sgml tags do not have width, correct this */
			for (p = t->t_str; *p; p++) {
				if (!instr && *p == '<') {
					sgml = 1;
				}
				if (!sgml)
					PrChr(*p);
				else if (WWWMODE)
					ofile_putc(*p);
				if (*p == '"') {
					instr = !instr;
				} else if (!instr && sgml) {
					if (*p == '>')
						sgml = 0;
				}
			}
			break;
		}
		case T_REFERENCE:
			FormSub(t->t_str);
			break;
		case T_BEGHIDE:
			/*if( opt_hide != NO_HIDE ) */
			HideOn();
			break;
		case T_HIDETEXT:
			HideText();
			break;
		case T_ENDHIDE:
			/* if( opt_hide != NO_HIDE ) */
			HideOff();
			break;
		case T_BEGARCHIVE:
			archived = 1;
			break;
		case T_ENDARCHIVE:
			archived = 0;
		case T_NONE:
			PrStr(t->t_str);
			break;
		default:
			Fatal("FormBlk", "Unknown directive:%c", t->t_dir);
		}
	}
}

void
FormTitle(void)
{
	if (bodymode == 0)
		return;
	if (!(mx_author && mx_title))
		return;

	if (*mx_title) {
		PrCmd(TEXMODE ? "\\title{" : TEXIMODE ? "@titlepage\n@title " : WWWMODE ? "<center><h1>" : ".TL\n\\s+2");
		PrText(mx_title);
		if (mx_version && *mx_version) {
			PrCmd(TEXMODE ? "\\\\\nVersion\\ " : TEXIMODE ? "Version " : WWWMODE ? "</h1><h2>" : "\n.sp\n\\fIVersion ");
			PrText(mx_version);
		}
		PrCmd(TEXMODE ? "}\n" : TEXIMODE ? "\n" : WWWMODE ? "</h2></center>\n" : "\\s-2\n");
	}
	if (*mx_author) {
		PrCmd(TEXMODE ? "\\author{" : TEXIMODE ? "@author " : WWWMODE ? "<center><h3>" : ".AU\n\\s+1");
		PrText(mx_author);
		PrCmd(TEXMODE ? "}\n" : TEXIMODE ? "\n" : WWWMODE ? "</h3></center>" : "\\s-1\n");
	}
	if (mx_date && *mx_date) {
		PrCmd(TEXMODE ? "\\date{" : TEXIMODE ? "" : WWWMODE ? "<center><h3>" : "\n\\fBDate\\fP");
		PrText(mx_date);
		PrCmd(TEXMODE ? "}\n" : TEXIMODE ? "\n" : WWWMODE ? "<p></font></h3></center>" : "\n");
	}
	PrCmd(TEXMODE ? "\\maketitle\n" : TEXIMODE ? "@end titlepage\n" : "");

	if (textmode == M_MS)
		PrCmd(".sp 2\n");
	if (texDocStyle == (char *) 0)
		PrCont();
}

void
FormSub(char *str)
{
	Def *d;
	char **argv = MkArgv(str);
	int env = pr_env;
	char *p = (char *) strchr(str, '(');

	if (textmode == M_MS)
		PrCmd("\\fI");

	if ((d = GetDef(argv[0])) != 0)
		if (p)
			*p = 0;

	if (WWWMODE) {
		if (env == E_CODE) {
			PrCodeline();
			pr_env = E_CMD;
		}
		if (d) {
			PrCmd("<a href=\"#");
			PrCmd(d->d_cmd);
			PrCmd("\">");
		}
		PrCmd("<i>");
		PrStr(str);

		PrCmd("</i>");
		if (d)
			PrCmd("</a>");
	} else {
		PrStr(str);

		PrRef(d ? d->d_mod : 0, d ? d->d_sec : 0);
	}
	if (d && p) {
		char *backup = d->d_blk;

		*p = '(';
		d->d_blk = p;
		FormBlk(d);
		d->d_blk = backup;
	}
	if (textmode == M_MS)
		PrCmd("\\fI");
	else if (WWWMODE && (env == E_CODE))
		pr_env = E_CODE;
	PrEnv(env);
}

int wwwmod = 0, wwwsec = 0, wwwpar = 0;

/* handling a form module */
void
FormMod(char *str, int mod)
{
	mx_title = str;

	if (TEXMODE || TEXIMODE)
		FormHeader();
	if (TEXMODE) {
		if (bodymode == 0)
			PrCmd("\\vfill\\clearpage");
		PrCmd("\\section{");
	} else if TEXIMODE {
		PrCmd("\n@chapter ");
	} else if WWWMODE {
		if (bodymode == 0)
			PrCmd("<hr size=1 noshade><hr size=1 noshade><br><br>");
		PrCmd("<a name=\"mod_");
		PrNum(wwwmod = mod);
		PrCmd("_0_0\"></a>\n<h2>");
		if (bodymode == 0) {
			PrNum(mod);
			PrChr(' ');
		}
		wwwpar = 1;
	} else {
		PrCmd(".ps+2\n.SH\n");
		PrNum(mod);
		PrStr(" ");
	}
	PrEnv(E_TEXT);
	PrStr(str);

	if TEXMODE {
		PrCmd("}\n");
	} else if TEXIMODE {
		PrCmd("\n");
	} else if WWWMODE {
		PrCmd("</h2>\n");
	} else
		PrCmd("\n.ps -2\n.LP\n");
}

void
FormSec(char *str, int mod, int sec)
{
	if TEXMODE {
		PrCmd("\\subsection{");
	} else if TEXIMODE {
		PrCmd("\n@section ");
	} else if WWWMODE {
		if (bodymode == 0)
			PrCmd("<hr size=1 noshade>");
		PrCmd("<br><a name=\"mod_");
		PrNum(wwwmod = mod);
		PrChr('_');
		PrNum(wwwsec = sec);
		PrCmd("_0\"></a>\n<h4>");
		wwwpar = 1;
		if (bodymode == 0) {
			PrNum(mod);
			PrChr('.');
			PrNum(sec);
			PrChr(' ');
		}
	} else {
		PrCmd(".SH\n");
		PrNum(mod);
		PrChr('.');
		PrNum(sec);
		PrChr(' ');
	}
	PrEnv(E_TEXT);
	PrStr(str);

	if TEXMODE {
		PrCmd("}\n");
	} else if TEXIMODE {
		PrCmd("\n");
	} else if WWWMODE {
		PrCmd("</h4>");
	} else
		PrCmd("\n.LP\n");
}

void
FormSubSec(char *str)
{
	if (str &&(strlen(str) > 0)) {
		if TEXMODE {
			PrCmd("\\subsubsection{");
		} else if TEXIMODE {
			PrCmd("\n@subsection ");
		} else if WWWMODE {
			PrCmd("<br><a name=\"mod_");
			PrNum(wwwmod);
			PrChr('_');
			PrNum(wwwsec);
			PrChr('_');
			PrNum(wwwpar);
			PrCmd("\"></a>\n<h5><b>");
			if (bodymode == 0) {
				PrNum(wwwmod);
				PrChr('.');
				PrNum(wwwsec);
				PrChr('.');
				PrNum(wwwpar++);
				PrChr(' ');
			}
		} else {
			PrCmd("\n.LP\n");
		}
		PrEnv(E_TEXT);
		PrStr(str);

		if TEXMODE {
			PrCmd("}\n");
		} else if TEXIMODE {
			PrCmd("\n");
		} else if WWWMODE {
			PrCmd("</b></h5>\n");
		} else {
			PrCmd("\n.LP\n");
		}
	} else {
		if TEXMODE {
			if (bodymode == 0)
				PrCmd("\\smallskip\n\\noindent");
		} else if TEXIMODE {
			PrCmd("\n");
		} else if WWWMODE {
			PrCmd("<p>");
		} else {
			PrCmd("\n.LP\n");
		}
	}
}

void
FormPar(char *str)
{
	if (str &&(strlen(str) > 0)) {
		if TEXMODE {
			PrCmd("\\paragraph{");
		} else if TEXIMODE {
			PrCmd("\n");
		} else if WWWMODE {
			PrCmd("<br><h5>");
		} else {
			PrCmd("\n.LP\n");
		}
		PrEnv(E_TEXT);
		PrStr(str);

		if TEXMODE {
			PrCmd("}\n");
		} else if TEXIMODE {
			PrCmd("\n");
		} else if WWWMODE {
			PrCmd("</h5>\n");
		} else {
			PrCmd("\n.LP\n");
		}
	} else {
		if TEXMODE {
			if (bodymode == 0)
				PrCmd("\\smallskip\n\\noindent");
		} else if TEXIMODE {
			PrCmd("\n");
		} else if WWWMODE {
			PrCmd("<p>");
		} else {
			PrCmd("\n.LP\n");
		}
	}
}

void
FormHeader(void)
{
	extern char *texDocStyle;

	if TEXIMODE
		return;
	if TEXMODE
		if (texDocStyle != NULL)
			return;

	if TEXMODE
		PrCmd("\\markboth{ \\ \\ ");
	else if TEXIMODE
		PrCmd("@settitle ");
	else if WWWMODE
		PrCmd("<p><h1 align=center>");
	else
		PrCmd(".TL\n");

	PrText(mx_title);

	if TEXMODE
		PrCmd("\\hfill ");
	else if TEXIMODE
		PrCmd("\n@author ");
	else if WWWMODE
		PrCmd("</h1>\n\n<h2 align=center>");
	else
		PrCmd(".AU\n");

	PrText(mx_author);

	if TEXMODE
		PrCmd("}{\\ \\ ");
	else if TEXIMODE
		PrCmd("\n@c ");
	else if WWWMODE
		PrCmd("</h2>\n\n<h3 align=center>");
	else
		PrCmd("\\sp 2\n\\fBVersion \\fP");

	PrText(mx_version);

	if TEXMODE
		PrCmd("\\hfill ");
	else if WWWMODE
		PrCmd("</h3>\n\n<h4 align=center>");
	else
		PrCmd("\n");

	PrText(mx_date);

	if TEXMODE
		PrCmd("}\n");
	else if TEXIMODE
		PrCmd("\n");
	else if WWWMODE
		PrCmd("</h4>\n\n");
}
