/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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

#include	<monetdb_config.h>
#include	<stdio.h>
/* not available on win32, not needed on linux, test on Sun/Irix/aix
#include 	<pwd.h>
  */
#include 	<ctype.h>
#include	"Mx.h"
#include	"MxFcnDef.h"

#define Newline		if TEXIMODE {PrCmd("\n");} 

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

	for (d = defs; d < defs + ndef && (d->d_dir != Bfile); dirbak = d->d_dir, d++)
		;
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
			PrPostlude();
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
			if (dirbak >= Qcode)
				PrRule(0);
			FormPar(d->d_cmd);
			PrEnv(E_TEXT);
			FormBlk(d);
			break;
		case Ifdef:
		case Ifndef:
		case Endif:
			if (dirbak >= Qcode)
				PrRule(0);
			env = pr_env;
			PrEnv(E_TEXT);
			FormIf(d);
			Newline;
			PrEnv(env);
			break;
		case Continue:
			if (dirbak >= Qcode)
				PrRule(0);
			FormBlk(d);
			Newline;
			break;
		case Mxmacro:
			PrEnv(E_TEXT);
			PrRule(0);
			PrCode(d->d_cmd);
			PrCode(" ::=\n");
			PrEnv(E_CODE);
			FormBlk(d);
			PrEnv(E_TEXT);
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
			if (dirbak >= Qcode) {
				d->d_dir = dirbak;	/* dirbak=0; */
				goto again;
			}
			break;
		case OutHide:
			HideOff();
			if (dirbak >= Qcode) {
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
				case CCyacc:
					PrCodeDisplay(d, "yy");
					break;
				case CClex:
					PrCodeDisplay(d, "ll");
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
	switch (d->d_dir) {
	case Ifdef:
		break;
	case Ifndef:
		PrTxt(d->d_cmd);
		break;
	case Endif:
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

	for (t = FstTok(d->d_blk); t; t = NxtTok(t)) {
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
	if (bodymode)
		return;

	if (!(mx_author && mx_title))
		return;
	PrCmd("@titlepage\n");
	if (*mx_title) {
		PrCmd("@title ");
		PrText(mx_title);
		if (mx_version && *mx_version) {
			PrCmd("Version ");
			PrText(mx_version);
		}
		PrCmd("\n");
	}
	if (*mx_author) {
		PrCmd("@author ");
		PrText(mx_author);
		PrCmd("\n");
	}
	if (mx_date && *mx_date) {
		PrText(mx_date);
		PrCmd("\n");
	}
	PrCmd("@end titlepage\n");
}

void
FormSub(char *str)
{
	Def *d;
	char **argv = MkArgv(str);
	int env = pr_env;
	char *p = (char *) strchr(str, '(');

	if ((d = GetDef(argv[0])) != 0)
		if (p)
			*p = 0;

	PrStr(str);
	PrRef(d ? d->d_mod : 0, d ? d->d_sec : 0);

	if (d && p) {
		char *backup = d->d_blk;

		*p = '(';
		d->d_blk = p;
		FormBlk(d);
		d->d_blk = backup;
	}
	PrEnv(env);
}

/* handling a form module */
void
FormMod(char *str, int mod)
{
	(void) mod;
	mx_title = str;

	FormHeader();
	PrCmd("\n@chapter ");
	PrEnv(E_TEXT);
	PrStr(str);
	PrCmd("\n");
}

void
FormSec(char *str, int mod, int sec)
{
	(void) mod;
	(void) sec;
	PrCmd("\n@section ");

	PrEnv(E_TEXT);
	PrStr(str);

	PrCmd("\n");
}

void
FormSubSec(char *str)
{
	if (str &&(strlen(str) > 0)) {
		PrCmd("\n@subsection ");
		PrEnv(E_TEXT);
		PrStr(str);
		PrCmd("\n");
	} else {
		PrCmd("\n");
	}
}

void
FormPar(char *str)
{
	if (str &&(strlen(str) > 0)) {
		PrCmd("\n");
		PrEnv(E_TEXT);
		PrStr(str);
		PrCmd("\n");
	} else {
		PrCmd("\n");
	}
}

void
FormHeader(void)
{
	/* disabled ?? */
	if TEXIMODE
		return;
	PrText(mx_title);
	PrCmd("\n@author ");
	PrText(mx_author);
	PrCmd("\n@c ");
	PrText(mx_version);
	PrCmd("\n");
	PrText(mx_date);
	PrCmd("\n");
}
