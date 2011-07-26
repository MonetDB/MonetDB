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

#include	<monetdb_config.h>
#include	<stdio.h>
#include	<stdlib.h>
#include	<ctype.h>
#include	"Mx.h"
#include	"MxFcnDef.h"

void
GenCode(void)
{
	Def *d;
	char *fname = NULL;
	CmdCode bak = Nop;

	for (d = defs; d < defs + ndef; d++) {
		mx_file = d->d_file;
		mx_line = d->d_line;
	  again:
		switch (d->d_dir) {
		case Bfile:
		case Ofile:{
			char *s;

			fname = d->d_cmd;
			for (s = fname; *s && !isspace((int) (*s)); s++)
				;
			if (isspace((int) (*s)))
				*s = 0;
			if (s == fname) {
				Error("File name missing. %d", d->d_line);
			}
		}
			break;
		case Efile:
			UpdateFiles();
			break;
		case Title:
		case Author:
		case Version:
		case Date:
			break;
		case Module:
		case Section:
		case Ifdef:
		case Ifndef:
		case Endif:
		case Subsection:
		case Paragraph:
		case Continue:
		case Qcode:
			break;
		case Cdef:
		case Csrc:
		case Clex:
		case Cyacc:
		case Prolog:
		case Haskell:
		case OQLspec:
		case ODLspec:
		case SQL:
		case HTML:
		case MALcode:
		case Qnap:
		case Pspec:
		case ProC:
		case Shell:
		case Pimpl:
		case Java:
		case fGrammar:
		case Macro:
		case XML:
		case DTD:
		case XSL:
		case Config:
		case CCyacc:
		case CClex:
			if (!extract(d->d_dir))
				break;
			IoWriteFile(fname, d->d_dir);
			mode = d->d_dir;
			CodeBlk(d->d_blk);
			break;
		case Mxmacro:
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
			if (bak >= Qcode) {
				d->d_dir = bak;
				goto again;
			}
			break;
		case OutHide:
			HideOff();
			if (bak >= Qcode) {
				d->d_dir = bak;
				goto again;
			}
			break;
		case Comment:
			WriteComment(fname, d->d_blk);
			break;
		default:
			Fatal("GenCode", "Unknown directive:%c", d->d_dir);
		}
		bak = d->d_dir;
	}
}

char *
Strndup(const char *src, size_t n)
{
	char *dst = (char *) Malloc(n + 1);

	strncpy(dst, src, n);
	dst[n] = '\0';
	return dst;
}

Tok *
solveCond(Tok * t)
{
	char *arg[2] = {0, 0};
	int inside = 0;
	int ok = 1;
	int cmp = 0;
	char *s = t->t_str;

	for (; (t->t_str[0] && ok); t->t_str++) {
		switch (t->t_str[0]) {
		case '"':{
			inside ^= 1;
		};
			break;
		case '=':{
			if (!inside) {
				if (!cmp)
					arg[cmp] = Strndup(s, (t->t_str - s));
				s = t->t_str + 1;
				cmp = 1;
			}
		};
			break;
		case ':':{
			if (!inside) {
				t->t_str--;
				ok = 0;
			}
		};
			break;
		}
	}
	arg[cmp] = Strndup(s, (t->t_str - s));
	if (cmp)
		ok = !strcmp(arg[0], arg[1]);
	else
		ok = (arg[0][0] != '\0');
	if (ok)
		t->t_dir = *t->t_str;
	t->t_str++;
	Free(arg[0]);
	if (cmp)
		Free(arg[1]);
	DbTok(t);
	return t;
}

int _level = 0;

void
CodeBlk(char *blk)
{
	Tok *t;
	char *c;
	int cond = 0;

	_level++;

	CodeLine();

	for (t = FstTok(blk); t != (Tok *) NULL; t = NxtTok(t)) {
		cond = 0;
		switch (t->t_dir) {
		case T_INDEX:
			ofile_printf("%s", t->t_str);
			break;
		case T_NEGCOND:
			cond = 1;
		case T_POSCOND:
			t = solveCond(t);
			cond ^= (t->t_dir == T_REFERENCE);
			if (!cond)
				break;
		case T_REFERENCE:
			CodeSub(t->t_str);
			CodeLine();
			break;
		case T_SGML:
		case T_NONE:
			for (c = t->t_str; *c; c++)
				if (*c == '\n')
					mx_line++;
			ofile_printf("%s", t->t_str);
			break;
		case T_HIDETEXT:
			t = SkipTok(t, T_ENDHIDE);
			break;
		case T_BEGARCHIVE:
			t = SkipTok(t, T_ENDARCHIVE);
			CodeLine();
			break;
		case T_ENDARCHIVE:
		case T_BEGHIDE:
		case T_ENDHIDE:
			break;
		default:
			Fatal("CodeBlk", "Unknown directive:%c", t->t_dir);
		}
	}
	ofile_printf("\n");
	_level--;
}

void
CodeSub(char *call)
{
	Def *def;
	char *blk;
	char *file = mx_file;
	int line = mx_line;
	char **argv = MkArgv(call);

	def = GetDef(argv[0]);
	if (def) {
		blk = CodeSubBlk(def->d_blk, argv);
		mx_file = def->d_file;
		mx_line = def->d_line;
		if (blk)
			CodeBlk(blk);
	} else
		UnRef(argv[0]);

	argv = RmArgv(argv);
	mx_file = file;
	mx_line = line;
}

#define blk_size (1<<18)
/* #define blk_size 1024 */
static char blk[blk_size];

char *
CodeSubBlk(char *sub, char **argv)
{
	char *s;
	char *b;
	char *a;
	int pos = 0;

	s = sub;
	b = blk;
	if (!s)
		return s;
	while (*s) {
		if (s[0] == MARK && (s == sub || s[-1] != '\\') && '1' <= s[1] && s[1] <= '9') {
			/* argument @<digit> */
			s++;
			for (a = argv[*s - '0']; a && *a;) {
				if (pos++ == blk_size - 2)
					goto outofmem;
				*b++ = *a++;
			}
			s++;
		} else if (s[0] == MARK && (s == sub || s[-1] != '\\') && s[1] == '[' && isdigit((int) s[2])) {
			/* EXPANDED ARGUMENT LIST: @[<digit>+] */
			char *olds = s;
			int n = 0;
			s += 2;	/* skip @[ */
			n = 0;
			while (isdigit((int) *s))
				n = 10 * n + *s++ - '0';
			if (*s != ']' || n >= M_ARGS) {
				/* if @[<digit>+ not followed by ], just copy the whole thing */
				s = olds;
				goto copy;
			}
			if (n >= M_ARGS) {
				Error("No more than %d arguments allowed.", M_ARGS);
				exit(1);
			}
			if (n == 0) {
				Error("Arguments start counting at 1, not 0.");
				exit(1);
			}
			for (a = argv[n]; a && *a;) {
				if (pos++ == blk_size - 2)
					goto outofmem;
				*b++ = *a++;
			}
			s++;
			
		} else {
  copy:
			*b++ = *s++;
		}
		if (pos++ == blk_size - 2)
			goto outofmem;
	}
	*b = '\0';
	return StrDup(blk);

      outofmem:
	Error("Limit of %d characters per block reached.", blk_size);
	exit(1);
	return 0;

}

void
UnRef(char *ref)
{
	Error("Unresolved reference:%s", ref);
	if (mode & M_CODE && (textmode == M_TEXI))
		ofile_printf("/* Unresolved Reference :%s */", ref);
	/* should text for actual source format */
}

void
CodeLine(void)
{
	char *s;

	if (!noline) {
		switch (mode) {
		case Macro:
		case HTML:
		case Shell:
		case XML:
		case DTD:
		case XSL:
		case Cyacc:
		case Clex:
		case CCyacc:
		case CClex:
		case MALcode:
			break;

		case Haskell:
		case SQL:
			ofile_printf("\n-- %s:%d \n", mx_file, mx_line);
			break;

		case Prolog:
		case Java:
		case fGrammar:
			ofile_printf("\n/* %s:%d */\n", mx_file, mx_line);
			break;
		case Config:
			ofile_printf("\n# %s:%d \n", mx_file, mx_line);
			break;
		default:
			ofile_printf("\n#line %d \"", mx_line);
			for (s = mx_file; s[0]; s++) {
				ofile_putc(s[0]);
#ifdef NATIVE_WIN32
				if (s[0] == '\\' && s[1] != ' ')
					ofile_putc('\\');
#endif
			}
			ofile_putc('"');
			ofile_putc('\n');
			break;
		}
	} else {
		ofile_printf("\n");
	}
}
