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
#include        <string.h>
#include 	<time.h>
#include	"Mx.h"
#include	"MxFcnDef.h"

extern char *mx_title;
extern char *mx_date;
extern char *mx_version;
extern char *mx_author;

extern int condSP;

#define MAXIF   20
struct {
	int defined;
	char *macro;
} condStack[MAXIF];

Def *defs = 0;
int ndef = 0;


#define topCond() (condStack[condSP-1].defined)
#define topMacro() (condStack[condSP-1].macro)
#define pushCond(X,N) { condStack[condSP].defined = (X);\
			condStack[condSP++].macro = StrDup(N); }
#define toggle() {if(topCond())condStack[condSP-1].defined=0;\
                  else condStack[condSP-1].defined=1;}
#define popCond() {if(condSP) condSP--;else Fatal("DefDir","IFDEF error");}

int
allTrue(void)
{
	int i;

	for (i = 0; i < condSP; i++)
		if (!condStack[i].defined)
			return 0;
	return 1;
}



Def *
NwDef(CmdCode dir, int mod, int sec, int lino, char *file)
{
	Def *d;

	if (ndef == M_DEFS)
		Fatal("NwDef", "Too many definitions.");
	d = defs + ndef++;

	d->d_dir = dir;
	d->d_cmd = 0;
	d->d_blk = 0;
	d->d_mod = mod;
	d->d_sec = sec;
	d->d_file = file;
	d->d_line = lino;
	DbDef(d);
	return d;
}

void
InitDef(void)
{
	time_t clock;

	mx_title = 0;
	time(&clock);
	mx_date = ctime(&clock);
	mx_author = "";
	mx_version = "";
	pushCond(1, "");
	if (defs == 0)
		defs = (Def *) Malloc(sizeof(Def) * M_DEFS);
}

void
MakeDefs(char *name)
{
	Def *d;
	CmdCode dir;
	char *line, *cmd, *blk, *file;
	extern int pr_hide;
	int mod = 0, sec = 0, lino = 0;
	CmdCode lastdir = Continue;

	IoReadFile(name);
	d = NwDef(Bfile, mod, sec, 0, mx_file);
	d->d_cmd = name;

	while ((line = NextLine()) && *line != '@')
		;
	PrevLine();
	while (!EofFile()) {
		dir = DefDir();
		lino = mx_line;
		file = mx_file;
		cmd = DefCmd();
		blk = DefBlk();

		switch (dir) {
/*
 * conditional text and code
 * Syntax:
 *     @if <macroId>\n <block> [@else <block> ] @endif
 */
		case Ifdef:
			pushCond(GetDef(cmd) != NULL, cmd);
			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				d->d_blk = NULL;
				if (allTrue()) {
					d = NwDef(lastdir, mod, sec, lino, file);
					d->d_cmd = NULL;
					d->d_blk = blk;
				}
			}

			break;
		case Ifndef:
			toggle();
			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = topMacro();
				d->d_blk = NULL;
				d = NwDef(lastdir, mod, sec, lino, file);
				d->d_cmd = NULL;
				d->d_blk = blk;
			}
			break;
		case Endif:
			if (!topCond())
				toggle();

			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = topMacro();
				d->d_blk = NULL;
			}
			popCond();
			if (allTrue()) {
				d = NwDef(lastdir, mod, sec, lino, file);
				d->d_cmd = NULL;
				d->d_blk = blk;
			}
			break;
			/*
			 * Define index.
			 * Syntax:@<n><string>\n@...
			 */
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
			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				d->d_blk = blk;
				lastdir = Continue;
			}
			break;
			/*
			 * Define title, author, date, version.
			 * Syntax: @[tavd]<string>\n@...
			 */
		case Title:
			if (allTrue()) {
				mx_title = cmd;
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				lastdir = Continue;

			}
			break;
		case Version:
			if (allTrue()) {
				mx_version = cmd;
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				lastdir = Continue;
			}
			break;
		case Author:
			if (allTrue()) {
				mx_author = cmd;
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				lastdir = Continue;
			}
			break;
		case Date:
			if (allTrue()) {
				mx_date = cmd;
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				lastdir = Continue;
			}
			break;
			/*
			 * Text directives: chapter, section, paragraph.
			 * Syntax:@[*+ ]<string>\n<blok>@...
			 * Synatx:@\n<blok>@...
			 */
		case Module:
			if (allTrue()) {
				if (!Hide()) {
					mod++;
					sec = 0;
				}
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				d->d_blk = blk;
				lastdir = Continue;
			}
			break;
		case Section:
			if (allTrue()) {
				if (!Hide())
					sec++;
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				d->d_blk = blk;
				lastdir = Continue;
			}
			break;
		case Subsection:
		case Paragraph:
			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				d->d_blk = blk;
				d->d_cmd = cmd;
				lastdir = Continue;
			}
			break;
		case Qcode:
		case Continue:
			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				d->d_blk = blk;
				lastdir = Continue;
			}
			break;
			/*
			 * Code directives, 
			 * Pool: spec, impl
			 * C:    .h, .c, .cc, .y, .l
			 * Basename
			 * Syntax:@[sihcylfp]\n<blk>\n@...
			 */
		case Ofile:
			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				/* specially for Windows: replace all /'s with DIR_SEP's */
				if (DIR_SEP != '/') {
					char *tmp = cmd;

					while ((tmp = strchr(tmp, '/')) != NULL)
						*tmp++ = DIR_SEP;
				}
				d->d_cmd = cmd;
				lastdir = Continue;
			}
			break;
		case Pspec:
		case Pimpl:
		case Cdef:
		case Csrc:
		case Cyacc:
		case Clex:
		case Prolog:
		case Haskell:
		case MALcode:
		case HTML:
		case ODLspec:
		case OQLspec:
		case SQL:
		case Java:
		case Qnap:
		case ProC:
		case Shell:
		case fGrammar:
		case Macro:
		case XML:
		case DTD:
		case XSL:
		case Config:
		case CCyacc:
		case CClex:
			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				d->d_blk = blk;
				lastdir = dir;
			}
			break;
/*
 * Macro definitions.
 * Syntax:@=<def>\n<blok>\n@... 
 */
		case Mxmacro:
			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = cmd;
				d->d_blk = blk;
				/*add information */
				lastdir = Continue;
			}
			break;
		case InHide:
			d = NwDef(dir, mod, sec, lino, file);
			d->d_cmd = cmd;
			d->d_blk = blk;
			HideOn();
			break;
		case OutHide:
			d = NwDef(dir, mod, sec, lino, file);
			d->d_cmd = cmd;
			d->d_blk = blk;
			HideOff();
			break;
			/*
			 * Comment statement
			 * Syntax:@/<blk>\n...
			 */
		case Comment:
			if (allTrue()) {
				d = NwDef(dir, mod, sec, lino, file);
				d->d_cmd = NULL;
				d->d_blk = blk;
			}
			break;
		default:
			Fatal("MakeDefs", "Unknown directive:%c%c", MARK, dir);
			break;
		}
	}
	d = NwDef(Efile, mod, sec, mx_line, mx_file);
	CloseFile();
	pr_hide = 0;
}

char *
dir2ext(CmdCode dir)
{
	Directive *d = str2dir;

	if (dir == Bfile) {
		if (textmode == M_TEXI) {
			if (bodymode)
				return "bdy.texi";
			return "texi";
		}
	}

	while (d->cmd != (char *) 0) {
		if (d->code == dir)
			return d->ext;
		d++;
	}
	return "";
}


CmdCode
lookup(char *str)
{
	Directive *d = str2dir;

	while (d->cmd != (char *) 0) {
		if (strcmp(str, d->cmd) == 0)
			return d->code;

		d++;
	}
	return Nop;
}

char *
dir2str(CmdCode dir)
{
	Directive *d = str2dir;

	while (d->cmd != (char *) 0) {
		if (dir == d->code)
			return d->cmd;
		d++;
	}
	return "(nil)";
}

char *line;

char *
substr(char *s, char *sep)
{
	size_t loc;

	loc = strcspn(s, sep);
	if (loc == strlen(s))
		return NULL;
	else
		return s + loc;
}

CmdCode
DefDir(void)
{
	char *dir;
	CmdCode dircode;

	line = NextLine();

	if ((line[0] == MARK) && ((dir = substr(line, " \t\n")) != NULL)) {
		*dir = '\0';
		dircode = lookup(line + 1);
		line = dir + 1;
		if (dircode)
			return dircode;
	} else
		return Nop;

	Error("Non directive:%s", line);
	return Nop;
}

char *
DefCmd(void)
{

	char *f = line;
	char *l;

	while (*f && ((*f == ' ') || (*f == '\t')))
		f++;
	l = f;
	while (*l && (*l != '\n')) {
		if ((*l == MARK) && (*(l - 1) != '\\')) {
			Error("Mark(%c) unexpected:%s", MARK, f);
			return 0;
		}
		l++;
	}
	*l = '\0';
	return StrDup(f);
}

static char blk[M_BLK + 2];
char *
DefBlk(void)
{
	char *f;
	size_t size = 0;
	char *dir = NULL;
	char sep;

	f = blk;

	line = NextLine();

	*f = '\0';
	while (line != NULL) {
		size = strlen(line);
		if ((*line == MARK) && ((dir = substr(line + 1, " \t\n")) != NULL)) {
			sep = *dir;
			*dir = '\0';
			if (lookup(line + 1))
				break;
			*dir = sep;
		}
		if (f + size + 1 > blk + M_BLK)
			Fatal("Mx:Too long block, use extra directives:[%s:%d].\n", mx_file, mx_line);
		strncat(f, line, size + 1);
		f += size;
		line = NextLine();
	}
	if (line) {
		*dir = ' ';
		PrevLine();
	}
	if (f == blk)
		return 0;



	return StrDup(blk);
}

Def *
GetDef(char *str)
{
	Def *d;

	for (d = defs; d < defs + ndef; d++) {
		if ((d->d_cmd) && (strcmp(str, d->d_cmd) == 0))
			switch (d->d_dir) {
			case Ifdef:
			case Endif:
			case Ifndef:
				break;
			default:
				return d;
			}
	}
	return 0;
}

void
DbDef(Def * d)
{
	if ((db_flag & DB_DEF) != DB_DEF)
		return;
	Message("Def:d %c;c %s;m %d;s %d\n", d->d_dir, d->d_cmd, d->d_mod, d->d_sec);
	Message("Def:b>>%s<<\n", d->d_blk);
}
