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
#include	<ctype.h>
#include        <stdlib.h>
#include        <string.h>

#include	"Mx.h"
#include	"MxFcnDef.h"

Tok *
FstTok(char *s)
{
	Tok *t;

	if (s == 0)
		return 0;

	t = (Tok *) Malloc(sizeof(Tok));
	t->t_dir = '\0';
	t->t_str = 0;
	t->t_ext = '\0';
	t->t_nxt = s;
	t->t_chr = s[0];

	t = NxtTok(t);
	return t;
}

Tok *
NxtTok(Tok * t)
{
	int inside = 0;

/* since NxtTok() is only called in FstTok (see above) and in two for-loop
   in Form.C & Code.c that do roughly the following: for(t=FstTok(blk); t; t=NxtTok(t)) ,
   we assume, we can savely ignore NULL arguments, here.... 
*/
	if (t == NULL)
		return t;

/* Restore */
	t->t_nxt[0] = t->t_chr;
	if (t->t_dir) {
		if (t->t_chr == MARK)
			t->t_nxt++;
		if (t->t_ext != '\0')
			t->t_nxt++;
	}

/* NxtTok */
	if (t->t_nxt[0] == '\0') {
		free((char *) t);
		t = 0;
		return t;
	}
	if ((t->t_nxt[0] == MARK) && (CallDir(t->t_nxt[1]))) {
		int hook = 0;
		int ok = 1;

		t->t_nxt++;
		t->t_dir = *t->t_nxt++;
		t->t_str = t->t_nxt;

		if (!HideDir(t->t_dir))
			for (; (t->t_nxt[0] && ok); t->t_nxt++) {
				switch (t->t_nxt[0]) {
				case '\n':
					ok = 0;
					break;
				case '\\':
					if (*(t->t_nxt + 1))
						t->t_nxt++;
					break;
				case '"':
					inside ^= 1;
					break;
				case '(':
					if (!inside)
						hook++;
					break;
				case ')':
					if (!inside)
						hook--;
					break;
				case MARK:
					if (!(inside || hook))
						ok = 0;
					break;
				}
			}
		t->t_chr = t->t_nxt[0];
		t->t_nxt[-1] = '\0';
		if (t->t_dir == '`')
			t->t_ext = t->t_nxt[1];
		DbTok(t);
		return t;
	}

	if ((t->t_nxt[0] == MARK) && (CtlDir(t->t_nxt[1]))) {
		t->t_nxt++;
		t->t_dir = *t->t_nxt++;
		t->t_str = t->t_nxt;

		if (!HideDir(t->t_dir))
			for (; t->t_nxt[0]; t->t_nxt++) {
				if ((t->t_nxt[0] == '\n'))
					break;
				if ((t->t_nxt[0] == MARK) && (t->t_nxt[-1] != '\\'))
					break;
			}
		t->t_chr = t->t_nxt[0];
		t->t_nxt[0] = '\0';
		if (t->t_dir == '`')
			t->t_ext = t->t_nxt[1];
		DbTok(t);
		return t;
	}

	if ((t->t_nxt[0] == '\\') && (t->t_nxt[1] == MARK)) {
		t->t_dir = '\0';
		t->t_str = t->t_nxt + 1;
		t->t_ext = '\0';
		t->t_nxt += 2;
		t->t_chr = t->t_nxt[0];
		t->t_nxt[0] = '\0';
		return t;
	}

	t->t_dir = '\0';
	t->t_str = t->t_nxt;
	t->t_ext = '\0';
	t->t_nxt++;
	t->t_chr = t->t_nxt[0];
	t->t_nxt[0] = '\0';

	return t;
}

Tok *
SkipTok(Tok * t, char tok)
{
	char *s;

	t->t_str[0] = t->t_chr;
	for (s = t->t_str; *s; s++) {
		switch (*s) {
		case MARK:
			if (s[1] == tok)
				return FstTok(s);
		case '\n':
			mx_line++;
			break;
		case '\\':
			if (s[1] == MARK)
				s += 2;
			break;
		}
	}
	return 0;

}

void
DbTok(Tok * t)
{
	if ((db_flag & DB_TOK) == DB_TOK) {
		if (t != 0)
			fprintf(stderr, "Tok[d:%c,s:%s,e:%c,c:%c,n:%c]\n", t->t_dir, t->t_str, t->t_ext, t->t_chr, t->t_nxt[1]);
		else
			fprintf(stderr, "Tok[NIL]\n");
	}
}

/* Argument Handling
 */
char **
MkArgv(char *str)
{
	char **argv;
	int argc = 0;
	int inside = 0;
	int level = 0;
	char *c;
	int i;

	argv = (char **) Malloc(sizeof(char *) * (M_ARGS + 1));
	for (argc = 0; argc <= M_ARGS; argc++)
		argv[argc] = 0;
	argc = -1;
	argv[0] = c = StrDup(str);

	i = 0;
	for (c = argv[0]; *c; c++) {
		switch (*c) {
		case '\\':
			if (*(c + 1))
				c++;
			break;
		case '"':
			inside ^= 1;
			break;
		case '(':
			if (!inside) {
				level++;
				if (level == 1) {
					*c = '\0';
					argv[++i] = c + 1;
				}
			}
			break;
		case ')':
			if (!inside) {
				if (level == 1) {
					*c = '\0';
					argc = i;
				}
				level--;
			}
			break;
		case ',':
			if (!inside) {
				if (level == 1) {
					*c = '\0';
					argv[++i] = c + 1;
				}
			}
			break;
		}
		if (argc == (M_ARGS + 1))
			Fatal("MkArgv", "Too many arguments:%s", str);
	}
	if (level != 0)
		Fatal("MkArgv", "Unbalanced Argument:%s", str);

	DbArgv(argv);
	return argv;
}

char **
RmArgv(char **argv)
{
	Free((char *) argv[0]);
	Free((char *) argv);

	return 0;
}

void
DbArgv(char **argv)
{
	int argc;

	if ((db_flag & DB_TOK) == DB_TOK) {
		fprintf(stderr, "Argv:");
		for (argc = 0; *argv && (argc <= M_ARGS); argc++)
			fprintf(stderr, "argv[%d]:%s\n", argc, argv[argc]);
	}
}
