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

#include <monetdb_config.h>
#include <stdio.h>
#include <string.h>

#include "Mx.h"
#include "MxFcnDef.h"
#include "disclaimer.h"

extern int pr_env;

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
		PrCmd("@b{");
		PrText(s);
		PrCmd("}");
		break;
	case T_ITALIC:
		PrCmd("@i{");
		PrText(s);
		PrCmd("}");
		break;
	case T_CODE:
		PrCmd("@i{");
		PrText(s);
		PrCmd("}");
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
	extern size_t pr_pos;

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
	if TEXIMODE 
		if (tail) 
			ofile_printf("\n\n");
	somethingPrinted = 0;
}

extern char *bname;

void
PrPrelude(char *file)
{
	char *s, *t, full[200];

	/* find out the full name in 'full', the basename in 's', end in 't' */
	strncpy(full, file, sizeof(full));
	for (s = full; s[1]; s++)
		;
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


	if (bodymode == 0) 
		ofile_printf("\\input texinfo\n");
	PrEnv(E_CMD);
}

void
PrPostlude(void)
{
	PrEnv(E_CMD);
	if (TEXIMODE && bodymode == 0) 
		ofile_printf("@bye\n");
}
