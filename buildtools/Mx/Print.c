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

#include	"Mx.h"
#include	"MxFcnDef.h"

int pr_hide = 0;
int pr_hide_text = 0;
int pr_env = 0;
int pr_math = 0;
size_t pr_pos = 0;

void
PrEnv(int env)
{
	if (Hide())
		return;

	if (env == pr_env) {
		return;
	}

	switch (pr_env) {
	case E_CODE:
		ofile_printf("@end example\n");
		break;
	case E_TEXT:
	case E_CMD:
		break;
	}
	pr_env = env;
	pr_pos = 0;
	switch (pr_env) {
	case E_CODE:
		ofile_printf("@example\n");
		break;
	case E_TEXT:
	case E_CMD:
		break;
	}
}

/* Low Level Print routines
 */

void
PrRef(int mod, int sec)
{
	PrChr('[');
	if (mod != 0)
		PrNum(mod);
	else
		PrChr('?');
	if (sec != 0) {
		PrChr('.');
		PrNum(sec);
	}
	PrChr(']');
}

void
PrNum(int n)
{
	if (Hide())
		return;

	ofile_printf("%d", n);
}

void
PrTxt(char *s)
{

	char c = *s;

	while ((c = (*s++)))
		switch (c) {
		case '\t':
		case '\n':
		case ' ':
			MathOff();
			ofile_printf("%c", c);
			break;
		case '~':
			MathOff();
			ofile_printf("\\~\\,");
			break;
		case '[':
		case ']':
			MathOff();
			ofile_printf("{%c}", c);
			break;
		case '@':
			ofile_puts("@@");
			break;
		case '#':
			ofile_puts("\\#");
			break;
		case '{':
		case '}':
			ofile_printf("@%c", c);
			break;
		case '&':
		case '%':
		case '$':
		case '_':
			MathOff();
			ofile_printf("\\%c", c);
			break;
		default:
			MathOff();
			ofile_printf("%c", c);
			break;
		case '\\':
			MathOn();
			ofile_printf("\\backslash");
			break;
		case '/':
		case '|':
		case '-':
		case '+':
		case '<':
		case '>':
		case '*':
		case '=':
			MathOn();
			ofile_printf("%c", c);
			break;
		}
}


void
PrStr(char *s)
{
	char *c;

	if (Hide())
		return;

	for (c = s; *c != '\0'; c++) {
		if ((c[0] == '\\') && (c[1] == '@'))
			c++;
		PrChr(*c);
	}
	MathOff();
}

extern int codeline;

void
PrChr(char c)
{
 	size_t start_pos = pr_pos;

	if (Hide())
		return;

	/* administer pr_pos and codeline */
	switch (c) {
	case '\t':
		pr_pos++;
		break;
	case '\n':
		codeline++;
		pr_pos = 0;
		break;
	default:
		pr_pos++;
	}

	if ((pr_env & E_TEXT) == E_TEXT) {
		ofile_putc(c);
		return;
	}

	if ((pr_env & E_CODE) == E_CODE) {
		switch (c) {
		case '\t':
			while (start_pos++ < pr_pos)
				ofile_putc(' ');
			break;
		/* now idea how to translate backslashes savely */
		case '\\':
			ofile_putc(' ');
			break;
		case '{':
		case '}':
		case '@':
			ofile_printf("@%c", c);
			break;
		default:
			ofile_putc(c);
		}
		return;
	}

	if ((pr_env & E_CMD) == E_CMD) {
		switch (c) {
		default:
			ofile_putc(c);
			break;
		}
	}
}

void
MathOn(void)
{
	pr_math = 1;
}

void
MathOff(void)
{
	pr_math = 0;
}

void
HideOn(void)
{
	pr_hide++;
}

void
HideText(void)
{
	pr_hide_text = 1;
}

void
HideOff(void)
{
	extern char *defHideText;

	if (pr_hide == opt_hide && !pr_hide_text && defHideText != 0)
		ofile_printf("%s", defHideText);
	pr_hide--;
	pr_hide_text = 0;
}

int
Hide(void)
{
	if (archived)
		return 1;
	return (((opt_hide == NO_HIDE) && pr_hide_text) || ((pr_hide != opt_hide) && pr_hide_text) || ((pr_hide >= opt_hide) && (opt_hide != NO_HIDE) && !pr_hide_text));
}
