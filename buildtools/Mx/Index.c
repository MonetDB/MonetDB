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

#include	<mx_config.h>
#include	<stdio.h>
#include        <string.h>
#include	"Mx.h"
#include	"MxFcnDef.h"

#define TEXMODE (textmode==M_TEX)
#define TEXIMODE (textmode==M_TEXI)
#define WWWMODE (textmode==M_WWW )

Itable *itable = 0;
int itable_done = 0;


void
InitIndex(void)
{
	Itable *it;
	char name[16];

	if (itable == 0) {
		itable = (Itable *) Malloc(sizeof(Itable) * M_ITABLE);
		for (it = itable; it < itable + M_ITABLE; it++)
			it->it_entrys = (Ientry *)
			    Malloc(sizeof(Ientry) * M_IENTRY);
	}

	for (it = itable; it < itable + M_ITABLE; it++) {
		snprintf(name, sizeof(name), "Index %ld", (long) (it - itable));
		it->it_name = StrDup(name);
		it->it_nentry = 0;
	}
	itable[ICONT].it_name = StrDup("Table of contents");
	itable[IFRAG].it_name = StrDup("Table of fragments");
	itable[IMACRO].it_name = StrDup("Table of macro's");
}

void
IndexTable(int it, char *name)
{
	/* test it, for legal values  */

	itable[it].it_name = StrDup(name);
}

void
IndexEntry(int nit, char *name, int mod, int sec)
{
	Itable *it = itable + nit;
	Ientry *ie = it->it_entrys + it->it_nentry++;

	/* test it and ie, for legal values  */
	if (name == 0 || *name == 0)
		return;
	ie->ie_name = StrDup(name);
	ie->ie_mod = mod;
	ie->ie_sec = sec;
}

void
SortIndex(void)
{
	Itable *it;
	Ientry *ie1;
	Ientry *ie2;
	Ientry iet;

	for (it = itable; it < itable + M_ITABLE; it++)
		for (ie1 = it->it_entrys; ie1 < it->it_entrys + it->it_nentry; ie1++)
			for (ie2 = ie1 + 1; ie2 < it->it_entrys + it->it_nentry; ie2++)
				if (strcmp(ie1->ie_name, ie2->ie_name) > 0) {
					iet = *ie1;
					*ie1 = *ie2;
					*ie2 = iet;
				}
}

void
PrCont(void)
{
	Itable *it;
	Ientry *ie;
	extern int opt_column;

	it = itable + ICONT;
	if (it->it_nentry == 0)
		return;
	itable_done = 1;

	if (TEXIMODE) {
		PrCmd("@c Table contents implied\n@contents\n");
		return;
	}
	if WWWMODE {
		int m1 = 0, m2 = 0, m3 = 0, s1 = 0, s2 = 0, s3 = 0, i, n;
		char file[128], *b = BaseName(filename);

		/* print the simple HTML1.0 TOC in the main doc (noframes) */
		mx_out = 1;
		if (bodymode == 0)
			PrCmd("\n<hr size=5 noshade>\n");
		PrCmd("<h2>Table Of Contents</h2>\n");
		for (i = 0, ie = it->it_entrys; i < it->it_nentry; i++, ie++) {
			if (ie->ie_sec == 0) {
				if (i)
					PrCmd("</ul>\n");
				PrCmd("<h3><a href=\"#mod_");
				PrNum(ie->ie_mod);
				PrCmd("_0_0\">");
				PrCmd(ie->ie_name);
				PrCmd("</a></h3>\n<ul><li>\n");
			} else {
				PrCmd("  [<a href=\"#mod_");
				PrNum(ie->ie_mod);
				PrCmd("_");
				PrNum(ie->ie_sec);
				PrCmd("_0\">");
				PrCmd(ie->ie_name);
				PrCmd("</a>]\n");
			}
		}
		if (i)
			PrCmd("</ul>\n");

		/* find out where second column starts */
		n = (it->it_nentry + 2) / 3;
		for (i = 0, ie = it->it_entrys; i < n; i++, ie++)
			if (ie->ie_sec == 0) {
				m2++;
				s2 = 0;
			} else {
				s2++;
			}
		for (m3 = m2, s3 = s2; i < n + n; i++, ie++)
			if (ie->ie_sec == 0) {
				m3++;
				s3 = 0;
			} else {
				s3++;
			}

		/* print the HTML3.0 TOC in the body frame file */
		mx_out = 2;
		snprintf(file, sizeof(file), "%s.body.html", b);
		PrCmd("<center><table border=0>\n");
		PrCmd("<tr><th colspan=2 align=right>");
		PrCmd("Table Of Contents</th>");
		PrCmd("<th align=right>(");
		PrCmd(b);
		PrCmd(".mx)</th>");
		PrCmd("</tr>\n");
		for (i = 0; i < n; i++) {
			ie = it->it_entrys + i;
			PrCmd("<tr><td>\n<font size=\"-1\">\n<a href=\"");
			PrCmd(file);
			PrCmd("#mod_");
			if (ie->ie_sec == 0) {
				PrNum(++m1);
				PrCmd("_0_0\" target=\"a\">");
				PrNum(m1);
				s1 = 0;
				PrCmd(" <b>");
				PrText(ie->ie_name);
				PrCmd("</b>");
			} else {
				PrNum(m1);
				PrCmd("_");
				PrNum(++s1);
				PrCmd("_0\" target=\"a\">");
				PrNum(m1);
				PrChr('.');
				PrNum(s1);
				PrChr(' ');
				PrText(ie->ie_name);
			}
			PrCmd("</a></font></td><td>\n<font size=\"-1\">\n");

			if (i + n == it->it_nentry) {
				PrCmd("</font></td><td></td></tr>\n");
				continue;
			}
			ie += n;
			PrCmd("<a href=\"");
			PrCmd(file);
			PrCmd("#mod_");
			if (ie->ie_sec == 0) {
				PrNum(++m2);
				PrCmd("_0_0\" target=\"a\">");
				PrNum(m2);
				s2 = 0;
				PrCmd(" <b>");
				PrText(ie->ie_name);
				PrCmd("</b>");
			} else {
				PrNum(m2);
				PrCmd("_");
				PrNum(++s2);
				PrCmd("_0\" target=\"a\">");
				PrNum(m2);
				PrChr('.');
				PrNum(s2);
				PrChr(' ');
				PrText(ie->ie_name);
			}
			PrCmd("</a></font></td><td>\n<font size=\"-1\">\n");

			if (i + n + n >= it->it_nentry) {
				PrCmd("</font></td></tr>\n");
				continue;
			}
			ie += n;
			PrCmd("<a href=\"");
			PrCmd(file);
			PrCmd("#mod_");
			if (ie->ie_sec == 0) {
				PrNum(++m3);
				PrCmd("_0_0\" target=\"a\">");
				PrNum(m3);
				s3 = 0;
				PrCmd(" <b>");
				PrText(ie->ie_name);
				PrCmd("</b>");
			} else {
				PrNum(m3);
				PrCmd("_");
				PrNum(++s3);
				PrCmd("_0\" target=\"a\">");
				PrNum(m3);
				PrChr('.');
				PrNum(s3);
				PrChr(' ');
				PrText(ie->ie_name);
			}
			PrCmd("</a></font></td></tr>\n");
		}
		PrCmd("</table></center>\n");
		mx_out = 5;
		return;
	}

	if TEXMODE
		PrCmd("\\twocolumn[\\begin{center} \\large\\bf ");
	else
		PrCmd(".SH\n");
	PrText(it->it_name);
	if TEXMODE
		PrCmd("\\end{center}]\n");
	else if TEXIMODE
		PrCmd("\n");
	else
		PrCmd("\n.LP\n");

	if TEXMODE
		PrCmd("\\begin{flushleft}\n");
	for (ie = it->it_entrys; ie < it->it_entrys + it->it_nentry; ie++) {
		if (ie->ie_sec == 0) {
			if TEXMODE
				PrCmd("\\makebox[3 em][l]{\\bf ");
			PrNum(ie->ie_mod);
			if TEXMODE
				PrCmd("}{\\bf ");
			else
				PrCmd("\t");
			PrText(ie->ie_name);
			if TEXMODE
				PrCmd("}");
			else
				PrCmd("\n");
		} else {
			if TEXMODE
				PrCmd("\\hspace*{2 em}\\ ");
			if TEXMODE
				PrCmd("\\makebox[2 em][l]{");
			else
				PrCmd("\t");
			PrNum(ie->ie_mod);
			PrChr('.');
			PrNum(ie->ie_sec);
			if TEXMODE
				PrCmd("}{");
			else
				PrCmd(" ");
			PrText(ie->ie_name);
			if TEXMODE
				PrCmd("}");
			else
				PrCmd("\n");
		}
		if TEXMODE
			PrCmd("\\newline\n");
		else
			PrCmd(".br\n");
	}
	if TEXMODE
		PrCmd("\\end{flushleft}");
	else
		PrCmd(".XE\n.PX\n");

	if TEXMODE {
		if (opt_column == 1)
			PrCmd("\\onecolumn\n");
		else {
			if (bodymode == 0)
				PrCmd("\\clearpage\n");
		}
	} else
		PrCmd(".sp 24i\n");	/* advance to the next column */
}

void
PrIndex(void)
{
	int it;

	if (TEXIMODE) {
		if (bodymode == 0)
			PrCmd("@c index table implicit\n");
		return;
	}
	if WWWMODE
		return;
	SortIndex();
	if (TEXMODE && bodymode == 0)
		PrCmd("\\clearpage\n");
	for (it = 1; it < M_ITABLE; it++)
		PrItable(it);
}

void
PrItable(int nit)
{
	Itable *it;
	Ientry *ie;

/*
int first;
*/

	it = itable + nit;
	if (it->it_nentry == 0)
		return;

	if (bodymode == 0) {
		if TEXMODE
			PrCmd("\\twocolumn[\\begin{center} \\large\\bf ");
		else
			PrCmd(".SH\n");
		PrText(it->it_name);
		if TEXMODE
			PrCmd("\\end{center}]");
		else if TEXIMODE
			PrCmd("\n");
		else
			PrCmd(".LP");
		PrCmd("\n");

		if TEXMODE
			PrCmd("\\begin{flushleft}\n");
		for (ie = it->it_entrys; ie < it->it_entrys + it->it_nentry; ie++) {
			if (ie->ie_sec == 0) {
				if TEXMODE {
					PrText(ie->ie_name);
					PrCmd("\\dotfill ");
					PrNum(ie->ie_mod);
				} else {
/*
				if( first) {
					PrCmd(".XS "); first=0;
				} else PrCmd(".XA ");
*/
					PrCmd(".br\n");
					PrNum(ie->ie_mod);
					PrCmd("\n");
					PrText(ie->ie_name);
				}
			} else {
				if TEXMODE {
					PrText(ie->ie_name);
					PrCmd("\\dotfill ");
					PrNum(ie->ie_mod);
					PrChr('.');
					PrNum(ie->ie_sec);
				} else {
/*
				if( first) {
					PrCmd(".XS "); first=0;
				} else PrCmd(".XA ");
*/
					PrCmd(".br\n");
					PrNum(ie->ie_mod);
					PrChr('.');
					PrNum(ie->ie_sec);
					PrCmd("\n");
					PrText(ie->ie_name);
				}
			}
			if TEXMODE
				PrCmd("\\newline");
			PrCmd("\n");
		}
		if TEXMODE
			PrCmd("\\end{flushleft}");

		if TEXMODE {
			if (opt_column == 1)
				PrCmd("\\onecolumn\n");
			else {
				if (bodymode == 0)
					PrCmd("\\clearpage\n");
			}
		} else
			PrCmd(".XE\n");
	}
}
