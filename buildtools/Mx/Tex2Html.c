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

#include <mx_config.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stddef.h>


char *PrintChar(char *);
char *find_blocks(int, char *, int);
int new_token(char *);
int translate_text(char *, char *, char **, char *);
char *table_format(char **);
int bib_insert(char *);
char *skip_opt(char **);
void initltab(int, int, int, int, int, int, int);
void subst_blocks(int, char *);
char *skip_param(char *, int, int *);
char *bib_entry(char *, char *);
int bib_look(char *, char *, char **);







/* WHAT: first we allowed users to type LaTex article-style code inside 
 * documents. But now we want to generate HTML output. What do we do? 
 * Translate all the LaTeX codes again.. We support also simple LaTeX tables,
 * bibtex citations, labels and refs, and epsf imagery.
 * No \defs, \newcommands, or \figures, please..
 * This is an opportunistic set of routines. 
 * You can compile a utility with the -DSTANDALONE flag.
 * STATUS: dirty hack^3
 * AUTHOR: The Spagetti Corp, Ltd. 
 */

#ifdef STANDALONE
char *outputdir = ".";
FILE *ofile = stdout;

PrChr(c)
char c;
{
	if (c == '&')
		ofile_puts("&#38;");
	else if (c == '"')
		ofile_puts("&#34;");
	else if (c == '<')
		ofile_puts("&#60;");
	else if (c == '>')
		ofile_puts("&#62;");
	else
		ofile_putc(c);
}
#else
#include        "Mx.h"
#include        "MxFcnDef.h"
extern FILE *ofile;
extern char *outputdir;
#endif

#define TYPE_HLINE  	0
#define TYPE_MBOX	36
#define TYPE_ITEM	37
#define TYPE_FOOTNOTE   40
#define TYPE_WWW 	41
#define TYPE_THANKS     42
#define TYPE_SECTION	43
#define TYPE_TITLE	46
#define TYPE_CITE	50
#define TYPE_BIB	51
#define TYPE_REF	52
#define TYPE_LABEL	53
#define TYPE_DRAWPSFIG	56
#define TYPE_TABULAR	57
#define TYPE_TT		59
#define TYPE_DESC	64
#define TYPE_VERBATIM	65
#define TYPE_TINY       67
#define TYPE_HUGE       76
#define TYPE_DEF        77
#define TYPE_UNKNOWN    80

char *keyword[] = {
	"hline", "rule", "newline", "newpage", "clearpage", "backslash",
	"bullet", "parallel", "cdot", "cdots", "ldots", "pounds", "copyright",
	"S", "not", "P", "times", "sigma", "theta", "equiv", "Join", "ae", "AE", " ", "skip", "bigskip", "leq",
	"geq", "div", "pm", "ll", "gg", "mid", "wedge", "vee", "sim", "mbox",
	"item", "indent", "itemindent", "footnote", "www", "thanks", "section",
	"subsection", "subsubsection", "title", "date", "author", "caption",
	"cite", "bibliography", "ref", "label", "epsffile", "epsfbox",
	"drawpsfig", "tabular", "array", "tt", "em", "bf", "center", "itemize",
	"description", "verbatim", "enumerate", "tiny", "scriptsize",
	"footnotesize", "small", "normalsize", "large", "Large", "LARGE",
	"huge", "Huge", "def", "renewcommand", "newcommand", "~sentinel~"
};

char *translation[] = {
	"<hr noshade size=1>\n", "<hr noshade size=1>\n", "<br>", "<p>\n",
	"<p>", "\\", "<b>&#183;</b>", "||", "&#183;", "&#183;&#183;", "..",
	"&#163;", "&#169;", "&#167;", "&#172;", "&#182;", "&#215;", "&sigma;", "&theta;", "&equiv;", "|&times;|", "&#230;",
	"&#189;", "&#160;", "<br><br>", "<br><p><br>", "&#60;=", "&#62;=",
	"&#247;", "&#177;", "&#60;&#60;", "&#62;&#62;", "|",
	" <i>and</i> ",
	" <i>or</i> ", "~", "", "<li>",
	"&#160;&#160;&#160;&#160;", "&#160;&#160;&#160;&#160;",
	"font size=\"-2\"", "font size=\"-1\"", "h2", "h3", "h4", "a href=\"",
	"h1", "h3", "h2", "i",
	"font size=\"-1\"", "", "", "", "", "", "", "table", "table", "tt", "i",
	"b", "center", "ul", "dl", "pre", "ol", "-4", "-2", "-1", "-1", "-1",
	"+0", "+1", "+2", "+3", "+4", "", "", "", "", 0
};

typedef struct {
	char *str;
	ptrdiff_t len;
	int nested[512];
	int nchildren;
} texblock_t;

texblock_t tb[1000];
int nblocks = 0;
char latexstr[100000] = { 0 };
char *latexend = latexstr;
char *table_center = "center";
char *table_left = "left";
char *table_right = "right";
char *table_empty = "";
char *table_columns[20];
int table_column, printed_column;
int table, indesclist, math;
char bibtexfile[256] = { 0 };
char *ref_names[1000];
int nrefs = 0;
char refbuf[10000] = { 0 }, *refend = refbuf;

/* table for html special characters */
typedef int larr[6];
larr ltab[256];

void
initltab(int lt, int p1, int p2, int p3, int p4, int p5, int p6)
{
	if (p1) {
		ltab[lt][0] = p1;
		ltab[lt - 'A' + 'a'][0] = p1 + 32;
	}
	if (p2) {
		ltab[lt][1] = p2;
		ltab[lt - 'A' + 'a'][1] = p2 + 32;
	}
	if (p3) {
		ltab[lt][2] = p3;
		ltab[lt - 'A' + 'a'][2] = p3 + 32;
	}
	if (p4) {
		ltab[lt][3] = p4;
		ltab[lt - 'A' + 'a'][3] = p4 + 32;
	}
	if (p5) {
		ltab[lt][4] = p5;
		ltab[lt - 'A' + 'a'][4] = p5 + 32;
	}
	if (p6) {
		ltab[lt][5] = p6;
		ltab[lt - 'A' + 'a'][5] = p6 + 32;
	}
}

/* insert an occurance of a reference into the table 
 */
int
bib_insert(char *str)
{
	int i;

	for (i = 0; i < nrefs; i++)
		if (!strcmp(str, ref_names[i]))
			 return i + 1;

	ref_names[nrefs] = refend;
	while ((*refend++ = *str ++) !=0) ;
	return ++nrefs;
}

/* find an entry named 'str' in a bibtex record at 'buf' 
 */
char *
bib_entry(char *str, char *buf)
{
	static char entry[1000];
	char *e = entry, *p = (char *) strstr(buf, str);
	int inword = 0, endchar = '"';

	if (!p || !*p)
		return 0;
	p = (char *) strchr(p, '=');
	if (!p || !*p)
		return 0;
	for (p++; p && *p && isspace((int) (*p)); p++) ;
	if (!p || !*p)
		return 0;
	if (*p == '"')
		p++;
	else
		endchar = ',';
	for (; *p && ((*p != endchar) || p[-1] == '\\'); p++) {
		if (isspace((int) (*p))) {
			if (inword)
				*e++ = ' ';
			inword = 0;
		} else {
			*e++ = *p;
			inword = 1;
		}
	}
	/* cut off spaces and dots at the end, we do formatting ourselves */
	while (e > entry && (isspace((int) (e[-1])) || (e[-1] == '.')))
		e--;
	*e = 0;
	return entry;
}

/* look for a bibtex record named 'str' in 'buf'. Finds 'hit' place and type 
 */
int
bib_look(char *str, char *buf, char **hit)
{
	size_t len = strlen(str);
	ptrdiff_t blen = 0;
	char *p1 = buf, *p2;

	while ((p1 = strstr(p1 + len, str)) && !(isalnum((int) (p1[-1])) || (p1[len] == ',') || isspace((int) (p1[len])))) ;

	if (!p1)
		return 0;
	while (*++p1 && *p1 != '\n') ;
	*hit = p1;
	if ((p2 = strstr(p1, "\n}")) != 0)
		blen = p2 - p1;
	return (int) blen;
}


#define output(s,p) do { snprintf(out,sizeof(oubuf) - (out - oubuf),s,p); out+=strlen(out); } while (0)
#define outputNArg(s) do { snprintf(out,sizeof(oubuf) - (out - oubuf),s); out+=strlen(out); } while (0)
/* print a bibtex reference list (in TeX!). Later, it is converted to html. 
 */
void
bib_print(void)
{
	char inbuf[100000], oubuf[100000], *out = oubuf;
	char *p, *www, *hit;
	FILE *fp;
	size_t nchars;
	int i, l;

	if ((fp = fopen(bibtexfile, "r")) == 0) {
		FILE *fileptr = ofile;

		ofile = stderr;
		ofile_printf("latex2html: cannot open %s.\n", bibtexfile);
		ofile = fileptr;
		return;
	}
	*bibtexfile = 0;
	nchars = fread(inbuf, 1, 100000, fp);
	fclose(fp);
	inbuf[nchars] = 0;

	outputNArg("\\skip{\\Large References}{\\small\\begin{description}\n");
	for (i = 0; i < nrefs; i++) {
		if (!(l = bib_look(ref_names[i], inbuf, &hit)))
			continue;
		hit[l] = 0;	/* temporarily close inbuf at hit end */
		output("\\item[[%d]]\n", i + 1);
		if ((www = bib_entry("www", hit)) != 0)
			output("\\www[%s]{", www);
		if ((p = bib_entry("author", hit)) != 0)
			output("%s: ", p);
		if ((p = bib_entry("title", hit)) != 0)
			output("{\\bf %s}", p);
		outputNArg(". ");
		if ((p = bib_entry("booktitle", hit)) != 0) {
			output("In {\\em %s}", p);
		} else if ((p = bib_entry("journal", hit)) != 0) {
			output("{\\em %s} ", p);
			if ((p = bib_entry("volume", hit)) != 0)
				output(", %s", p);
			if ((p = bib_entry("number", hit)) != 0)
				output("(%s)", p);
		}
		if ((p = bib_entry(", pages", hit)) != 0)
			output(", pages %s", p);
		if ((p = bib_entry("publisher", hit)) || (p = bib_entry("institution", hit)))
			output(". %s", p);
		if ((p = bib_entry("month", hit)) != 0) {
			if (!strcmp(p, "jan"))
				outputNArg(", Januari");
			else if (!strcmp(p, "feb"))
				outputNArg(", Februari");
			else if (!strcmp(p, "mar"))
				outputNArg(", March");
			else if (!strcmp(p, "may"))
				outputNArg(", May");
			else if (!strcmp(p, "jun"))
				outputNArg(", June");
			else if (!strcmp(p, "jul"))
				outputNArg(", July");
			else if (!strcmp(p, "aug"))
				outputNArg(", August");
			else if (!strcmp(p, "sep"))
				outputNArg(", September");
			else if (!strcmp(p, "oct"))
				outputNArg(", October");
			else if (!strcmp(p, "nov"))
				outputNArg(", November");
			else if (!strcmp(p, "dec"))
				outputNArg(", December");
			else
				outputNArg(",");
			if ((p = bib_entry("year", hit)) != 0)
				output(" %s", p);
		}
		outputNArg(".");
		if (www)
			outputNArg("}");
		hit[l] = ' ';	/* reopen inbuf */
	}
	outputNArg("\n\\end{description}}\n");
	*out = 0;
	latex2html(oubuf, 1, 1);
}


char *
PrintChar(char *s)
{
	if (*s == '@') {
		int index = 0;

		switch (s[1]) {
		case '`':
			index = 1;
		case ':':
		case '[':
			*s++ = 0;
			*s++ = 0;
			while (*s && *s != '@') {
				ofile_putc(*s);
				*s++ = 0;
			}
			*s = 0;
			if (index && isdigit((int) (s[1])))
				*++s = 0;
			return s;
		}
	}
	PrChr(*s);
	*s = 0;
	return s;
}

int
new_token(char *str)
{
	int i = nblocks++;

	tb[i].nchildren = 0;
	tb[i].str = str;

	return i;
}

/* parse the format specification of an array of tabular environment
 */
char *
table_format(char **s)
{
	char *format = 0, *p1;

	for (p1 = *s; p1 < latexend && *p1; p1++) {
		char c = *p1;

		*p1 = 0;	/* nullify */
		if (c == 'l') {
			format = table_left;
		} else if (c == 'c') {
			format = table_center;
		} else if (c == 'r') {
			format = table_right;
		} else if (c == 'p') {
			while (isspace((int) (*++p1))) ;
			if (*p1)
				continue;
			for (p1++; *p1; p1++)
				*p1 = 0;	/* nullify */
			format = table_left;
		} else
			continue;
		break;
	}
	*s = p1;
	return format;
}

/* skip the [XXX] option after a LaTeX command. set 'ptr' to end. return XXX. 
 */
char *
skip_opt(char **ptr)
{
	char *t, *s = *ptr, blk;

	if (*s != '[')
		return *ptr;

	t = ++s;
	for (blk = 1; blk; s++) {
		if (*s == '\n')
			break;
		if (s[-1] != '\\' && s[-1] != '@') {
			if (*s == ']')
				blk--;
			else if (*s == '[')
				blk++;
		}
	}
	s[-1] = 0;
	*ptr = s - 1;

	return t;
}

/* skip the {XXX} param after a LaTex command. return the end position.
 * this routine got complicated through the presence of verbatim blocks,
 * (then: readonly==1), and the '\begin{XXX}'-to-'\XXX' translation we
 * do here (though we shouldn't), *only* at block-start, not block-end. 
 */
char *
skip_param(char *s, int blockstart, int *readonly)
{
	char *t;

	while (*s && isspace((int) (*s)))
		s++;
	if (*s != '{')
		return s;
	if (blockstart && !*readonly)
		*s = '\\';
	t = ++s;
	while (*s && ((*s != '}') || (s[-1] == '\\')))
		s++;
	if (blockstart && !*readonly)
		*s = ' ';
	if ((*readonly != blockstart) && !strncmp(t, "verbatim", 8))
		*readonly = blockstart;
	if (!(blockstart || *readonly))
		memset(t - 1, 0, 2 + s - t);
	if (s[1] == '[' && strncmp(t, "figure", 6) == 0) {
		char *u = s + 1;

		while (*u && *u != '\n' && *u != ']')
			*u++ = ' ';
		if (*u)
			*u = ' ';
	}
	return s + 1;
}

/* Walk through the LaTeX text, discovering all nested blocks.
 * This structure is saved in the 'tb[]' table.
 * Blocks are marked by '{' ... '}' or '\begin{XXX}' .. '\end{XXX}'.
 * For simplicity, '\begin{XXX}' ..'\end{XXX}' blocks get converted
 * to '{\XXX ... }' blocks.
 */
char *
find_blocks(int vb, char *p1, int t)
{
	char c, *p2;
	int escaped = 0;

	while ((c = *p1) != 0) {
		int i = t;

		if (c == '{') {
			if (escaped) {
				p1[-1] = ' ';
			} else {
				if (vb == 0)
					*p1 = 0;
				tb[t].nested[tb[t].nchildren++] = i = new_token(++p1);
				p2 = find_blocks(vb, p1, i);
				tb[i].len = p2 - p1;
				p1 = p2;
				continue;
			}
		} else if (c == '}') {
			if (escaped) {
				p1[-1] = ' ';
			} else {
				if (vb == 0)
					*p1 = 0;
				return ++p1;
			}
		} else if (c == '\\') {
			int vb2 = vb;

			if (strncmp(p1 + 1, "begin", 5) == 0) {
				if (vb == 0)
					memset(p1, 0, 6);
				tb[t].nested[tb[t].nchildren++] = i = new_token(p1 += 6);
				p1 = skip_param(p1, 1, &vb2);
				p2 = find_blocks(vb2, p1, i);
				tb[i].len = p2 - p1;
				p1 = p2;
				continue;
			} else if (strncmp(p1 + 1, "end", 3) == 0) {
				p2 = skip_param(p1 + 4, 0, &vb2);
				if ((vb == 0) || (vb2 == 0))
					memset(p1, 0, 4);
				return p2;
			} else if (!escaped) {
				p1++;
				escaped = 1;
				continue;
			}
		}
		p1++;
		escaped = 0;
	}
	return p1;
}

#define STACK(s,buf)	{size_t l=strlen(s); *buf -= l; strncpy(*buf,s,l);}

/* parse a LaTex string (between 'p1' and 'p2') and echo the translation
 * into HTML on the fly. Closing codes of block-operands, that are to be
 * placed at block-end, are STACK-ed in 'buf', 2b flushed later.
 */
int
translate_text(char *p1, char *p2, char **stack, char *nextstack)
{
	char c, str[256], *opt_start, *opt_end, *p;
	int i, k;
	size_t len;

	nextstack[0] = 0;

	while (p1 < p2) {
		/* FIRST: table hack!! */
		if (table && (printed_column <= table_column)) {
			char *p4 = p2 + 1, *p5 = table_columns[table_column];
			int width = 1;

			/* just bypass the whole philosophy of this program now */
			if (strstr(p1, "\\multicolumn")) {
				sscanf(p2, "%d", &width);
				for (p4 = p2; *p4; p4++) ;	/* skip width */
				for (p4 += 2; (p1 = table_format(&p4)) != 0; p4++)
					p5 = p1;
				memset(p2, 0, p4 - p2);
				p1 = p2;
				table_column += width - 1;
				printed_column += width - 1;
			}
			ofile_printf("<td align=%s colspan=%d><font size=\"-1\">", p5, width);
			printed_column++;
		}

		/* non-escaped characters in sight */
		if (*p1 != '\\') {
			switch (*p1) {
			case '~':
				ofile_puts("&#160");
				break;
			case '$':
				if (math) {
				      mathoff:math = 0;
					ofile_puts("</i>");
				} else {
				      mathon:math = 1;
					ofile_puts("<i>");
				}
				break;
			case '&':
				if (table) {
					/* tab code in a table */
					ofile_printf("</font></td>\n");
					table_column++;
					break;
				}
				/* 
				 * proper handling of sub- & superscripts in math mode requires
				 * that there is an extra {}-block around '_'/'^', e.g.,
				 * "{^2}", "{_{max}}", "{_\theta{}}".
				 */
			case '^':
				if (math) {
					ofile_puts("<sup>");
					STACK("</sup>", stack);
					break;
				}
			case '_':
				if (math) {
					ofile_puts("<sub>");
					STACK("</sub>", stack);
					break;
				}
			case '\n':
				if (p1[1] == '\n') {
					ofile_printf("<br>");
					p1++;
					break;
				}
			case '\'':
			case '-':
				if (p1[1] == p1[0]) {
					ofile_putc(*p1++);
					break;
				}
			default:
				/* echo this plain and normal character. */
				if (*p1)
					p1 = PrintChar(p1);
			}
			p1++;
			continue;
		}

		/* do short codes first */
		c = *++p1;
		k = 0;
		switch (c) {
		case ']':
			goto mathoff;
		case '[':
			goto mathon;
		case '\\':	/* LaTeX newline */
			if (table) {
				ofile_printf("</font></td></tr>\n<tr>");
				table_column = printed_column = 0;
			} else
				ofile_puts("<br>");
		case '/':
			p1++;
			continue;
		case 'c':
			if (p1[1])
				break;	/* require block: disallow e.g. '\cite' */
			k++;
		case '`':
			k++;
		case '\'':
			k++;
		case '^':
			k++;
		case '~':
			k++;
		case '"':	/* handle TeX's escaped chars here */
			while ((++p1 < latexend) && ((!*p1) || (*p1 == '\\'))) ;
			if ((k = ltab[(int) *p1][5 - k]) != 0)
				ofile_printf("&%d;", k);
			*p1 = 0;	/* nullify */
		default:
			if (isprint((int) (*p1)))
				break;
			/* ignore nonprintable chars */
			p1++;
			continue;
		}

		/* so, this must be a LaTeX '\XXX[YYY]{ZZZ}' keyword. */
		for (len = 1; p1 + len < p2; len++)
			if ((!p1[len]) || isspace((int) (p1[len])) || (p1[len] == ',') || (p1[len] == '[') || (p1[len] == '*') || (p1[len] == '$') || p1[len] == '\\')
				break;

		for (i = TYPE_UNKNOWN; i >= TYPE_HLINE; i--)
			if ((strncmp(p1, keyword[i], len) == 0) && (len == strlen(keyword[i])))
				 break;

		if (p1[len] == '*')
			len++;	/* 'XXX*' equals to 'XXX' */
		p = p1;
		p1 += len;
		opt_start = skip_opt(&p1);	/* parse the '[YYY]' part */
		opt_end = p1 + 1;

		/* Now: switch for the type of keyword */
		if (i < 0) {
			/* unknown command. */
			if ((len == 1) || !isalnum((int) (c))) {
				p1 = p + 1;
				PrChr(*p);	/* some escaped character */
			} else if (math) {
				size_t j;

				/* in mathmode, we treat it as a predicate */
				ofile_puts(" <font size=\"-1\"><i>");
				for (j = 0; j < len; j++)
					PrChr(p[j]);
				ofile_puts("</i></font> ");
			} else {
				/* scientific approach: ignore unexplicable phenomena */
				while (p1 < p2 && isspace((int) (*p1)))
					p1++;
			}
		} else if (i < TYPE_FOOTNOTE) {
			/* a simple code: 1-1 translation possible. */
			if (table && (i == TYPE_HLINE)) {
				if (printed_column > 1) {
					ofile_printf("</font></td>\n");
					if (table > printed_column) {
						ofile_printf("<td colspan=%d><font size=\"+1\"> </font></td>", table - printed_column);
					}
					ofile_printf("</tr>\n<tr>");
					printed_column = 0;
				}
				table_column = 0;
			} else if (math && i == TYPE_MBOX) {
				ofile_puts("</i>");
				return 3;
			} else if (indesclist && (i == TYPE_ITEM)) {
				char stackbuf[129], *stackptr = stackbuf + 128;

				ofile_puts("<dt><b>");
				stackbuf[128] = 0;
				translate_text(opt_start, opt_end, &stackptr, nextstack);
				while (opt_start < opt_end)
					*opt_start++ = 0;
				ofile_puts(stackptr);
				ofile_puts("</b>\n<dd>");
			} else {
				ofile_puts(translation[i]);
			}
		} else if (i < TYPE_CITE) {
			/* sections and headers: parameter block is translated next. */
			while (isspace((int) (*p1)))
				p1++;
			if (!*p1) {
				if (i >= TYPE_TITLE)
					ofile_puts("<center>");
				if (i == TYPE_WWW) {
					ofile_printf("<a href=\"%s\">", opt_start);
					snprintf(nextstack, 1000, "</a>\n");
				} else {
					ofile_printf("<%s>", translation[i]);
					if (i <= TYPE_THANKS) {
						if (i == TYPE_THANKS)
							ofile_puts("<br>[");
						else
							ofile_puts("[");
						snprintf(nextstack, 1000, "]</%s>", translation[i]);
					} else if (i >= TYPE_TITLE) {
						snprintf(nextstack, 1000, "</%s></center>", translation[i]);
					} else {
						snprintf(nextstack, 1000, "</%s>", translation[i]);
					}
				}
			}
		} else if (i < TYPE_TABULAR) {
			/* 1 simple param: labels, citations, bibliography, and epsf */
			char *p3 = p2, *p4;

			while (p3 < latexend && !*p3)
				p3++;
			if (i >= TYPE_BIB) {	/* get a label or bibname */
				for (p4 = str; (*p4++ = *p3) != 0; *p3++ = 0) ;
				if (i == TYPE_BIB) {
					strncpy(p4 - 1, ".bib", sizeof(str) - (p4 - 1 - str));
					strncpy(bibtexfile, str, sizeof(bibtexfile));
				} else if (i == TYPE_LABEL) {
					ofile_printf("<a name=\"%s\"></a>&#60;%s&#62;", str, str);
				} else if (i == TYPE_REF) {
					ofile_printf("<a href=\"#%s\">&#60;%s&#62;</a>", str, str);
				} else {
					char cmd[512], file[512];
					FILE *fp;
					snprintf(file, sizeof(file), "%s%c%s", outputdir, DIR_SEP, str);

					p4 = strchr(file + strlen(outputdir), '.');
					if (p4)
						*p4 = 0;
					strncat(file, ".gif", sizeof(file) - strlen(file) - 1);
					if ((fp = fopen(file, "r")) != 0) {
						fclose(fp);
					} else {
						/* translate eps to 500x500-bounded gif */
						snprintf(cmd, sizeof(cmd), "epstogif %s %s 500 500\n", str, file);

						system(cmd);
					}
					ofile_printf("<img src=\"%s\" align=center><br>\n", file + strlen(outputdir) + 1);
				}
				if (i == TYPE_DRAWPSFIG) {
					while (p3 < latexend && !*p3)
						p3++;
					if (p3 >= latexend)
						continue;
					for (p4 = str; (*p4++ = *p3) != 0; *p3++ = 0) ;
					ofile_printf("<center><em>%s</em></center>\n", str);
				}
				continue;
			}
			/* LaTeX reference */
			ofile_putc('[');
			for (p4 = p3, i = -1; p3 < latexend && *p3; p3++) {
				if ((*p3 == ',') || isspace((int) (*p3))) {
					if (p3 > p4) {
						*p3 = 0;
						if (i >= 0)
							ofile_putc(',');
						ofile_printf("%d", i = bib_insert(p4));
					}
					p4 = p3 + 1;
				}
			}
			*p3 = 0;
			if (i >= 0)
				ofile_putc(',');
			ofile_printf("%d]", bib_insert(p4));
			memset(p2, 0, p3 - p2);
		} else if (i < TYPE_TT) {
			/* tables and arrays */
			char *format;

			while (*p1)
				p1++;
			for (p1++; (format = table_format(&p1)) != 0; p1++)
				table_columns[table++] = format;
			for (i = table; i < 20; i++)
				table_columns[i] = table_empty;
			ofile_printf("\n<table border=2>\n<tr>");
			printed_column = table_column = 0;
			if (table > 1) {
				snprintf(str, sizeof(str), "+</font></td><td colspan=%d align=right><font size=\"-1\">+</font></td></tr>\n</table>\n", table - 1);
				STACK(str, stack);
			} else {
				STACK("+</font></td></tr>\n</table>\n", stack);
			}
		} else if (i < TYPE_DEF) {
			/* a block code: 1 start-code now, stack the end-code. */
			if (i < TYPE_TINY) {
				snprintf(str, sizeof(str), " <%s>", translation[i]);
			} else {
				snprintf(str, sizeof(str), " <font size=\"%s\">", translation[i]);
			}
			ofile_puts(str);
			str[0] = '<';
			str[1] = '/';
			STACK(str, stack);

			if (i == TYPE_VERBATIM) {
				/* enter verbatim mode: echo the entire block. */
				while (isspace((int) (*p1)))
					p1++;
				while (p1 < latexend && *p1) {
					p1 = PrintChar(p1);
					p1++;
				}
				break;
			} else if (i == TYPE_DESC)
				indesclist = 1;
		} else if (i < TYPE_UNKNOWN) {
			/* kill the two renewcommand arguments */
			char *s = p1;

			while (s < latexend && *s)
				s++;
			s++;	/* skip { */
			while (s < latexend && *s)
				*s++ = 0;
			s++;	/* skip } */
			s++;	/* skip { */
			while (s < latexend && *s)
				*s++ = 0;
		} else {
			/* ignore definitions */
			char *s = p1;

			while (s < latexend && isspace((int) (*s)))
				*s++ = 0;
			if (*s++ == '\\') {
				fprintf(stderr, "Mx: tex2html ignored def '%s'\n", s);
				return 0;
			}
		}
	}
	return 1;
}

/* Walk through the LaTeX structure recursively, outputting block by block.
 * At the end of a block, the stack-buffer -- containing closing codes of 
 * opened blocks -- is flushed.
 */
void
subst_blocks(int t, char *initstack)
{
	char *p1 = tb[t].str, *p2;
	char nextstack[1000], stackbuf[1001], *stackptr = stackbuf + 1000;
	int i, save_tab = table;
	int dl = indesclist, printing = 1;

	stackbuf[1000] = 0;
	STACK(initstack, (&stackptr));

	for (i = 0; i < tb[t].nchildren; i++) {
		int nextblock = tb[t].nested[i];
		p2 = tb[nextblock].str;

		if (!printing) {
			/* keep ignoring blocks if only whtspce in between */
			char *p3 = p1;

			while (p3 < p2 && isspace((int) (*p3++))) ;
			if (p3 < p2)
				printing = 1;
		}
		printing &= translate_text(p1, p2, &stackptr, nextstack);
		p1 = p2 + tb[nextblock].len;
		if (printing) {
			subst_blocks(nextblock, nextstack);
			if (printing & 2) {	/* was doing a mbox in mathmode? */
				ofile_puts("<i>");
			}
		}
	}
	translate_text(p1, tb[t].str +tb[t].len, &stackptr, nextstack);

	table = save_tab;
	indesclist = dl;

	ofile_puts(stackptr);
}


/* The main routine. it can be called repeatedly, aggregating consecutive
 * LaTeX blocks, until 'flush' is set to 1.
 * LaTeX comment codes (lines starting with a '%') are filtered here.
 */
void
latex2html(char *s, int init, int flush)
{
	if (!s)
		return;
	if (init) {
		initltab('A', 0, 192, 193, 194, 195, 196);
		initltab('C', 199, 0, 0, 0, 0, 0);
		initltab('E', 0, 200, 201, 202, 0, 203);
		initltab('I', 0, 204, 205, 206, 0, 207);
		initltab('N', 0, 0, 0, 0, 209, 0);
		initltab('U', 0, 217, 218, 219, 0, 220);
		initltab('Y', 0, 221, 0, 0, 0, 0);
		latexstr[0] = 0;
		latexend = latexstr;
		math = indesclist = nblocks = 0;
		table = table_column = printed_column = 0;
	} else {
		*latexend++ = '\n';
	}
	while (*s) {
		char *t = s;

		while (*t && isspace((int) (*t)))
			t++;
		if (*t == '%')
			for (s = t; *s && *s != '\n'; s++) ;
		else
			do {
				*latexend++ = *s;
			} while ((*s++ != '\n') && *s);
	}
	if (flush) {
		int i = new_token(latexstr);

		tb[i].len = latexend - latexstr;
		*latexend = 0;
		find_blocks(0, latexstr, i);
		subst_blocks(i, "");
	}
}



#ifdef STANDALONE
int
main(argc, argv)
int argc;
char **argv;
{
	char buf[100000], oname[1024];
	int nchars = 0;
	FILE *fp = (argc == 0) ? stdin : fopen(argv[1], "r");

	if (argc > 1) {
		int len = strlen(argv[1]);

		strcpy(oname, argv[1]);
		strcpy(oname + len - (strncmp(oname + len - 4, ".tex") ? 0 : 4), ".html");
		ofile = fopen(oname, "w+");
		outputdir = argv[2];
	}
	if (!(fp || ofile))
		return 1;

	ofile_printf("<HTML>\n<HEAD>\n");
	ofile_printf("<TITLE>Html from %s</TITLE>\n", (argc == 0) ? "stdin" : argv[1]);
	ofile_printf("<BODY bgcolor=#f0f0f0 text=#000000 vlink==#6666ff link=#6666ff>");
	while (!feof(fp)) {
		nchars = fread(buf, 1, 100000, fp);
		buf[nchars] = 0;
		latex2html(buf, 1, 1);
	}
	ofile_printf("</BODY>\n");
	return 0;
}
#endif
