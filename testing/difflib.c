/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "helpers.h"
#include "difflib.h"

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#ifdef HAVE_IO_H
# include <io.h>
#endif
#include <stdlib.h>
#include <time.h>

#ifdef NATIVE_WIN32
#include <windows.h>
#define DIFF	"diff"		/* --binary */
#define COPY	"copy /y"	/* "cp -f" */
#define popen _popen
#define pclose _pclose

#ifndef DIR_SEP
# define DIR_SEP '\\'
#endif

#define getpid _getpid
#else
#ifndef DIFF
#define DIFF	"diff"
#endif

#ifndef DIR_SEP
# define DIR_SEP '/'
#endif
#endif

#ifdef DEBUG
#define UNLINK(x)
#define ERRHNDL(r,s,t,u) ErrXit(s,t,u)
#else
#define UNLINK(x) remove(x)
#define ERRHNDL(r,s,t,u) return r
#endif

#define SETBLACK(f)  if(clr[f]!=0  ) { fprintf(clmn_fp[f],"</span>"); clr[f]=0;   }
#define SETBLUE(f,m) if(clr[f]!=3-m) { if (clr[f]) fprintf(clmn_fp[f], "</span>"); fprintf(clmn_fp[f],"<span class='%sblue'>",(m?"light":"")); clr[f]=3-m; }
#define SETRED(f,m)  if(clr[f]!=5-m) { if (clr[f]) fprintf(clmn_fp[f], "</span>"); fprintf(clmn_fp[f],"<span class='%sred'>",(m?"light":"")); clr[f]=5-m; }
#define SETPINK(f,m) if(clr[f]!=7-m) { if (clr[f]) fprintf(clmn_fp[f], "</span>"); fprintf(clmn_fp[f],"<span class='%spink'>",(m?"light":"")); clr[f]=7-m; }

#define CMDLEN  8192
#define BUFLEN  16384
#define BUFLEN2 32768

static char *
HTMLsave(char *s)
{
	static char t[BUFLEN2];
	char *p = t;

	while (*s && p < t + BUFLEN2 - 7) {
		switch (*s) {
		case '<':
			*p++ = '&';
			*p++ = 'l';
			*p++ = 't';
			*p++ = ';';
			break;
		case '>':
			*p++ = '&';
			*p++ = 'g';
			*p++ = 't';
			*p++ = ';';
			break;
		case '&':
			*p++ = '&';
			*p++ = 'a';
			*p++ = 'm';
			*p++ = 'p';
			*p++ = ';';
			break;
		default:
			*p++ = *s;
			break;
		}
		s++;
	}
	*p++ = 0;

	return t;
}


static void
markNL(FILE *fp, int k)
{
	int i;

	for (i = 0; i < 6; i++)
		fprintf(fp, "@+-%06i\n", k);
}


FILE *
oldnew2u_diff(int mindiff, int context, char *ignore, char *function, char *old_fn, char *new_fn)
{
	char command[CMDLEN];
	char *_d = mindiff ? "-d" : "";

	TRACE(fprintf(STDERR, "oldnew2u_diff(%i,%i,%s,%s,%s,%s)\n", mindiff, context, ignore, function, old_fn, new_fn));

	sprintf(command, "%s %s %s -a %s -U%d %s    %s", DIFF, ignore, function, _d, context, old_fn, new_fn);
	return popen(command, "r");
}

/* oldnew2u_diff */


int
u_diff2l_diff(FILE *u_diff_fp, char *l_diff_fn)
{
	FILE *l_diff_fp;
	char *ok, line[BUFLEN];

	TRACE(fprintf(STDERR, "u_diff2l_diff(%p,%s)\n", u_diff_fp, l_diff_fn));

	if (!(ok = fgets(line, BUFLEN, u_diff_fp))) {
		pclose(u_diff_fp);
		ERRHNDL(0, "empty file in u_diff2l_diff:", "pipe", 1);
	}

	l_diff_fp = Wfopen(l_diff_fn);
	while (ok && strncmp(line, "@@ -", 4)) {
		fprintf(l_diff_fp, "%s", line);
		ok = fgets(line, BUFLEN, u_diff_fp);
	}
	while (ok) {
		do {
			fprintf(l_diff_fp, "%s", line);
		} while (line[strlen(line) - 1] != '\n' && (ok = fgets(line, BUFLEN, u_diff_fp)));
		while (ok && (ok = fgets(line, BUFLEN, u_diff_fp)) && strchr(" -+", line[0])) {
			char l0 = line[0];
			if (line[1] == '\n')
				sprintf(line + 1, "\2\n");
			do {
				fprintf(l_diff_fp, "%s", line);
			} while (line[strlen(line) - 1] != '\n' && (ok = fgets(line, BUFLEN, u_diff_fp)));
			fprintf(l_diff_fp, "%c\1\n", l0);
		}
	}
	fflush(l_diff_fp);
	fclose(l_diff_fp);

	pclose(u_diff_fp);
	return 1;
}

/* u_diff2l_diff */


static int
lw_diff2wc_diff(int mindiff, int doChar, char *lw_diff_fn, char *wc_diff_fn)
{
	FILE *lw_diff_fp, *wc_diff_fp, *fp[2], *pipe_fp;
	char line[BUFLEN], command[CMDLEN], pipe_ln[BUFLEN];
	char *ok, *fn[2];
	size_t i;
	int j;
	int space, alpha_, digit, l[2], k[2];
	char wc_old_fn[CMDLEN], wc_new_fn[CMDLEN];
	char *_d = mindiff ? "-d" : "";

	TRACE(fprintf(STDERR, "lw_diff2wc_diff(%i,%i,%s,%s)\n", mindiff, doChar, lw_diff_fn, wc_diff_fn));

	lw_diff_fp = Rfopen(lw_diff_fn);
	if (!(ok = fgets(line, BUFLEN, lw_diff_fp))) {
		fclose(lw_diff_fp);
		ERRHNDL(0, "empty file in lw_diff2wc_diff:", lw_diff_fn, 1);
	}

	sprintf(wc_old_fn, "%s%c.difflib-%ld-lw_diff2wc_diff-old", tmpdir(), DIR_SEP, (long) getpid());
	fn[0] = wc_old_fn;
	sprintf(wc_new_fn, "%s%c.difflib-%ld-lw_diff2wc_diff-new", tmpdir(), DIR_SEP, (long) getpid());
	fn[1] = wc_new_fn;

	wc_diff_fp = Wfopen(wc_diff_fn);
	while (ok && strncmp(line, "@@ -", 4)) {
		fprintf(wc_diff_fp, "%s", line);
		ok = fgets(line, BUFLEN, lw_diff_fp);
	}
	fflush(wc_diff_fp);
	fclose(wc_diff_fp);

	while (ok) {
		wc_diff_fp = Afopen(wc_diff_fn);
		do {
			fprintf(wc_diff_fp, "%s", line);
		} while (line[strlen(line) - 1] != '\n' && (ok = fgets(line, BUFLEN, lw_diff_fp)));
		fflush(wc_diff_fp);
		fclose(wc_diff_fp);

		l[0] = l[1] = k[0] = k[1] = 0;
		for (j = 0; j < 2; j++)
			fp[j] = Wfopen(fn[j]);
		while (ok && (ok = fgets(line, BUFLEN, lw_diff_fp)) && strchr(" -+", line[0])) {
			if (line[0] == ' ') {
				char l1 = line[1];
				while (k[0] < k[1]) {
					markNL(fp[0], k[0]++);
					l[0]++;
				}
				while (k[1] < k[0]) {
					markNL(fp[1], k[1]++);
					l[1]++;
				}
				i = 1;
				do {
					for (j = 0; j < 2; j++) {
						fprintf(fp[j], "%s", line+i);
					}
					i = 0;
				} while (line[strlen(line) - 1] != '\n' && (ok = fgets(line, BUFLEN, lw_diff_fp)));
				for (j = 0; j < 2; j++) {
					l[j]++;
				}
				if (l1 == '\1')
					for (j = 0; j < 2; j++) {
						markNL(fp[j], k[j]++);
						l[j]++;
					}
			} else {
				if (line[0] == '-')
					j = 0;
				else
					j = 1;
				if (line[1] == '\1') {
					fprintf(fp[j], "\1\n");
					markNL(fp[j], k[j]++);
					l[j] += 2;
				} else if (doChar) {
					i = 1;
					do {
						for (; line[i] != '\n' && line[i] != '\0'; i++) {
							fprintf(fp[j], "%c\n", line[i]);
							l[j]++;
						}
						i = 0;
					} while (line[strlen(line) - 1] != '\n' && (ok = fgets(line, BUFLEN, lw_diff_fp)));
				} else {
					space = isspace((unsigned char) (line[1]));
					alpha_ = isalpha((unsigned char) (line[1]));
					digit = isdigit((unsigned char) (line[1]));
					i = 1;
					do {
						for (; line[i] != '\n' && line[i] != '\0'; i++) {
							if ((space && !isspace((unsigned char) line[i])) ||
							    (!space && isspace((unsigned char) line[i])) ||
							    (alpha_ && !isalpha((unsigned char) line[i])) ||
							    (!alpha_ && isalpha((unsigned char) line[i])) ||
							    (digit && !isdigit((unsigned char) line[i])) ||
							    (!digit && isdigit((unsigned char) line[i])) ||
							    (!isspace((unsigned char) line[i]) &&
							     !isalpha((unsigned char) line[i]) &&
							     !isdigit((unsigned char) line[i]))) {
								fprintf(fp[j], "\n");
								space = isspace((unsigned char) line[i]);
								alpha_ = isalpha((unsigned char) line[i]);
								digit = isdigit((unsigned char) line[i]);
								l[j]++;
							}
							fprintf(fp[j], "%c", line[i]);
						}
						i = 0;
					} while (line[strlen(line) - 1] != '\n' && (ok = fgets(line, BUFLEN, lw_diff_fp)));
					fprintf(fp[j], "\n");
					l[j]++;
				}
			}
		}
		for (j = 0; j < 2; j++) {
			fflush(fp[j]);
			fclose(fp[j]);
		}

/*
      sprintf(command,
              "%s -a %s -u%i %s %s | egrep -v '^(@@ \\-|\\+\\+\\+ |\\-\\-\\- |[ \\+\\-]@\\+\\-)' >> %s",
              DIFF,_d,MAX(l[0],l[1]),fn[0],fn[1],wc_diff_fn);
      SYSTEM(command);
*/

		sprintf(command, "%s -a %s -U%d %s %s", DIFF, _d, MAX(l[0], l[1]), fn[0], fn[1]);

		pipe_fp = popen(command, "r");

		wc_diff_fp = Afopen(wc_diff_fn);
		while (fgets(pipe_ln, BUFLEN, pipe_fp)) {
			if (strncmp(pipe_ln, "@@ -", 4) &&
			    strncmp(pipe_ln, "+++ ", 4) &&
			    strncmp(pipe_ln, "--- ", 4) &&
			    strncmp(pipe_ln, " @+-", 4) &&
			    strncmp(pipe_ln, "+@+-", 4) &&
			    strncmp(pipe_ln, "-@+-", 4)) {
				fprintf(wc_diff_fp, "%s", pipe_ln);
			}
		}
		fflush(wc_diff_fp);
		fclose(wc_diff_fp);
		pclose(pipe_fp);
	}
	UNLINK(wc_old_fn);
	UNLINK(wc_new_fn);

	fclose(lw_diff_fp);
	return 1;
}

/* lw_diff2wc_diff */


int
l_diff2w_diff(int mindiff, char *l_diff_fn, char *w_diff_fn)
{
	TRACE(fprintf(STDERR, "l_diff2w_diff(%i,%s,%s)\n", mindiff, l_diff_fn, w_diff_fn));

	return lw_diff2wc_diff(mindiff, 0, l_diff_fn, w_diff_fn);
}

/* l_diff2w_diff */


int
w_diff2c_diff(int mindiff, char *w_diff_fn, char *c_diff_fn)
{
	TRACE(fprintf(STDERR, "w_diff2c_diff(%i,%s,%s)\n", mindiff, w_diff_fn, c_diff_fn));

	return lw_diff2wc_diff(mindiff, 1, w_diff_fn, c_diff_fn);
}

/* w_diff2c_diff */



int
oldnew2l_diff(int mindiff, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *l_diff_fn)
{
	FILE *u_diff_fp;
	int rtrn = 0;

	TRACE(fprintf(STDERR, "oldnew2l_diff(%i,%i,%s,%s,%s,%s,%s)\n", mindiff, context, ignore, function, old_fn, new_fn, l_diff_fn));

	if ((u_diff_fp = oldnew2u_diff(mindiff, context, ignore, function, old_fn, new_fn)) == NULL) {
		ERRHNDL(0, "oldnew2u_diff returns 0 in oldnew2l_diff", "", 1);
	}

	rtrn = u_diff2l_diff(u_diff_fp, l_diff_fn);

	return rtrn;
}

/* oldnew2l_diff */


int
oldnew2w_diff(int mindiff, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *w_diff_fn)
{
	char l_diff_fn[CMDLEN];
	int rtrn = 0;

	TRACE(fprintf(STDERR, "oldnew2w_diff(%i,%i,%s,%s,%s,%s,%s)\n", mindiff, context, ignore, function, old_fn, new_fn, w_diff_fn));

	sprintf(l_diff_fn, "%s%c.difflib-%ld-oldnew2w_diff-l_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!oldnew2l_diff(mindiff, context, ignore, function, old_fn, new_fn, l_diff_fn)) {
		UNLINK(l_diff_fn);
		ERRHNDL(0, "oldnew2l_diff returns 0 in oldnew2w_diff", "", 1);
	}

	rtrn = l_diff2w_diff(mindiff, l_diff_fn, w_diff_fn);

	UNLINK(l_diff_fn);
	return rtrn;
}

/* oldnew2w_diff */


int
oldnew2c_diff(int mindiff, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *c_diff_fn)
{
	char w_diff_fn[CMDLEN];
	int rtrn = 0;

	TRACE(fprintf(STDERR, "oldnew2c_diff(%i,%i,%s,%s,%s,%s,%s)\n", mindiff, context, ignore, function, old_fn, new_fn, c_diff_fn));

	sprintf(w_diff_fn, "%s%c.difflib-%ld-oldnew2c_diff-w_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!oldnew2w_diff(mindiff, context, ignore, function, old_fn, new_fn, w_diff_fn)) {
		UNLINK(w_diff_fn);
		ERRHNDL(0, "oldnew2w_diff returns 0 in oldnew2c_diff", "", 1);
	}

	rtrn = w_diff2c_diff(mindiff, w_diff_fn, c_diff_fn);

	UNLINK(w_diff_fn);
	return rtrn;
}

/* oldnew2c_diff */


int
oldnew2lwc_diff(int mindiff, int LWC, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *lwc_diff_fn)
{
	TRACE(fprintf(STDERR, "oldnew2lwc_diff(%i,%i,%i,%s,%s,%s,%s,%s)\n", mindiff, LWC, context, ignore, function, old_fn, new_fn, lwc_diff_fn));

	switch (LWC) {
	case 0:
		return oldnew2l_diff(mindiff, context, ignore, function, old_fn, new_fn, lwc_diff_fn);
	case 1:
		return oldnew2w_diff(mindiff, context, ignore, function, old_fn, new_fn, lwc_diff_fn);
	case 2:
		return oldnew2c_diff(mindiff, context, ignore, function, old_fn, new_fn, lwc_diff_fn);
	default:
		ErrXit("oldnew2lwc_diff called with wrong LWC", "", 1);
	}
	return 0;
}

/* oldnew2lwc_diff */


static char *doctype = "\
<!DOCTYPE html PUBLIC '-//W3C//DTD XHTML 1.0 Transitional//EN'\n\
                      'http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd'>\n\
";
static char *stylesheet = "\
.blue { color: #0000ff; }\n\
.lightblue { color: #00aaff; }\n\
.red { color: #ff0000; }\n\
.lightred { color: #ffaa00; }\n\
.pink { color: #ff00ff; }\n\
.lightpink { color: #ffaaff; }\n\
.colhead { font-family: monospace; color: blue; font-size: medium; }\n\
caption { font-weight: bold; font-family: sans-serif; }\n\
body { background-color: white; color: black; }\n\
a:link { color: green; }\n\
a:visited { color: darkgreen; }\n\
a:active { color: lime; }\n\
td { color: black; font-size: xx-small; font-family: monospace; white-space: pre-wrap; vertical-align: baseline; }\n\
";

int
lwc_diff2html(char *old_fn, char *new_fn, char *lwc_diff_fn, char *html_fn, char *caption, char *revision)
{
	FILE *html_fp, *lwc_diff_fp, *clmn_fp[5];
	char line[BUFLEN], fn_clmn[CMDLEN], *clmn_fn[5], c[3], *ok;
	char *old = NULL, *new = NULL, *old_time, *new_time, olns[24], nlns[24];
	int oln, nln, orn, nrn, i, clr[5], newline, newline_, minor = 0, Minor = 0, Major = 0;

	TRACE(fprintf(STDERR, "lwc_diff2html(%s,%s,%s,%s)\n", lwc_diff_fn, html_fn, caption, revision));

	lwc_diff_fp = Rfopen(lwc_diff_fn);

	if (!(ok = fgets(line, BUFLEN, lwc_diff_fp)))
		/*{ fclose(lwc_diff_fp); ERRHNDL(0,"empty file in lwc_diff2html:",lwc_diff_fn,1); } */
	{
		html_fp = Wfopen(html_fn);
/*
      fprintf(html_fp,"Content-type: text/html\n\n");
*/
		fprintf(html_fp, "%s<html>\n<head>\n<style>\n%s</style>\n</head>\n<body>\n<table align='center' border='1' cellspacing='0' cellpadding='1'>\n", doctype, stylesheet);
		if (*caption)
			fprintf(html_fp, "<caption>%s</caption>\n", caption);
		fprintf(html_fp, "<tr>");
		if (!new_fn)
			fprintf(html_fp, "<th></th>");
		fprintf(html_fp, "<th class='colhead'><a href='%s'>%s%s</a></th>", filename(old_fn), filename(old_fn), revision);
		fprintf(html_fp, "<th></th>");
		if (new_fn)
			fprintf(html_fp, "<th class='colhead'><a href='%s'>%s</a></th>", new_fn, new_fn);
		fprintf(html_fp, "</tr>\n");
		fprintf(html_fp, "<tr><th colspan='3' align='center'>No differences.</th></tr>\n");
		fprintf(html_fp, "</table>\n");
		fprintf(html_fp, "<hr/>\n");
		fprintf(html_fp, "</body>\n</html>\n");
		fprintf(html_fp, "<!--NoDiffs-->\n");
		fflush(html_fp);
		fclose(html_fp);
		return 0;
	}

	sprintf(fn_clmn, "%s%c.difflib-%ld-lwc_diff2html-clmn-0-0", tmpdir(), DIR_SEP, (long) getpid());
	for (i = 0; i < 5; i++) {
		clmn_fn[i] = strdup(fn_clmn);
		clmn_fn[i][strlen(clmn_fn[i]) - 3] += i;
	}

	html_fp = Wfopen(html_fn);
/*
  fprintf(html_fp,"Content-type: text/html\n\n");
*/
	fprintf(html_fp, "%s<html>\n<head>\n<style>\n%s</style>\n</head>\n<body>\n", doctype, stylesheet);
	fprintf(html_fp, "<table align='center' border='1' cellspacing='0' cellpadding='1' rules='groups'>\n");
	if (*caption)
		fprintf(html_fp, "<caption>%s</caption>\n", caption);

	fprintf(html_fp, "<colgroup/><colgroup/><colgroup/><colgroup/><colgroup/><colgroup/><colgroup/>\n");

	line[strlen(line) - 1] = '\0';
	while (ok && strncmp(line, "@@ -", 4)) {
		if (!strncmp(line, "--- ", 4))
			old = strdup(line + 4);
		else if (!strncmp(line, "+++ ", 4))
			new = strdup(line + 4);
		else
			fprintf(html_fp, "<tbody><tr><td colspan='7'>%s</td></tr></tbody>\n", HTMLsave(line));
		ok = fgets(line, BUFLEN, lwc_diff_fp);
		line[strlen(line) - 1] = '\0';
	}
	old_time = strchr(old, '\t');
	*old_time++ = '\0';
	new_time = strchr(new, '\t');
	*new_time++ = '\0';
	fprintf(html_fp, "<thead><tr><th colspan='3' align='center' class='colhead'><a href='%s'>%s%s</a> %s</th>", filename(old), filename(old_fn), revision, old_time);
	fprintf(html_fp, "<th></th>");
	fprintf(html_fp, "<th colspan='3' align='center' class='colhead'><a href='%s'>%s</a> %s</th></tr></thead>\n", new, new_fn, new_time);
	free(old);
	free(new);
	while (ok) {

		for (i = 0; i < 5; i++)
			clmn_fp[i] = Wfopen(clmn_fn[i]);
		sscanf(line, "@@ -%s +%s @@", olns, nlns);
		oln = atoi(olns);
		nln = atoi(nlns);
		if ((oln > 1) && (nln > 1)) {
			fprintf(html_fp, "<tbody><tr><td align='center'>...</td>");
			fprintf(html_fp, "<td align='center'>...</td>");
			fprintf(html_fp, "<td align='center'>...</td>");
			fprintf(html_fp, "<td></td>");
			fprintf(html_fp, "<td align='center'>...</td>");
			fprintf(html_fp, "<td align='center'>...</td>");
			fprintf(html_fp, "<td align='center'>...</td></tr></tbody>\n");
		}
		for (i = 0; i < 3 && ok; i++)
			ok = strchr(ok+1, '@');
		if (ok && ok[1] == ' ')
			fprintf(html_fp, "<td colspan='7' align='center'>%s</td>\n", HTMLsave(ok + 2));
		for (i = 0; i < 5; i++)
			clr[i] = 0;
		orn = nrn = 0;
		newline_ = 1;
		newline = 1;
		sprintf(c, "  ");
		ok = line;
		while (ok && (ok = fgets(line, BUFLEN, lwc_diff_fp)) && strchr(" -+", line[0])) {
			if (line[1] != '\3') {
				size_t sl = strlen(line) - 1;
				char nl = line[sl], l0, l1;

				if (newline_ || newline)
					Minor |= (minor = (strchr("#=\n\2", line[1]) != NULL));
				line[sl] = '\0';
				if (line[1] == '\2')
					sprintf(line + 1, " ");
				l0 = line[0];
				l1 = line[1];
				if (line[0] == ' ') {
					if (newline && (nrn < orn)) {
						while (nrn < orn) {
							SETBLUE(1, minor);
							fprintf(clmn_fp[1], "%i", oln++);
							SETBLACK(1);
							fprintf(clmn_fp[1], "\n");
							SETBLUE(2, minor);
							fprintf(clmn_fp[2], "-");
							SETBLACK(2);
							fprintf(clmn_fp[2], "\n");
							fprintf(clmn_fp[3], "\n");
							fprintf(clmn_fp[4], "\n");
							nrn++;
						}
					}
					SETBLACK(0);
					SETBLACK(4);
				}
				if (line[0] == '-') {
					c[0] = '-';
					SETBLUE(0, minor);
				}
				if (line[0] == '+') {
					c[1] = '+';
					SETRED(4, minor);
				}
				if (line[1] != '\1') {
					line[sl] = nl;
					line[sl+1] = '\0';
					i = 1;
					do {
						sl = strlen(line) - 1;
						nl = line[sl];
						if (nl == '\n') {
							line[sl] = '\0';
						}
						if (strchr(" -", l0)) {
							fprintf(clmn_fp[0], "%s", HTMLsave(line + i));
						}
						if (strchr(" +", l0)) {
							fprintf(clmn_fp[4], "%s", HTMLsave(line + i));
						}
						i = 0;
					} while (nl != '\n' && (ok = fgets(line, BUFLEN, lwc_diff_fp)));
					if (strchr(" -", l0)) {
						Major |= (clr[0] & 1);
					}
					if (strchr(" +", l0)) {
						Major |= (clr[4] & 1);
					}
				} else {
					if (line[0] == '-') {
						SETBLACK(0);
						fprintf(clmn_fp[0], "\n");
						orn++;
					}
					if (line[0] == '+') {
						if (orn > nrn) {
							SETPINK(1, minor);
							fprintf(clmn_fp[1], "%i", oln++);
							SETBLACK(1);
							fprintf(clmn_fp[1], "\n");
							SETPINK(2, minor);
							fprintf(clmn_fp[2], "!");
							SETBLACK(2);
							fprintf(clmn_fp[2], "\n");
							SETPINK(3, minor);
							fprintf(clmn_fp[3], "%i", nln++);
							SETBLACK(3);
							fprintf(clmn_fp[3], "\n");
						} else {
							SETBLACK(0);
							fprintf(clmn_fp[0], "\n");
							orn++;
							SETBLACK(1);
							fprintf(clmn_fp[1], "\n");
							SETRED(2, minor);
							fprintf(clmn_fp[2], "+");
							SETBLACK(2);
							fprintf(clmn_fp[2], "\n");
							SETRED(3, minor);
							fprintf(clmn_fp[3], "%i", nln++);
							SETBLACK(3);
							fprintf(clmn_fp[3], "\n");
						}
						SETBLACK(4);
						fprintf(clmn_fp[4], "\n");
						nrn++;
					}
					if (line[0] == ' ') {
						if (!strncmp(c, "  ", 2)) {
							SETBLACK(1);
							SETBLACK(2);
							SETBLACK(3);
							fprintf(clmn_fp[2], "\n");
						} else {
							SETPINK(1, minor);
							SETPINK(3, minor);
						}
						if (!strncmp(c, "-+", 2)) {
							SETPINK(2, minor);
							fprintf(clmn_fp[2], "!");
							SETBLACK(2);
							fprintf(clmn_fp[2], "\n");
						}
						if (!strncmp(c, "- ", 2)) {
							SETBLUE(2, minor);
							fprintf(clmn_fp[2], "-");
							SETBLACK(2);
							fprintf(clmn_fp[2], "\n");
						}
						if (!strncmp(c, " +", 2)) {
							SETRED(2, minor);
							fprintf(clmn_fp[2], "+");
							SETBLACK(2);
							fprintf(clmn_fp[2], "\n");
						}
						fprintf(clmn_fp[1], "%i", oln++);
						SETBLACK(1);
						fprintf(clmn_fp[1], "\n");
						fprintf(clmn_fp[3], "%i", nln++);
						SETBLACK(3);
						fprintf(clmn_fp[3], "\n");
						SETBLACK(0);
						fprintf(clmn_fp[0], "\n");
						SETBLACK(4);
						fprintf(clmn_fp[4], "\n");
					}
					sprintf(c, "  ");
				}
				newline_ = newline;
				newline = (l1 == '\1');
			}
		}

		if (nrn < orn) {
			SETBLACK(3);
			SETBLACK(4);
			while (nrn < orn) {
				SETBLUE(1, minor);
				fprintf(clmn_fp[1], "%i", oln++);
				SETBLACK(1);
				fprintf(clmn_fp[1], "\n");
				SETBLUE(2, minor);
				fprintf(clmn_fp[2], "-");
				SETBLACK(2);
				fprintf(clmn_fp[2], "\n");
				fprintf(clmn_fp[3], "\n");
				fprintf(clmn_fp[4], "\n");
				nrn++;
			}
		}
		if (orn < nrn) {
			SETBLACK(0);
			SETBLACK(1);
			while (orn < nrn) {
				fprintf(clmn_fp[0], "\n");
				orn++;
				fprintf(clmn_fp[1], "\n");
				SETRED(2, minor);
				fprintf(clmn_fp[2], "+");
				SETBLACK(2);
				fprintf(clmn_fp[2], "\n");
				SETRED(3, minor);
				fprintf(clmn_fp[3], "%i", nln++);
				SETBLACK(3);
				fprintf(clmn_fp[3], "\n");
			}
		}

		for (i = 0; i < 5; i++) {
			fclose(clmn_fp[i]);
			clmn_fp[i] = Rfopen(clmn_fn[i]);
		}

		fprintf(html_fp, "<tbody>\n");
		for (;;) {
			char ln[BUFLEN], buf1[128], buf2[128], buf3[128];

			if (fgets(ln, sizeof(ln), clmn_fp[0]) == NULL)
				break;
			if (fgets(buf1, sizeof(buf1), clmn_fp[1]) == NULL)
				break; /* shouldn't happen */
			buf1[strlen(buf1) - 1] = 0;
			if (fgets(buf2, sizeof(buf2), clmn_fp[2]) == NULL)
				break; /* shouldn't happen */
			buf2[strlen(buf2) - 1] = 0;
			if (fgets(buf3, sizeof(buf3), clmn_fp[3]) == NULL)
				break; /* shouldn't happen */
			buf3[strlen(buf3) - 1] = 0;

			fprintf(html_fp, "<tr>");
			fprintf(html_fp, "<td align='center'>%s</td>\n", buf1);
			fprintf(html_fp, "<td>");
			while (ln[strlen(ln) - 1] != '\n') {
				fprintf(html_fp, "%s", ln);
				if (!fgets(ln, sizeof(ln), clmn_fp[0])) {
					ln[0] = '\n';
					ln[1] = 0;
					break;
				}
			}
			ln[strlen(ln) - 1] = 0;
			fprintf(html_fp, "%s</td>\n", ln);
			fprintf(html_fp, "<td align='center'>%s</td>\n", buf1);
			fprintf(html_fp, "<td align='center'>%s</td>\n", buf2);
			fprintf(html_fp, "<td align='center'>%s</td>\n", buf3);
			if (fgets(ln, sizeof(ln), clmn_fp[4]) == NULL)
				break; /* shouldn't happen */
			fprintf(html_fp, "<td>");
			while (ln[strlen(ln) - 1] != '\n') {
				fprintf(html_fp, "%s", ln);
				if (!fgets(ln, sizeof(ln), clmn_fp[4])) {
					ln[0] = '\n';
					ln[1] = 0;
					break;
				}
			}
			ln[strlen(ln) - 1] = 0;
			fprintf(html_fp, "%s</td>\n", ln);
			fprintf(html_fp, "<td align='center'>%s</td>\n", buf3);
			fprintf(html_fp, "</tr>\n");
		}
		fprintf(html_fp, "</tbody>\n");
		for (i = 0; i < 5; i++)
			fclose(clmn_fp[i]);

		TRACE(for (i = 0; i < 5; i++) clmn_fn[i][strlen(clmn_fn[i]) - 1]++) ;
	}

	fprintf(html_fp, "</table>\n");
	fprintf(html_fp, "<hr/>\n");
	fprintf(html_fp, "</body>\n</html>\n");
	fprintf(html_fp, "<!--%sDiffs-->\n", Major ? "Major" : (Minor ? "Minor" : "No"));
	fflush(html_fp);
	fclose(html_fp);

	for (i = 0; i < 5; i++) {
		UNLINK(clmn_fn[i]);
		free(clmn_fn[i]);
	}

	fclose(lwc_diff_fp);
	return (Major ? 2 : (Minor != 0));
}

/* lwc_diff2html */


int
oldnew2html(int mindiff, int LWC, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *html_fn, char *caption, char *revision)
{
	char lwc_diff_fn[CMDLEN];
	int rtrn;

	TRACE(fprintf(STDERR, "oldnew2html(%i,%i,%i,%s,%s,%s,%s,%s,%s,%s)\n", mindiff, LWC, context, ignore, function, old_fn, new_fn, html_fn, caption, revision));

	sprintf(lwc_diff_fn, "%s%c.difflib-%ld-oldnew2html-lwc_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!oldnew2lwc_diff(mindiff, LWC, context, ignore, function, old_fn, new_fn, lwc_diff_fn))
		/* { UNLINK(lwc_diff_fn); ERRHNDL(0,"oldnew2lwc_diff returns 0 in oldnew2html","",1); } */
		fclose(Wfopen(lwc_diff_fn));

	rtrn = lwc_diff2html(old_fn, new_fn, lwc_diff_fn, html_fn, caption, revision);

	UNLINK(lwc_diff_fn);
	return rtrn;
}

/* oldnew2u_diff */
