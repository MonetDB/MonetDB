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

#ifndef DIR_SEP
# define DIR_SEP '\\'
#endif

#define getpid _getpid
#define unlink _unlink
#define strdup _strdup
#else
#define DIFF	"diff"

#ifndef DIR_SEP
# define DIR_SEP '/'
#endif
#endif

#define SYSTEM(cmd)	{ int r;					    \
			  TRACE(fprintf(STDERR,"%s => \n",cmd));	    \
			  r = system(cmd); (void) r;			    \
			  TRACE(fprintf(STDERR,"\t => %i\n",r));	  }

#ifdef DEBUG
#define UNLINK(x)
#define ERRHNDL(r,s,t,u) ErrXit(s,t,u)
#else
#define UNLINK(x) unlink(x)
#define ERRHNDL(r,s,t,u) return r
#endif

#define SETBLACK(f)  if(clr[f]!=0  ) { fprintf(clmn_fp[f],"</FONT><FONT SIZE=1 COLOR=#000000>"              ); clr[f]=0;   }
#define SETBLUE(f,m) if(clr[f]!=3-m) { fprintf(clmn_fp[f],"</FONT><FONT SIZE=1 COLOR=#00%sff>",(m?"aa":"00")); clr[f]=3-m; }
#define SETRED(f,m)  if(clr[f]!=5-m) { fprintf(clmn_fp[f],"</FONT><FONT SIZE=1 COLOR=#ff%s00>",(m?"aa":"00")); clr[f]=5-m; }
#define SETPINK(f,m) if(clr[f]!=7-m) { fprintf(clmn_fp[f],"</FONT><FONT SIZE=1 COLOR=#ff%sff>",(m?"aa":"00")); clr[f]=7-m; }

#define BUFLEN  16384
#define BUFLEN2 32768

#if !HAVE_DECL_STRDUP
#ifdef HAVE_STRDUP
extern char *strdup(const char *);
#else
#define strdup(s)      strcpy(malloc(strlen(s)),(s))
#endif
#endif

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


void
markNL(FILE *fp, int k)
{
	int i;

	for (i = 0; i < 6; i++)
		fprintf(fp, "@+-%06i\n", k);
}


int
oldnew2u_diff(int mindiff, int context, char *ignore, char *old_fn, char *new_fn, char *u_diff_fn)
{
	char command[BUFLEN];
	struct stat old_fstat, new_fstat;
	FILE *u_diff_fp, *oldnew_fp;
	char c, line[BUFLEN];
	char *_d = mindiff ? "-d" : "";

	TRACE(fprintf(STDERR, "oldnew2u_diff(%i,%i,%s,%s,%s,%s)\n", mindiff, context, ignore, old_fn, new_fn, u_diff_fn));

	stat(old_fn, &old_fstat);
	stat(new_fn, &new_fstat);

	if ((old_fstat.st_size != 0) && (new_fstat.st_size != 0)) {
#ifdef NATIVE_WIN32
		sprintf(command, "%s %s %s.cp > nul", COPY, old_fn, old_fn);
		SYSTEM(command);
		sprintf(command, "%s %s %s.cp > nul", COPY, new_fn, new_fn);
		SYSTEM(command);

		sprintf(command, "%s %s -a %s -U%d %s.cp %s.cp > %s", DIFF, ignore, _d, context, old_fn, new_fn, u_diff_fn);
#else
		sprintf(command, "%s %s -a %s -U%d %s    %s    > %s", DIFF, ignore, _d, context, old_fn, new_fn, u_diff_fn);
#endif

		SYSTEM(command);

#ifdef NATIVE_WIN32
#ifdef DEBUG
		sprintf(command, "dir %s* %s* %s*", old_fn, new_fn, u_diff_fn);
		SYSTEM(command);
#endif
		sprintf(command, "del %s.cp %s.cp", old_fn, new_fn);
		SYSTEM(command);
#endif
	} else {
		u_diff_fp = Wfopen(u_diff_fn);
		fprintf(u_diff_fp, "--- %s\t%s", old_fn, ctime(&old_fstat.st_mtime));
		fprintf(u_diff_fp, "+++ %s\t%s", new_fn, ctime(&new_fstat.st_mtime));
		if (old_fstat.st_size == 0) {
			c = '+';
			fprintf(u_diff_fp, "@@ -0,0 +1,0 @@\n");
			oldnew_fp = Rfopen(new_fn);
		} else {
			c = '-';
			fprintf(u_diff_fp, "@@ -1,0 +0,0 @@\n");
			oldnew_fp = Rfopen(old_fn);
		}
		while (fgets(line, BUFLEN, oldnew_fp)) {
			fprintf(u_diff_fp, "%c%s", c, line);
		}
		fclose(oldnew_fp);
		fflush(u_diff_fp);
		fclose(u_diff_fp);
	}

	return 1;
}

/* oldnew2u_diff */


int
u_diff2l_diff(char *u_diff_fn, char *l_diff_fn)
{
	FILE *u_diff_fp, *l_diff_fp;
	char *ok, line[BUFLEN];

	TRACE(fprintf(STDERR, "u_diff2l_diff(%s,%s)\n", u_diff_fn, l_diff_fn));

	u_diff_fp = Rfopen(u_diff_fn);
	if (!(ok = fgets(line, BUFLEN, u_diff_fp))) {
		fclose(u_diff_fp);
		ERRHNDL(0, "empty file in u_diff2l_diff:", u_diff_fn, 1);
	}

	l_diff_fp = Wfopen(l_diff_fn);
	while (ok && strncmp(line, "@@ -", 4)) {
		fprintf(l_diff_fp, "%s", line);
		ok = fgets(line, BUFLEN, u_diff_fp);
	}
	while (ok) {
		fprintf(l_diff_fp, "%s", line);
		while ((ok = fgets(line, BUFLEN, u_diff_fp)) && strchr(" -+", line[0])) {
			if (line[1] == '\n')
				sprintf(line + 1, "\2\n");
			fprintf(l_diff_fp, "%s%c\1\n", line, line[0]);
		}
	}
	fflush(l_diff_fp);
	fclose(l_diff_fp);

	fclose(u_diff_fp);
	return 1;
}

/* u_diff2l_diff */


int
lw_diff2wc_diff(int mindiff, int doChar, char *lw_diff_fn, char *wc_diff_fn)
{
	FILE *lw_diff_fp, *wc_diff_fp, *fp[2], *pipe_fp;
	char line[BUFLEN], command[BUFLEN], pipe_ln[BUFLEN], pipe_fn[1024];
	char *ok, *fn[2];
	size_t i;
	int j;
	int space, alpha_, digit, l[2], k[2];
	char wc_old_fn[BUFLEN], wc_new_fn[BUFLEN];
	char *_d = mindiff ? "-d" : "";

	TRACE(fprintf(STDERR, "lw_diff2wc_diff(%i,%i,%s,%s)\n", mindiff, doChar, lw_diff_fn, wc_diff_fn));

	sprintf(pipe_fn, "%s-pipe", wc_diff_fn);

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
		fprintf(wc_diff_fp, "%s", line);
		fflush(wc_diff_fp);
		fclose(wc_diff_fp);

		l[0] = l[1] = k[0] = k[1] = 0;
		for (j = 0; j < 2; j++)
			fp[j] = Wfopen(fn[j]);
		while ((ok = fgets(line, BUFLEN, lw_diff_fp)) && strchr(" -+", line[0])) {
			if (line[0] == ' ') {
				while (k[0] < k[1]) {
					markNL(fp[0], k[0]++);
					l[0]++;
				}
				while (k[1] < k[0]) {
					markNL(fp[1], k[1]++);
					l[1]++;
				}
				for (j = 0; j < 2; j++) {
					fprintf(fp[j], "%s", line + 1);
					l[j]++;
				}
				if (line[1] == '\1')
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
					for (i = 1; i < strlen(line) - 1; i++) {
						fprintf(fp[j], "%c\n", line[i]);
						l[j]++;
					}
				} else {
					space = isspace((int) (line[1]));
					alpha_ = isalpha_((int) (line[1]));
					digit = isdigit((int) (line[1]));
					for (i = 1; i < strlen(line) - 1; i++) {
						if ((space && !isspace((int) line[i])) ||
						    (!space && isspace((int) line[i])) ||
						    (alpha_ && !isalpha_((int) line[i])) ||
						    (!alpha_ && isalpha_((int) line[i])) ||
						    (digit && !isdigit((int) line[i])) ||
						    (!digit && isdigit((int) line[i])) ||
						    (!isspace((int) line[i]) &&
						     !isalpha_((int) line[i]) &&
						     !isdigit((int) line[i]))) {
							fprintf(fp[j], "\n");
							space = isspace((int) line[i]);
							alpha_ = isalpha_((int) line[i]);
							digit = isdigit((int) line[i]);
							l[j]++;
						}
						fprintf(fp[j], "%c", line[i]);
					}
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

#ifdef NATIVE_WIN32
		sprintf(command, "%s %s %s.cp > nul", COPY, fn[0], fn[0]);
		SYSTEM(command);
		sprintf(command, "%s %s %s.cp > nul", COPY, fn[1], fn[1]);
		SYSTEM(command);

		sprintf(command, "%s -a %s -U%d %s.cp %s.cp > %s", DIFF, _d, MAX(l[0], l[1]), fn[0], fn[1], pipe_fn);
#else
		sprintf(command, "%s -a %s -U%d %s %s > %s", DIFF, _d, MAX(l[0], l[1]), fn[0], fn[1], pipe_fn);
#endif

		SYSTEM(command);

#ifdef NATIVE_WIN32
#ifdef DEBUG
		sprintf(command, "dir %s* %s* %s*", fn[0], fn[1], pipe_fn);
		SYSTEM(command);
#endif
		sprintf(command, "del %s.cp %s.cp", fn[0], fn[1]);
		SYSTEM(command);
#endif

		pipe_fp = Rfopen(pipe_fn);
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
		fclose(pipe_fp);
		UNLINK(pipe_fn);
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
l_diff2c_diff(int mindiff, char *l_diff_fn, char *c_diff_fn)
{
	char w_diff_fn[BUFLEN];
	int rtrn = 0;

	TRACE(fprintf(STDERR, "l_diff2c_diff(%i,%s,%s)\n", mindiff, l_diff_fn, c_diff_fn));

	sprintf(w_diff_fn, "%s%c.difflib-%ld-l_diff2c_diff-w_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!l_diff2w_diff(mindiff, l_diff_fn, w_diff_fn)) {
		UNLINK(w_diff_fn);
		ERRHNDL(0, "l_diff2w_diff returns 0 in l_diff2c_diff", "", 1);
	}

	rtrn = w_diff2c_diff(mindiff, w_diff_fn, c_diff_fn);

	UNLINK(w_diff_fn);
	return rtrn;
}

/* l_diff2c_diff */


int
u_diff2w_diff(int mindiff, char *u_diff_fn, char *w_diff_fn)
{
	char l_diff_fn[BUFLEN];
	int rtrn = 0;

	TRACE(fprintf(STDERR, "u_diff2w_diff(%i,%s,%s)\n", mindiff, u_diff_fn, w_diff_fn));

	sprintf(l_diff_fn, "%s%c.difflib-%ld-u_diff2w_diff-l_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!u_diff2l_diff(u_diff_fn, l_diff_fn)) {
		UNLINK(l_diff_fn);
		ERRHNDL(0, "u_diff2l_diff returns 0 in u_diff2w_diff", "", 1);
	}

	rtrn = l_diff2w_diff(mindiff, l_diff_fn, w_diff_fn);

	UNLINK(l_diff_fn);
	return rtrn;
}

/* u_diff2w_diff */


int
u_diff2c_diff(int mindiff, char *u_diff_fn, char *c_diff_fn)
{
	char w_diff_fn[BUFLEN];
	int rtrn = 0;

	TRACE(fprintf(STDERR, "u_diff2c_diff(%i,%s,%s)\n", mindiff, u_diff_fn, c_diff_fn));

	sprintf(w_diff_fn, "%s%c.difflib-%ld-u_diff2c_diff-w_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!u_diff2w_diff(mindiff, u_diff_fn, w_diff_fn)) {
		UNLINK(w_diff_fn);
		ERRHNDL(0, "u_diff2w_diff returns 0 in u_diff2c_diff", "", 1);
	}

	rtrn = w_diff2c_diff(mindiff, w_diff_fn, c_diff_fn);

	UNLINK(w_diff_fn);
	return rtrn;
}

/* u_diff2c_diff */


int
u_diff2lwc_diff(int mindiff, int LWC, char *u_diff_fn, char *lwc_diff_fn)
{
	TRACE(fprintf(STDERR, "u_diff2lwc_diff(%i,%s,%s)\n", LWC, u_diff_fn, lwc_diff_fn));

	switch (LWC) {
	case 0:
		return (u_diff2l_diff(u_diff_fn, lwc_diff_fn));
	case 1:
		return (u_diff2w_diff(mindiff, u_diff_fn, lwc_diff_fn));
	case 2:
		return (u_diff2c_diff(mindiff, u_diff_fn, lwc_diff_fn));
	default:
		ErrXit("u_diff2lwc_diff called with wrong LWC", "", 1);
	}
	return 0;
}

/* u_diff2lwc_diff */


int
oldnew2l_diff(int mindiff, int context, char *ignore, char *old_fn, char *new_fn, char *l_diff_fn)
{
	char u_diff_fn[BUFLEN];
	int rtrn = 0;

	TRACE(fprintf(STDERR, "oldnew2l_diff(%i,%i,%s,%s,%s,%s)\n", mindiff, context, ignore, old_fn, new_fn, l_diff_fn));

	sprintf(u_diff_fn, "%s%c.difflib-%ld-oldnew2l_diff-u_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!oldnew2u_diff(mindiff, context, ignore, old_fn, new_fn, u_diff_fn)) {
		UNLINK(u_diff_fn);
		ERRHNDL(0, "oldnew2u_diff returns 0 in oldnew2l_diff", "", 1);
	}

	rtrn = u_diff2l_diff(u_diff_fn, l_diff_fn);

	UNLINK(u_diff_fn);
	return rtrn;
}

/* oldnew2l_diff */


int
oldnew2w_diff(int mindiff, int context, char *ignore, char *old_fn, char *new_fn, char *w_diff_fn)
{
	char l_diff_fn[BUFLEN];
	int rtrn = 0;

	TRACE(fprintf(STDERR, "oldnew2w_diff(%i,%i,%s,%s,%s,%s)\n", mindiff, context, ignore, old_fn, new_fn, w_diff_fn));

	sprintf(l_diff_fn, "%s%c.difflib-%ld-oldnew2w_diff-l_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!oldnew2l_diff(mindiff, context, ignore, old_fn, new_fn, l_diff_fn)) {
		UNLINK(l_diff_fn);
		ERRHNDL(0, "oldnew2l_diff returns 0 in oldnew2w_diff", "", 1);
	}

	rtrn = l_diff2w_diff(mindiff, l_diff_fn, w_diff_fn);

	UNLINK(l_diff_fn);
	return rtrn;
}

/* oldnew2w_diff */


int
oldnew2c_diff(int mindiff, int context, char *ignore, char *old_fn, char *new_fn, char *c_diff_fn)
{
	char w_diff_fn[BUFLEN];
	int rtrn = 0;

	TRACE(fprintf(STDERR, "oldnew2c_diff(%i,%i,%s,%s,%s,%s)\n", mindiff, context, ignore, old_fn, new_fn, c_diff_fn));

	sprintf(w_diff_fn, "%s%c.difflib-%ld-oldnew2c_diff-w_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!oldnew2w_diff(mindiff, context, ignore, old_fn, new_fn, w_diff_fn)) {
		UNLINK(w_diff_fn);
		ERRHNDL(0, "oldnew2w_diff returns 0 in oldnew2c_diff", "", 1);
	}

	rtrn = w_diff2c_diff(mindiff, w_diff_fn, c_diff_fn);

	UNLINK(w_diff_fn);
	return rtrn;
}

/* oldnew2c_diff */


int
oldnew2lwc_diff(int mindiff, int LWC, int context, char *ignore, char *old_fn, char *new_fn, char *lwc_diff_fn)
{
	TRACE(fprintf(STDERR, "oldnew2lwc_diff(%i,%i,%i,%s,%s,%s,%s)\n", mindiff, LWC, context, ignore, old_fn, new_fn, lwc_diff_fn));

	switch (LWC) {
	case 0:
		return oldnew2l_diff(mindiff, context, ignore, old_fn, new_fn, lwc_diff_fn);
	case 1:
		return oldnew2w_diff(mindiff, context, ignore, old_fn, new_fn, lwc_diff_fn);
	case 2:
		return oldnew2c_diff(mindiff, context, ignore, old_fn, new_fn, lwc_diff_fn);
	default:
		ErrXit("oldnew2lwc_diff called with wrong LWC", "", 1);
	}
	return 0;
}

/* oldnew2lwc_diff */



int
lwc_diff2html(char *old_fn, char *new_fn, char *lwc_diff_fn, char *html_fn, char *caption, char *revision)
{
	FILE *html_fp, *lwc_diff_fp, *clmn_fp[5];
	char line[BUFLEN], ln[BUFLEN], fn_clmn[BUFLEN], *clmn_fn[5], c[3], *ok;
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
		fprintf(html_fp, "<HTML>\n<BODY BGCOLOR=#ffffff TEST=#000000 LINK=#00AA00 VLINK=#005500 ALINK=#00ff00>\n");
		fprintf(html_fp, "<CENTER>\n<TABLE ALIGN=ABSCENTER BORDER=1 CELLSPACING=0 CELLPADDING=1>\n");
		if (*caption)
			fprintf(html_fp, "<CAPTION><FONT FACE='helvetica, arial'><B>%s</B></FONT></CAPTION>\n", caption);
		fprintf(html_fp, "<TR>");
		if (!new_fn)
			fprintf(html_fp, "<TH>&nbsp;</TH>");
		fprintf(html_fp, "<TH><FONT SIZE=3 COLOR=#0000ff><CODE><A HREF='%s'>%s%s</A></CODE></FONT></TH>", filename(old_fn), filename(old_fn), revision);
		fprintf(html_fp, "<TH>&nbsp;</TH>");
		if (new_fn)
			fprintf(html_fp, "<TH><FONT SIZE=3 COLOR=#ff0000><CODE><A HREF='%s'>%s</A></CODE></FONT></TH>", new_fn, new_fn);
		fprintf(html_fp, "</TR>\n");
		fprintf(html_fp, "<TR><TH COLSPAN=3 ALIGN=CENTER>No differences.</TH></TR>\n");
		fprintf(html_fp, "</TABLE></CENTER>\n");
		fprintf(html_fp, "<HR>\n");
		fprintf(html_fp, "</BODY>\n</HTML>\n");
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
	fprintf(html_fp, "<HTML>\n<BODY BGCOLOR=#ffffff TEST=#000000 LINK=#00AA00 VLINK=#005500 ALINK=#00ff00>\n");
	fprintf(html_fp, "<CENTER>\n<TABLE ALIGN=ABSCENTER BORDER=1 CELLSPACING=0 CELLPADDING=1>\n");
	if (*caption)
		fprintf(html_fp, "<CAPTION><FONT FACE='helvetica, arial'><B>%s</B></FONT></CAPTION>\n", caption);

	line[strlen(line) - 1] = '\0';
	while (ok && strncmp(line, "@@ -", 4)) {
		if (!strncmp(line, "--- ", 4))
			old = strdup(line + 4);
		else if (!strncmp(line, "+++ ", 4))
			new = strdup(line + 4);
		else
			fprintf(html_fp, "<TR><TD COLSPAN=7><FONT SIZE=1 COLOR=#000000><CODE>%s</CODE></FONT></TD></TR>\n", HTMLsave(line));
		ok = fgets(line, BUFLEN, lwc_diff_fp);
		line[strlen(line) - 1] = '\0';
	}
	old_time = strchr(old, '\t');
	*old_time++ = '\0';
	new_time = strchr(new, '\t') + 1;
	*new_time++ = '\0';
#ifdef NATIVE_WIN32
	if (!strcmp(strrchr(old, '.'), ".cp"))
		*strrchr(old, '.') = '\0';
	if (!strcmp(strrchr(new, '.'), ".cp"))
		*strrchr(new, '.') = '\0';
#endif
	fprintf(html_fp, "<TR><TH COLSPAN=3 ALIGN=CENTER><FONT SIZE=3 COLOR=#0000ff><CODE><A HREF='%s'>%s%s</A>\t%s</CODE></FONT></TH>", filename(old), filename(old_fn), revision, old_time);
	fprintf(html_fp, "<TH>&nbsp;</TH>");
	fprintf(html_fp, "<TH COLSPAN=3 ALIGN=CENTER><FONT SIZE=3 COLOR=#ff0000><CODE><A HREF='%s'>%s</A>\t%s</CODE></FONT></TH></TR>\n", new, new_fn, new_time);
	free(old);
	free(new);
	while (ok) {

		for (i = 0; i < 5; i++)
			clmn_fp[i] = Wfopen(clmn_fn[i]);
		sscanf(line, "@@ -%s +%s @@", olns, nlns);
		oln = atoi(olns);
		nln = atoi(nlns);
		if ((oln > 1) && (nln > 1)) {
			fprintf(html_fp, "<TR><TD ALIGN=CENTER><FONT SIZE=1 COLOR=#000000><CODE>...</CODE></FONT></TD>");
			fprintf(html_fp, "<TD ALIGN=CENTER><FONT SIZE=1 COLOR=#000000><CODE>...</CODE></FONT></TD>");
			fprintf(html_fp, "<TD ALIGN=CENTER><FONT SIZE=1 COLOR=#000000><CODE>...</CODE></FONT></TD>");
			fprintf(html_fp, "<TD>&nbsp;</TD>");
			fprintf(html_fp, "<TD ALIGN=CENTER><FONT SIZE=1 COLOR=#000000><CODE>...</CODE></FONT></TD>");
			fprintf(html_fp, "<TD ALIGN=CENTER><FONT SIZE=1 COLOR=#000000><CODE>...</CODE></FONT></TD>");
			fprintf(html_fp, "<TD ALIGN=CENTER><FONT SIZE=1 COLOR=#000000><CODE>...</CODE></FONT></TD></TR>\n");
		}
		for (i = 0; i < 5; i++)
			clr[i] = 0;
		orn = nrn = 0;
		newline_ = 1;
		newline = 1;
		sprintf(c, "  ");
		while ((ok = fgets(line, BUFLEN, lwc_diff_fp)) && strchr(" -+", line[0])) {
			if (line[1] != '\3') {
				if (newline_ || newline)
					Minor |= (minor = (strchr("#=\n\2", line[1]) ? 1 : 0));
				line[strlen(line) - 1] = '\0';
				if (line[1] == '\2')
					sprintf(line + 1, " ");
				if (line[0] == ' ') {
					if (newline && (nrn < orn)) {
						SETBLUE(1, minor);
						SETBLUE(2, minor);
						while (nrn < orn) {
							fprintf(clmn_fp[1], "%i\n", oln++);
							fprintf(clmn_fp[2], "-\n");
							fprintf(clmn_fp[3], "&nbsp;\n");
							fprintf(clmn_fp[4], "&nbsp;\n");
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
					if (strchr(" -", line[0])) {
						fprintf(clmn_fp[0], "%s", HTMLsave(line + 1));
						Major |= (clr[0] & 1);
					}
					if (strchr(" +", line[0])) {
						fprintf(clmn_fp[4], "%s", HTMLsave(line + 1));
						Major |= (clr[4] & 1);
					}
				} else {
					if (line[0] == '-') {
						fprintf(clmn_fp[0], "\n");
						orn++;
					}
					if (line[0] == '+') {
						if (orn > nrn) {
							SETPINK(1, minor);
							fprintf(clmn_fp[1], "%i\n", oln++);
							SETPINK(2, minor);
							fprintf(clmn_fp[2], "!\n");
							SETPINK(3, minor);
							fprintf(clmn_fp[3], "%i\n", nln++);
						} else {
							SETRED(2, minor);
							SETRED(3, minor);
							fprintf(clmn_fp[0], "&nbsp;\n");
							orn++;
							fprintf(clmn_fp[1], "&nbsp;\n");
							fprintf(clmn_fp[2], "+\n");
							fprintf(clmn_fp[3], "%i\n", nln++);
						}
						fprintf(clmn_fp[4], "\n");
						nrn++;
					}
					if (line[0] == ' ') {
						if (!strncmp(c, "  ", 2)) {
							SETBLACK(1);
							SETBLACK(3);
							fprintf(clmn_fp[2], "&nbsp;\n");
						} else {
							SETPINK(1, minor);
							SETPINK(3, minor);
						}
						if (!strncmp(c, "-+", 2)) {
							SETPINK(2, minor);
							fprintf(clmn_fp[2], "!\n");
						}
						if (!strncmp(c, "- ", 2)) {
							SETBLUE(2, minor);
							fprintf(clmn_fp[2], "-\n");
						}
						if (!strncmp(c, " +", 2)) {
							SETRED(2, minor);
							fprintf(clmn_fp[2], "+\n");
						}
						fprintf(clmn_fp[1], "%i\n", oln++);
						fprintf(clmn_fp[3], "%i\n", nln++);
						fprintf(clmn_fp[0], "\n");
						fprintf(clmn_fp[4], "\n");
					}
					sprintf(c, "  ");
				}
				newline_ = newline;
				newline = (line[1] == '\1');
			}
		}

		if (nrn < orn) {
			SETBLUE(1, minor);
			SETBLUE(2, minor);
			while (nrn < orn) {
				fprintf(clmn_fp[1], "%i\n", oln++);
				fprintf(clmn_fp[2], "-\n");
				fprintf(clmn_fp[3], "&nbsp;\n");
				fprintf(clmn_fp[4], "&nbsp;\n");
				nrn++;
			}
		}
		if (orn < nrn) {
			SETRED(2, minor);
			SETRED(3, minor);
			while (orn < nrn) {
				fprintf(clmn_fp[0], "&nbsp;\n");
				orn++;
				fprintf(clmn_fp[1], "&nbsp;\n");
				fprintf(clmn_fp[2], "+\n");
				fprintf(clmn_fp[3], "%i\n", nln++);
			}
		}

		for (i = 0; i < 5; i++) {
			fflush(clmn_fp[i]);
			fclose(clmn_fp[i]);
		}

		fprintf(html_fp, "<TR>");
		i = 1;
		fprintf(html_fp, "<TD VALIGN=TOP ALIGN=CENTER><PRE><FONT SIZE=1 COLOR=#000000>");
		clmn_fp[i] = Rfopen(clmn_fn[i]);
		while (fgets(ln, BUFLEN, clmn_fp[i]))
			fprintf(html_fp, "%s", ln);
		fclose(clmn_fp[i]);
		fprintf(html_fp, "</FONT></PRE></TD>");
		i = 0;
		fprintf(html_fp, "<TD VALIGN=TOP><PRE><FONT SIZE=1 COLOR=#000000>");
		clmn_fp[i] = Rfopen(clmn_fn[i]);
		while (fgets(ln, BUFLEN, clmn_fp[i]))
			fprintf(html_fp, "%s", ln);
		fclose(clmn_fp[i]);
		fprintf(html_fp, "</FONT></PRE></TD>");
		i = 1;
		fprintf(html_fp, "<TD VALIGN=TOP ALIGN=CENTER><PRE><FONT SIZE=1 COLOR=#000000>");
		clmn_fp[i] = Rfopen(clmn_fn[i]);
		while (fgets(ln, BUFLEN, clmn_fp[i]))
			fprintf(html_fp, "%s", ln);
		fclose(clmn_fp[i]);
		fprintf(html_fp, "</FONT></PRE></TD>");
		i = 2;
		fprintf(html_fp, "<TD VALIGN=TOP ALIGN=CENTER><PRE><FONT SIZE=1 COLOR=#000000>");
		clmn_fp[i] = Rfopen(clmn_fn[i]);
		while (fgets(ln, BUFLEN, clmn_fp[i]))
			fprintf(html_fp, "%s", ln);
		fclose(clmn_fp[i]);
		fprintf(html_fp, "</FONT></PRE></TD>");
		i = 3;
		fprintf(html_fp, "<TD VALIGN=TOP ALIGN=CENTER><PRE><FONT SIZE=1 COLOR=#000000>");
		clmn_fp[i] = Rfopen(clmn_fn[i]);
		while (fgets(ln, BUFLEN, clmn_fp[i]))
			fprintf(html_fp, "%s", ln);
		fclose(clmn_fp[i]);
		fprintf(html_fp, "</FONT></PRE></TD>");
		i = 4;
		fprintf(html_fp, "<TD VALIGN=TOP><PRE><FONT SIZE=1 COLOR=#000000>");
		clmn_fp[i] = Rfopen(clmn_fn[i]);
		while (fgets(ln, BUFLEN, clmn_fp[i]))
			fprintf(html_fp, "%s", ln);
		fclose(clmn_fp[i]);
		fprintf(html_fp, "</FONT></PRE></TD>");
		i = 3;
		fprintf(html_fp, "<TD VALIGN=TOP ALIGN=CENTER><PRE><FONT SIZE=1 COLOR=#000000>");
		clmn_fp[i] = Rfopen(clmn_fn[i]);
		while (fgets(ln, BUFLEN, clmn_fp[i]))
			fprintf(html_fp, "%s", ln);
		fclose(clmn_fp[i]);
		fprintf(html_fp, "</FONT></PRE></TD>");
		fprintf(html_fp, "</TR>\n");

		TRACE(for (i = 0; i < 5; i++) clmn_fn[i][strlen(clmn_fn[i]) - 1]++) ;
	}

	fprintf(html_fp, "</TABLE></CENTER>\n");
	fprintf(html_fp, "<HR>\n");
	fprintf(html_fp, "</BODY>\n</HTML>\n");
	fprintf(html_fp, "<!--%sDiffs-->\n", Major ? "Major" : (Minor ? "Minor" : "No"));
	fflush(html_fp);
	fclose(html_fp);

	for (i = 0; i < 5; i++) {
		UNLINK(clmn_fn[i]);
		free(clmn_fn[i]);
	}

	fclose(lwc_diff_fp);
	return (Major ? 2 : (Minor ? 1 : 0));
}

/* lwc_diff2html */


int
u_diff2html(int mindiff, int LWC, char *u_diff_fn, char *html_fn, char *caption, char *revision)
{
	char lwc_diff_fn[BUFLEN];
	int rtrn;

	TRACE(fprintf(STDERR, "u_diff2html(%i,%s,%s,%s,%s)\n", LWC, u_diff_fn, html_fn, caption, revision));

	sprintf(lwc_diff_fn, "%s%c.difflib-%ld-u_diff2html-lwc_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!u_diff2lwc_diff(mindiff, LWC, u_diff_fn, lwc_diff_fn))
		/* { UNLINK(lwc_diff_fn); ERRHNDL(0,"u_diff2lwc_diff returns 0 in u_diff2html","",1); } */
		system(strconcat("touch ", lwc_diff_fn));

	rtrn = lwc_diff2html(u_diff_fn, (char *) 0, lwc_diff_fn, html_fn, caption, revision);

	UNLINK(lwc_diff_fn);
	return rtrn;
}

/* u_diff2html */


int
oldnew2html(int mindiff, int LWC, int context, char *ignore, char *old_fn, char *new_fn, char *html_fn, char *caption, char *revision)
{
	char lwc_diff_fn[BUFLEN];
	int rtrn;

	TRACE(fprintf(STDERR, "oldnew2html(%i,%i,%i,%s,%s,%s,%s,%s,%s)\n", mindiff, LWC, context, ignore, old_fn, new_fn, html_fn, caption, revision));

	sprintf(lwc_diff_fn, "%s%c.difflib-%ld-oldnew2html-lwc_diff", tmpdir(), DIR_SEP, (long) getpid());

	if (!oldnew2lwc_diff(mindiff, LWC, context, ignore, old_fn, new_fn, lwc_diff_fn))
		/* { UNLINK(lwc_diff_fn); ERRHNDL(0,"oldnew2lwc_diff returns 0 in oldnew2html","",1); } */
		 system(strconcat("touch ", lwc_diff_fn));

	rtrn = lwc_diff2html(old_fn, new_fn, lwc_diff_fn, html_fn, caption, revision);

	UNLINK(lwc_diff_fn);
	return rtrn;
}

/* oldnew2u_diff */
