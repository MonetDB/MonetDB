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

#include <monetdb_config.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "Mx.h"
#include "MxFcnDef.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#if HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_UTIME_H
#include <utime.h>
#else
#ifdef HAVE_SYS_UTIME_H
#include <sys/utime.h>
#endif
#endif
#include "disclaimer.h"

#ifdef NATIVE_WIN32
/* The POSIX name for this item is deprecated. Instead, use the ISO
   C++ conformant name: _unlink. See online help for details. */
#define unlink _unlink
#endif

#ifndef S_ISDIR			/* On Windows :-( */
#define S_ISDIR(x)	(((x) & S_IFMT) == S_IFDIR)
#endif

File files[M_FILES];
int nfile = 0;

static struct {
	FILE *fp;
	char *name;
	int line;
} fpstack[16], *fptop = fpstack;
#define ifile (fptop->fp)
FILE *ofile = 0;
FILE *ofile_body = 0;

char *outputdir;
char *inputdir;
size_t outputdir_len;

FILE *fmustopen(char *, char *);
int CompareFiles(char *, char *);


void
OutputDir(char *dir)
{
	struct stat buf;

	if (stat(dir, &buf) == -1 || !S_ISDIR(buf.st_mode)) {
		fprintf(stderr, "Invalid target directory (%s), using \".\" instead\n", dir);
		return;
	}
	outputdir = StrDup(dir);
	outputdir_len = strlen(dir);
}

File *
GetFile(char *s, CmdCode m)
{
	File *f;
	char *fname;
	char *bname;

	bname = BaseName(s);
	fname = (char *) malloc(outputdir_len + strlen(bname) + strlen(dir2ext(m)) + 16);

	if (strlen(dir2ext(m)) == 0)
		snprintf(fname, outputdir_len + strlen(bname) + strlen(dir2ext(m)) + 16, "%s%c%s", outputdir, DIR_SEP, BaseName(s));
	else
		snprintf(fname, outputdir_len + strlen(bname) + strlen(dir2ext(m)) + 16, "%s%c%s.%s", outputdir, DIR_SEP, BaseName(s), dir2ext(m));

	for (f = files; f < files + nfile; f++) {
		if (strcmp(f->f_name, fname) == 0) {
			return f;
		}
	}
	if (nfile == M_FILES)
		Fatal("GetFile", "Too many files");

	f->f_name = fname;

	f->f_str = fname + strlen(fname);;
	while (f->f_str > fname && f->f_str[-1] != DIR_SEP
#ifdef WIN32
		&& f->f_str[-1] != '/'
#endif
	       )
		f->f_str--;

	f->f_tmp = (char *) malloc(strlen(outputdir) + strlen(bname) + strlen(dir2ext(m)) + 17);
	snprintf(f->f_tmp, strlen(outputdir) + strlen(bname) + strlen(dir2ext(m)) + 17, "%s%c%s.%s", outputdir, DIR_SEP, TempName(s), dir2ext(m));

	f->f_mode = 0;

	nfile++;
	return f;
}

int
HasSuffix(char *name, char *suffix)
{
	if (strlen(name) <= strlen(suffix))
		return 0;
	return (strcmp(name + strlen(name) - strlen(suffix), suffix) == 0);
}

/* the FileName, BaseName, TempName return names for immediate consumption */
char bname[1024];

/* the name without preappended subdirectories */
char *
FileName(char *name)
{
	char *p;

	if ((p = strrchr(name, DIR_SEP)) != NULL)
		p++;
	else
		p = name;
#ifdef WIN32
	/* on Windows also recognize / as separator */
	name = p;
	if ((p = strrchr(name, '/')) != NULL)
		p++;
	else
		p = name;
#endif
	strncpy(bname, p, sizeof(bname));
	return bname;
}

/* the name without extension */
char *
BaseName(char *name)
{
	char *b = strrchr(FileName(name), '.');

	if (b != NULL)
		*b = '\0';
	return bname;
}

/* the name with '.' prepended */
char *
TempName(char *name)
{
	char *p, *r = p = bname + strlen(BaseName(name));

	while (--r >= bname)
		if (*r == DIR_SEP
#ifdef WIN32
		    || *r == '/'
#endif
		   )
			break;
	for (r++, p[1] = 0; p > r; p--)
		p[0] = p[-1];
	*r = '.';
	return bname;
}

FILE *
fmustopen(char *fname, char *mode)
{
	char *p;
	struct stat buf;

	for (p = fname; *p; p++) {
		if (*p == DIR_SEP
#ifdef WIN32
		    || *p == '/'
#endif
		   ) {
			*p = '\0';
			if (p > fname /* avoid trying "" */ &&
#ifdef WIN32
			    /* avoid trying things like "C:" */
			    (p != fname + 2 || fname[1] != ':') &&
#endif
			    stat(fname, &buf) < 0 && mkdir(fname, S_IRWXU) < 0)
				Fatal("fmustopen:", "Can't create directory %s:%s\n", fname, strerror(errno));
			*p = DIR_SEP;
		}
	}
	return fopen(fname, mode);

}


char fname[1024];

void
IoWriteFile(char *s, CmdCode m)
{
	File *f;

	if (ofile) {
		if (TEXIMODE && bodymode == 0) 
			ofile_printf("@bye\n");
		fclose(ofile);
	}

	f = GetFile(s, m);
	if ((f->f_mode & m) == m) {
		ofile = fmustopen(f->f_tmp, "a");
		if (ofile == NULL)
			Fatal("IoWriteFile", "can't append to %s: %s", f->f_tmp, strerror(errno));
	} else {
		f->f_mode |= m;
		ofile = fmustopen(f->f_tmp, "w");
		if (ofile == NULL)
			Fatal("IoWriteFile", "can't create %s: %s", f->f_tmp, strerror(errno));
		if (disclaimer)
			insertDisclaimer(ofile, f->f_tmp);
	}
}

/* this function replaces the fork of the 'cmp' utility 
 * had to be done on WIN32 because return values were screwed
 */
int
CompareFiles(char *nm1, char *nm2)
{
	FILE *fp1, *fp2;
	int ret = 2;

	if ((fp2 = fopen(nm2, "r")) == NULL)
		return ret;
	if ((fp1 = fopen(nm1, "r")) == NULL)
		Fatal("CompareFiles",
		      "Internal error: cannot open temporary file");

	while (!feof(fp1) && !feof(fp2)) {
		if (getc(fp1) != getc(fp2)) {
			break;
		}
	}
	ret = !feof(fp1) || !feof(fp2);
	fclose(fp2);
	fclose(fp1);
	return ret;
}

void
UpdateFiles(void)
{
	File *f;
	int status;

	if (ofile)
		fclose(ofile);
	ofile = NULL;

	for (f = files; f < files + nfile; f++) {
		if (mx_err) {
			/* in case of error, don't produce/change output */
			unlink(f->f_tmp);
			continue;
		}
		status = CompareFiles(f->f_tmp, f->f_name);
		switch (status) {
		case 0:	/* identical files, remove temporary file */
			printf("%s: %s - not modified \n", mx_file, f->f_name);
			if (!notouch) {
				utime(f->f_name, 0);	/* touch the file */
			}
			unlink(f->f_tmp);
			break;
		case 1:	/* different file */
			printf("%s: %s - modified \n", mx_file, f->f_name);
			unlink(f->f_name);
			if (rename(f->f_tmp, f->f_name))
				perror("rename");
			break;
		default:	/* new file, move file */
			printf("%s: %s - created \n", mx_file, f->f_name);
			if (rename(f->f_tmp, f->f_name))
				perror("rename");
			break;
		}
	}
	nfile = 0;
}

void
IoReadFile(char *name)
{
	char *p;

	if (!HasSuffix(name, ".mx"))
		Fatal("IoReadFile", "Not an mx-file:%s", name);
	if ((ifile = fopen(name, "r")) == 0)
		Fatal("IoReadFile", "Can't process %s", name);
	p = (inputdir = StrDup(name)) + strlen(name);
	while (--p >= inputdir && *p != DIR_SEP
#ifdef WIN32
		&& *p != '/'
#endif
	       )
		;
	p[1] = 0;
	fptop->name = name;
	mx_file = name;
	mx_line = 1;
}


void
CloseFile(void)
{
	fclose(ifile);
	/* mx_file= 0; */
	mx_line = 1;
}

#define MAXLINE 2048


int
EofFile(void)
{
	if (feof(ifile)) {
		if (fptop == fpstack)
			return 1;
		fclose(ifile);
		fptop--;
		mx_file = fptop->name;
		mx_line = fptop->line;
		return EofFile();
	}
	return 0;
}

#define MX_INCLUDE "@include"
#define MX_COMMENT "@'"
static int fullbuf = 0;
static char linebuf[MAXLINE];

char *
NextLine(void)
{
	if (fullbuf) {
		mx_line++;
		fullbuf = 0;
		return linebuf;
	} else {
		char *s, *t;

		do {
			mx_line++;
			s = fgets(linebuf, MAXLINE, ifile);
		} while (s == NULL && !EofFile());

		if (s && strncmp(s, MX_COMMENT, strlen(MX_COMMENT)) == 0)
			s[0] = '\0';

		if (s && strncmp(s, MX_INCLUDE, strlen(MX_INCLUDE)) == 0) {
			char path[1024];

			s += strlen(MX_INCLUDE);
			while (isspace((int) (*s)))
				s++;
			for (t = s; *t && !isspace((int) (*t)); t++)
				;
			*path = *t = 0;
			if (*inputdir && *s != DIR_SEP) {	/* absolute path */
				snprintf(path, sizeof(path), "%s%c", inputdir, DIR_SEP);
			}
			strncat(path, s, sizeof(path) - strlen(path) - 1);
			fptop[1].fp = fopen(path, "r");
			if (fptop[1].fp == NULL) {
				fprintf(stderr, "Mx: failed to include '%s'.\n", s);
			} else {
				fptop->line = mx_line;
				fptop++;
				fptop->name = strdup(path);
				mx_file = fptop->name;
				mx_line = 1;
			}
			return NextLine();
		}
		if (s) {
			/* filter out \r if the source file got them and your libc does not ignore them */
			size_t len = strlen(s);

			if (len > 1 && s[len - 1] == '\n' && s[len - 2] == '\r') {
				s[len - 2] = '\n';
				s[len - 1] = 0;
			}
		}
		return s;
	}
}

void
PrevLine(void)
{
	mx_line--;
	fullbuf = 1;
}
