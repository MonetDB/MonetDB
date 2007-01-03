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

#ifndef DIFFLIB_H
#define DIFFLIB_H

#ifdef NATIVE_WIN32
#define STDERR stdout
#else
#define STDERR stderr
#endif

#ifdef DEBUG
#define TRACE(x) {fflush(stdout); x; fflush(STDERR);}
#else
#define TRACE(x)
#endif


int oldnew2u_diff(int mindiff, int context, char *ignore, char *old_fn, char *new_fn, char *u_diff_fn);
int oldnew2l_diff(int mindiff, int context, char *ignore, char *old_fn, char *new_fn, char *l_diff_fn);
int oldnew2w_diff(int mindiff, int context, char *ignore, char *old_fn, char *new_fn, char *w_diff_fn);
int oldnew2c_diff(int mindiff, int context, char *ignore, char *old_fn, char *new_fn, char *c_diff_fn);
int oldnew2lwc_diff(int mindiff, int LWC, int context, char *ignore, char *old_fn, char *new_fn, char *lwc_diff_fn);

int u_diff2l_diff(char *u_diff_fn, char *l_diff_fn);
int u_diff2w_diff(int mindiff, char *u_diff_fn, char *w_diff_fn);
int u_diff2c_diff(int mindiff, char *u_diff_fn, char *c_diff_fn);
int u_diff2lwc_diff(int mindiff, int LWC, char *u_diff_fn, char *lwc_diff_fn);

int l_diff2w_diff(int mindiff, char *l_diff_fn, char *w_diff_fn);
int l_diff2c_diff(int mindiff, char *l_diff_fn, char *c_diff_fn);
int w_diff2c_diff(int mindiff, char *w_diff_fn, char *c_diff_fn);

int oldnew2html(int mindiff, int LWC, int context, char *ignore, char *old_fn, char *new_fn, char *html_fn, char *caption, char *revision);
int u_diff2html(int mindiff, int LWC, char *u_diff_fn, char *html_fn, char *caption, char *revision);
int lwc_diff2html(char *old_fn, char *new_fn, char *lwc_diff_fn, char *html_fn, char *caption, char *revision);

#endif /* DIFFLIB_H */
