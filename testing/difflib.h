/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef DIFFLIB_H
#define DIFFLIB_H

#ifdef NATIVE_WIN32
#define STDERR stdout
#else
#define STDERR stderr
#endif

#ifdef DEBUG
#define TRACE(x) do {fflush(stdout); x; fflush(STDERR);} while (0)
#else
#define TRACE(x)
#endif


FILE *oldnew2u_diff(int mindiff, int context, char *ignore, char *function, char *old_fn, char *new_fn);
int oldnew2l_diff(int mindiff, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *l_diff_fn);
int oldnew2w_diff(int mindiff, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *w_diff_fn);
int oldnew2c_diff(int mindiff, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *c_diff_fn);
int oldnew2lwc_diff(int mindiff, int LWC, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *lwc_diff_fn);

int u_diff2l_diff(FILE *u_diff_fp, char *l_diff_fn);

int l_diff2w_diff(int mindiff, char *l_diff_fn, char *w_diff_fn);
int w_diff2c_diff(int mindiff, char *w_diff_fn, char *c_diff_fn);

int oldnew2html(int mindiff, int LWC, int context, char *ignore, char *function, char *old_fn, char *new_fn, char *html_fn, char *caption, char *revision);
int lwc_diff2html(char *old_fn, char *new_fn, char *lwc_diff_fn, char *html_fn, char *caption, char *revision);

#endif /* DIFFLIB_H */
