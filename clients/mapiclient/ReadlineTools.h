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

#ifndef READLINETOOLS_H_INCLUDED
#define READLINETOOLS_H_INCLUDED

#ifdef HAVE_LIBREADLINE

#include <mapi.h>

void init_readline(Mapi mid, char *language, int save_history);
void deinit_readline(void);
void save_line(const char *s);
rl_completion_func_t *suspend_completion(void);
void continue_completion(rl_completion_func_t * func);

#endif /* HAVE_LIBREADLINE */
#endif /* READLINETOOLS_H_INCLUDED */
