/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef READLINETOOLS_H_INCLUDED
#define READLINETOOLS_H_INCLUDED

#ifdef HAVE_LIBREADLINE

#include "mapi.h"

void init_readline(Mapi mid, const char *language, bool save_history);
void deinit_readline(void);
void save_line(const char *s);
rl_completion_func_t *suspend_completion(void);
void continue_completion(rl_completion_func_t * func);
void readline_int_handler(void);

#endif /* HAVE_LIBREADLINE */
#endif /* READLINETOOLS_H_INCLUDED */
