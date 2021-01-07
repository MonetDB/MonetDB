/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _MAPI_PROMPT_H_INCLUDED
#define _MAPI_PROMPT_H_INCLUDED 1

/* prompts for MAPI protocol, also in monetdb_config.h.in */
#define PROMPTBEG	'\001'	/* start prompt bracket */
#define PROMPT1		"\001\001\n"	/* prompt: ready for new query */
#define PROMPT2		"\001\002\n"	/* prompt: more data needed */
#define PROMPT3		"\001\003\n"	/* prompt: get file content */

#endif /* _MAPI_PROMPT_H_INCLUDED */
