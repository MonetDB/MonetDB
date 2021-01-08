/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef EXCEPTION_BUFFER_H
#define EXCEPTION_BUFFER_H

#include "monetdb_config.h"
#include <setjmp.h>

typedef struct exception_buffer {
	jmp_buf state;
	int code;
	char *msg;
	int enabled;
} exception_buffer;

extern exception_buffer *eb_init( exception_buffer *eb );

/* != 0 on when we return to the savepoint */
#define eb_savepoint(eb) ((eb)->enabled=1,setjmp((eb)->state))
extern _Noreturn void eb_error( exception_buffer *eb, char *msg, int val );

#endif /* EXCEPTION_BUFFER_H */
