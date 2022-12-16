/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _REL_FILE_LOADER_H_
#define _REL_FILE_LOADER_H_

#include "sql_types.h"

/* TODO think of set of file names */
typedef int (*fl_add_types_fptr)(sql_subfunc *f, char *filename);
typedef int (*fl_load_fptr)(sql_subfunc *f, char *filename);

typedef struct file_loader_t {
	char *name;
	fl_add_types_fptr add_types;
	fl_load_fptr load;
} file_loader_t;

extern int fl_register(char *name, fl_add_types_fptr add_types, fl_load_fptr fl_load);
extern file_loader_t* fl_find(char *name);

extern void fl_exit(void);

#endif /*_REL_FILE_LOADER_H_*/
