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
#include "sql_mvc.h"

/* TODO think of set of file names */
typedef str (*fl_add_types_fptr)(mvc *sql, sql_subfunc *f, char *filename, list *res_exps, char *name);
typedef int (*fl_load_fptr)(mvc *sql, sql_subfunc *f, char *filename);

typedef struct file_loader_t {
	char *name;
	fl_add_types_fptr add_types;
	/* api needs more designing */
	// void *fl_create(); load meta data from file
	// fl_nrows(metadata); return number of rows
	// fl_columnname(metadata, int i); return name of nth column
	// fl_loadchunk(metadata, int colnr, output_buf, nrows);
	fl_load_fptr load;
} file_loader_t;

extern int fl_register(char *name, fl_add_types_fptr add_types, fl_load_fptr fl_load);
extern file_loader_t* fl_find(char *name);

extern void fl_exit(void);

#endif /*_REL_FILE_LOADER_H_*/
