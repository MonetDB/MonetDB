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

#include "monetdb_config.h"
#include "rel_file_loader.h"

#define NR_FILE_LOADERS 255
static file_loader_t file_loaders[NR_FILE_LOADERS] = { 0 };

void
fl_exit(void)
{
	for (int i = 0; i < NR_FILE_LOADERS; i++) {
		if (file_loaders[i].name)
			GDKfree(file_loaders[i].name);
	}
}

void
fl_unregister(char *name)
{
	file_loader_t *fl = fl_find(name);
	if (fl) {
		GDKfree(fl->name);
		fl->name = NULL;
	}
}

int
fl_register(char *name, fl_add_types_fptr add_types, fl_load_fptr load)
{
	file_loader_t *fl = fl_find(name);
	if (fl) {
		printf("re-registering %s\n", name);
		GDKfree(fl->name);
		fl->name = NULL;
	}

	for (int i = 0; i < NR_FILE_LOADERS; i++) {
		if (file_loaders[i].name == NULL) {
			file_loaders[i].name = GDKstrdup(name);
			file_loaders[i].add_types = add_types;
			file_loaders[i].load = load;
			return 0;
		}
	}

	/* all file_loaders array locations are occupied */
	return -1;	/* could not register file_loader */
}

file_loader_t*
fl_find(char *name)
{
	if (!name)
		return NULL;
	for (int i = 0; i < NR_FILE_LOADERS; i++) {
		if (file_loaders[i].name && strcmp(file_loaders[i].name, name) == 0)
			return file_loaders+i;
	}
	return NULL;
}
