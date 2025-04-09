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
#include "rel_proto_loader.h"

#define NR_PROTO_LOADERS 255
static proto_loader_t proto_loaders[NR_PROTO_LOADERS] = { 0 };

proto_loader_t*
pl_find(const char *name)
{
	if (!name)
		return NULL;
	for (int i = 0; i < NR_PROTO_LOADERS; i++) {
		if (proto_loaders[i].name && strcmp(proto_loaders[i].name, name) == 0)
			return proto_loaders+i;
	}
	return NULL;
}

int
pl_register(const char *name, pl_add_types_fptr add_types, pl_load_fptr load)
{
	proto_loader_t *fl = pl_find(name);
	if (fl) {
		TRC_WARNING(SQL_TRANS,"proto_loader re-registering %s\n", name);
		GDKfree(fl->name);
		fl->name = NULL;
	}

	for (int i = 0; i < NR_PROTO_LOADERS; i++) {
		if (proto_loaders[i].name == NULL) {
			proto_loaders[i].name = GDKstrdup(name);
			proto_loaders[i].add_types = add_types;
			proto_loaders[i].load = load;
			return 0;
		}
	}

	/* all proto_loaders array locations are occupied */
	return -1;	/* could not register proto_loader */
}

void
pl_unregister(const char *name)
{
	proto_loader_t *fl = pl_find(name);
	if (fl) {
		GDKfree(fl->name);
		fl->name = NULL;
	}
}

void
pl_exit(void)
{
	for (int i = 0; i < NR_PROTO_LOADERS; i++) {
		if (proto_loaders[i].name)
			GDKfree(proto_loaders[i].name);
	}
}
