/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef OPT_BACKEND_H
#define OPT_BACKEND_H

#include "mal.h"
#include "mal_client.h"
#include "sql_mvc.h"

struct subbackend;

extern struct subbackend *subbackend_create(mvc *m, Client c);

typedef struct subbackend *(*subbackend_reset)(struct subbackend *b);
typedef void (*subbackend_destroy)(struct subbackend *b);
typedef int (*subbackend_check)(struct subbackend *b, sql_rel *r);
typedef str (*subbackend_exec)(struct subbackend *b, sql_rel *r, int result_id, res_table **T);

typedef struct subbackend {
	subbackend_reset reset;
	subbackend_destroy destroy;
	subbackend_check check;
	subbackend_exec exec;
} subbackend;

#endif /*OPT_BACKEND_H*/
