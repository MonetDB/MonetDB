/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include "sql_storage.h"
#include "sql_scenario.h"
#include "store_sequence.h"
#include "sql_datetime.h"
#include "rel_optimizer.h"
#include "rel_distribute.h"
#include "rel_select.h"
#include "rel_exp.h"
#include "rel_dump.h"
#include "clients.h"
#include "mal_instruction.h"

#define CONCAT_2(a, b)		a##b
#define CONCAT_3(a, b, c)	a##b##c

#define NIL(t)			CONCAT_2(t, _nil)
#define ISNIL(t)		CONCAT_3(is_, t, _nil)
#define TPE(t)			CONCAT_2(TYPE_, t)
#define GDKmin(t)		CONCAT_3(GDK_, t, _min)
#define GDKmax(t)		CONCAT_3(GDK_, t, _max)
#define FUN(a, b)		CONCAT_3(a, _, b)

#define STRING(a)		#a

#define TYPE flt
#include "sql_fround_impl.h"
#undef TYPE

#define TYPE dbl
#include "sql_fround_impl.h"
#undef TYPE
