/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _MAL_PROPERTIES_
#define _MAL_PROPERTIES_
#include "mal.h"
#include "mal_namespace.h"

typedef enum prop_op_t {
	op_lt = 0,
	op_lte = 1,
	op_eq = 2,
	op_gte = 3,
	op_gt = 4,
	op_ne = 5
} prop_op_t;

mal_export sht PropertyIndex(str name);
mal_export str PropertyName(sht idx);
mal_export prop_op_t PropertyOperator( str s );
mal_export str PropertyOperatorString( prop_op_t op );

#endif

