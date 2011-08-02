/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _MAL_PROPERTIES_
#define _MAL_PROPERTIES_
#include "mal.h"
#include "mal_namespace.h"

typedef struct MalProp {
	bte idx;
	bte op;
	int var;
} *MalPropPtr, MalProp;

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

