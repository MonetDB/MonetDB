/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

/* these three are for all simple comparisons (6 in all) */
#define TYPE_TPE		TYPE_bit
#define TPE			bit
#define TPE_nil			bit_nil
#define is_TPE_nil		is_bit_nil

/* ---------------------------------------------------------------------- */
/* generic comparison (any "linear" type) */

/* #define CMP(a, b)	((bte) ((a) < (b) ? -1 : (a) > (b))) */
#define CMP(a, b)	((bte) (((a) > (b)) - ((a) < (b))))

#undef TYPE_TPE
#undef TPE
#undef TPE_nil
#undef is_TPE_nil

#define TYPE_TPE		TYPE_bte
#define TPE			bte
#define TPE_nil			bte_nil
#define is_TPE_nil		is_bte_nil

#define OP			CMP
#define op_typeswitchloop	cmp_typeswitchloop
#define BATcalcop_intern	BATcalccmp_intern
#define BATcalcop		BATcalccmp
#define BATcalcopcst		BATcalccmpcst
#define BATcalccstop		BATcalccstcmp
#define VARcalcop		VARcalccmp

#include "gdk_calc_compare.h"

#undef OP
#undef op_typeswitchloop
#undef BATcalcop_intern
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

#undef TYPE_TPE
#undef TPE
#undef TPE_nil
#undef is_TPE_nil
