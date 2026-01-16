/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see README.rst in the top level directory.
 */

#include "monetdb_config.h"
#include "gdk.h"

#ifdef HAVE_HGE
#define NG_TYPE		uhge
#define NG_TYPEID	TYPE_hge
#define NG_TYPENIL	hge_nil
#define NG_BITS		128
#define NG_MASK(s)	(s & (NG_BITS - 1))
#else
#define NG_TYPE		ulng
#define NG_TYPEID	TYPE_lng
#define NG_TYPENIL	lng_nil
#define NG_BITS		64
#define NG_MASK(s)	(s & (NG_BITS - 1))
#endif

#define BIGRAM_SZ	(NG_BITS * NG_BITS)
#define NG_MULTIPLE 16

#define TOKEN1(s)	(*s)
#define TOKEN2(s)	(*(s + 1))
#define BIGRAM(s)	(TOKEN1(s) && TOKEN2(s))
#define ENC_TOKEN1(t) NG_MASK(*t)
#define ENC_TOKEN2(t) NG_MASK(*(t + 1))

typedef struct {
	NG_TYPE *idx;
	NG_TYPE *sigs;
	unsigned *histogram;
	unsigned min, max;
	unsigned *lists;
	oid *rids;
} NGrams;
