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
#include "gdk.h"

#ifdef HAVE_HGE
#define NGRAM_TYPE hge
#define NGRAM_TYPENIL hge_nil
#define NGRAM_CST(v) ((hge)LL_CONSTANT(v))
#define NGRAM_BITS 127
#define CHARMAP(s) (s & NGRAM_BITS)
#define SZ 128
#else
#define NGRAM_TYPE lng
#define NGRAM_TYPEID TYPE_lng
#define NGRAM_TYPENIL lng_nil
#define NGRAM_CST(v) LL_CONSTANT(v)
#define NGRAM_BITS 63
#define CHARMAP(s) (s & NGRAM_BITS)
#define SZ 64
#endif

#define BIGRAM_SZ (SZ * SZ)
#define NGRAM_MULTIPLE 16
#define TOKEN1(s) (*s)
#define TOKEN2(s) (*(s + 1))
#define BIGRAM(s) (TOKEN1(s) && TOKEN2(s))

#define ENC_TOKEN1(t) CHARMAP(*t)
#define ENC_TOKEN2(t) CHARMAP(*(t + 1))

typedef struct {
	NGRAM_TYPE *idx;
	NGRAM_TYPE *sigs;
	unsigned *histogram;
	unsigned min, max;
	unsigned *lists;
	unsigned *rids;
} Ngrams;
