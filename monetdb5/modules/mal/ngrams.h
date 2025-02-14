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
#include "mal_interpreter.h"
#include "mal_exception.h"
#include "string.h"

#if 0
#define GZ 128
#define CHAR_MAP(s) (s&127)
#else
#define GZ 64
#define CHAR_MAP(s) (s&63)
#endif

#define UNIGRAM_SZ GZ
#define BIGRAM_SZ (GZ*GZ)
#define TRIGRAM_SZ (GZ*GZ*GZ)

#if 0
#define NGRAM_TYPE hge
#define NGRAM_TYPEID TYPE_hge
#define NGRAM_TYPENIL hge_nil
#define NGRAM_CST(v) ((hge)LL_CONSTANT(v))
#define NGRAM_BITS 127
#else
#define NGRAM_TYPE lng
#define NGRAM_TYPEID TYPE_lng
#define NGRAM_TYPENIL lng_nil
#define NGRAM_CST(v) LL_CONSTANT(v)
#define NGRAM_BITS 63
#endif

#define NGRAM_MULTIPLE 16

#define SET_EMPTY_BAT_PROPS(B)					\
	do {										\
		B->tnil = false;						\
		B->tnonil = true;						\
		B->tkey = true;							\
		B->tsorted = true;						\
		B->trevsorted = true;					\
		B->tseqbase = 0;						\
	} while (0)

typedef struct {
	NGRAM_TYPE *idx;
	NGRAM_TYPE *sigs;
	unsigned *h;
	unsigned max, min;
	unsigned *pos;
	unsigned *rid;
} Ngrams;
