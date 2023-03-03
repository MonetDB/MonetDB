/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef SQL_BINCOPYCONVERT_H
#define SQL_BINCOPYCONVERT_H

#include "monetdb_config.h"
#include "gdk.h"


// Dispatcher table for type conversions.
//
// There are three kinds of conversions.
//
// 1) Trivial conversions do not need any work. The data can be read directly
//    into the BAT heap without any conversion. Example: integer and float types
//    in the native endianness. Also, uuids.
//
// 2) Fixed width types whose encoding is also fixed width, such as the temporal
//    types and integer and float types in the wrong endianness. These are read
//    into a scratch buffer and then the .decode field points to a function
//    which is called to convert them and write them into the BAT.
//
//    For some types, the conversion is only nontrivial if the endianness is
//    wrong. The field .trivial_if_no_byteswap can be used to indicate that
//    the conversion can be regarded as trivial if the byte order is correct.
//
// 3) Some types have a variable width or otherwise nontrivial encoding.
//    For these types the .loader field points to a function that takes
//    the stream and the BAT and does everything necessary to load the data into
//    the BAT.

typedef str (*bincopy_decoder_t)(void *dst,void *src, size_t count, int width, bool byteswap);
typedef str (*bincopy_loader_t)(BAT *bat, stream *s, int *eof_reached, int width, bool byteswap);

typedef str (*bincopy_encoder_t)(void *dst, void *src, size_t count, int width, bool byteswap);
typedef str (*bincopy_dumper_t)(BAT *bat, stream *s, BUN start, BUN length, bool byteswap);

struct type_record_t {
	char *method;
	char *gdk_type;
	size_t record_size;
	bool trivial_if_no_byteswap;

	bool decoder_trivial;
	bincopy_decoder_t decoder;
	bincopy_loader_t loader;

	bool encoder_trivial;
	bincopy_encoder_t encoder;
	bincopy_dumper_t dumper;
};
typedef const struct type_record_t type_record_t;

extern type_record_t *find_type_rec(const char *name);

extern bool can_dump_binary_column(type_record_t const *rec);

extern str dump_binary_column(type_record_t const *rec, BAT *b, BUN start, BUN length, bool byteswap, stream *s);


#endif
