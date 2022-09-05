/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef SQL_BINCOPYCONVERT_H
#define SQL_BINCOPYCONVERT_H

#include "monetdb_config.h"
#include "gdk.h"


// Dispatcher table for imports. We dispatch on a string value instead of for
// example the underlying gdktype so we have freedom to some day implement for
// example both zero-terminated strings and newline-terminated strings.
//
// An entry must fill one field of the following three: 'loader',
// 'convert_fixed_width', or 'convert_in_place'.

// A 'loader' has complete freedom. It is handed a BAT and a stream and it can
// then do whatever it wants. We use it to read strings and json and other
// variable-width data.
//
// If an entry has has 'convert_in_place' this means the external and internal
// forms have the same size and are probably identical. In this case, the data
// is loaded directly into the bat heap and then the 'convert_in_place' function
// is called once for the whole block to perform any necessary tweaking of the data.
// We use this for example for the integer types, on little-endian platforms no
// tweaking is necessary and on big-endian platforms we byteswap the data.
//
// Finally, if an entry has 'convert_fixed_width' it means the internal and
// external forms are both fixed width but different in size. The data is loaded into
// intermediate buffers first and the conversion function copies the data from
// an array of incoming data in the buffer to an array of internal
// representations in the BAT.
//
// A note about the function signatures: we use start/end pointers instead of
// start/size pairs because this way there can be no confusion about whether
// the given size is a byte count or an item count.

typedef str (*bincopy_loader_t)(BAT *bat, stream *s, int *eof_reached);
typedef str (*bincopy_converter_t)(void *dst_start, void *dst_end, void *src_start, void *src_end, bool byteswap);
typedef str(*bincopy_convert_in_place_t)(void *start, void *end, bool byteswap);

struct type_rec {
	char *method;
	char *gdk_type;
	size_t record_size;
	bincopy_loader_t loader;
	bincopy_converter_t convert_fixed_width;
	bincopy_convert_in_place_t convert_in_place;
};

extern struct type_rec *find_type_rec(str name);

#define bailout(...) do { \
		msg = createException(MAL, "sql.importColumn", SQLSTATE(42000) __VA_ARGS__); \
		goto end; \
	} while (0)



#endif
