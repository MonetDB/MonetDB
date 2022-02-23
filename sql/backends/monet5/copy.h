/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _COPY_H_
#define _COPY_H_


#define MAX_LINE_LENGTH (32 * 1024 * 1024)

#define bailout(f, ...) do { \
		msg = createException(SQL, f,  __VA_ARGS__); \
		goto end; \
	} while (0)


struct error_handling {
	int rel_row;
	int count;
	char message[512];
};

void copy_report_error(struct error_handling *restrict admin, int rel_row, _In_z_ _Printf_format_string_ const char *restrict format, ...)
	__attribute__((__format__(__printf__, 3, 4)));

typedef str (*bulk_converter)(struct error_handling*, void *parms, int count, void *dest, char *data, int *offsets);

str parse_fixed_width_column(bat *ret, const char *fname, bat block_bat_id, bat offsets_bat_id, int tpe, bulk_converter f, void *parms);


extern str COPYparse_generic(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_integer_bte(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *dummy);
extern str COPYparse_integer_sht(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *dummy);
extern str COPYparse_integer_int(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *dummy);
extern str COPYparse_integer_lng(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *dummy);
#ifdef HAVE_HGE
extern str COPYparse_integer_hge(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *dummy);
#endif
extern str COPYparse_decimal_bte(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, int *dummy);
extern str COPYparse_decimal_sht(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, int *dummy);
extern str COPYparse_decimal_int(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, int *dummy);
extern str COPYparse_decimal_lng(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, int *dummy);
#ifdef HAVE_HGE
extern str COPYparse_decimal_hge(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, int *dummy);
#endif


struct decimal_parms {
	int digits;
	int scale;
};


#ifdef __GNUC__
/* __builtin_expect returns its first argument; it is expected to be
 * equal to the second argument */
#define unlikely(expr)	__builtin_expect((expr) != 0, 0)
#define likely(expr)	__builtin_expect((expr) != 0, 1)
#else
#define unlikely(expr)	(expr)
#define likely(expr)	(expr)
#endif


#endif /*_COPY_H_*/
