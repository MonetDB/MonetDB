/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _COPY_H_
#define _COPY_H_

#include "streams.h"

#define MAX_LINE_LENGTH (32 * 1024 * 1024)

#define bailout(f, ...) do { \
		msg = createException(SQL, f,  __VA_ARGS__); \
		goto end; \
	} while (0)


////////////////////////////////////////////////////////////////////////
// copy_misc.c

struct error_handling {
	bat failures_bat_id;
	BAT *failures_bat;
	bool inhibit_deletes;
	lng count;
	lng starting_row;
	int default_col_no;
	str column_name;
	char buffer[512];
	bool fatal;
};

void copy_init_error_handling(struct error_handling *admin, bat failures_bat, lng starting_row, int default_col_no, const char *column_name);
void copy_error_handling_inhibit_deletes(struct error_handling *admin);
gdk_return copy_report_error(struct error_handling *admin, int rel_row, int column, _In_z_ _Printf_format_string_ const char *restrict format, ...)
	__attribute__((__format__(__printf__, 4, 5)));
str copy_check_too_many_errors(struct error_handling *admin, const char *fname);
const char *copy_error_message(struct error_handling *admin);
void copy_destroy_error_handling(struct error_handling *admin);

extern str COPYset_blocksize(int *dummy, int *blocksize);
extern str COPYget_blocksize(int *blocksize);
extern str COPYset_parallel(bit *dummy, int *level);
extern str COPYget_parallel(int *level);
extern str COPYstr2buf(bat *bat_id, str *s);
extern str COPYbuf2str(str *ret, bat *bat_id);

void dump_block(const char *msg, BAT *b);


////////////////////////////////////////////////////////////////////////
// copy_convert.c

typedef void (*bulk_converter)(struct error_handling*, void *parms, int count, void *dest, char *data, int *offsets);
extern str parse_fixed_width_column(bat *ret, struct error_handling *errors, const char *fname, bat block_bat_id, bat offsets_bat_id, int tpe, bulk_converter f, void *parms);

extern str COPYparse_generic(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_string(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *maxlen, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);

extern str COPYparse_integer_bte(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, bte *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
extern str COPYparse_integer_sht(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, sht *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
extern str COPYparse_integer_int(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
extern str COPYparse_integer_lng(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, lng *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
#ifdef HAVE_HGE
extern str COPYparse_integer_hge(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, hge *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
#endif

extern str COPYparse_decimal_bte(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, bte *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
extern str COPYparse_decimal_sht(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, sht *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
extern str COPYparse_decimal_int(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, int *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
extern str COPYparse_decimal_lng(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, lng *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
#ifdef HAVE_HGE
extern str COPYparse_decimal_hge(bat *parsed_bat_id, bat *block_bat_id, bat *offsets_bat_id, int *digits_p, int *scale_p, hge *dummy, bat *failures_bat, lng *starting_row, int *col_no, str *col_name);
#endif

extern str COPYscale_bte(bat *result_bat_id, bat *values_bat_id, int *factor, bat *failures_bat_id, lng *starting_row, int *col_no, str *col_name);
extern str COPYscale_sht(bat *result_bat_id, bat *values_bat_id, int *factor, bat *failures_bat_id, lng *starting_row, int *col_no, str *col_name);
extern str COPYscale_int(bat *result_bat_id, bat *values_bat_id, int *factor, bat *failures_bat_id, lng *starting_row, int *col_no, str *col_name);
extern str COPYscale_lng(bat *result_bat_id, bat *values_bat_id, int *factor, bat *failures_bat_id, lng *starting_row, int *col_no, str *col_name);
#ifdef HAVE_HGE
extern str COPYscale_hge(bat *result_bat_id, bat *values_bat_id, int *factor, bat *failures_bat_id, lng *starting_row, int *col_no, str *col_name);
#endif

////////////////////////////////////////////////////////////////////////
// copy_scan.c

struct scan_state {
	// these remain constant
	int quote_char;
	int line_sep;
	int col_sep;
	bool escape_enabled;
	unsigned char *start;
	unsigned char *end;
	// these are updated
	unsigned char *pos;
	bool quoted;
	bool escape_pending;
};

extern str scan_fields(
	struct error_handling *errors, struct scan_state *state,
	char *null_repr, int ncols, int nrows, int **columns);


////////////////////////////////////////////////////////////////////////
// copy_io.c

extern str COPYdefer_close_stream(bat *container, Stream *s);
extern str COPYrequest_upload(Stream *upload, str *filename, bit *binary);
extern str COPYfrom_stdin(Stream *s, lng *offset, lng *lines, bit *stoponemptyline, str *linesep_arg, str *quote_arg, bit *escape);


////////////////////////////////////////////////////////////////////////
// copy.c

extern int get_sep_char(str sep, bool backslash_escapes);

////////////////////////////////////////////////////////////////////////
// inline function belonging to copy_scan.c

#ifdef __GNUC__
/* __builtin_expect returns its first argument; it is expected to be
 * equal to the second argument */
#define unlikely(expr)	__builtin_expect((expr) != 0, 0)
#define likely(expr)	__builtin_expect((expr) != 0, 1)
#else
#define unlikely(expr)	(expr)
#define likely(expr)	(expr)
#endif

static inline bool
find_end_of_line(struct scan_state *st)
{
	bool found = false;
	int quote_char = st->quote_char;
	int line_sep = st->line_sep;
	bool escape_enabled = st->escape_enabled;
	unsigned char *end = st->end;
	// these are updated
	unsigned char *pos = st->pos;
	bool quoted = st->quoted;
	bool escape_pending = st->escape_pending;

	for (; pos < end; pos++) {
		if (escape_pending) {
			escape_pending = false;
			continue;
		}
		if (escape_enabled && *pos == '\\') {
			escape_pending = true;
			continue;
		}
		bool is_quote = (quote_char != 0 && *pos == quote_char);
		quoted ^= is_quote;
		if (!quoted && *pos == line_sep) {
			found = true;
			break;
		}
	}

	st->pos = pos;
	st->quoted = quoted;
	st->escape_pending = escape_pending;

	return found;
}


#endif /*_COPY_H_*/
