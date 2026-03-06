/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _COPY_H_
#define _COPY_H_

#include "streams.h"
#include "mal_pipelines.h"

#define MAX_LINE_LENGTH (32 * 64 * 1024 * 1024)

#define bailout(f, ...) do { \
		msg = createException(SQL, f,  __VA_ARGS__); \
		goto end; \
	} while (0)


typedef struct bufferstream {
	size_t *sz;
	size_t *len;
	size_t *pos;
	size_t *jmp;
	unsigned char **buf;
	BUN *seq;

	int nr_bufs;
	int cur_buf;
	int eof;
	stream *s;
} bufferstream;

typedef struct reader {
	Sink sink;
	stream *s;
	BUN sz;
	BUN offset;
	BUN maxcount;
	lng linecount;
	bool done;
	bool error;
	bool can_jump;
	ATOMIC_TYPE seqnr;
	ATOMIC_TYPE offset_seqnr;
	ATOMIC_TYPE jump_seqnr;
	unsigned char *col_sep_str;
	int col_sep_len;
	unsigned char *line_sep_str;
	int line_sep_len;
	unsigned char *quote_str;
	unsigned char *null_repr;
	int null_repr_len;
	bool escape_enabled;
	bool best_effort;

	int col_sep;
	int line_sep;
	int quote_char;
	lng *line_count;
	bufferstream *bs;
} reader;



////////////////////////////////////////////////////////////////////////
// copy_misc.c

struct error_handling {
	bool init;
	Client cntxt;
	reader *r;
	lng count;
	lng starting_row;
	int default_col_no;
	bat rows_batid;
	BAT *rows;
	str column_name;
	char buffer[512];
	bool fatal;
};

void copy_init_error_handling(struct error_handling *admin, Client cntxt, lng starting_row, int default_col_no, const char *column_name, bat rows);
void copy_error_handling_inhibit_deletes(struct error_handling *admin);
gdk_return copy_report_error(struct error_handling *admin, lng rel_row, int column, _In_z_ _Printf_format_string_ const char *restrict format, ...)
	__attribute__((__format__(__printf__, 4, 5)));
str copy_check_too_many_errors(struct error_handling *admin, const char *fname);
const char *copy_error_message(struct error_handling *admin);
void copy_destroy_error_handling(struct error_handling *admin);

extern str COPYset_blocksize(Client ctx, int *dummy, int *blocksize);
extern str COPYget_blocksize(Client ctx, int *blocksize);

void dump_block(const char *msg, BAT *b);


////////////////////////////////////////////////////////////////////////
// copy_convert.c

typedef void (*bulk_converter)(struct error_handling*, void *parms, int count, void *dest, char *data, int *offsets);
extern str parse_fixed_width_column(bat *ret, struct error_handling *errors, const char *fname, bat block_bat_id, Pipeline *p, bat offsets_bat_id, int tpe, bulk_converter f, void *parms);

extern str COPYparse_generic(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_string(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_float(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

extern str COPYparse_integer_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_integer_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_integer_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_integer_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#ifdef HAVE_HGE
extern str COPYparse_integer_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif

extern str COPYparse_decimal_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_decimal_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_decimal_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYparse_decimal_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#ifdef HAVE_HGE
extern str COPYparse_decimal_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif

extern str COPYscale_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYscale_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYscale_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYscale_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#ifdef HAVE_HGE
extern str COPYscale_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif

////////////////////////////////////////////////////////////////////////
// copy_scan.c

struct scan_state {
	// these remain constant
	int quote_char;
	int line_sep;
	unsigned char *line_sep_str;
	int line_sep_len;
	int col_sep;
	unsigned char *col_sep_str;
	int col_sep_len;
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
	unsigned char *null_repr, int null_repr_len, int ncols, lng nrows, int **columns);

extern str scan_fields1( /* no quote_char, sep len == 1 */
	struct error_handling *errors, struct scan_state *state,
	unsigned char *null_repr, int null_repr_len, int ncols, lng nrows, int **columns);

extern str scan_fieldsN(
	struct error_handling *errors, struct scan_state *state,
	unsigned char *null_repr, int null_repr_len, int ncols, lng nrows, int **columns);


////////////////////////////////////////////////////////////////////////
// copy_io.c

extern str COPYrequest_upload(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str COPYfrom_stdin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);


////////////////////////////////////////////////////////////////////////
// copy.c

extern int check_sep(str sep, bool backslash_escapes);

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

static inline bool
find_end_of_lineN(struct scan_state *st)
{
	bool found = false;
	int quote_char = st->quote_char;
	unsigned char ls0 = st->line_sep_str[0];
	char *line_sep = (char*)st->line_sep_str;
	int line_sep_len = st->line_sep_len;
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
		if (!quoted && *pos == ls0 && strncmp((char*)pos, line_sep, line_sep_len)==0) {
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
