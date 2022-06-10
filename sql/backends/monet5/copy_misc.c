/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#include "copy.h"
#include "rel_copy.h"

static void
dump_char(int c)
{
	if (c == '\n')
		fprintf(stderr, "⏎");
	else if (isspace(c))
		fprintf(stderr, "·");
	else if (isprint(c)) {
		fprintf(stderr, "%c", c);
	} else {
		fprintf(stderr, "░");
	}
}

static void
dump_data(const char *msg, const char *data, int count)
{
	fprintf(stderr, "%s: %d bytes", msg, count);
	if (count == 0) {
		fprintf(stderr, ".\n");
		return;
	}

	int n = 10;
	fprintf(stderr, ": ");
	if (count <= 2 * n) {
		for (int i = 0; i < count; i++)
			dump_char(data[i]);
	} else {
		for (int i = 0; i < n; i++)
			dump_char(data[i]);
		fprintf(stderr, " . . . ");
		for (int i = count - n; i < count; i++)
			dump_char(data[i]);
	}
	fprintf(stderr, "\n");
}

void
dump_block(const char *msg, BAT *b)
{
	int start = (int)b->batInserted;
	int end = (int)BATcount(b);
	assert(start <= end);
	int len = end - start;
	fprintf(stderr, "%s: bat id %d, range is %d..%d (%d bytes)\n", msg, b->batCacheid, start, end, len);
	dump_data("    ", Tloc(b, start), len);
}


void
copy_init_error_handling(struct error_handling *admin, lng starting_row, int default_col_no, const char *column_name)
{
	admin->count = 0;
	admin->starting_row = starting_row;
	admin->default_col_no = default_col_no;
	admin->column_name = column_name ? GDKstrdup(column_name) : NULL;
	admin->buffer[0] = '\0';
}

void
copy_destroy_error_handling(struct error_handling *admin)
{
	GDKfree(admin->column_name);
}

gdk_return
copy_report_error(struct error_handling *restrict admin, int rel_row, int column, _In_z_ _Printf_format_string_ const char *restrict format, ...)
{
	// We remember only the first error and count the rest
	if (admin->count == 0) {
		lng row_number = admin->starting_row + 1 + rel_row;
		if (column < 0)
			column = admin->default_col_no;
		int column_1based = column + 1;
		char col_msg[100];
		if (column < 0)
			col_msg[0] = '\0';
		else if (column != admin->default_col_no || admin->column_name == NULL)
			snprintf(col_msg, sizeof(col_msg), " column %d", column_1based);
		else
			snprintf(col_msg, sizeof(col_msg), " column %d '%s'", column_1based, admin->column_name);

		char *buf = admin->buffer;
		char *buf_end = admin->buffer + sizeof(admin->buffer);
		int n = snprintf(buf, buf_end - buf, "Row %ld%s: ", row_number, col_msg);
		if (n < buf_end - buf) {
			buf += n;
			va_list ap;
			va_start(ap, format);
			n = vsnprintf(buf, buf_end - buf, format, ap);
			if (n < 0) {
				snprintf(buf, buf_end - buf, "An error occurred during error reporting");
			}
			va_end(ap);
		}
	}
	admin->count++;

	// what to do if this fails?
	return GDK_FAIL;
}

str
copy_check_too_many_errors(struct error_handling *admin, const char *fname)
{
	// no support for BEST EFFORT yet
	lng error_count = admin->count;
	if  (error_count > 0) {
		const char *message = copy_error_message(admin);
		if (error_count == 1)
			throw(MAL, fname, "%s", message);
		else
			throw(MAL, fname, "At least %ld conversion errors, example: %s", error_count, message);

	} else {
		return MAL_SUCCEED;
	}
}

const char *
copy_error_message(struct error_handling *admin)
{
	if (admin->count == 0)
		return MAL_SUCCEED;
	else
		return admin->buffer;
}


str
COPYset_blocksize(int *dummy, int *blocksize)
{
	(void)dummy;
	char buffer[128];
	sprintf(buffer, "%d", *blocksize);
	GDKsetenv(COPY_BLOCKSIZE_SETTING, buffer);
	return MAL_SUCCEED;
}

str
COPYget_blocksize(int *blocksize)
{
	int size = GDKgetenv_int(COPY_BLOCKSIZE_SETTING, -1);
	*blocksize = size > 0 ? size : DEFAULT_COPY_BLOCKSIZE;
	return MAL_SUCCEED;
}

str
COPYset_parallel(bit *dummy, int *level)
{
	(void)dummy;
	char buf[20];
	snprintf(buf, sizeof(buf), "%d", *level);
	GDKsetenv(COPY_PARALLEL_SETTING, buf);
	return MAL_SUCCEED;
}

str
COPYget_parallel(int *level)
{
	*level = GDKgetenv_int(COPY_PARALLEL_SETTING, int_nil);
	return MAL_SUCCEED;
}

str
COPYstr2buf(bat *bat_id, str *s)
{
	str msg = MAL_SUCCEED;
	str content = *s;
	size_t content_len = strlen(content);
	BAT *b = NULL;

	b = COLnew(0, TYPE_bte, content_len, TRANSIENT);
	if (!b)
			bailout("copy.str2buf", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	memcpy(Tloc(b, 0), content, content_len);
	BATsetcount(b, content_len);

end:
	if (b) {
		if (msg == MAL_SUCCEED) {
			*bat_id = b->batCacheid;
			BBPkeepref(b);
		} else {
			BBPunfix(b->batCacheid);
		}
	}
	return msg;
}

str
COPYbuf2str(str *ret, bat *bat_id)
{
	str msg = MAL_SUCCEED;
	char *s = NULL;
	BUN len;

	BAT *b = BATdescriptor(*bat_id);
	if (!b)
		bailout("copy.buf2str", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	len = BATcount(b);

	for (BUN i = 0; i < len; i++) {
		if (*(char*)Tloc(b, i) == '\0')
			bailout("copy.buf2str", "BAT contains a 0 at index %ld", (long)i);
	}

	s = GDKmalloc(len + 1);
	if (!s)
		bailout("copy.str2buf", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	s[len] = '\0';
	memcpy(s, Tloc(b, 0), len);

	*ret = s;
end:
	if (b)
		BBPunfix(b->batCacheid);
	return msg;
}

