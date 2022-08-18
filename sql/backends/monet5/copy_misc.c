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
copy_init_error_handling(struct error_handling *admin, bat failures_bat, lng starting_row, int default_col_no, const char *column_name)
{
	admin->failures_bat_id = failures_bat;
	admin->failures_bat = NULL;
	admin->inhibit_deletes = false;
	admin->count = 0;
	admin->fatal = false;
	admin->starting_row = starting_row;
	admin->default_col_no = default_col_no;
	admin->column_name = column_name ? GDKstrdup(column_name) : NULL;
	admin->buffer[0] = '\0';
}

void copy_error_handling_inhibit_deletes(struct error_handling *admin)
{
	admin->inhibit_deletes = true;
}

void
copy_destroy_error_handling(struct error_handling *admin)
{
	if (admin->failures_bat != NULL)
		BBPunfix(admin->failures_bat->batCacheid);
	GDKfree(admin->column_name);
}

static void
format_error(struct error_handling *restrict admin, lng row_number, int column_1based, const char *colname,char *buf, char *buf_end, const char *format, va_list ap)
	__attribute__((__format__(__printf__, 7, 0)));

static void
format_error(struct error_handling *restrict admin, lng row_1based, int column_1based, const char *colname,char *buf, char *buf_end, const char *format, va_list ap)
{
	char col_msg[100];
	if (is_int_nil(column_1based))
		col_msg[0] = '\0';
	else if (colname)
		snprintf(col_msg, sizeof(col_msg), " column %d '%s'", column_1based, admin->column_name);
	else
		snprintf(col_msg, sizeof(col_msg), " column %d", column_1based);

	int n = snprintf(buf, buf_end - buf, "Row %ld%s: ", row_1based, col_msg);
	if (n < buf_end - buf) {
		buf += n;
		n = vsnprintf(buf, buf_end - buf, format, ap);
		if (n < 0) {
			snprintf(buf, buf_end - buf, "An error occurred during error reporting");
		}
	}
}

static void
copy_add_to_rejects(lng row_1based, int column_1based, const char *msg)
{
	bool ok = false;

	Client cntxt = getClientContext();
	MT_lock_set(&cntxt->error_lock);

	if (cntxt->error_row == NULL) {
		cntxt->error_row = COLnew(0, TYPE_lng, 1000, TRANSIENT);
		cntxt->error_fld = COLnew(0, TYPE_int, 1000, TRANSIENT);
		cntxt->error_msg = COLnew(0, TYPE_str, 1000, TRANSIENT);
		cntxt->error_input = COLnew(0, TYPE_str, 1000, TRANSIENT);

		ok = ( cntxt->error_row != NULL
			&& cntxt->error_fld != NULL
			&& cntxt->error_msg != NULL
			&& cntxt->error_input != NULL);
		if (!ok)
			goto end;

		// By default TRANSIENT BATs are tied to the thread that created them.
		// However, these BATs are used from multiple threads so we have to
		// remove that association.
		BBP_pid(cntxt->error_row->batCacheid) = 0;
		BBP_pid(cntxt->error_fld->batCacheid) = 0;
		BBP_pid(cntxt->error_msg->batCacheid) = 0;
		BBP_pid(cntxt->error_input->batCacheid) = 0;
	}

	ok = ( BUNappend(cntxt->error_row, &row_1based, false) == GDK_SUCCEED
		&& BUNappend(cntxt->error_fld, &column_1based, false) == GDK_SUCCEED
		&& BUNappend(cntxt->error_msg, msg, false) == GDK_SUCCEED
		&& BUNappend(cntxt->error_input, str_nil, false) == GDK_SUCCEED);

end:
	if (!ok) {
		// BEST EFFORT is on a best effort basis. We don't want the error
		// handling of the error handling to complicate the rest of the code.
		if (cntxt->error_row != NULL) {
			BBPunfix(cntxt->error_row->batCacheid);
			cntxt->error_row = NULL;
		}
		if (cntxt->error_fld != NULL) {
			BBPunfix(cntxt->error_fld->batCacheid);
			cntxt->error_fld = NULL;
		}
		if (cntxt->error_msg != NULL) {
			BBPunfix(cntxt->error_msg->batCacheid);
			cntxt->error_msg = NULL;
		}
		if (cntxt->error_input != NULL) {
			BBPunfix(cntxt->error_input->batCacheid);
			cntxt->error_input = NULL;
		}
	}
	MT_lock_unset(&cntxt->error_lock);
}

static bool
too_many_errors(struct error_handling *admin)
{
	bool best_effort = !is_bat_nil(admin->failures_bat_id);
	return admin->fatal || (admin->count > 0 && !best_effort);
}

static BAT *
get_failures_bat(struct error_handling *admin)
{
	if (is_bat_nil(admin->failures_bat_id))
		return NULL;
	if (admin->failures_bat != NULL)
		return admin->failures_bat;

	admin->failures_bat = BATdescriptor(admin->failures_bat_id);
	if (admin->failures_bat == NULL)
		admin->fatal = true;
	return admin->failures_bat;
}


gdk_return
copy_report_error(struct error_handling *restrict admin, int rel_row, int column, _In_z_ _Printf_format_string_ const char *restrict format, ...)
{
	va_list ap;
	const char *col_name;
	char buffer[sizeof(admin->buffer)];
	assert(sizeof(buffer) == 512);

	// The first error message goes to the persistent buffer, the rest go to the scratch buffer
	char *buf, *buf_end;
	if (admin->count++ == 0) {
		buf = admin->buffer;
		buf_end = buf + sizeof(admin->buffer);
	} else {
		buf = buffer;
		buf_end = buf + sizeof(buffer);
	}

	// Format the error message
	if (column < 0)
		column = admin->default_col_no;
	if (column == admin->default_col_no && admin->column_name != NULL)
		col_name = admin->column_name;
	else
		col_name = NULL;
	int column_1based = column >= 0 ? column + 1 : int_nil;
	lng row_1based = admin->starting_row + 1 + rel_row;
	va_start(ap, format);
	format_error(admin, row_1based, column_1based, col_name, buf, buf_end, format, ap);
	va_end(ap);

	copy_add_to_rejects(row_1based, column_1based, buf);

	// In BEST EFFORT mode, keep track of the failed lines.
	if (!admin->inhibit_deletes) {
		BAT *failures = get_failures_bat(admin);
		if (failures != NULL) {
			oid row_as_oid = (oid)(admin->starting_row + rel_row);
			gdk_return ret = BUNappend(failures, &row_as_oid, false);
			if (ret != GDK_SUCCEED)
				admin->fatal = true;
		}
	}

	return too_many_errors(admin) ? GDK_FAIL : GDK_SUCCEED;;
}

str
copy_check_too_many_errors(struct error_handling *admin, const char *fname)
{
	if  (too_many_errors(admin)) {
		const char *message = copy_error_message(admin);
		lng error_count = admin->count;
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

