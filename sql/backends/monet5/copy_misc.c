/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
copy_init_error_handling(struct error_handling *admin, Client cntxt, lng starting_row, int default_col_no, const char *column_name, bat rows)
{
	admin->cntxt = cntxt;
	admin->count = 0;
	admin->fatal = false;
	admin->starting_row = starting_row;
	admin->default_col_no = default_col_no;
	admin->column_name = column_name ? GDKstrdup(column_name) : NULL;
	admin->buffer[0] = '\0';
	admin->init = true;
	admin->rows_batid = rows;
	admin->rows = NULL;
}

void
copy_destroy_error_handling(struct error_handling *admin)
{
	GDKfree(admin->column_name);
	if (admin->rows)
		BBPunfix(admin->rows->batCacheid);
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

	int n = snprintf(buf, buf_end - buf, "Row "LLFMT"%s: ", row_1based, col_msg);
	if (n < buf_end - buf) {
		buf += n;
		n = vsnprintf(buf, buf_end - buf, format, ap);
		if (n < 0) {
			snprintf(buf, buf_end - buf, "An error occurred during error reporting");
		}
	}
	if (admin->r && !admin->r->best_effort)
		admin->r->error = true;
}

static void
copy_add_to_rejects(Client cntxt, lng row_1based, int column_1based, const char *msg)
{
	bool ok = false;
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
	return admin->fatal || (admin->count > 0 && !(admin->r && admin->r->best_effort));
}

gdk_return
copy_report_error(struct error_handling *restrict admin, lng rel_row, int column, _In_z_ _Printf_format_string_ const char *restrict format, ...)
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
	//assert(column_1based != int_nil || format[0] == 't' || format[1] == 'o');
	lng row_1based = admin->starting_row + 1 + rel_row;
	va_start(ap, format);
	format_error(admin, row_1based, column_1based, col_name, buf, buf_end, format, ap);
	va_end(ap);

	copy_add_to_rejects(admin->cntxt, row_1based, column_1based, buf);
	if (admin->r && admin->r->best_effort) {
		if (!admin->rows)
			admin->rows = BATdescriptor(admin->rows_batid);
		if (!admin->rows)
			return GDK_FAIL;
		BAT *b = admin->rows;
		BUN cnt = BATcount(b);
		if (cnt && !admin->rows->tvheap) {
			Heap *mask;
			if ((mask = GDKmalloc(sizeof(Heap))) == NULL){
				return GDK_FAIL;
			}
			char *nme = BBP_physical(b->batCacheid);
			*mask = (Heap) {
				.farmid = 0,//BBPselectfarm(b->batRole, b->ttype, varheap),
				.parentid = b->batCacheid,
				.dirty = true,
				.refs = ATOMIC_VAR_INIT(1),
			};
			strtconcat(mask->filename, sizeof(mask->filename), nme, ".theap", NULL);

			BUN nmask = (cnt + 31) / 32;
			if (mask->farmid < 0 ||
				HEAPalloc(mask, nmask + (sizeof(ccand_t)/sizeof(uint32_t)), sizeof(uint32_t)) != GDK_SUCCEED) {
				GDKfree(mask);
				return GDK_FAIL;
			}
			ccand_t *c = (ccand_t *) mask->base;
			*c = (ccand_t) {
				.type = CAND_MSK,
				//.mask = true,
			};
			mask->free = sizeof(ccand_t) + nmask * sizeof(uint32_t);
			uint32_t *r = (uint32_t*)(mask->base + sizeof(ccand_t));
			memset(r, ~0, (nmask-1) * sizeof(uint32_t));
			r[cnt/32] = (1U << (cnt % 32)) - 1;
			b->tvheap = mask;
		}
		if (cnt) {
			uint32_t *r = (uint32_t*)(admin->rows->tvheap->base + sizeof(ccand_t));
			int w = (int)(rel_row%32);
			/* unset rel_row bit */
			r[rel_row/32] &= ~(1<<w);
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
			throw(MAL, fname, "At least "LLFMT" conversion errors, example: %s", error_count, message);

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
COPYset_blocksize(Client ctx, int *dummy, int *blocksize)
{
	(void)ctx;
	(void)dummy;
	char buffer[128];
	sprintf(buffer, "%d", *blocksize);
	GDKsetenv(COPY_BLOCKSIZE_SETTING, buffer);
	return MAL_SUCCEED;
}

str
COPYget_blocksize(Client ctx, int *blocksize)
{
	(void)ctx;
	int size = GDKgetenv_int(COPY_BLOCKSIZE_SETTING, -1);
	*blocksize = size > 0 ? size : DEFAULT_COPY_BLOCKSIZE;
	return MAL_SUCCEED;
}
