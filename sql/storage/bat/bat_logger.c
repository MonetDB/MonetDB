/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_logger.h"
#include "bat_utils.h"
#include "sql_types.h" /* EC_POS */
#include "wlc.h"
#include "gdk_logger_internals.h"
#include "mutils.h"

#define CATALOG_AUG2018 52202
#define CATALOG_JUN2020 52204

/* return GDK_SUCCEED if we can handle the upgrade from oldversion to
 * newversion */
static gdk_return
bl_preversion(sqlstore *store, int oldversion, int newversion)
{
	(void)newversion;

#ifdef CATALOG_JUN2020
	if (oldversion == CATALOG_JUN2020) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

	return GDK_FAIL;
}

#define N(schema, table, column)	schema "_" table "_" column

#define D(schema, table)	"D_" schema "_" table

#if defined CATALOG_AUG2018 || defined CATALOG_JUN2020
#if 0
static int
find_table_id(logger *lg, const char *val, int *sid)
{
	BAT *s = NULL;
	BAT *b, *t;
	BATiter bi;
	oid o;
	int id;

	b = temp_descriptor(logger_find_bat(lg, 2003)); /* schemas.name */
	if (b == NULL)
		return 0;
	s = BATselect(b, NULL, "sys", NULL, 1, 1, 0);
	bat_destroy(b);
	if (s == NULL)
		return 0;
	if (BATcount(s) == 0) {
		bat_destroy(s);
		return 0;
	}
	o = BUNtoid(s, 0);
	bat_destroy(s);
	b = temp_descriptor(logger_find_bat(lg, 2002)); /* schemas.id */
	if (b == NULL)
		return 0;
	bi = bat_iterator(b);
	id = * (const int *) BUNtloc(bi, o - b->hseqbase);
	bat_destroy(b);
	/* store id of schema "sys" */
	*sid = id;

	b = temp_descriptor(logger_find_bat(lg, 2069)); /* _tables.name */
	if (b == NULL) {
		bat_destroy(s);
		return 0;
	}
	s = BATselect(b, NULL, val, NULL, 1, 1, 0);
	bat_destroy(b);
	if (s == NULL)
		return 0;
	if (BATcount(s) == 0) {
		bat_destroy(s);
		return 0;
	}
	b = temp_descriptor(logger_find_bat(lg, 2070)); /* _tables.schema_id */
	if (b == NULL) {
		bat_destroy(s);
		return 0;
	}
	t = BATselect(b, s, &id, NULL, 1, 1, 0);
	bat_destroy(b);
	bat_destroy(s);
	s = t;
	if (s == NULL)
		return 0;
	if (BATcount(s) == 0) {
		bat_destroy(s);
		return 0;
	}

	o = BUNtoid(s, 0);
	bat_destroy(s);

	b = temp_descriptor(logger_find_bat(lg, 2068)); /* _tables.id */
	if (b == NULL)
		return 0;
	bi = bat_iterator(b);
	id = * (const int *) BUNtloc(bi, o - b->hseqbase);
	bat_destroy(b);
	return id;
}

static gdk_return
tabins(void *lg, bool first, int tt, const char *nname, const char *sname, const char *tname, ...)
{
	va_list va;
	char lname[64];
	const char *cname;
	const void *cval;
	gdk_return rc;
	BAT *b;
	int len;

	va_start(va, tname);
	while ((cname = va_arg(va, char *)) != NULL) {
		cval = va_arg(va, void *);
		len = snprintf(lname, sizeof(lname), "%s_%s_%s", sname, tname, cname);
		if (len == -1 || (size_t)len >= sizeof(lname) ||
			(b = temp_descriptor(logger_find_bat(lg, lname, 0, 0))) == NULL) {
			va_end(va);
			return GDK_FAIL;
		}
		if (first) {
			BAT *bn;
			if ((bn = COLcopy(b, b->ttype, true, PERSISTENT)) == NULL) {
				BBPunfix(b->batCacheid);
				va_end(va);
				return GDK_FAIL;
			}
			BBPunfix(b->batCacheid);
			if (BATsetaccess(bn, BAT_READ) != GDK_SUCCEED ||
			    logger_add_bat(lg, bn, lname, 0, 0) != GDK_SUCCEED) {
				BBPunfix(bn->batCacheid);
				va_end(va);
				return GDK_FAIL;
			}
			b = bn;
		}
		rc = BUNappend(b, cval, true);
		BBPunfix(b->batCacheid);
		if (rc != GDK_SUCCEED) {
			va_end(va);
			return rc;
		}
	}
	va_end(va);

	if (tt >= 0) {
		if ((b = COLnew(0, tt, 0, PERSISTENT)) == NULL)
			return GDK_FAIL;
		if ((rc = BATsetaccess(b, BAT_READ)) == GDK_SUCCEED)
			rc = logger_add_bat(lg, b, nname, 0, 0);
		BBPunfix(b->batCacheid);
		if (rc != GDK_SUCCEED)
			return rc;
	}
	return GDK_SUCCEED;
}
#endif
#endif

static gdk_return
bl_postversion(void *Store, void *lg)
{
	sqlstore *store = Store;
	(void)store;
	(void)lg;
#ifdef CATALOG_JUN2020
	if (store->catalog_version <= CATALOG_JUN2020) {
		lng lid;
		BAT *fid = temp_descriptor(logger_find_bat(lg, 2017)); /* functions.id */
		if (logger_sequence(lg, OBJ_SID, &lid) == 0 ||
		    fid == NULL) {
			bat_destroy(fid);
			return GDK_FAIL;
		}
		BAT *sem = COLnew(fid->hseqbase, TYPE_bit, BATcount(fid), PERSISTENT);
		if (sem == NULL) {
			bat_destroy(fid);
			return GDK_FAIL;
		}
		bit *fsys = (bit *) Tloc(sem, 0);
		for (BUN p = 0, q = BATcount(fid); p < q; p++) {
			fsys[p] = 1;
		}

		sem->tkey = false;
		sem->tsorted = sem->trevsorted = true;
		sem->tnonil = true;
		sem->tnil = false;
		BATsetcount(sem, BATcount(fid));
		bat_destroy(fid);
		if (BATsetaccess(sem, BAT_READ) != GDK_SUCCEED ||
		    log_bat_persists(lg, sem, 2162) != GDK_SUCCEED) {

			bat_destroy(sem);
			return GDK_FAIL;
		}
		bat_destroy(sem);
#if 0 /* TODO */
		int sid;
		int tid = find_table_id(lg, "functions", &sid);
		if (tabins(lg, true, -1, NULL, "sys", "_columns",
			   "id", &id,
			   "name", "semantics",
			   "type", "boolean",
			   "type_digits", &((const int) {1}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &((const int) {11}),
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		{	/* move sql.degrees, sql.radians, sql.like and sql.ilike functions from 09_like.sql and 10_math.sql script to sql_types list */
			BAT *func_func = temp_descriptor(logger_find_bat(lg, N("sys", "functions", "name"), 0, 0));
			if (func_func == NULL) {
				return GDK_FAIL;
			}
			BAT *degrees_func = BATselect(func_func, NULL, "degrees", NULL, 1, 1, 0);
			if (degrees_func == NULL) {
				bat_destroy(func_func);
				return GDK_FAIL;
			}
			BAT *radians_func = BATselect(func_func, NULL, "radians", NULL, 1, 1, 0);
			if (radians_func == NULL) {
				bat_destroy(degrees_func);
				return GDK_FAIL;
			}

			BAT *cands = BATmergecand(degrees_func, radians_func);
			bat_destroy(degrees_func);
			bat_destroy(radians_func);
			if (cands == NULL) {
				bat_destroy(func_func);
				return GDK_FAIL;
			}

			BAT *like_func = BATselect(func_func, NULL, "like", NULL, 1, 1, 0);
			if (like_func == NULL) {
				bat_destroy(func_func);
				bat_destroy(cands);
				return GDK_FAIL;
			}

			BAT *ncands = BATmergecand(cands, like_func);
			bat_destroy(cands);
			bat_destroy(like_func);
			if (ncands == NULL) {
				bat_destroy(func_func);
				return GDK_FAIL;
			}

			BAT *ilike_func = BATselect(func_func, NULL, "ilike", NULL, 1, 1, 0);
			bat_destroy(func_func);
			if (ilike_func == NULL) {
				bat_destroy(ncands);
				return GDK_FAIL;
			}

			BAT *final_cands = BATmergecand(ncands, ilike_func);
			bat_destroy(ncands);
			bat_destroy(ilike_func);
			if (final_cands == NULL) {
				return GDK_FAIL;
			}

			BAT *sys_funcs = temp_descriptor(logger_find_bat(lg, D("sys", "functions"), 0, 0));
			if (sys_funcs == NULL) {
				bat_destroy(final_cands);
				return GDK_FAIL;
			}
			gdk_return res = BATappend(sys_funcs, final_cands, NULL, true);
			bat_destroy(final_cands);
			bat_destroy(sys_funcs);
			if (res != GDK_SUCCEED)
				return res;
			if ((res = logger_upgrade_bat(lg, D("sys", "functions"), LOG_TAB, 0)) != GDK_SUCCEED)
				return res;
		}
		{	/* Fix SQL aggregation functions defined on the wrong modules: sql.null, sql.all, sql.zero_or_one and sql.not_unique */
			BAT *func_mod = temp_descriptor(logger_find_bat(lg, N("sys", "functions", "mod"), 0, 0));
			if (func_mod == NULL)
				return GDK_FAIL;

			BAT *sqlfunc = BATselect(func_mod, NULL, "sql", NULL, 1, 1, 0); /* Find the functions defined on sql module */
			if (sqlfunc == NULL) {
				bat_destroy(func_mod);
				return GDK_FAIL;
			}
			BAT *func_type = temp_descriptor(logger_find_bat(lg, N("sys", "functions", "type"), 0, 0));
			if (func_type == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlfunc);
				return GDK_FAIL;
			}
			int three = 3; /* and are aggregates */
			BAT *sqlaggr_func = BATselect(func_type, sqlfunc, &three, NULL, 1, 1, 0);
			bat_destroy(sqlfunc);
			if (sqlaggr_func == NULL) {
				bat_destroy(func_mod);
				return GDK_FAIL;
			}

			BAT *func_func = temp_descriptor(logger_find_bat(lg, N("sys", "functions", "func"), 0, 0));
			if (func_func == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				return GDK_FAIL;
			}
			BAT *nullfunc = BATselect(func_func, sqlaggr_func, "null", NULL, 1, 1, 0);
			if (nullfunc == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				bat_destroy(func_func);
				return GDK_FAIL;
			}
			BAT *allfunc = BATselect(func_func, sqlaggr_func, "all", NULL, 1, 1, 0);
			if (allfunc == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				bat_destroy(func_func);
				bat_destroy(nullfunc);
				return GDK_FAIL;
			}
			BAT *zero_or_onefunc = BATselect(func_func, sqlaggr_func, "zero_or_one", NULL, 1, 1, 0);
			if (zero_or_onefunc == NULL) {
				bat_destroy(func_func);
				bat_destroy(sqlaggr_func);
				bat_destroy(func_mod);
				bat_destroy(nullfunc);
				bat_destroy(allfunc);
				return GDK_FAIL;
			}
			BAT *not_uniquefunc = BATselect(func_func, sqlaggr_func, "not_unique", NULL, 1, 1, 0);
			bat_destroy(func_func);
			bat_destroy(sqlaggr_func);
			if (not_uniquefunc == NULL) {
				bat_destroy(func_mod);
				bat_destroy(nullfunc);
				bat_destroy(allfunc);
				bat_destroy(zero_or_onefunc);
				return GDK_FAIL;
			}

			BAT *cands1 = BATmergecand(nullfunc, allfunc);
			bat_destroy(nullfunc);
			bat_destroy(allfunc);
			if (cands1 == NULL) {
				bat_destroy(func_mod);
				bat_destroy(zero_or_onefunc);
				bat_destroy(not_uniquefunc);
				return GDK_FAIL;
			}
			BAT *cands2 = BATmergecand(cands1, zero_or_onefunc);
			bat_destroy(zero_or_onefunc);
			bat_destroy(cands1);
			if (cands2 == NULL) {
				bat_destroy(func_mod);
				bat_destroy(not_uniquefunc);
				return GDK_FAIL;
			}
			BAT *cands3 = BATmergecand(cands2, not_uniquefunc);
			bat_destroy(not_uniquefunc);
			bat_destroy(cands2);
			if (cands3 == NULL) {
				bat_destroy(func_mod);
				return GDK_FAIL;
			}

			BAT *cands_project = BATproject(cands3, func_mod);
			if (cands_project == NULL) {
				bat_destroy(func_mod);
				bat_destroy(cands3);
				return GDK_FAIL;
			}
			const char *right_module = "aggr"; /* set module to 'aggr' */
			BAT *update_bat = BATconstant(cands_project->hseqbase, TYPE_str, right_module, BATcount(cands_project), TRANSIENT);
			bat_destroy(cands_project);
			if (update_bat == NULL) {
				bat_destroy(func_mod);
				bat_destroy(cands3);
				return GDK_FAIL;
			}

			gdk_return res = BATreplace(func_mod, cands3, update_bat, TRUE);
			bat_destroy(func_mod);
			bat_destroy(cands3);
			bat_destroy(update_bat);
			if (res != GDK_SUCCEED)
				return res;
			if ((res = logger_upgrade_bat(lg, N("sys", "functions", "mod"), LOG_COL, 0)) != GDK_SUCCEED)
				return res;
		}
#endif
	}
#endif

	return GDK_SUCCEED;
}

static int
bl_create(sqlstore *store, int debug, const char *logdir, int cat_version)
{
	if (store->logger)
		return LOG_ERR;
	store->logger = logger_create(debug, "sql", logdir, cat_version, (preversionfix_fptr)&bl_preversion, (postversionfix_fptr)&bl_postversion, store);
	if (store->logger)
		return LOG_OK;
	return LOG_ERR;
}

static void
bl_destroy(sqlstore *store)
{
	logger *l = store->logger;

	store->logger = NULL;
	if (l) {
		close_stream(l->output_log);
		GDKfree(l->fn);
		GDKfree(l->dir);
		GDKfree(l->local_dir);
		GDKfree(l->buf);
		GDKfree(l);
	}
}

static int
bl_flush(sqlstore *store, lng save_id)
{
	if (store->logger)
		return logger_flush(store->logger, save_id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	return LOG_OK;
}

static int
bl_changes(sqlstore *store)
{
	return (int) MIN(logger_changes(store->logger), GDK_int_max);
}

static int
bl_get_sequence(sqlstore *store, int seq, lng *id)
{
	return logger_sequence(store->logger, seq, id);
}

static int
bl_log_isnew(sqlstore *store)
{
	logger *bat_logger = store->logger;
	if (BATcount(bat_logger->catalog_bid) > 10) {
		return 0;
	}
	return 1;
}

static int
bl_tstart(sqlstore *store)
{
	return log_tstart(store->logger) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
bl_tend(sqlstore *store)
{
	return log_tend(store->logger) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static lng
bl_tid(sqlstore *store)
{
	return log_save_id(store->logger);
}

static int
bl_sequence(sqlstore *store, int seq, lng id)
{
	return log_sequence(store->logger, seq, id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

/* Write a plan entry to copy part of the given file.
 * That part of the file must remain unchanged until the plan is executed.
 */
static void
snapshot_lazy_copy_file(stream *plan, const char *name, uint64_t extent)
{
	mnstr_printf(plan, "c %" PRIu64 " %s\n", extent, name);
}

/* Write a plan entry to write the current contents of the given file.
 * The contents are included in the plan so the source file is allowed to
 * change in the mean time.
 */
static gdk_return
snapshot_immediate_copy_file(stream *plan, const char *path, const char *name)
{
	gdk_return ret = GDK_FAIL;
	const size_t bufsize = 64 * 1024;
	struct stat statbuf;
	char *buf = NULL;
	stream *s = NULL;
	size_t to_copy;

	if (MT_stat(path, &statbuf) < 0) {
		GDKsyserror("stat failed on %s", path);
		goto end;
	}
	to_copy = (size_t) statbuf.st_size;

	s = open_rstream(path);
	if (!s) {
		GDKerror("%s", mnstr_peek_error(NULL));
		goto end;
	}

	buf = GDKmalloc(bufsize);
	if (!buf) {
		GDKerror("GDKmalloc failed");
		goto end;
	}

	mnstr_printf(plan, "w %zu %s\n", to_copy, name);

	while (to_copy > 0) {
		size_t chunk = (to_copy <= bufsize) ? to_copy : bufsize;
		ssize_t bytes_read = mnstr_read(s, buf, 1, chunk);
		if (bytes_read < 0) {
			char *err = mnstr_error(s);
			GDKerror("Reading bytes of component %s failed: %s", path, err);
			free(err);
			goto end;
		} else if (bytes_read < (ssize_t) chunk) {
			char *err = mnstr_error(s);
			GDKerror("Read only %zu/%zu bytes of component %s: %s", (size_t) bytes_read, chunk, path, err);
			free(err);
			goto end;
		}

		ssize_t bytes_written = mnstr_write(plan, buf, 1, chunk);
		if (bytes_written < 0) {
			GDKerror("Writing to plan failed");
			goto end;
		} else if (bytes_written < (ssize_t) chunk) {
			GDKerror("write to plan truncated");
			goto end;
		}
		to_copy -= chunk;
	}

	ret = GDK_SUCCEED;
end:
	GDKfree(buf);
	if (s)
		close_stream(s);
	return ret;
}

/* Add plan entries for all relevant files in the Write Ahead Log */
static gdk_return
snapshot_wal(logger *bat_logger, stream *plan, const char *db_dir)
{
	char log_file[FILENAME_MAX];
	int len;

	len = snprintf(log_file, sizeof(log_file), "%s/%s%s", db_dir, bat_logger->dir, LOGFILE);
	if (len == -1 || (size_t)len >= sizeof(log_file)) {
		GDKerror("Could not open %s, filename is too large", log_file);
		return GDK_FAIL;
	}
	snapshot_immediate_copy_file(plan, log_file, log_file + strlen(db_dir) + 1);

	for (lng id = bat_logger->saved_id+1; id <= bat_logger->id; id++) {
		struct stat statbuf;

		len = snprintf(log_file, sizeof(log_file), "%s/%s%s." LLFMT, db_dir, bat_logger->dir, LOGFILE, id);
		if (len == -1 || (size_t)len >= sizeof(log_file)) {
			GDKerror("Could not open %s, filename is too large", log_file);
			return GDK_FAIL;
		}
		if (MT_stat(log_file, &statbuf) == 0) {
			snapshot_lazy_copy_file(plan, log_file + strlen(db_dir) + 1, statbuf.st_size);
		} else {
			GDKerror("Could not open %s", log_file);
			return GDK_FAIL;
		}
	}
	return GDK_SUCCEED;
}

static gdk_return
snapshot_heap(stream *plan, const char *db_dir, uint64_t batid, const char *filename, const char *suffix, uint64_t extent)
{
	char path1[FILENAME_MAX];
	char path2[FILENAME_MAX];
	const size_t offset = strlen(db_dir) + 1;
	struct stat statbuf;
	int len;

	// first check the backup dir
	len = snprintf(path1, FILENAME_MAX, "%s/%s/%" PRIo64 "%s", db_dir, BAKDIR, batid, suffix);
	if (len == -1 || len >= FILENAME_MAX) {
		path1[FILENAME_MAX - 1] = '\0';
		GDKerror("Could not open %s, filename is too large", path1);
		return GDK_FAIL;
	}
	if (MT_stat(path1, &statbuf) == 0) {
		snapshot_lazy_copy_file(plan, path1 + offset, extent);
		return GDK_SUCCEED;
	}
	if (errno != ENOENT) {
		GDKsyserror("Error stat'ing %s", path1);
		return GDK_FAIL;
	}

	// then check the regular location
	len = snprintf(path2, FILENAME_MAX, "%s/%s/%s%s", db_dir, BATDIR, filename, suffix);
	if (len == -1 || len >= FILENAME_MAX) {
		path2[FILENAME_MAX - 1] = '\0';
		GDKerror("Could not open %s, filename is too large", path2);
		return GDK_FAIL;
	}
	if (MT_stat(path2, &statbuf) == 0) {
		snapshot_lazy_copy_file(plan, path2 + offset, extent);
		return GDK_SUCCEED;
	}
	if (errno != ENOENT) {
		GDKsyserror("Error stat'ing %s", path2);
		return GDK_FAIL;
	}

	GDKerror("One of %s and %s must exist", path1, path2);
	return GDK_FAIL;
}

/* Add plan entries for all persistent BATs by looping over the BBP.dir.
 * Also include the BBP.dir itself.
 */
static gdk_return
snapshot_bats(stream *plan, const char *db_dir)
{
	char bbpdir[FILENAME_MAX];
	stream *cat = NULL;
	char line[1024];
	int gdk_version, len;
	gdk_return ret = GDK_FAIL;

	len = snprintf(bbpdir, FILENAME_MAX, "%s/%s/%s", db_dir, BAKDIR, "BBP.dir");
	if (len == -1 || len >= FILENAME_MAX) {
		GDKerror("Could not open %s, filename is too large", bbpdir);
		goto end;
	}
	ret = snapshot_immediate_copy_file(plan, bbpdir, bbpdir + strlen(db_dir) + 1);
	if (ret != GDK_SUCCEED)
		goto end;

	// Open the catalog and parse the header
	cat = open_rastream(bbpdir);
	if (cat == NULL) {
		GDKerror("Could not open %s for reading: %s", bbpdir, mnstr_peek_error(NULL));
		goto end;
	}
	if (mnstr_readline(cat, line, sizeof(line)) < 0) {
		GDKerror("Could not read first line of %s", bbpdir);
		goto end;
	}
	if (sscanf(line, "BBP.dir, GDKversion %d", &gdk_version) != 1) {
		GDKerror("Invalid first line of %s", bbpdir);
		goto end;
	}
	if (gdk_version != 061043U) {
		// If this version number has changed, the structure of BBP.dir
		// may have changed. Update this whole function to take this
		// into account.
		// Note: when startup has completed BBP.dir is guaranteed
		// to the latest format so we don't have to support any older
		// formats in this function.
		GDKerror("GDK version mismatch in snapshot yet");
		goto end;
	}
	if (mnstr_readline(cat, line, sizeof(line)) < 0) {
		GDKerror("Couldn't skip the second line of %s", bbpdir);
		goto end;
	}
	if (mnstr_readline(cat, line, sizeof(line)) < 0) {
		GDKerror("Couldn't skip the third line of %s", bbpdir);
		goto end;
	}

	/* TODO get transaction id and last processed log file id */
	if (mnstr_readline(cat, line, sizeof(line)) < 0) {
		GDKerror("Couldn't skip the 4th line of %s", bbpdir);
		goto end;
	}

	while (mnstr_readline(cat, line, sizeof(line)) > 0) {
		uint64_t batid;
		uint64_t tail_free;
		uint64_t theap_free;
		char filename[sizeof(BBP_physical(0))];
		// The lines in BBP.dir come in various lengths.
		// we try to parse the longest variant then check
		// the return value of sscanf to see which fields
		// were actually present.
		int scanned = sscanf(line,
				// Taken from the sscanf in BBPreadEntries() in gdk_bbp.c.
				// 8 fields, we need field 1 (batid) and field 4 (filename)
				"%" SCNu64 " %*s %*s %19s %*s %*s %*s %*s"

				// Taken from the sscanf in heapinit() in gdk_bbp.c.
				// 14 fields, we need field 10 (free)
				" %*s %*s %*s %*s %*s %*s %*s %*s %*s %" SCNu64 " %*s %*s %*s %*s"

				// Taken from the sscanf in vheapinit() in gdk_bbp.c.
				// 3 fields, we need field 1 (free).
				"%" SCNu64 " %*s ^*s"
				,
				&batid, filename,
				&tail_free,
				&theap_free);

		// The following switch uses fallthroughs to make
		// the larger cases include the work of the smaller cases.
		switch (scanned) {
			default:
				GDKerror("Couldn't parse (%d) %s line: %s", scanned, bbpdir, line);
				goto end;
			case 4:
				// tail and theap
				ret = snapshot_heap(plan, db_dir, batid, filename, ".theap", theap_free);
				if (ret != GDK_SUCCEED)
					goto end;
				/* fallthrough */
			case 3:
				// tail only
				snapshot_heap(plan, db_dir, batid, filename, ".tail", tail_free);
				if (ret != GDK_SUCCEED)
					goto end;
				/* fallthrough */
			case 2:
				// no tail?
				break;
		}
	}

end:
	if (cat) {
		close_stream(cat);
	}
	return ret;
}

/* Add a file to the plan which records the current wlc status, if any.
 * In particular, `wlc_batches`.
 *
 * With this information, a replica initialized from this snapshot can
 * be configured to catch up with its master by replaying later transactions.
 */
static gdk_return
snapshot_wlc(stream *plan, const char *db_dir)
{
	const char name[] = "wlr.config.in";
	char buf[1024];
	int len;

	(void)db_dir;

	if (wlc_state != WLC_RUN)
		return GDK_SUCCEED;

	len = snprintf(buf, sizeof(buf),
		"beat=%d\n"
		"batches=%d\n"
		, wlc_beat, wlc_batches
	);

	mnstr_printf(plan, "w %d %s\n", len, name);
	mnstr_write(plan, buf, 1, len);

	return GDK_SUCCEED;
}

static gdk_return
snapshot_vaultkey(stream *plan, const char *db_dir)
{
	char path[FILENAME_MAX];
	struct stat statbuf;

	int len = snprintf(path, FILENAME_MAX, "%s/.vaultkey", db_dir);
	if (len == -1 || len >= FILENAME_MAX) {
		path[FILENAME_MAX - 1] = '\0';
		GDKerror("Could not open %s, filename is too large", path);
		return GDK_FAIL;
	}
	if (MT_stat(path, &statbuf) == 0) {
		snapshot_lazy_copy_file(plan, ".vaultkey", statbuf.st_size);
		return GDK_SUCCEED;
	}
	if (errno == ENOENT) {
		// No .vaultkey? Fine.
		return GDK_SUCCEED;
	}

	GDKsyserror("Error stat'ing %s", path);
	return GDK_FAIL;
}
static gdk_return
bl_snapshot(sqlstore *store, stream *plan)
{
	logger *bat_logger = store->logger;
	gdk_return ret;
	char *db_dir = NULL;
	size_t db_dir_len;

	// Farm 0 is always the persistent farm.
	db_dir = GDKfilepath(0, NULL, "", NULL);
	db_dir_len = strlen(db_dir);
	if (db_dir[db_dir_len - 1] == DIR_SEP)
		db_dir[db_dir_len - 1] = '\0';

	mnstr_printf(plan, "%s\n", db_dir);

	// Please monetdbd
	mnstr_printf(plan, "w 0 .uplog\n");

	ret = snapshot_vaultkey(plan, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = snapshot_bats(plan, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = snapshot_wal(bat_logger, plan, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = snapshot_wlc(plan, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = GDK_SUCCEED;
end:
	if (db_dir)
		GDKfree(db_dir);
	return ret;
}

void
bat_logger_init( logger_functions *lf )
{
	lf->create = bl_create;
	lf->destroy = bl_destroy;
	lf->flush = bl_flush;
	lf->changes = bl_changes;
	lf->get_sequence = bl_get_sequence;
	lf->log_isnew = bl_log_isnew;
	lf->log_tstart = bl_tstart;
	lf->log_tend = bl_tend;
	lf->log_save_id = bl_tid;
	lf->log_sequence = bl_sequence;
	lf->get_snapshot_files = bl_snapshot;
}
