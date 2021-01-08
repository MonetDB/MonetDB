/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"

#include <dirent.h>
#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include "msabaoth.h"
#include "mapi.h"
#include "snapshot.h"
#include "stream.h"
#include "utils/database.h"
#include "forkmserver.h"

struct dir_helper {
	char *dir;
	int fd;
};

static bool parse_snapshot_name(const char *filename, char **dbname, time_t *timestamp);
static err validate_location(const char *path);
static err unpack_tarstream(stream *tarstream, char *destdir, int skipcomponents);

static err parse_snapshot_name_selftest(void);

const char *VALID_EXTENSIONS[] = {
		".tar.lz4",
		".tar",
		".tar.gz",
		".tar.xz",
		".tar.bz2",
		NULL
};

static char*
snapshot_database_stream_helper(
	void *priv, const char *filename,
	 const void *data, size_t size)
{
	if (size == 0)
		return NULL;

	(void)filename;
	stream **ss = (stream**)priv;
	stream *s = *ss;
	if (!s)
		return newErr("attempted another write after failure");
	ssize_t nwritten = mnstr_write(s, data, 1, size);
	if (nwritten != (ssize_t)size) {
		*ss = NULL;
		return newErr("putfile write failed");
	}

	return NULL;
}

err
snapshot_database_stream(char *dbname, stream *s)
{
	err e = NO_ERR;
	sabdb *stats = NULL;
	Mapi conn = NULL;
	MapiHdl handle = NULL;
	stream *ss = s;

	/* Start the database if necessary */
	e = forkMserver(dbname, &stats, false);
	if (e != NO_ERR) {
		goto bailout;
	}

	if (!stats) {
		e = newErr("No such database: '%s'", dbname);
		goto bailout;
	}

	if (stats->secret == NULL) {
#if !defined(HAVE_OPENSSL) && !defined(HAVE_COMMONCRYPTO)
		e = newErr("snapshotting only works when MonetDB has been compiled with secure crypto support");
#else
		e = newErr("Cannot find secret for database '%s'", dbname);
#endif
		goto bailout;
	}

	/* Set up the connection. Connect directly to the unix domain socket */
	if (stats->conns == NULL || stats->conns[0].val == NULL ||
		(conn = mapi_mapiuri(stats->conns[0].val, ".snapshot",
							 stats->secret, "sql")) == NULL) {
		e = newErr("internal error: no conn");
		goto bailout;
	}
	if (mapi_error(conn) != MOK) {
		e = newErr("connection error: %s", mapi_error_str(conn));
		goto bailout;
	}
	mapi_reconnect(conn);
	if (mapi_error(conn) != MOK) {
		e = newErr("connection error: %s", mapi_error_str(conn));
		goto bailout;
	}

	mapi_setfilecallback(conn, NULL, snapshot_database_stream_helper, &ss);

	handle = mapi_prepare(conn, "CALL sys.hot_snapshot('/root/dummy', 0)");
	if (handle == NULL || mapi_error(conn)) {
		e = newErr("mapi_prepare: %s", mapi_error_str(conn));
		goto bailout;
	}
	if (mapi_execute(handle) != MOK) {
		e = newErr("%s", mapi_result_error(handle));
		goto bailout;
	}

	/* everything seems ok. Leave e == NO_ERR */
bailout:
	if (handle != NULL)
		mapi_close_handle(handle);
	if (conn != NULL)
		mapi_destroy(conn);
	if (stats != NULL)
		msab_freeStatus(&stats);
	return e;
}


/* Create a snapshot of database dbname to file dest.
 */
err
snapshot_database_to(char *dbname, char *dest)
{
	err e = NO_ERR;
	sabdb *stats = NULL;
	Mapi conn = NULL;
	MapiHdl handle = NULL;

	/* Start the database if necessary */
	e = forkMserver(dbname, &stats, false);
	if (e != NO_ERR) {
		goto bailout;
	}

	if (!stats) {
		e = newErr("No such database: '%s'", dbname);
		goto bailout;
	}

	if (stats->secret == NULL) {
#if !defined(HAVE_OPENSSL) && !defined(HAVE_COMMONCRYPTO)
		e = newErr("snapshotting only works when MonetDB has been compiled with secure crypto support");
#else
		e = newErr("Cannot find secret for database '%s'", dbname);
#endif
		goto bailout;
	}

	/* Do not overwrite random files on the system. */
	e = validate_location(dest);
	if (e != NO_ERR) {
		goto bailout;
	}

	/* Set up the connection. Connect directly to the unix domain socket */
	if (stats->conns == NULL || stats->conns[0].val == NULL ||
		(conn = mapi_mapiuri(stats->conns[0].val, ".snapshot", stats->secret, "sql")) == NULL) {
		e = newErr("internal error: no conn");
		goto bailout;
	}
	if (mapi_error(conn) != MOK) {
		e = newErr("connection error: %s", mapi_error_str(conn));
		goto bailout;
	}
	mapi_reconnect(conn);
	if (mapi_error(conn) != MOK) {
		e = newErr("connection error: %s", mapi_error_str(conn));
		goto bailout;
	}

	/* Trigger the snapshot */
	handle = mapi_prepare(conn, "CALL sys.hot_snapshot(?)");
	if (handle == NULL || mapi_error(conn)) {
		e = newErr("mapi_prepare: %s", mapi_error_str(conn));
		goto bailout;
	}
	if (mapi_param_string(handle, 0, 12, dest, NULL) != MOK) {
		e = newErr("mapi_param_string: %s", mapi_error_str(conn));
		goto bailout;
	}
	if (mapi_execute(handle) != MOK) {
		e = newErr("%s", mapi_result_error(handle));
		goto bailout;
	}

	/* everything seems ok. Leave e == NO_ERR */
bailout:
	if (handle != NULL)
		mapi_close_handle(handle);
	if (conn != NULL)
		mapi_destroy(conn);
	if (stats != NULL)
		msab_freeStatus(&stats);
	return e;
}

/* Restore a database from a snapshot file.
 * TODO: Make it work for databases without monetdb/monetdb root account.
 */
err
snapshot_restore_from(char *dbname, char *source)
{
	err e;
	(void)source;
	sabdb *existing = NULL;
	char *dbfarm = NULL;
	char *tmppath = NULL;
	char *destpath = NULL;
	stream *instream = NULL;

	e = db_validname(dbname);
	if (e != NO_ERR)
		goto bailout;

	/* Do not read random files on the system. */
	e = validate_location(source);
	if (e != NO_ERR) {
		goto bailout;
	}

	/* Check if the target db already exists */
	e = msab_getStatus(&existing, dbname);
	if (e != NO_ERR)
		goto bailout;

	/* If the database already exists it will be destroyed.
	 * We will lock it before unpacking the tar file and stop it after.
	 */
	if (existing != NULL && !existing->locked) {
		e = db_lock(dbname);
		if (e != NO_ERR)
			goto bailout;
	}

	/* Figure out the directory to create */
	e = msab_getDBfarm(&dbfarm);
	if (e != NO_ERR)
		goto bailout;
	tmppath = malloc(strlen(dbfarm) + strlen(dbname) + 100);
	sprintf(tmppath, "%s/.tmp.%s", dbfarm, dbname);
	destpath = malloc(strlen(dbfarm) + strlen(dbname) + 100);
	sprintf(destpath, "%s/%s", dbfarm, dbname);

	/* Remove the tmp directory, if it exists. */
	e = deletedir(tmppath);
	if (e != NO_ERR)
		goto bailout;

	/* Open the file */
	instream = open_rstream(source);
	if (instream == NULL) {
		e = newErr("%s", mnstr_peek_error(NULL));
		goto bailout;
	}

	/* Now create the tmp directory */
	if (mkdir(tmppath, 0755) < 0) {
		e = newErr("could not create dir '%s': %s", tmppath, strerror(errno));
		goto bailout;
	}

	/* fill it */
	e = unpack_tarstream(instream, tmppath, 1);
	if (e != NO_ERR)
		goto bailout;

	/* Stop the existing database, if any. Don't bother with a graceful shutdown. */
	if (existing != NULL) {
		if (existing->pid != 0) {
			kill(existing->pid, SIGKILL);
			sleep(1); // not sure if this is needed
		}
		db_destroy(existing->dbname);
	}

	/* remove whatever's laying around in the destination directory */
	e = deletedir(destpath);
	if (e != NO_ERR)
		goto bailout;

	/* and finally, move the thing into place */
	if (rename(tmppath, destpath) < 0) {
		e = newErr("Couldn't rename '%s' to '%s': %s",
			tmppath, destpath, strerror(errno));
		goto bailout;
	}

bailout:
	if (existing != NULL)
		msab_freeStatus(&existing);
	if (instream != NULL)
		close_stream(instream);
	free(dbfarm);
	free(tmppath);
	free(destpath);
	return e;
}

err
snapshot_destroy_file(char *path)
{
	char *e = validate_location(path);
	if (e != NULL)
		return e;

	if (remove(path) < 0)
		return newErr("Could not remove '%s': %s", path, strerror(errno));

	return NO_ERR;
}

err
snapshot_list(int *nsnapshots, struct snapshot **snapshots)
{
	err e = NO_ERR;
	DIR *dir = NULL;
	struct dirent *ent;
	char *snapdir;

	e = parse_snapshot_name_selftest();
	if (e != NO_ERR)
		goto bailout;

	/* get the snapdir */
	snapdir = getConfVal(_mero_props, "snapshotdir");
	if (snapdir == NULL || snapdir[0] == '\0') {
		e = newErr("Snapshot target file not allowed because no 'snapshotdir' has been configured");
		goto bailout;
	}

	dir = opendir(snapdir);
	if (dir == NULL) {
		e = newErr("could not open dir %s: %s", snapdir, strerror(errno));
		goto bailout;
	}
	while ((ent = readdir(dir)) != NULL) {
		struct stat statbuf;
		if (fstatat(dirfd(dir), ent->d_name, &statbuf, 0) < 0) {
			e = newErr("couldn't stat %s/%s: %s", snapdir, ent->d_name, strerror(errno));
			goto bailout;
		}
		if ((statbuf.st_mode & S_IFREG) == 0)
			continue;
		time_t timestamp = 0;
		char *dbname = NULL;
		if (!parse_snapshot_name(ent->d_name, &dbname, &timestamp))
			continue;

		struct snapshot *snap = push_snapshot(snapshots, nsnapshots);
		snap->dbname = dbname;
		snap->time = timestamp;
		snap->size = statbuf.st_size;
		snap->path = malloc(strlen(snapdir) + 1 + strlen(ent->d_name) + 1);
		sprintf(snap->path, "%s/%s", snapdir, ent->d_name);
	}

bailout:
	if (dir != NULL)
		closedir(dir);
	return e;
}


static err
parse_snapshot_name_selftest(void)
{
	struct {
		char *filename;
		bool expected_result;
		char *expected_dbname;
		time_t expected_time;
	} *tc, testcases[] = {
		{ "banana", false, NULL, 0 },
		{ "", false, NULL, 0 },
		{ "banana.tar.gz", false, NULL, 0 },
		{ "_20200402T114224UTC.tar.gz", false, NULL, 0 },
		{ "foo_20200402T114224UTC.tar.gz", true, "foo", 1585827744L },
		{ "foo_20200402T114224.tar.gz", false, NULL, 0},
		// terminator:
		{ NULL, false, NULL, 0},
	};

	char *errmsg = NULL;
	char *dbname = NULL;
	time_t timestamp = 0;
	bool result;
	for (tc = &testcases[0]; tc->filename != NULL; tc++) {
		free(dbname);
		dbname = NULL;
		timestamp = 0;
		result = parse_snapshot_name(tc->filename, &dbname, &timestamp);
		if (result != tc->expected_result) {
			errmsg = "didn't parse as expected";
			break;
		}
		if (result == false)
			continue;
		assert(tc->expected_dbname != NULL);
		if (dbname == NULL || strcmp(dbname, tc->expected_dbname) != 0) {
			errmsg = "dbname mismatch";
			break;
		}
		assert(tc->expected_time != 0);
		if (timestamp != tc->expected_time) {
			errmsg = "timestamp mismatch";
			break;
		}
	}

	if (errmsg != NULL) {
		errmsg = newErr(
			"parse_snapshot_name selftest failure for '%s':"
			" %s (%s, '%s', %" PRIu64 ")\n",
			tc->filename, errmsg,
			result ? "true" : "false",
			dbname ? dbname : "<uninit>",
			(uint64_t)timestamp);
		free(dbname);
		return errmsg;
	}
	free(dbname);

	return NO_ERR;
}

/* Return 0 if the filename is not a valid snapshot name for the
 * given datase. Otherwise, return the timestamp encoded in
 * the filename.
 */
static bool
parse_snapshot_name(const char *filename, char **dbname, time_t *timestamp)
{
	// Copy the whole filename to 'name'.  We will then chop bits off,
	// starting with the extension.
	char *name = strdup(filename);
	char *end = name + strlen(name);
	char *extension = NULL;

	// Look for a known extension
	for (const char **ext = &VALID_EXTENSIONS[0]; *ext != NULL; ext++) {
		if (strlen(*ext) >= strlen(name))
			continue;
		char *candidate = end - strlen(*ext);
		if (strcmp(*ext, candidate) == 0) {
			extension = candidate; // important that it points into 'name'
			break;
		}
	}
	if (extension == NULL) {
		free(name);
		return false;
	}
	*extension = '\0'; // chop!

	// Find the timestamp:
	// dbname_20200402T114224UTC.tar.gz
	//       ^^^^^^^^^^^^^^^^^^^ 19 chars back
	if (strlen(name) <= 19) {
		free(name);
		return false;
	}

	struct tm tm = {0};
	char *ts = extension - 19;
	char *ts_end = strptime(ts, "_%Y%m%dT%H%M%SUTC", &tm);
	if (ts_end != extension) {
		free(name);
		return false;
	}
	*ts = '\0'; // chop!

	// We want to interpret the timestamp as UTC.
	// Unfortunately, mktime interprets it as Localtime.
	time_t wrong = mktime(&tm);

	// Let's adjust it.
	// Take a timestamp, render it in UTC, then read back as Localtime.
	// Look at the difference.
	time_t before = time(NULL);
	gmtime_r(&before, &tm);
	time_t after = mktime(&tm);
	time_t correct = before - after + wrong;

	// Return the results
	*timestamp = correct;
	*dbname = name;
	return true;
}

err
snapshot_default_filename(char **ret, const char *dbname)
{
	err e = NO_ERR;
	time_t now;
	size_t len;
	char *name_buf = NULL;
	char *p;
	char *snapext_conf;
	char *snapext = NULL;

	/* get the snapdir */
	char *snapdir = getConfVal(_mero_props, "snapshotdir");
	if (snapdir == NULL || snapdir[0] == '\0') {
		e = newErr("Snapshot target file not allowed because no 'snapshotdir' has been configured");
		goto bailout;
	}

	/* get and verify the extension */
	snapext_conf = getConfVal(_mero_props, "snapshotcompression");
	if (snapext_conf != NULL) {
		for (const char **ext = &VALID_EXTENSIONS[0]; *ext != NULL; ext++) {
			if (strcmp(*ext, snapext_conf) == 0) {
				snapext = snapext_conf;
				break;
			}
		}
	}
	if (snapext == NULL) {
		e = newErr("Unsupported file format: '%s'",
			snapext_conf != NULL ? snapext_conf : "");
			goto bailout;
	}

	/* get the current time, in UTC */
	now  = time(NULL);
	struct tm time_parts;
	gmtime_r(&now, &time_parts);

	/* allocate and write the filename buffer */
	len = strlen(dbname) + strlen(snapdir) + 100;
	name_buf = malloc(len + 1);
	p = name_buf;
	p += sprintf(p, "%s%c%s_", snapdir, DIR_SEP, dbname);
	p += strftime(p, name_buf + len - p, "%Y%m%dT%H%M%SUTC", &time_parts);
	strcpy(p, snapext);

	/* return it */
	*ret = name_buf;

bailout:
	if (e != NULL)
		free(name_buf);
	return e;
}


static err
validate_location(const char *dest)
{
	err e = NO_ERR;
	/* these are malloc'ed: */
	char *resolved_snapdir = NULL;
	char *dest_dup = NULL;
	char *resolved_destination = NULL;
	/* and these are not: */
	char *configured_snapdir = NULL;
	char *dest_dir = NULL;

	/* Get the canonical path of the snapshot directory */
	configured_snapdir = getConfVal(_mero_props, "snapshotdir");
	if (configured_snapdir == NULL || configured_snapdir[0] == '\0') {
		e = newErr("Action not allowed because no 'snapshotdir' has been configured");
		goto bailout;
	}
	if (configured_snapdir[0] != '/') {
		e = newErr("Setting 'snapshotdir' should be an absolute path");
		goto bailout;
	}
	resolved_snapdir = realpath(configured_snapdir, NULL);
	if (resolved_snapdir == NULL) {
		e = newErr("Cannot resolve configured snapshot directory '%s': %s",
			configured_snapdir, strerror(errno));
		goto bailout;
	}

	/* resolve the directory the snapshot is about to be written to */
	dest_dup = strdup(dest);
	dest_dir = dirname(dest_dup);

	resolved_destination = realpath(dest_dir, NULL);
	if (resolved_destination == NULL) {
		e = newErr("Cannot resolve snapshot directory '%s': %s",
			dest_dir, strerror(errno));
		goto bailout;
	}

	/* snapdir must be a prefix of destination */
	if (
		strlen(resolved_snapdir) > strlen(resolved_destination)
		|| 0 != strncmp(
			resolved_snapdir,
			resolved_destination,
			strlen(resolved_snapdir))
	) {
		e = newErr("Snapshot directory '%s' is not inside configured snapshot directory '%s'",
			dest, resolved_snapdir);
		goto bailout;
	}

bailout:
	free(resolved_snapdir);
	free(dest_dup);
	free(resolved_destination);

	return e;
}

/* Create the directory 'filename' is in, if necessary, and take care
 * of fsync'ing it first when called with a different `dir`. Wrap up if
 * filename is set to NULL.
 */
static err
prepare_directory(struct dir_helper *state, const char *prefix_dir, const char *filename)
{
	char *e = NO_ERR;
	size_t file_len;
	size_t prefix_len;
	int ret;
	int fd;
	char *dir_buf = NULL;
	char *dir = NULL;

	if (filename != NULL) {
		dir_buf = strdup(filename);
		dir = dirname(dir_buf);
	}

	// If we are already tracking a directory and this one is the same,
	// there's no need to do anything.
	if (dir != NULL && state->dir != NULL && strcmp(state->dir, dir) == 0) {
		free(dir_buf);
		return NULL;
	}

	// Before switching to the new directory, first fsync the old one, if any.
	if (state->dir != NULL) {
		if (fsync(state->fd) < 0) {
			e = newErr("fsync %s: %s", state->dir, strerror(errno));
			goto bailout;
		}
		if (close(state->fd) < 0) {
			e = newErr("close %s: %s", state->dir, strerror(errno));
			state->fd = -1;
			goto bailout;
		}
		state->fd = -1;
		free(state->dir);
		state->dir = NULL;
	}

	if (dir == NULL) {
		// no new directory to set up, we just had to sync the current one
		return NO_ERR;
	}

	// Error checking on the new file.
	file_len = strlen(filename);
	prefix_len = strlen(prefix_dir);
	if (prefix_len > file_len || strncmp(filename, prefix_dir, prefix_len) != 0) {
		e = newErr("File '%s' is not inside prefix '%s'", filename, prefix_dir);
		goto bailout;
	}

	// Create any missing directories beyond the prefix.
	for (char *p = dir + prefix_len + 1; *p != '\0'; p++) {
		if (*p == '/') {
			*p = '\0';
			ret = mkdir(dir, 0755);
			if (ret < 0 && errno != EEXIST) {
				e = newErr("Could not create directory %s: %s", dir, strerror(errno));
				*p = '/';
				goto bailout;
			}
			*p = '/';
		}
	}
	// The final part of the directory is not covered by the loop above
	ret = mkdir(dir, 0755);
	if (ret < 0 && errno != EEXIST) {
		e = newErr("Could not create directory %s: %s", dir, strerror(errno));
		goto bailout;
	}

	// Open the directory so we can fsync it later
	fd = open(dir, O_RDONLY);
	if (fd < 0) {
		e = newErr("couldn't open directory %s: %s", dir, strerror(errno));
		goto bailout;
	}

	state->fd = fd;
	state->dir = strdup(dir);

	free(dir_buf);
	return NULL;
bailout:
	free(dir_buf);
	if (state->fd > 0) {
		close(state->fd);
		state->fd = -1;
	}
	free(state->dir);
	state->dir = NULL;
	return e;
}


#define TAR_BLOCK_SIZE (512)

/* Read a full block from the stream.
 * Return 0 on succes, -1 on failure.
 * Set *err to NULL if the failure is caused by an end of file
 * on a block boundary, non-NULL on read error.
 */
static int
read_tar_block(stream *s, char *block, err *error)
{
	ssize_t nread = mnstr_read(s, block, 1, TAR_BLOCK_SIZE);
	if (nread <= 0) {
		if (mnstr_errnr(s) != 0) {
			/* failure */
			char *err = mnstr_error(s);
			*error = newErr("Read error (%zd): %s", nread, err);
			free(err);
		} else {
			*error = NULL;
		}
		return -1;
	}
	if (nread < TAR_BLOCK_SIZE) {
		*error = newErr("Incomplete read");
		return -1;
	}
	*error = NULL;
	return 0;
}

static char *
extract_tar_member_filename(const char *block)
{
	const char *name_field = block;
	size_t name_len = strnlen(name_field, 100);
	const char *prefix_field = block + 345;
	size_t prefix_len = strnlen(prefix_field, 155);
	char *buf = malloc(100 + 1 + 155 + 1); // prefix slash name nul

	if (prefix_len == 0) {
		memmove(buf, name_field, name_len);
		buf[name_len] = '\0';
	} else {
		memmove(buf, prefix_field, prefix_len);
		buf[prefix_len] = '/';
		memmove(buf + prefix_len + 1, name_field, name_len);
		buf[prefix_len + 1 + name_len] = '\0';
	}

	return buf;
}

static ssize_t
extract_tar_member_size(const char *block)
{
	size_t size;
	char buf[13];
	memmove(buf, block + 124, 12);
	buf[12] = '\0';
	if (sscanf(buf, "%zo", &size) != 1)
		return -1;
	return (ssize_t) size;
}


/* We're implementing our own untar instead of invoking /usr/bin/tar. Why?
 * Wrapping /usr/bin/tar correctly is quite a lot of work. We have to fork, we
 * have to get any error messages from tar to the child process and then to the
 * parent process, etc. Also, we cannot depend on /usr/bin/tar being able to
 * decompress any compression algorithms so to do it right we should decompress
 * the data ourselves and pipe it to tar to unpack it. A lot of work and a lot
 * of corner cases. Moreover, the snapshot generating tar code is also home
 * grown. This is necessary because we snapshot parts of files, not whole files.
 */
static err
unpack_tarstream(stream *tarstream, char *destdir, int skipfirstcomponent)
{
	char block[TAR_BLOCK_SIZE];
	err e = NO_ERR;
	char *whole_filename = NULL;
	char *destfile = NULL;
	FILE *outfile = NULL;
	struct dir_helper dirhelper = {0};

	while (read_tar_block(tarstream, block, &e) >= 0) {
		if (e != NO_ERR)
			return e;

		// Determine the target filename
		free(whole_filename);
		whole_filename = extract_tar_member_filename(block);
		if (strlen(whole_filename) == 0) {
			goto bailout; // done
		}

		char *useful_part;
		if (skipfirstcomponent) {
			useful_part = strchr(whole_filename, '/');
			if (useful_part != NULL)
				useful_part += 1;
			else
				useful_part = whole_filename;
		} else {
			useful_part = whole_filename;
		}
		destfile = realloc(destfile, strlen(destdir) + 1 + strlen(useful_part) + 1);
		sprintf(destfile, "%s/%s", destdir, useful_part);

		// Create any missing directories and take care of fsync'ing them
		e = prepare_directory(&dirhelper, destdir, destfile);
		if (e)
			goto bailout;

		// Copy the data
		outfile = fopen(destfile, "w");
		if (outfile == NULL) {
			e = newErr("Could not open %s", destfile);
			goto bailout;
		}
		ssize_t size = extract_tar_member_size(block);
		while (size > 0) {
			int read_result = read_tar_block(tarstream, block, &e);
			if (e != NO_ERR) {
				goto bailout;
			}
			if (read_result < 0) {
				e = newErr("unexpected end of tar file");
				goto bailout;
			}
			size_t nwrite = size > TAR_BLOCK_SIZE ? TAR_BLOCK_SIZE : size;
			size_t written = fwrite(block, 1, nwrite, outfile);
			if (written < nwrite) {
				e = newErr("Error writing %s: %s", destfile, strerror(errno));
				goto bailout;
			}
			size -= TAR_BLOCK_SIZE;
		}

		if (fsync(fileno(outfile)) < 0) {
			e = newErr("fsync %s: %s", destfile, strerror(errno));
			goto bailout;
		}
		fclose(outfile);
		outfile = NULL;
	}
	prepare_directory(&dirhelper, NULL, NULL);

bailout:
	if (outfile != NULL)
		fclose(outfile);
	free(whole_filename);
	free(destfile);
	return e;
}
