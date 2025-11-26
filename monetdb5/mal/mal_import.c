/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/* Author(s) M.L. Kersten
 * Module import
 * The import statement simple switches the parser to a new input file, which
 * takes precedence. The context for which the file should be interpreted
 * is determined by the module name supplied.
 * Typically this involves a module, whose definitions are stored at
 * a known location.
 * The import context is located. If the module already exists,
 * we should silently skip parsing the file. This is handled at the parser level.
 * The files are extracted from a default location,
 * namely the DBHOME/modules directory.
 *
 * If the string starts with '/' or '~' the context is not changed.
 *
 * Every IMPORT statement denotes a possible dynamic load library.
 * Make sure it is loaded as well.
*/

#include "monetdb_config.h"
#include "mal_import.h"
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_linker.h"			/* for loadModuleLibrary() */
#include "mal_scenario.h"
#include "mal_parser.h"
#include "mal_authorize.h"
#include "mal_private.h"
#include "mal_internal.h"
#include "mal_session.h"
#include "mal_utils.h"

void
slash_2_dir_sep(str fname)
{
	char *s;

	for (s = fname; *s; s++)
		if (*s == '/')
			*s = DIR_SEP;
}

static str
malResolveFile(allocator *ma, const char *fname)
{
	char path[FILENAME_MAX];
	str script;
	int written;

	written = snprintf(path, sizeof(path), "%s", fname);
	if (written == -1 || written >= FILENAME_MAX)
		return NULL;
	slash_2_dir_sep(path);
	if ((script = MSP_locate_script(ma, path)) == NULL) {
		/* this function is also called for scripts that are not located
		 * in the modpath, so if we can't find it, just default to
		 * whatever was given, as it can be in current dir, or an
		 * absolute location to somewhere */
		script = ma_strdup(ma, fname);
	}
	return script;
}

static stream *
malOpenSource(str file)
{
	stream *fd = NULL;

	if (file)
		fd = open_rastream(file);
	return fd;
}

/*
 * The malLoadScript routine merely reads the contents of a file into
 * the input buffer of the client. It is typically used in situations
 * where an intermediate file is used to pass commands around.
 * Since the parser needs access to the complete block, we first have
 * to find out how long the input is.
*/
static str
malLoadScript(str name, bstream **fdin)
{
	stream *fd;
	size_t sz;

	fd = malOpenSource(name);
	if (fd == NULL || mnstr_errnr(fd) == MNSTR_OPEN_ERROR) {
		close_stream(fd);
		throw(MAL, "malInclude", "could not open file: %s: %s", name,
			  mnstr_peek_error(NULL));
	}
	sz = getFileSize(fd);
	if (sz > (size_t) 1 << 29) {
		close_stream(fd);
		throw(MAL, "malInclude", "file %s too large to process", name);
	}
	*fdin = bstream_create(fd, sz == 0 ? (size_t) (2 * 128 * BLOCK) : sz);
	if (*fdin == NULL) {
		close_stream(fd);
		throw(MAL, "malInclude", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (bstream_next(*fdin) < 0) {
		bstream_destroy(*fdin);
		*fdin = NULL;
		throw(MAL, "malInclude", "could not read %s", name);
	}
	return MAL_SUCCEED;
}

/*
 * Beware that we have to isolate the execution of the source file
 * in its own environment. E.g. we have to remove the execution
 * state until we are finished.
 * The script being read may contain errors, such as non-balanced
 * brackets as indicated by blkmode.
 * It should be reset before continuing.
*/
#define restoreClient1 \
	if (c->fdin)  \
		bstream_destroy(c->fdin); \
	c->fdin = oldfdin;  \
	c->qryctx.bs = oldfdin;  \
	c->yycur = oldyycur;  \
	c->listing = oldlisting; \
	c->mode = oldmode; \
	c->blkmode = oldblkmode; \
	c->bak = oldbak; \
	c->srcFile = oldsrcFile; \
	c->prompt = oldprompt; \
	c->promptlength = strlen(c->prompt);
#define restoreClient2 \
	assert(c->glb == 0 || c->glb == oldglb); /* detect leak */ \
	c->glb = oldglb; \
	c->usermodule = oldusermodule; \
	c->curmodule = oldcurmodule; \
	c->curprg = oldprg;
#define restoreClient \
	restoreClient1 \
	restoreClient2

str
malIncludeString(Client c, const char *name, str mal, int listing,
				 MALfcn address)
{
	str msg = MAL_SUCCEED;

	bstream *oldfdin = c->fdin;
	size_t oldyycur = c->yycur;
	int oldlisting = c->listing;
	enum clientmode oldmode = c->mode;
	int oldblkmode = c->blkmode;
	ClientInput *oldbak = c->bak;
	const char *oldprompt = c->prompt;
	const char *oldsrcFile = c->srcFile;

	MalStkPtr oldglb = c->glb;
	Module oldusermodule = c->usermodule;
	Module oldcurmodule = c->curmodule;
	Symbol oldprg = c->curprg;

	c->prompt = "";				/* do not produce visible prompts */
	c->promptlength = 0;
	c->listing = listing;
	c->fdin = NULL;
	c->qryctx.bs = NULL;

	size_t mal_len = strlen(mal);
	buffer *mal_buf;
	stream *mal_stream;
	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);

	if ((mal_buf = ma_alloc(ta, sizeof(buffer))) == NULL) {
		ma_close(&ta_state);
		throw(MAL, "malIncludeString", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if ((mal_stream = buffer_rastream(mal_buf, name)) == NULL) {
		ma_close(&ta_state);
		throw(MAL, "malIncludeString", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	buffer_init(mal_buf, mal, mal_len);
	c->srcFile = name;
	c->yycur = 0;
	c->bak = NULL;
	if ((c->fdin = bstream_create(mal_stream, mal_len)) == NULL) {
		mnstr_destroy(mal_stream);
		ma_close(&ta_state);
		throw(MAL, "malIncludeString", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	c->qryctx.bs = c->fdin;
	bstream_next(c->fdin);
	parseMAL(c, c->curprg, 1, INT_MAX, address);
	bstream_destroy(c->fdin);
	c->fdin = NULL;
	c->qryctx.bs = NULL;
	ma_close(&ta_state);

	restoreClient;
	return msg;
}

/*
 * The include operation parses the file identified and
 * leaves the MAL code behind in the 'main' function.
 */
str
malInclude(Client c, const char *name, int listing)
{
	str msg = MAL_SUCCEED;
	str filename;
	str p;

	bstream *oldfdin = c->fdin;
	size_t oldyycur = c->yycur;
	int oldlisting = c->listing;
	enum clientmode oldmode = c->mode;
	int oldblkmode = c->blkmode;
	ClientInput *oldbak = c->bak;
	const char *oldprompt = c->prompt;
	const char *oldsrcFile = c->srcFile;

	MalStkPtr oldglb = c->glb;
	Module oldusermodule = c->usermodule;
	Module oldcurmodule = c->curmodule;
	Symbol oldprg = c->curprg;
	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);

	c->prompt = "";				/* do not produce visible prompts */
	c->promptlength = 0;
	c->listing = listing;
	c->fdin = NULL;
	c->qryctx.bs = NULL;

	if ((filename = malResolveFile(ta, name)) != NULL) {
		do {
			p = strchr(filename, PATH_SEP);
			if (p)
				*p = '\0';
			c->srcFile = filename;
			c->yycur = 0;
			c->bak = NULL;
			if ((msg = malLoadScript(filename, &c->fdin)) == MAL_SUCCEED) {
				parseMAL(c, c->curprg, 1, INT_MAX, 0);
				bstream_destroy(c->fdin);
			} else {
				/* TODO output msg ? */
				msg = MAL_SUCCEED;
			}
			if (p)
				filename = p + 1;
		} while (p);
		c->srcFile = NULL;
		c->fdin = NULL;
		c->qryctx.bs = NULL;
	}
	ma_close(&ta_state);
	restoreClient;
	return msg;
}
