/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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
#include "mal_linker.h"		/* for loadModuleLibrary() */
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
malResolveFile(const char *fname)
{
	char path[FILENAME_MAX];
	str script;
	int written;

	written = snprintf(path, FILENAME_MAX, "%s", fname);
	if (written == -1 || written >= FILENAME_MAX)
		return NULL;
	slash_2_dir_sep(path);
	if ((script = MSP_locate_script(path)) == NULL) {
		/* this function is also called for scripts that are not located
		 * in the modpath, so if we can't find it, just default to
		 * whatever was given, as it can be in current dir, or an
		 * absolute location to somewhere */
		script = GDKstrdup(fname);
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
		throw(MAL, "malInclude", "could not open file: %s: %s", name, mnstr_peek_error(NULL));
	}
	sz = getFileSize(fd);
	if (sz > (size_t) 1 << 29) {
		close_stream(fd);
		throw(MAL, "malInclude", "file %s too large to process", name);
	}
	*fdin = bstream_create(fd, sz == 0 ? (size_t) (2 * 128 * BLOCK) : sz);
	if(*fdin == NULL) {
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
malIncludeString(Client c, const char *name, str mal, int listing, MALfcn address)
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

	size_t mal_len = strlen(mal);
	buffer* mal_buf;
	stream* mal_stream;

	if ((mal_buf = GDKmalloc(sizeof(buffer))) == NULL)
		throw(MAL, "malIncludeString", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((mal_stream = buffer_rastream(mal_buf, name)) == NULL) {
		GDKfree(mal_buf);
		throw(MAL, "malIncludeString", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	buffer_init(mal_buf, mal, mal_len);
	c->srcFile = name;
	c->yycur = 0;
	c->bak = NULL;
	if ((c->fdin = bstream_create(mal_stream, mal_len)) == NULL) {
		mnstr_destroy(mal_stream);
		GDKfree(mal_buf);
		throw(MAL, "malIncludeString", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bstream_next(c->fdin);
	parseMAL(c, c->curprg, 1, INT_MAX, address);
	bstream_destroy(c->fdin);
	c->fdin = NULL;
	GDKfree(mal_buf);

	restoreClient;
	return msg;
}

/*
 * The include operation parses the file indentified and
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

	c->prompt = "";				/* do not produce visible prompts */
	c->promptlength = 0;
	c->listing = listing;
	c->fdin = NULL;

	if ((filename = malResolveFile(name)) != NULL) {
		char *fname = filename;
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
				freeException(msg);
				msg = MAL_SUCCEED;
			}
			if (p)
				filename = p + 1;
		} while (p);
		GDKfree(fname);
		c->fdin = NULL;
	}
	restoreClient;
	return msg;
}


/* patch a newline character if needed */
static str
mal_cmdline(char *s, size_t *len)
{
	if (*len && s[*len - 1] != '\n') {
		char *n = GDKmalloc(*len + 2);
		if (n == NULL)
			return s;
		memcpy(n, s, *len);
		n[*len] = '\n';
		n[*len + 1] = 0;
		(*len)++;
		return n;
	}
	return s;
}

str
compileString(Symbol *fcn, Client cntxt, str s)
{
	Client c, c_old;
	QryCtx *qc_old;
	size_t len = strlen(s);
	buffer *b;
	str msg = MAL_SUCCEED;
	str qry;
	str old = s;
	stream *bs;
	bstream *fdin = NULL;

	s = mal_cmdline(s, &len);
	qry = s;
	if (old == s) {
		qry = GDKstrdup(s);
		if(!qry)
			throw(MAL,"mal.eval",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	mal_unquote(qry);
	b = (buffer *) GDKzalloc(sizeof(buffer));
	if (b == NULL) {
		GDKfree(qry);
		throw(MAL,"mal.eval",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	buffer_init(b, qry, len);
	bs = buffer_rastream(b, "compileString");
	if (bs == NULL) {
		GDKfree(qry);
		GDKfree(b);
		throw(MAL,"mal.eval",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	fdin = bstream_create(bs, b->len);
	if (fdin == NULL) {
		GDKfree(qry);
		GDKfree(b);
		throw(MAL,"mal.eval",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	strncpy(fdin->buf, qry, len+1);

	c_old = setClientContext(NULL); // save context
	qc_old = MT_thread_get_qry_ctx();
	// compile in context of called for
	c = MCinitClient(MAL_ADMIN, fdin, 0);
	if( c == NULL){
		GDKfree(qry);
		GDKfree(b);
		setClientContext(c_old); // restore context
		MT_thread_set_qry_ctx(qc_old);
		throw(MAL,"mal.eval","Can not create user context");
	}
	c->curmodule = c->usermodule = cntxt->usermodule;
	c->promptlength = 0;
	c->listing = 0;

	if ( (msg = defaultScenario(c)) ) {
		GDKfree(qry);
		GDKfree(b);
		c->usermodule= 0;
		MCcloseClient(c);
		setClientContext(c_old); // restore context
		MT_thread_set_qry_ctx(qc_old);
		return msg;
	}

	msg = MSinitClientPrg(c, "user", "main");/* create new context */
	if(msg == MAL_SUCCEED && c->phase[MAL_SCENARIO_PARSER])
		msg = (str) (*c->phase[MAL_SCENARIO_PARSER])(c);
	if(msg == MAL_SUCCEED && c->phase[MAL_SCENARIO_OPTIMIZE])
		msg = (str) (*c->phase[MAL_SCENARIO_OPTIMIZE])(c);

	*fcn = c->curprg;
	c->curprg = 0;
	c->usermodule= 0;
	/* restore IO channel */
	MCcloseClient(c);
	setClientContext(c_old); // restore context
	MT_thread_set_qry_ctx(qc_old);
	GDKfree(qry);
	GDKfree(b);
	return msg;
}
