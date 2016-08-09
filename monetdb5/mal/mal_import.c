/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
#include "mal_parser.h"
#include "mal_private.h"

void
slash_2_dir_sep(str fname)
{
	char *s;

	for (s = fname; *s; s++)
		if (*s == '/')
			*s = DIR_SEP;
}

static str
malResolveFile(str fname)
{
	char path[PATHLENGTH];
	str script;

	snprintf(path, PATHLENGTH, "%s", fname);
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

#ifndef HAVE_EMBEDDED
/*
 * The malLoadScript routine merely reads the contents of a file into
 * the input buffer of the client. It is typically used in situations
 * where an intermediate file is used to pass commands around.
 * Since the parser needs access to the complete block, we first have
 * to find out how long the input is.
*/
static str
malLoadScript(Client c, str name, bstream **fdin)
{
	stream *fd;
	size_t sz;

	fd = malOpenSource(name);
	if (fd == 0 || mnstr_errnr(fd) == MNSTR_OPEN_ERROR) {
		mnstr_destroy(fd);
		throw(MAL, "malInclude", "could not open file: %s", name);
	}
	sz = getFileSize(fd);
	if (sz > (size_t) 1 << 29) {
		mnstr_destroy(fd);
		throw(MAL, "malInclude", "file %s too large to process", name);
	}
	*fdin = bstream_create(fd, sz == 0 ? (size_t) (2 * 128 * BLOCK) : sz);
	if (bstream_next(*fdin) < 0)
		mnstr_printf(c->fdout, "!WARNING: could not read %s\n", name);
	return MAL_SUCCEED;
}
#endif

/*
 * Beware that we have to isolate the execution of the source file
 * in its own environment. E.g. we have to remove the execution
 * state until we are finished.
 * The script being read may contain errors, such as non-balanced
 * brackets as indicated by blkmode.
 * It should be reset before continuing.
*/
#define restoreState \
	bstream *oldfdin = c->fdin; \
	int oldyycur = c->yycur; \
	int oldlisting = c->listing; \
	enum clientmode oldmode = c->mode; \
	int oldblkmode = c->blkmode; \
	str oldsrcFile = c->srcFile; \
	ClientInput *oldbak = c->bak; \
	str oldprompt = c->prompt; \
	Module oldnspace = c->nspace; \
	Symbol oldprg = c->curprg; \
	MalStkPtr oldglb = c->glb	/* ; added by caller */
#define restoreState3 \
	enum clientmode oldmode = c->mode; \
	int oldblkmode = c->blkmode; \
	str oldsrcFile = c->srcFile; \
	Module oldnspace = c->nspace; \
	Symbol oldprg = c->curprg; \
	MalStkPtr oldglb = c->glb	/* ; added by caller */

#define restoreClient1 \
	if (c->fdin)  \
		(void) bstream_destroy(c->fdin); \
	c->fdin = oldfdin;  \
	c->yycur = oldyycur;  \
	c->listing = oldlisting; \
	c->mode = oldmode; \
	c->blkmode = oldblkmode; \
	c->bak = oldbak; \
	c->srcFile = oldsrcFile; \
	if(c->prompt) GDKfree(c->prompt); \
	c->prompt = oldprompt; \
	c->promptlength= (int)strlen(c->prompt);
#define restoreClient2 \
	assert(c->glb == 0 || c->glb == oldglb); /* detect leak */ \
	c->glb = oldglb; \
	c->nspace = oldnspace; \
	c->curprg = oldprg;
#define restoreClient \
	restoreClient1 \
	restoreClient2
#define restoreClient3 \
	if (c->fdin)  \
		MCpopClientInput(c); \
	c->mode = oldmode; \
	c->blkmode = oldblkmode; \
	c->srcFile = oldsrcFile;


#ifdef HAVE_EMBEDDED
extern char* mal_init_inline;
#endif
/*
 * The include operation parses the file indentified and
 * leaves the MAL code behind in the 'main' function.
 */
str
malInclude(Client c, str name, int listing)
{
	str s= MAL_SUCCEED;
	str filename;
	str p;

	bstream *oldfdin = c->fdin;
	int oldyycur = c->yycur;
	int oldlisting = c->listing;
	enum clientmode oldmode = c->mode;
	int oldblkmode = c->blkmode;
	ClientInput *oldbak = c->bak;
	str oldprompt = c->prompt;
	str oldsrcFile = c->srcFile;

	MalStkPtr oldglb = c->glb;
	Module oldnspace = c->nspace;
	Symbol oldprg = c->curprg;

	c->prompt = GDKstrdup("");	/* do not produce visible prompts */
	c->promptlength = 0;
	c->listing = listing;
	c->fdin = NULL;

#ifdef HAVE_EMBEDDED
	(void) filename;
	(void) p;
	{
		size_t mal_init_len = strlen(mal_init_inline);
		buffer* mal_init_buf = buffer_create(mal_init_len);
		stream* mal_init_stream = buffer_rastream(mal_init_buf, name);
		buffer_init(mal_init_buf, mal_init_inline, mal_init_len);
		c->srcFile = name;
		c->yycur = 0;
		c->bak = NULL;
		c->fdin = bstream_create(mal_init_stream, mal_init_len);
		bstream_next(c->fdin);
		parseMAL(c, c->curprg, 1);
		free(mal_init_buf);
		free(mal_init_stream);
		free(c->fdin);
		c->fdin = NULL;
	}
#else
	if ((filename = malResolveFile(name)) != NULL) {
		name = filename;
		do {
			p = strchr(filename, PATH_SEP);
			if (p)
				*p = '\0';
			c->srcFile = filename;
			c->yycur = 0;
			c->bak = NULL;
			if ((s = malLoadScript(c, filename, &c->fdin)) == MAL_SUCCEED) {
				parseMAL(c, c->curprg, 1);
				bstream_destroy(c->fdin);
			} else {
				GDKfree(s); // not interested in error here
				s = MAL_SUCCEED;
			}
			if (p)
				filename = p + 1;
		} while (p);
		GDKfree(name);
		c->fdin = NULL;
	}
#endif
	restoreClient;
	return s;
}

/*File and input processing
 * A recurring situation is to execute a stream of simple MAL instructions
 * stored on a file or comes from standard input. We parse one MAL
 * instruction line at a time and attempt to execute it immediately.
 * Note, this precludes entering complex MAL structures on the primary
 * input channel, because 1) this requires complex code to keep track
 * that we are in 'definition mode' 2) this requires (too) careful
 * typing by the user, because he cannot make a typing error
 *
 * Therefore, all compound code fragments should be loaded and executed
 * using the evalFile and callString command. It will parse the complete
 * file into a MAL program block and execute it.
 *
 * Running looks much like an Import operation, except for the execution
 * phase. This is performed in the context of an a priori defined
 * stack frame. Life becomes a little complicated when the script contains
 * a definition.
 */
str
evalFile(Client c, str fname, int listing)
{
	restoreState;
	stream *fd;
	str p;
	str filename;
	str msg = MAL_SUCCEED;

	c->prompt = GDKstrdup("");  /* do not produce visible prompts */
	c->promptlength = 0;
	c->listing = listing;

	c->fdin = NULL;

	filename = malResolveFile(fname);
	if (filename == NULL) {
		mnstr_printf(c->fdout, "#WARNING: could not open file: %s\n", fname);
		restoreClient3;
		restoreClient;
		return msg;
	}

	fname = filename;
	while ((p = strchr(filename, PATH_SEP)) != NULL) {
		*p = '\0';
		fd = malOpenSource(filename);
		if (fd == 0 || mnstr_errnr(fd) == MNSTR_OPEN_ERROR) {
			if(fd) mnstr_destroy(fd);
			mnstr_printf(c->fdout, "#WARNING: could not open file: %s\n",
					filename);
		} else {
			c->srcFile = filename;
			c->yycur = 0;
			c->bak = NULL;
			MSinitClientPrg(c, "user", "main");     /* re-initialize context */
			MCpushClientInput(c, bstream_create(fd, 128 * BLOCK), c->listing, "");
			msg = runScenario(c);
			if (msg != MAL_SUCCEED) {
				dumpExceptionsToStream(c->fdout, msg);
				GDKfree(msg);
			}
		}
		filename = p + 1;
	}
	fd = malOpenSource(filename);
	if (fd == 0 || mnstr_errnr(fd) == MNSTR_OPEN_ERROR) {
		if (fd)
			mnstr_destroy(fd);
		msg = createException(MAL,"mal.eval", "WARNING: could not open file: %s\n", filename);
	} else {
		c->srcFile = filename;
		c->yycur = 0;
		c->bak = NULL;
		MSinitClientPrg(c, "user", "main");     /* re-initialize context */
		MCpushClientInput(c, bstream_create(fd, 128 * BLOCK), c->listing, "");
		msg = runScenario(c);
	}
	GDKfree(fname);

	restoreClient3;
	restoreClient;
	return msg;
}

/* patch a newline character if needed */
static str mal_cmdline(char *s, int *len)
{
	if (s[*len - 1] != '\n') {
		char *n = GDKmalloc(*len + 1 + 1);
		if (n == NULL)
			return s;
		strncpy(n, s, *len);
		n[*len] = '\n';
		n[*len + 1] = 0;
		(*len)++;
		return n;
	}
	return s;
}

str
compileString(Symbol *fcn, Client c, str s)
{
	restoreState3;
	int len = (int) strlen(s);
	buffer *b;
	str msg = MAL_SUCCEED;
	str qry;
	str old = s;

	c->srcFile = NULL;

	s = mal_cmdline(s, &len);
	mal_unquote(qry = GDKstrdup(s));
	if (old != s)
		GDKfree(s);
	b = (buffer *) GDKmalloc(sizeof(buffer));
	if (b == NULL) {
		GDKfree(qry);
		return MAL_MALLOC_FAIL;
	}

	buffer_init(b, qry, len);
	if (MCpushClientInput(c, bstream_create(buffer_rastream(b, "compileString"), b->len), 0, "") < 0) {
		GDKfree(qry);
		GDKfree(b);
		return MAL_MALLOC_FAIL;
	}
	c->curprg = 0;
	MSinitClientPrg(c, "user", "main");  /* create new context */
	if (msg == MAL_SUCCEED && c->phase[MAL_SCENARIO_READER] &&
		(msg = (str) (*c->phase[MAL_SCENARIO_READER])(c))) {
		GDKfree(qry);
		GDKfree(b);
		restoreClient3;
		return msg;
	}
	if (msg == MAL_SUCCEED && c->phase[MAL_SCENARIO_PARSER] &&
		(msg = (str) (*c->phase[MAL_SCENARIO_PARSER])(c))) {
		GDKfree(qry);
		GDKfree(b);
		/* error occurred  and ignored */
		restoreClient3;
		return msg;
	}
	*fcn = c->curprg;
	/* restore IO channel */
	restoreClient3;
	restoreClient2;
	GDKfree(qry);
	GDKfree(b);
	return MAL_SUCCEED;
}
#define runPhase(X, Y) \
	if (msg == MAL_SUCCEED && c->phase[X] && (msg = (str) (*c->phase[X])(c))) {	\
		/* error occurred  and ignored */ \
		GDKfree(msg); msg = MAL_SUCCEED; \
		Y; \
		if (b) \
			GDKfree(b);	\
		if (qry) \
			GDKfree(qry); \
		return 0; \
	}

int
callString(Client c, str s, int listing)
{
	restoreState3;
	int len = (int) strlen(s);
	buffer *b;
	str msg = MAL_SUCCEED, qry;
	str old = s;

	c->srcFile = NULL;

	s = mal_cmdline(s, &len);
	mal_unquote(qry = GDKstrdup(s));
	if (old != s)
		GDKfree(s);
	b = (buffer *) GDKmalloc(sizeof(buffer));
	if (b == NULL){
		GDKfree(qry);
		return -1;
	}
	buffer_init(b, qry, len);
	if (MCpushClientInput(c, bstream_create(buffer_rastream(b, "callString"), b->len), listing, "") < 0) {
		GDKfree(b);
		GDKfree(qry);
		return -1;
	}
	c->curprg = 0;
	MSinitClientPrg(c, "user", "main");  /* create new context */
	runPhase(MAL_SCENARIO_READER, restoreClient3);
	runPhase(MAL_SCENARIO_PARSER, restoreClient3);
	/* restore IO channel */
	restoreClient3;
	runPhase(MAL_SCENARIO_OPTIMIZE, restoreClient2);
	runPhase(MAL_SCENARIO_SCHEDULER, restoreClient2);
	runPhase(MAL_SCENARIO_ENGINE, restoreClient2);
	restoreClient2;
	GDKfree(qry);
	GDKfree(b);
	return 0;
}
