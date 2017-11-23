/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
	char path[FILENAME_MAX];
	str script;

	snprintf(path, FILENAME_MAX, "%s", fname);
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
	if(*fdin == NULL) {
		mnstr_destroy(fd);
		throw(MAL, "malInclude", MAL_MALLOC_FAIL);
	}
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
	if(c->prompt) GDKfree(c->prompt); \
	c->prompt = oldprompt; \
	c->promptlength= (int)strlen(c->prompt);
#define restoreClient2 \
	assert(c->glb == 0 || c->glb == oldglb); /* detect leak */ \
	c->glb = oldglb; \
	c->usermodule = oldusermodule; \
	c->curmodule = oldcurmodule;; \
	c->curprg = oldprg;
#define restoreClient \
	restoreClient1 \
	restoreClient2

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
	str msg = MAL_SUCCEED;
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
	Module oldusermodule = c->usermodule;
	Module oldcurmodule = c->curmodule; 
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
		buffer* mal_init_buf;
		stream* mal_init_stream;

		if ((mal_init_buf = GDKmalloc(sizeof(buffer))) == NULL)
			throw(MAL, "malInclude", MAL_MALLOC_FAIL);
		if ((mal_init_stream = buffer_rastream(mal_init_buf, name)) == NULL) {
			GDKfree(mal_init_buf);
			throw(MAL, "malInclude", MAL_MALLOC_FAIL);
		}
		buffer_init(mal_init_buf, mal_init_inline, mal_init_len);
		c->srcFile = name;
		c->yycur = 0;
		c->bak = NULL;
		if ((c->fdin = bstream_create(mal_init_stream, mal_init_len)) == NULL) {
			mnstr_destroy(mal_init_stream);
			GDKfree(mal_init_buf);
			throw(MAL, "malInclude", MAL_MALLOC_FAIL);
		}
		bstream_next(c->fdin);
		parseMAL(c, c->curprg, 1, INT_MAX);
		free(mal_init_buf);
		free(mal_init_stream);
		free(c->fdin);
		c->fdin = NULL;
		GDKfree(mal_init_buf);
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
			if ((msg = malLoadScript(c, filename, &c->fdin)) == MAL_SUCCEED) {
				parseMAL(c, c->curprg, 1, INT_MAX);
				bstream_destroy(c->fdin);
			} else {
				/* TODO output msg ? */
				freeException(msg);
				msg = MAL_SUCCEED;
			}
			if (p)
				filename = p + 1;
		} while (p);
		GDKfree(name);
		c->fdin = NULL;
	}
#endif
	restoreClient;
	return msg;
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
evalFile(str fname, int listing)
{	
	Client c;
	stream *fd;
	str filename;
	str msg = MAL_SUCCEED;

	filename = malResolveFile(fname);
	if (filename == NULL) 
		throw(MAL, "mal.eval","could not open file: %s\n", fname);
	fd = malOpenSource(filename);
	GDKfree(filename);
	if (fd == 0 || mnstr_errnr(fd) == MNSTR_OPEN_ERROR) {
		if (fd)
			mnstr_destroy(fd);
		throw(MAL,"mal.eval", "WARNING: could not open file\n");
	} 

	c= MCinitClient((oid)0, bstream_create(fd, 128 * BLOCK),0);
	if( c == NULL){
		throw(MAL,"mal.eval","Can not create user context");
	}
	c->curmodule = c->usermodule = userModule();
	c->promptlength = 0;
	c->listing = listing;

	if ( (msg = defaultScenario(c)) ) {
		MCcloseClient(c);
		throw(MAL,"mal.eval","%s",msg);
	}
	if((msg = MSinitClientPrg(c, "user", "main")) != MAL_SUCCEED) {
		MCcloseClient(c);
		return msg;
	}

	msg = runScenario(c,0);
	MCcloseClient(c);
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
compileString(Symbol *fcn, Client cntxt, str s)
{	
	Client c;
	int len = (int) strlen(s);
	buffer *b;
	str msg = MAL_SUCCEED;
	str qry;
	str old = s;
	bstream *fdin = NULL;

	s = mal_cmdline(s, &len);
	qry = s;
	if (old == s)
		qry = GDKstrdup(s);
	mal_unquote(qry);
	b = (buffer *) GDKzalloc(sizeof(buffer));
	if (b == NULL) {
		GDKfree(qry);
		throw(MAL,"mal.eval",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	buffer_init(b, qry, len);
	fdin = bstream_create(buffer_rastream(b, "compileString"), b->len);
	strncpy(fdin->buf, qry, len+1);

	// compile in context of called for
	c= MCinitClient((oid)0, fdin, 0);
	if( c == NULL){
		GDKfree(qry);
		GDKfree(b);
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
		throw(MAL,"mal.compile","%s",msg);
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
	GDKfree(qry);
	GDKfree(b);
	return msg;
}

str
callString(Client cntxt, str s, int listing)
{	Client c;
	int i, len = (int) strlen(s);
	buffer *b;
	str old =s;
	str msg = MAL_SUCCEED, qry;

	s = mal_cmdline(s, &len);
	qry = s;
	if (old == s) {
		qry = GDKstrdup(s);
		if(!qry)
			throw(MAL,"callstring", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	mal_unquote(qry);
	b = (buffer *) GDKzalloc(sizeof(buffer));
	if (b == NULL){
		GDKfree(qry);
		throw(MAL,"callstring", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	buffer_init(b, qry, len);
	c= MCinitClient((oid)0, bstream_create(buffer_rastream(b, "callString"), b->len),0);
	strncpy(c->fdin->buf, qry, len+1);
	if( c == NULL){
		GDKfree(b);
		GDKfree(qry);
		throw(MAL,"mal.call","Can not create user context");
	}
	c->curmodule = c->usermodule =  cntxt->usermodule;
	c->promptlength = 0;
	c->listing = listing;

	if ( (msg = defaultScenario(c)) ) {
		c->usermodule = 0;
		GDKfree(b);
		GDKfree(qry);
		MCcloseClient(c);
		throw(MAL,"mal.call","%s",msg);
	}

	if((msg = MSinitClientPrg(c, "user", "main")) != MAL_SUCCEED) {/* create new context */
		c->usermodule = 0;
		GDKfree(b);
		GDKfree(qry);
		MCcloseClient(c);
		return msg;
	}
	runScenario(c,1);
	// The command may have changed the environment of the calling client.
	// These settings should be propagated for further use.
	//if( msg == MAL_SUCCEED){
		cntxt->scenario = c->scenario;
		c->scenario = 0;
		cntxt->sqlcontext = c->sqlcontext;
		c->sqlcontext = 0;
		for(i=1; i< SCENARIO_PROPERTIES; i++){
			cntxt->state[i] = c->state[i];
			c->state[i]  = 0;
			cntxt->phase[i] = c->phase[i];
			c->phase[i]  = 0;
		}
		if(msg == MAL_SUCCEED && cntxt->phase[0] != c->phase[0]){
			cntxt->phase[0] = c->phase[0];
			cntxt->state[0] = c->state[0];
			msg = (str) (*cntxt->phase[0])(cntxt); 	// force re-initialize client context
		}
	//}
	c->usermodule = 0; // keep it around
	bstream_destroy(c->fdin);
	c->fdin = 0;
	MCcloseClient(c);
	GDKfree(qry);
	GDKfree(b);
	return msg;
}
