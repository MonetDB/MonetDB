/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
*/
/*
 * (author) L.Sidirourgos
 *
 * Shredder for RDF Documents
 */
#include "monetdb_config.h"
#include "mal_exception.h"
#include "url.h"
#include "tokenizer.h"
#include <gdk.h>
#include <rdf.h>
#include <raptor.h>

typedef struct graphBATdef {
	graphBATType batType;    /* BAT type             */
	str name;                /* name of the BAT      */
	int headType;            /* type of left column  */
	int tailType;            /* type of right column */
} graphBATdef;

static BUN batsz = 10000000;

/* this list should be kept alligned with the graphBATType enum */
#if STORE == TRIPLE_STORE
 static graphBATdef graphdef[N_GRAPH_BAT] = {
	{S_sort,   "_s_sort",   TYPE_void, TYPE_oid},
	{P_sort,   "_p_sort",   TYPE_void, TYPE_oid},
	{O_sort,   "_o_sort",   TYPE_void, TYPE_oid},

	{P_PO,     "_p_po",     TYPE_void, TYPE_oid},
	{O_PO,     "_o_po",     TYPE_void, TYPE_oid},
	{P_OP,     "_p_op",     TYPE_void, TYPE_oid},
	{O_OP,     "_o_op",     TYPE_void, TYPE_oid},

	{S_SO,     "_s_so",     TYPE_void, TYPE_oid},
	{O_SO,     "_o_so",     TYPE_void, TYPE_oid},
	{S_OS,     "_s_os",     TYPE_void, TYPE_oid},
	{O_OS,     "_o_os",     TYPE_void, TYPE_oid},

	{S_SP,     "_s_sp",     TYPE_void, TYPE_oid},
	{P_SP,     "_p_sp",     TYPE_void, TYPE_oid},
	{S_PS,     "_s_ps",     TYPE_void, TYPE_oid},
	{P_PS,     "_p_ps",     TYPE_void, TYPE_oid},

	{MAP_LEX, "_map_lex",   TYPE_void, TYPE_str}
 };
#elif STORE == MLA_STORE
 static graphBATdef graphdef[N_GRAPH_BAT] = {
	{S_sort,   "_s_sort",   TYPE_void, TYPE_oid},
	{P_sort,   "_p_sort",   TYPE_void, TYPE_oid},
	{O_sort,   "_o_sort",   TYPE_void, TYPE_oid},
	{MAP_LEX, "_map_lex",   TYPE_void, TYPE_str}
 };
#endif /* STORE */

typedef struct parserData {
	                              /**PROPERTIES             */
	str location;                 /* rdf data file location */
	oid tcount;                   /* triple count           */
	raptor_parser *rparser;       /* the parser object      */
	                              /**ERROR HANDLING         */
	int exception;                /* raise an exception     */
	int warning;                  /* number of warning msgs */
	int error;                    /* number of error   msgs */
	int fatal;                    /* number of fatal   msgs */
	const char *exceptionMsg;     /* exception msgs         */
	const char *warningMsg;       /* warning msgs           */
	const char *errorMsg;         /* error   msgs           */
	const char *fatalMsg;         /* fatal   msgs           */
	int line;                     /* locator for errors     */
	int column;                   /* locator for errors     */
	                              /**GRAPH DATA             */
	BAT **graph;                  /* BATs for the result
	                                 shredded RDF graph     */
} parserData;

/*
 * The (fatal) errors and warnings produced by the raptor parser are handled
 * by the next three message handler functions.
 */
#define raptor_exception(P,M) \
P->exception++;\
P->exceptionMsg = M;\
raptor_parse_abort (P->rparser);

static void
fatalHandler (void *user_data, raptor_locator* locator,
		const char *message)
{
	parserData *pdata = (parserData *) user_data;
	pdata->fatalMsg = GDKstrdup(message);
	mnstr_printf(GDKout, "rdflib: fatal:%s\n", pdata->fatalMsg);
	pdata->fatal++;

	/* check for a valid locator object and only then use it */
	if (locator != NULL) {
		pdata->line = locator->line;
		pdata->column = locator->column;
	} else {
	}
}

errorHandler (void *user_data, raptor_locator* locator,
		const char *message)
{
	parserData *pdata = (parserData *) user_data;
	pdata->errorMsg = GDKstrdup(message);
	mnstr_printf(GDKout, "rdflib: error:%s\n", pdata->errorMsg);
	pdata->error++;

	/* check for a valid locator object and only then use it */
	if (locator != NULL) {
		pdata->line = locator->line;
		pdata->column = locator->column;
	} else {
	}
}

warningHandler (void *user_data, raptor_locator* locator,
		const char *message)
{
	parserData *pdata = (parserData *) user_data;
	pdata->warningMsg = GDKstrdup(message);
	mnstr_printf(GDKout, "rdflib: warning:%s\n", pdata->warningMsg);
	pdata->warning++;

	/* check for a valid locator object and only then use it */
	if (locator != NULL) {
		pdata->line = locator->line;
		pdata->column = locator->column;
	} else {
	}
}


/*
 * The raptor parser needs to register a callback function that handles one triple
 * at a time. Function rdf_parser_triple_handler() does exactly this.
 */

#define rdf_BUNappend_unq(X,Y)\
bun = BUNfnd(BATmirror(X),(ptr)Y);\
if (bun == BUN_NONE) {\
	if (BATcount(X) > 4 * X->T->hash->mask) {\
		HASHdestroy(X);\
		BAThash(BATmirror(X), 2*BATcount(X));\
	}\
	bun = (BUN) X->batCount;\
	X = BUNappend(X, (ptr)Y, TRUE);\
	if (X == NULL) {\
		raptor_exception(pdata, "could not append");\
	}\
}

#define rdf_BUNappend(X,Y) \
{X = BUNappend(X, Y, TRUE);}\
if (X  == NULL) {\
	raptor_exception(pdata, "could not append");\
}

static void
tripleHandler(void* user_data, const raptor_statement* triple)
{
	parserData *pdata = ((parserData *) user_data);
	BUN bun = BUN_NONE;
	BAT **graph = pdata->graph;

	if (triple->subject_type == RAPTOR_IDENTIFIER_TYPE_RESOURCE
			|| triple->subject_type == RAPTOR_IDENTIFIER_TYPE_ANONYMOUS) {
#ifdef _TKNZR_H
{
	str t = (str)triple->subject;
	TKNZRappend(&bun,&t);
}
#else
		 rdf_BUNappend_unq(graph[MAP_LEX], (str)triple->sibject);
#endif
		rdf_BUNappend(graph[S_sort], &bun);
		bun = BUN_NONE;
	} else {
		raptor_exception(pdata, "could not determine type of subject");
	}

	if (triple->predicate_type == RAPTOR_IDENTIFIER_TYPE_RESOURCE) {
#ifdef _TKNZR_H
{
	str t = (str)triple->predicate;
	TKNZRappend(&bun,&t);
}
#else
		 rdf_BUNappend_unq(pdate, (str)triple->predicate);
#endif
		rdf_BUNappend(graph[P_sort], &bun);
		bun = BUN_NONE;
	} else {
		raptor_exception(pdata, "could not determine type of property");
	}

	if (triple->object_type == RAPTOR_IDENTIFIER_TYPE_RESOURCE
			|| triple->object_type == RAPTOR_IDENTIFIER_TYPE_ANONYMOUS) {
#ifdef _TKNZR_H
{
	str t = (str)triple->object;
	TKNZRappend(&bun,&t);
}
#else
	 rdf_BUNappend_unq(graph[MAP_LEX], (str)triple->object);
#endif
		rdf_BUNappend(graph[O_sort], &bun);
		bun = BUN_NONE;
	} else if (triple->object_type == RAPTOR_IDENTIFIER_TYPE_LITERAL) {
		bun = BUNfnd(BATmirror(graph[MAP_LEX]),(ptr)triple->object);
		if (bun == BUN_NONE) {
			if (graph[MAP_LEX]->T->hash && BATcount(graph[MAP_LEX]) > 4 * graph[MAP_LEX]->T->hash->mask) {
				HASHdestroy(graph[MAP_LEX]);
				BAThash(BATmirror(graph[MAP_LEX]), 2*BATcount(graph[MAP_LEX]));
			}
			bun = (BUN) ((graph[MAP_LEX])->hseqbase + (graph[MAP_LEX])->batCount);
			graph[MAP_LEX] = BUNappend(graph[MAP_LEX], (ptr)triple->object, TRUE);
			if (graph[MAP_LEX] == NULL) {
				raptor_exception(pdata, "could not append ingraph[MAP_LEX]");
			}
		} else {
			bun = (graph[MAP_LEX])->hseqbase + bun;
		}

		rdf_BUNappend(graph[O_sort], &bun);
		bun = BUN_NONE;
	} else {
		raptor_exception(pdata, "could not determine type of object");
	}

	pdata->tcount++;

	return;
}

/*
 * Function RDFParser() is the entry point to parse an RDF document.
 */
/* creates a BAT for the triple table */
static BAT*
create_BAT(int ht, int tt, int size)
{
	BAT *b = BATnew(ht, tt, size);
	if (b == NULL) {
		return b;
	}
	BATseqbase(b, 0);

	/* disable all properties */
	b->tsorted = FALSE;
	b->tdense = FALSE;
	b->tkey = FALSE;
	b->hdense = TRUE;

	return b;
}

static parserData*
parserData_create (str location, BAT** graph)
{
	int i;

	parserData *pdata = (parserData *) GDKmalloc(sizeof(parserData));
	if (pdata == NULL) return NULL;

	pdata->tcount = 0;
	pdata->exception = 0;
	pdata->fatal = 0;
	pdata->error = 0;
	pdata->warning = 0;
	pdata->location = location;
	pdata->graph = graph;

	for (i = 0; i <= N_GRAPH_BAT; i++) {
		pdata->graph[i] = NULL;
	}

	/* New empty BATs for shredding. We only reserve memory for
	 * S_sort, P_sort, O_sort and MAP_LEX in this stage, since these
	 * are the ones to be populated now, while the rest will be
	 * created in a post-shredding processing step
	 */
	for (i = 0; i <= O_sort; i++) {
		pdata->graph[i] = create_BAT (
				graphdef[i].headType,
				graphdef[i].tailType,
				batsz);                       /* DOTO: estimate size */
		if (pdata->graph[i] == NULL) {
			return NULL;
		}
	}

	/* create the MAP_LEX BAT */
	pdata->graph[MAP_LEX] = create_BAT (
			graphdef[MAP_LEX].headType,
			graphdef[MAP_LEX].tailType,
			batsz);                           /* DOTO: estimate size */
	if (pdata->graph[MAP_LEX] == NULL) {
		return NULL;
	}
	/* MAP_LEX must have the key property */
	BATseqbase(pdata->graph[MAP_LEX], RDF_MIN_LITERAL);
	pdata->graph[MAP_LEX]->tkey = BOUND2BTRUE;
	pdata->graph[MAP_LEX]->T->nokey[0] = 0;
	pdata->graph[MAP_LEX]->T->nokey[1] = 0;

	return pdata;
}

/*
 * After the RDF document has been shredded into 3 bats and a lexical value
 * dictionary, a post-shred processing step follows that orders the lexical
 * dictionary, re-maps oids to match the ordered dictionary and finally creates
 * all 6 permutations of the (subject, predicate, object) order.
 *
 * However, it is still to be examined if it worth the time to refine the order
 * of the last column. In most cases, during query time, the last column will need
 * to be re-ordered for a subsequent sort-merge join. We introduce order3 and order2
 * so we can investigate both possibilities. In addition, the first column needs to
 * be stored only once for each couple of orders with the same first column. For
 * example, it holds that S_SPO == S_SOP.
 */
static str
post_processing (parserData *pdata)
{
	BAT *map_oid = NULL, *S = NULL, *P = NULL, *O = NULL;
	BAT **graph = pdata->graph;
#if STORE == TRIPLE_STORE
	BAT *o1,*o2,*o3;
	BAT *g1,*g2,*g3;
#endif
#ifdef _TKNZR_H
	BATiter bi, mi;
	BUN p, d, r;
	oid *bt;

	/* order MAP_LEX */
	BATorder(BATmirror(graph[MAP_LEX]));
	map_oid = BATmark(graph[MAP_LEX], RDF_MIN_LITERAL);   /* BATmark will create a copy */
	BATorder(map_oid);
	BATsetaccess(map_oid, BAT_READ);        /* force BAtmark not to copy bat */
	map_oid = BATmirror(BATmark(BATmirror(map_oid), RDF_MIN_LITERAL));
	BATsetaccess(graph[MAP_LEX], BAT_READ); /* force BATmark not to copy bat */
	graph[MAP_LEX] = BATmirror(BATmark(BATmirror(graph[MAP_LEX]), RDF_MIN_LITERAL));

	/* convert old oids of O_sort to new ones */
	bi = bat_iterator(graph[O_sort]);
	mi = bat_iterator(map_oid);
	BATloop(graph[O_sort], p, d) {
		bt = (oid *) BUNtloc(bi, p);
		if (*bt >= (RDF_MIN_LITERAL)) {
			BUNfndVOID(r, mi, bt);
			void_inplace(graph[O_sort], p, BUNtloc(mi, r), 1);
		}
	}
	BBPreclaim(map_oid);

	S = graph[S_sort];
	P = graph[P_sort];
	O = graph[O_sort];
#else
	/* order MAP_LEX */
	BATorder(BATmirror(graph[MAP_LEX]));
	map_oid = BATmark(graph[MAP_LEX], 0);   /* BATmark will create a copy */
	BATorder(map_oid);
	BATsetaccess(map_oid, BAT_READ);        /* force BAtmark not to copy bat */
	map_oid = BATmirror(BATmark(BATmirror(map_oid), 0));
	BATsetaccess(graph[MAP_LEX], BAT_READ); /* force BATmark not to copy bat */
	graph[MAP_LEX] = BATmirror(BATmark(BATmirror(graph[MAP_LEX]), 0));

	/* convert old oids of S_sort, P_sort, O_sort to new ones */
	S = BATproject(graph[S_sort], map_oid);
	if (S == NULL) goto bailout;
	BBPreclaim(graph[S_sort]);
	P = BATproject(graph[P_sort], map_oid);
	if (P == NULL) goto bailout;
	BBPreclaim(graph[P_sort]);
	O = BATproject(graph[O_sort], map_oid);
	if (O == NULL) goto bailout;
	BBPreclaim(graph[O_sort]);
	BBPreclaim(map_oid);
#endif

#if STORE == MLA_STORE
	graph[S_sort] = S;
	graph[P_sort] = P;
	graph[O_sort] = O;

	return MAL_SUCCEED;

#elif STORE == TRIPLE_STORE
	/* order SPO/SOP */
	if (BATsubsort(&graph[S_sort], &o1, &g1, S, NULL, NULL, 0, 0) == GDK_FAIL)
		goto bailout;
	if (BATsubsort(&graph[P_PO], &o2, &g2, P, o1, g1, 0, 0) == GDK_FAIL)
		goto bailout;
	if (BATsubsort(&graph[O_PO], &o3, &g3, O, o2, g2, 0, 0) == GDK_FAIL)
		goto bailout;
	BBPunfix(o2->batCacheid);
	BBPunfix(g2->batCacheid);
	BBPunfix(o3->batCacheid);
	BBPunfix(g3->batCacheid);
	if (BATsubsort(&graph[O_OP], &o2, &g2, O, o1, g1, 0, 0) == GDK_FAIL)
		goto bailout;
	if (BATsubsort(&graph[P_OP], &o3, &g3, P, o2, g2, 0, 0) == GDK_FAIL)
		goto bailout;
	BBPunfix(o1->batCacheid);
	BBPunfix(g1->batCacheid);
	BBPunfix(o2->batCacheid);
	BBPunfix(g2->batCacheid);
	BBPunfix(o3->batCacheid);
	BBPunfix(g3->batCacheid);

	/* order PSO/POS */
	if (BATsubsort(&graph[P_sort], &o1, &g1, P, NULL, NULL, 0, 0) == GDK_FAIL)
		goto bailout;
	if (BATsubsort(&graph[S_SO], &o2, &g2, S, o1, g1, 0, 0) == GDK_FAIL)
		goto bailout;
	if (BATsubsort(&graph[O_SO], &o3, &g3, O, o2, g2, 0, 0) == GDK_FAIL)
		goto bailout;
	BBPunfix(o2->batCacheid);
	BBPunfix(g2->batCacheid);
	BBPunfix(o3->batCacheid);
	BBPunfix(g3->batCacheid);
	if (BATsubsort(&graph[O_OS], &o2, &g2, O, o1, g1, 0, 0) == GDK_FAIL)
		goto bailout;
	if (BATsubsort(&graph[S_OS], &o3, &g3, S, o2, g2, 0, 0) == GDK_FAIL)
		goto bailout;
	BBPunfix(o1->batCacheid);
	BBPunfix(g1->batCacheid);
	BBPunfix(o2->batCacheid);
	BBPunfix(g2->batCacheid);
	BBPunfix(o3->batCacheid);
	BBPunfix(g3->batCacheid);

	/* order OPS/OSP */
	if (BATsubsort(&graph[O_sort], &o1, &g1, O, NULL, NULL, 0, 0) == GDK_FAIL)
		goto bailout;
	if (BATsubsort(&graph[P_PS], &o2, &g2, P, o1, g1, 0, 0) == GDK_FAIL)
		goto bailout;
	if (BATsubsort(&graph[S_PS], &o3, &g3, S, o2, g2, 0, 0) == GDK_FAIL)
		goto bailout;
	BBPunfix(o2->batCacheid);
	BBPunfix(g2->batCacheid);
	BBPunfix(o3->batCacheid);
	BBPunfix(g3->batCacheid);
	if (BATsubsort(&graph[S_SP], &o2, &g2, S, o1, g1, 0, 0) == GDK_FAIL)
		goto bailout;
	if (BATsubsort(&graph[P_SP], &o3, &g3, P, o2, g2, 0, 0) == GDK_FAIL)
		goto bailout;
	BBPunfix(o1->batCacheid);
	BBPunfix(g1->batCacheid);
	BBPunfix(o2->batCacheid);
	BBPunfix(g2->batCacheid);
	BBPunfix(o3->batCacheid);
	BBPunfix(g3->batCacheid);

	/* free memory */
	BBPreclaim(S);
	BBPreclaim(P);
	BBPreclaim(O);

	return MAL_SUCCEED;

bailout:
	if (map_oid != NULL) BBPreclaim(map_oid);
	if (S       != NULL) BBPreclaim(S);
	if (P       != NULL) BBPreclaim(P);
	if (O       != NULL) BBPreclaim(O);
	return NULL;
#endif
}

#define clean() \
if (pdata != NULL) {\
	for (iret = 0; iret < N_GRAPH_BAT; iret++) {\
		if (pdata->graph[iret] != NULL)\
			BBPreclaim(pdata->graph[iret]);\
	}\
	GDKfree(pdata);\
}

#define RDF_CHUNK_SIZE 100*1024*1024

/* Main RDF parser function that drives raptor */
str
RDFParser (BAT **graph, str *location, str *graphname, str *schema)
{
	raptor_parser *rparser;
	parserData *pdata;
	raptor_uri *uri;
	bit isURI;
	str ret;
	int iret;
	(void) graphname;

	/* init tokenizer */
#ifdef _TKNZR_H
	if (TKNZRopen (NULL, schema) != MAL_SUCCEED) {
		throw(RDF, "rdf.rdfShred",
				"could not open the tokenizer\n");
	}
#else
	(void) schema;
#endif

	/* Init pdata  */
	pdata = parserData_create(*location,graph);
	if (pdata == NULL) {
#ifdef _TKNZR_H
		TKNZRclose(&iret);
#endif
		clean();
		throw(RDF, "rdf.rdfShred",
				"could not allocate enough memory for pdata\n");
	}

	/* Init raptor */
	raptor_init();
	pdata->rparser = rparser = raptor_new_parser("guess");
	if (rparser == NULL) {
#ifdef _TKNZR_H
		TKNZRclose(&iret);
#endif
		raptor_finish();
		clean();
		throw(RDF, "rdf.rdfShred", "could not create raptor parser object\n");
	}
	/* set callback handler for triples */
	raptor_set_statement_handler   (rparser, pdata, tripleHandler);
	/* set message handlers */
	raptor_set_fatal_error_handler (rparser, pdata, fatalHandler);
	raptor_set_error_handler       (rparser, pdata, errorHandler);
	raptor_set_warning_handler     (rparser, pdata, warningHandler);

	raptor_set_parser_strict(rparser, 0);

	/* Parse URI or local file. */
	ret = URLisaURL(&isURI, location);
	if (ret != MAL_SUCCEED) {
#ifdef _TKNZR_H
		TKNZRclose(&iret);
#endif
		clean();
		return ret;
	} else if (isURI) {
		uri = raptor_new_uri((unsigned char *) pdata->location);
		iret = raptor_parse_uri(rparser, uri, NULL);
	} else {
		
		/* Too slow loading --> use old code 
		FILE *fp = fopen(pdata->location, "r");
		char *buf = (char*) GDKmalloc(RDF_CHUNK_SIZE);
		if (buf == NULL) {
			throw(RDF, "rdf.rdfShred",
				"could not allocate a %dMB file buffer\n", (int) (RDF_CHUNK_SIZE>>20));
		}
		uri = raptor_new_uri(raptor_uri_filename_to_uri_string(pdata->location));
		iret = raptor_start_parse(rparser, uri);
		while(fp && iret == 0) {
			ssize_t len = (ssize_t) fread(buf, 1, RDF_CHUNK_SIZE, fp);  
			iret = raptor_parse_chunk(rparser, (const unsigned char*) buf, (size_t) len, len < RDF_CHUNK_SIZE);
		}
		fclose(fp);
		
		*/

		/* does/may? not work on large files -- therefore the abpove chunked read
                   iret = raptor_parse_file_stream(rparser, fp, pdata->location, uri); 
   		 */

		/* Old code */
                uri = raptor_new_uri(
                                raptor_uri_filename_to_uri_string(pdata->location));
                iret = raptor_parse_file(rparser, uri, NULL);
	}
	/* Free memory of raptor */
	raptor_free_parser(rparser);
	raptor_free_uri(uri);
	raptor_finish();

#ifdef _TKNZR_H
	TKNZRclose(&iret);
#endif

	assert (pdata->tcount == BATcount(graph[S_sort]) &&
			pdata->tcount == BATcount(graph[P_sort]) &&
			pdata->tcount == BATcount(graph[O_sort]));

	/* error check */
	if (iret) {
		clean();
		throw(RDF, "rdf.rdfShred", "parsing failed\n");
	}
	if (pdata->exception) {
		throw(RDF, "rdf.rdfShred", "%s\n", pdata->exceptionMsg);
	} else if (pdata->fatal) {
		throw(RDF, "rdf.rdfShred", "last fatal error was:\n%s\n",
				pdata->fatalMsg);
	} else if (pdata->error) {
		throw(RDF, "rdf.rdfShred", "last error was:\n%s\n",
				pdata->errorMsg);
	} else if (pdata->warning) {
		throw(RDF, "rdf.rdfShred", "last warning was:\n%s\n",
				pdata->warningMsg);
	}

	/* post processing step */
	ret = post_processing(pdata);
	if (ret != MAL_SUCCEED) {
		clean();
		throw(RDF, "rdf.rdfShred", "could not post-proccess data");
	}
	GDKfree(pdata);
	return MAL_SUCCEED;
}
