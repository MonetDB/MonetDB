/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* (c) M.L. Kersten
Most optimizers need easy access to key information 
for proper plan generation. Amongst others, this
information consists of the tuple count, size,
min- and max-value, and the null-density.
They are kept around as persistent tables, modeled 
directly as a collection of BATs.

We made need an directly accessible structure to speedup
analysis by optimizers.
*/
#include "monetdb_config.h"
#include "sql_statistics.h"
#include "sql_execute.h"

#define atommem(size)					\
	do {						\
		if (*dst == NULL || *len < (size)) {	\
			GDKfree(*dst);			\
			*len = (size);			\
			*dst = GDKmalloc(*len);		\
			if (*dst == NULL)		\
				return -1;		\
		}					\
	} while (0)

static ssize_t
strToStrSQuote(char **dst, size_t *len, const void *src)
{
	ssize_t l = 0;

	if (GDK_STRNIL((str) src)) {
		atommem(4);

		return snprintf(*dst, *len, "nil");
	} else {
		size_t sz = escapedStrlen(src, NULL, NULL, '\'');
		atommem(sz + 3);
		l = (ssize_t) escapedStr((*dst) + 1, src, *len - 1, NULL, NULL, '\'');
		l++;
		(*dst)[0] = (*dst)[l++] = '"';
		(*dst)[l] = 0;
	}
	return l;
}

str
sql_analyze(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	sql_trans *tr = m->session->tr;
	node *nsch, *ntab, *ncol;
	char *query = NULL, *dquery;
	size_t querylen = 0;
	char *maxval = NULL, *minval = NULL;
	size_t minlen = 0, maxlen = 0;
	str sch = 0, tbl = 0, col = 0;
	int sorted, revsorted;
	lng nils = 0;
	lng uniq = 0;
	lng samplesize = *getArgReference_lng(stk, pci, 2);
	int argc = pci->argc;
	int width = 0;
	int minmax = *getArgReference_int(stk, pci, 1);
	int sfnd = 0, tfnd = 0, cfnd = 0;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	dquery = (char *) GDKzalloc(96);
	if (dquery == NULL) {
		throw(SQL, "analyze", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	switch (argc) {
	case 6:
		col = *getArgReference_str(stk, pci, 5);
		/* fall through */
	case 5:
		tbl = *getArgReference_str(stk, pci, 4);
		/* fall through */
	case 4:
		sch = *getArgReference_str(stk, pci, 3);
	}
#ifdef DEBUG_SQL_STATISTICS
	fprintf(stderr, "analyze %s.%s.%s sample " LLFMT "%s\n", (sch ? sch : ""), (tbl ? tbl : " "), (col ? col : " "), samplesize, (minmax)?"MinMax":"");
#endif
	for (nsch = tr->schemas.set->h; nsch; nsch = nsch->next) {
		sql_base *b = nsch->data;
		sql_schema *s = (sql_schema *) nsch->data;
		if (!isalpha((unsigned char) b->name[0]))
			continue;

		if (sch && strcmp(sch, b->name))
			continue;
		sfnd = 1;
		if (s->tables.set)
			for (ntab = (s)->tables.set->h; ntab; ntab = ntab->next) {
				sql_base *bt = ntab->data;
				sql_table *t = (sql_table *) bt;

				if (tbl && strcmp(bt->name, tbl))
					continue;
				if (t->persistence != SQL_PERSIST) {
					GDKfree(dquery);
					GDKfree(query);
					GDKfree(maxval);
					GDKfree(minval);
					throw(SQL, "analyze", SQLSTATE(42S02) "Table '%s' is not persistent", bt->name);
				}
				tfnd = 1;
				if (isTable(t) && t->columns.set)
					for (ncol = (t)->columns.set->h; ncol; ncol = ncol->next) {
						sql_base *bc = ncol->data;
						sql_column *c = (sql_column *) ncol->data;
						BAT *bn, *br;
						BAT *bsample;
						lng sz;
						ssize_t (*tostr)(str*,size_t*,const void*);
						void *val=0;

						if (col && strcmp(bc->name, col))
							continue;

						/* remove cached value */
						if (c->min)
							c->min = NULL;
						if (c->max)
							c->max = NULL;

						if ((bn = store_funcs.bind_col(tr, c, RDONLY)) == NULL) {
							/* XXX throw error instead? */
							continue;
						}
						sz = BATcount(bn);
						tostr = BATatoms[bn->ttype].atomToStr;

						if (tostr == BATatoms[TYPE_str].atomToStr)
							tostr = strToStrSQuote;

						snprintf(dquery, 96, "delete from sys.statistics where \"column_id\" = %d;", c->base.id);
						cfnd = 1;
						if (samplesize > 0) {
							bsample = BATsample(bn, (BUN) samplesize);
						} else
							bsample = NULL;
						br = BATselect(bn, bsample, ATOMnilptr(bn->ttype), NULL, 1, 0, 0);
						if (br == NULL) {
							BBPunfix(bn->batCacheid);
							/* XXX throw error instead? */
							continue;
						}
						nils = BATcount(br);
						BBPunfix(br->batCacheid);
						if (bn->tkey)
							uniq = sz;
						else if (!minmax) {
							BAT *en;
							if (bsample)
								br = BATproject(bsample, bn);
							else
								br = bn;
							if (br && (en = BATunique(br, NULL)) != NULL) {
								uniq = BATcount(en);
								BBPunfix(en->batCacheid);
							} else
								uniq = 0;
							if (bsample && br)
								BBPunfix(br->batCacheid);
						}
						if (bsample)
							BBPunfix(bsample->batCacheid);
						/* use BATordered(_rev)
						 * and not
						 * BATt(rev)ordered
						 * because we want to
						 * know for sure */
						sorted = BATordered(bn);
						revsorted = BATordered_rev(bn);

						// Gather the min/max value for builtin types
						width = bn->twidth;

						if (maxlen < 4) {
							GDKfree(maxval);
							maxval = GDKmalloc(4);
							if (maxval == NULL) {
								GDKfree(dquery);
								GDKfree(minval);
								throw(SQL, "analyze", SQLSTATE(HY001) MAL_MALLOC_FAIL);
							}
							maxlen = 4;
						}
						if (minlen < 4) {
							GDKfree(minval);
							minval = GDKmalloc(4);
							if (minval == NULL){
								GDKfree(dquery);
								GDKfree(maxval);
								throw(SQL, "analyze", SQLSTATE(HY001) MAL_MALLOC_FAIL);
							}
							minlen = 4;
						}
						if (tostr) {
							if ((val = BATmax(bn,0)) == NULL)
								strcpy(maxval, "nil");
							else {
								if (tostr(&maxval, &maxlen, val) < 0) {
									GDKfree(val);
									GDKfree(dquery);
									GDKfree(minval);
									GDKfree(maxval);
									throw(SQL, "analyze", GDK_EXCEPTION);
								}
								GDKfree(val);
							}
							if ((val = BATmin(bn,0)) == NULL)
								strcpy(minval, "nil");
							else {
								if (tostr(&minval, &minlen, val) < 0) {
									GDKfree(val);
									GDKfree(dquery);
									GDKfree(minval);
									GDKfree(maxval);
									throw(SQL, "analyze", GDK_EXCEPTION);
								}
								GDKfree(val);
							}
						} else {
							strcpy(maxval, "nil");
							strcpy(minval, "nil");
						}
						if (strlen(minval) + strlen(maxval) + 1024 > querylen) {
							querylen = strlen(minval) + strlen(maxval) + 1024;
							GDKfree(query);
							query = GDKmalloc(querylen);
							if (query == NULL) {
								GDKfree(dquery);
								GDKfree(maxval);
								GDKfree(minval);
								throw(SQL, "analyze", SQLSTATE(HY001) MAL_MALLOC_FAIL);
							}
						}
						snprintf(query, querylen, "insert into sys.statistics (column_id,type,width,stamp,\"sample\",count,\"unique\",nils,minval,maxval,sorted,revsorted) values(%d,'%s',%d,now()," LLFMT "," LLFMT "," LLFMT "," LLFMT ",'%s','%s',%s,%s);", c->base.id, c->type.type->sqlname, width, (samplesize ? samplesize : sz), sz, uniq, nils, minval, maxval, sorted ? "true" : "false", revsorted ? "true" : "false");
#ifdef DEBUG_SQL_STATISTICS
						fprintf(stderr, "%s\n", dquery);
						fprintf(stderr, "%s\n", query);
#endif
						BBPunfix(bn->batCacheid);
						msg = SQLstatementIntern(cntxt, &dquery, "SQLanalyze", TRUE, FALSE, NULL);
						if (msg) {
							GDKfree(dquery);
							GDKfree(query);
							GDKfree(maxval);
							GDKfree(minval);
							return msg;
						}
						msg = SQLstatementIntern(cntxt, &query, "SQLanalyze", TRUE, FALSE, NULL);
						if (msg) {
							GDKfree(dquery);
							GDKfree(query);
							GDKfree(maxval);
							GDKfree(minval);
							return msg;
						}
					}
			}
	}
	GDKfree(dquery);
	GDKfree(query);
	GDKfree(maxval);
	GDKfree(minval);
	if (sch && !sfnd)
		throw(SQL, "analyze", SQLSTATE(3F000) "Schema '%s' does not exist", sch);
	if (tbl && !tfnd)
		throw(SQL, "analyze", SQLSTATE(42S02) "Table '%s' does not exist", tbl);
	if (col && !cfnd)
		throw(SQL, "analyze", SQLSTATE(38000) "Column '%s' does not exist", col);
	return MAL_SUCCEED;
}
