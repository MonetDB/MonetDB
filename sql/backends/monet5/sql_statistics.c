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

#define atommem(TYPE, size)					\
	do {							\
		if (*dst == NULL || *len < (int) (size)) {	\
			GDKfree(*dst);				\
			*len = (size);				\
			*dst = (TYPE *) GDKmalloc(*len);	\
			if (*dst == NULL)			\
				return -1;			\
		}						\
	} while (0)

static int
strToStrSQuote(char **dst, int *len, const void *src)
{
	int l = 0;

	if (GDK_STRNIL((str) src)) {
		atommem(char, 4);

		return snprintf(*dst, *len, "nil");
	} else {
		int sz = escapedStrlen(src, NULL, NULL, '\'');
		atommem(char, sz + 3);
		l = escapedStr((*dst) + 1, src, *len - 1, NULL, NULL, '\'');
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
	char *query, *dquery;
	size_t querylen;
	char *maxval = NULL, *minval = NULL;
	int minlen = 0, maxlen = 0;
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

	querylen = 0;
	query = NULL;
	dquery = (char *) GDKzalloc(8192);
	if (dquery == NULL) {
		throw(SQL, "analyze", MAL_MALLOC_FAIL);
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
		if (!isalpha((int) b->name[0]))
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
				tfnd = 1;
				if (isTable(t) && t->columns.set)
					for (ncol = (t)->columns.set->h; ncol; ncol = ncol->next) {
						sql_base *bc = ncol->data;
						sql_column *c = (sql_column *) ncol->data;
						BAT *bn, *br;
						BAT *bsample;
						lng sz;
						int (*tostr)(str*,int*,const void*);
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

						snprintf(dquery, 8192, "delete from sys.statistics where \"column_id\" = %d;", c->base.id);
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
						if( bsample)
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
							if( maxval== NULL) {
								GDKfree(dquery);
								throw(SQL, "analyze", MAL_MALLOC_FAIL);
							}
							maxlen = 4;
						}
						if (minlen < 4) {
							GDKfree(minval);
							minval = GDKmalloc(4);
							if( minval== NULL){
								GDKfree(dquery);
								GDKfree(maxval);
								throw(SQL, "analyze", MAL_MALLOC_FAIL);
							}
							minlen = 4;
						}
						if (tostr) {
							if ((val = BATmax(bn,0)) == NULL)
								strcpy(maxval, "nil");
							else {
								tostr(&maxval, &maxlen, val);
								GDKfree(val);
							}
							if ((val = BATmin(bn,0)) == NULL)
								strcpy(minval, "nil");
							else {
								tostr(&minval, &minlen, val);
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
								throw(SQL, "analyze", MAL_MALLOC_FAIL);
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
		throw(SQL, "analyze", "Schema '%s' does not exist", sch);
	if (tbl && !tfnd)
		throw(SQL, "analyze", "Table '%s' does not exist", tbl);
	if (col && !cfnd)
		throw(SQL, "analyze", "Column '%s' does not exist", col);
	return MAL_SUCCEED;
}
