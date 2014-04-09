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
#include "sql_scenario.h"

str
sql_analyze(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	sql_trans *tr = m->session->tr;
	node *nsch, *ntab, *ncol;
	char *query, *dquery;
	char *maxval, *minval;
	str sch = 0, tbl = 0, col = 0;
	int sorted;
	lng nils = 0;
	lng uniq = 0;
	lng samplesize = 0;
	int argc = pci->argc;
	int width = 0;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (argc > 1 && getVarType(mb, getArg(pci, argc - 1)) == TYPE_lng) {
		samplesize = *(lng *) getArgReference(stk, pci, pci->argc - 1);
		argc--;
	}
	dquery = (char *) GDKzalloc(8192);
	query = (char *) GDKzalloc(8192);
	maxval = (char *) GDKzalloc(8192);
	minval = (char *) GDKzalloc(8192);
	if (!(dquery && query && maxval && minval)) {
		GDKfree(dquery);
		GDKfree(query);
		GDKfree(maxval);
		GDKfree(minval);
		throw(SQL, "analyze", MAL_MALLOC_FAIL);
	}

	switch (argc) {
	case 4:
		col = *(str *) getArgReference(stk, pci, 3);
	case 3:
		tbl = *(str *) getArgReference(stk, pci, 2);
	case 2:
		sch = *(str *) getArgReference(stk, pci, 1);
	}
#ifdef DEBUG_SQL_STATISTICS
	mnstr_printf(cntxt->fdout, "analyze %s.%s.%s sample " LLFMT "\n", (sch ? sch : ""), (tbl ? tbl : " "), (col ? col : " "), samplesize);
#endif
	for (nsch = tr->schemas.set->h; nsch; nsch = nsch->next) {
		sql_base *b = nsch->data;
		sql_schema *s = (sql_schema *) nsch->data;
		if (!isalpha((int) b->name[0]))
			continue;

		if (sch && strcmp(sch, b->name))
			continue;
		if (s->tables.set)
			for (ntab = (s)->tables.set->h; ntab; ntab = ntab->next) {
				sql_base *bt = ntab->data;
				sql_table *t = (sql_table *) bt;

				if (tbl && strcmp(bt->name, tbl))
					continue;
				if (isTable(t) && t->columns.set)
					for (ncol = (t)->columns.set->h; ncol; ncol = ncol->next) {
						sql_base *bc = ncol->data;
						sql_column *c = (sql_column *) ncol->data;
						BAT *bn = store_funcs.bind_col(tr, c, 0), *br;
						BAT *bsample;
						lng sz = BATcount(bn);

						if (col && strcmp(bc->name, col))
							continue;
						snprintf(dquery, 8192, "delete from sys.statistics where \"schema\" ='%s' and \"table\"='%s' and \"column\"='%s';", b->name, bt->name, bc->name);
						if (samplesize > 0) {
							bsample = BATsample(bn, (BUN) 25000);
						} else
							bsample = bn;
						br = BATselect(bsample, ATOMnil(bn->ttype), 0);
						nils = BATcount(br);
						BBPunfix(br->batCacheid);
						if (bn->tkey)
							uniq = sz;
						else {
							br = BATkunique(BATmirror(bsample));
							uniq = BATcount(br);
							BBPunfix(br->batCacheid);
						}
						if (samplesize > 0) {
							BBPunfix(bsample->batCacheid);
						}
						sorted = BATtordered(bn);

						// Gather the min/max value for builtin types
#define minmax(TYPE,FMT) \
{\
	TYPE *val=0;\
	val= BATmax(bn,0);\
	if ( ATOMcmp(bn->ttype,val, ATOMnil(bn->ttype))== 0)\
		snprintf(maxval,8192,"nil");\
	else snprintf(maxval,8192,FMT,*val);\
	GDKfree(val);\
	val= BATmin(bn,0);\
	if ( ATOMcmp(bn->ttype,val, ATOMnil(bn->ttype))== 0)\
		snprintf(minval,8192,"nil");\
	else snprintf(minval,8192,FMT,*val);\
	GDKfree(val);\
	break;\
}
						width = bn->T->width;
						switch (bn->ttype) {
						case TYPE_sht:
							minmax(sht, "%d");
						case TYPE_int:
							minmax(int, "%d");
						case TYPE_lng:
							minmax(lng, LLFMT);
						case TYPE_flt:
							minmax(flt, "%f");
						case TYPE_dbl:
							minmax(dbl, "%f");
						case TYPE_str:
						{
							BUN p, q;
							double sum = 0;
							BATiter bi = bat_iterator(bn);
							BATloop(bn, p, q) {
								str s = BUNtail(bi, p);
								if (s != NULL && strcmp(s, str_nil))
									sum += (int) strlen(s);
							}
							if (sz)
								width = (int) (sum / sz);
						}

						default:
							snprintf(maxval, 8192, "nil");
							snprintf(minval, 8192, "nil");
						}
						snprintf(query, 8192, "insert into sys.statistics values('%s','%s','%s','%s',%d,now()," LLFMT "," LLFMT "," LLFMT "," LLFMT ",'%s','%s',%s);", b->name, bt->name, bc->name, c->type.type->sqlname, width,
							 (samplesize ? samplesize : sz), sz, uniq, nils, minval, maxval, sorted ? "true" : "false");
#ifdef DEBUG_SQL_STATISTICS
						mnstr_printf(cntxt->fdout, "%s\n", dquery);
						mnstr_printf(cntxt->fdout, "%s\n", query);
#endif
						BBPunfix(bn->batCacheid);
						msg = SQLstatementIntern(cntxt, &dquery, "SQLanalyze", TRUE, FALSE);
						if (msg) {
							GDKfree(dquery);
							GDKfree(query);
							GDKfree(maxval);
							GDKfree(minval);
							return msg;
						}
						msg = SQLstatementIntern(cntxt, &query, "SQLanalyze", TRUE, FALSE);
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
	return MAL_SUCCEED;
}
