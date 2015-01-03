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
 * Copyright August 2008-2015 MonetDB B.V.
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
	char *maxval = NULL, *minval = NULL;
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
		samplesize = *getArgReference_lng(stk, pci, pci->argc - 1);
		argc--;
	}
	dquery = (char *) GDKzalloc(8192);
	query = (char *) GDKzalloc(8192);
	if (!(dquery && query)) {
		GDKfree(dquery);
		GDKfree(query);
		throw(SQL, "analyze", MAL_MALLOC_FAIL);
	}

	switch (argc) {
	case 4:
		col = *getArgReference_str(stk, pci, 3);
	case 3:
		tbl = *getArgReference_str(stk, pci, 2);
	case 2:
		sch = *getArgReference_str(stk, pci, 1);
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
						BAT *bn = store_funcs.bind_col(tr, c, RDONLY), *br;
						BAT *bsample;
						lng sz = BATcount(bn);
						int (*tostr)(str*,int*,const void*) = BATatoms[bn->ttype].atomToStr; \
						int len = 0;
						void *val=0;

						if (col && strcmp(bc->name, col))
							continue;
						snprintf(dquery, 8192, "delete from sys.statistics where \"column_id\" = %d;", c->base.id);
						if (samplesize > 0) {
							bsample = BATsample(bn, (BUN) 25000);
						} else
							bsample = NULL;
						br = BATsubselect(bn, bsample, ATOMnilptr(bn->ttype), NULL, 0, 0, 0);
						nils = BATcount(br);
						BBPunfix(br->batCacheid);
						if (bn->tkey)
							uniq = sz;
						else {
							BAT *en;
							if (bsample)
								br = BATproject(bsample, bn);
							else
								br = bn;
							if (br && (en = BATsubunique(br, NULL)) != NULL) {
								uniq = BATcount(en);
								BBPunfix(en->batCacheid);
							} else
								uniq = 0;
							if (bsample && br)
								BBPunfix(br->batCacheid);
						}
						if( bsample)
							BBPunfix(bsample->batCacheid);
						sorted = BATtordered(bn);

						// Gather the min/max value for builtin types
						width = bn->T->width;

						if (tostr) { 
							val = BATmax(bn,0); len = 0;
							tostr(&maxval, &len,val); 
							GDKfree(val);
							val = BATmin(bn,0); len = 0;
							tostr(&minval, &len,val); 
							GDKfree(val);
						} else {
							maxval = (char *) GDKzalloc(4);
							minval = (char *) GDKzalloc(4);
							snprintf(maxval, 4, "nil");
							snprintf(minval, 4, "nil");
						}
						snprintf(query, 8192, "insert into sys.statistics values(%d,'%s',%d,now()," LLFMT "," LLFMT "," LLFMT "," LLFMT ",'%s','%s',%s);", c->base.id, c->type.type->sqlname, width, (samplesize ? samplesize : sz), sz, uniq, nils, minval, maxval, sorted ? "true" : "false");
#ifdef DEBUG_SQL_STATISTICS
						mnstr_printf(cntxt->fdout, "%s\n", dquery);
						mnstr_printf(cntxt->fdout, "%s\n", query);
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
	return MAL_SUCCEED;
}
