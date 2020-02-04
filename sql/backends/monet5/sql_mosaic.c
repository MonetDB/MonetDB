/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/* (c) M.L. Kersten
 * The mosaic interace routines are defined here.
*/
#include "monetdb_config.h"
#include "mal_backend.h"
#include "sql_scenario.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_env.h"
#include "sql_mvc.h"
#include "sql_mosaic.h"
#include "mosaic.h"
#include "sql_scenario.h"

// TODO: clean up this function a bit.
str
sql_mosaicLayout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	sql_trans *tr = m->session->tr;
	node *nsch, *ntab, *ncol;
	str sch = 0, tbl = 0, col = 0;
	BAT *bn, *btech, *bcount, *binput, *boutput, *bproperties, *bbsn;
	int *tech,*count,*input,*output, *properties, *bsn;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	btech = COLnew((oid)0, TYPE_str,0, TRANSIENT);
	if( btech == NULL)
		throw(SQL,__func__, MAL_MALLOC_FAIL);
	tech = getArgReference_bat(stk, pci, 0);
	*tech = btech->batCacheid;

	bcount = COLnew((oid)0, TYPE_lng,0, TRANSIENT);
	if( bcount == NULL){
		BBPunfix(btech->batCacheid);
		throw(SQL,__func__, MAL_MALLOC_FAIL);
	}
	count = getArgReference_bat(stk, pci, 1);
	*count = bcount->batCacheid;

	binput = COLnew((oid)0, TYPE_lng,0, TRANSIENT);
	if( binput == NULL){
		BBPunfix(btech->batCacheid);
		BBPunfix(bcount->batCacheid);
		throw(SQL,__func__, MAL_MALLOC_FAIL);
	}
	input = getArgReference_bat(stk, pci, 2);
	*input = binput->batCacheid;

	boutput = COLnew((oid)0, TYPE_lng,0, TRANSIENT);
	if( boutput == NULL){
		BBPunfix(btech->batCacheid);
		BBPunfix(bcount->batCacheid);
		BBPunfix(binput->batCacheid);
		throw(SQL,__func__, MAL_MALLOC_FAIL);
	}
	output = getArgReference_bat(stk, pci, 3);
	*output = boutput->batCacheid;

	bproperties = COLnew((oid)0, TYPE_str,0, TRANSIENT);
	if( bproperties == NULL){
		BBPunfix(btech->batCacheid);
		BBPunfix(bcount->batCacheid);
		BBPunfix(binput->batCacheid);
		BBPunfix(boutput->batCacheid);
		throw(SQL,__func__, MAL_MALLOC_FAIL);
	}
	properties = getArgReference_bat(stk, pci, 4);
	*properties = bproperties->batCacheid;

	bbsn = COLnew((oid)0, TYPE_lng,0, TRANSIENT);
	if( bbsn == NULL){
		BBPunfix(btech->batCacheid);
		BBPunfix(bcount->batCacheid);
		BBPunfix(binput->batCacheid);
		BBPunfix(boutput->batCacheid);
		BBPunfix(bproperties->batCacheid);
		throw(SQL,__func__, MAL_MALLOC_FAIL);
	}
	bsn = getArgReference_bat(stk, pci, 5);
	*bsn = bbsn->batCacheid;

	sch = *getArgReference_str(stk, pci, 6);
	tbl = *getArgReference_str(stk, pci, 7);
	col = *getArgReference_str(stk, pci, 8);

#ifdef DEBUG_SQL_MOSAIC
	mnstr_printf(cntxt->fdout, "#mosaic layout %s.%s.%s \n", sch, tbl, col);
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
						if (col && strcmp(bc->name, col))
							continue;
						// perform the analysis
						bn = store_funcs.bind_col(m->session->tr, c, 0);

						if ((msg = MOSlayout(bn, bbsn, btech, bcount, binput, boutput, bproperties)) != MAL_SUCCEED) {
							break;
						}
						BBPunfix(bn->batCacheid);
						(void) c;
					}
			}
	}

	if (msg != MAL_SUCCEED) {
		BBPunfix(btech->batCacheid);
		BBPunfix(bcount->batCacheid);
		BBPunfix(binput->batCacheid);
		BBPunfix(boutput->batCacheid);
		BBPunfix(bproperties->batCacheid);
		BBPunfix(bbsn->batCacheid);
		return msg;
	}

	BBPkeepref(*tech);
	BBPkeepref(*count);
	BBPkeepref(*input);
	BBPkeepref(*output);
	BBPkeepref(*properties);
	BBPkeepref(*bsn);
	return MAL_SUCCEED;
}

str
sql_mosaicAnalysis(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	sql_trans *tr = m->session->tr;
	node *nsch, *ntab, *ncol;
	str sch = 0, tbl = 0, col = 0;
	int *tech,*output, *factor, *compress, *decompress;
	BAT *bn, *btech, *boutput, *bfactor, *bcompress, *bdecompress;
	str compressions = NULL;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	btech = COLnew((oid)0, TYPE_str,0, TRANSIENT);
	if( btech == NULL)
		throw(SQL,"mosaicAnalysis", MAL_MALLOC_FAIL);
	tech = getArgReference_bat(stk, pci, 0);
	*tech = btech->batCacheid;

	boutput = COLnew((oid)0, TYPE_lng,0, TRANSIENT);
	if( boutput == NULL){
		BBPunfix(btech->batCacheid);
		throw(SQL,"mosaicAnalysis", MAL_MALLOC_FAIL);
	}
	output = getArgReference_bat(stk, pci, 1);
	*output = boutput->batCacheid;

	bfactor = COLnew((oid)0, TYPE_dbl,0, TRANSIENT);
	if( bfactor == NULL){
		BBPunfix(btech->batCacheid);
		BBPunfix(boutput->batCacheid);
		throw(SQL,"mosaicAnalysis", MAL_MALLOC_FAIL);
	}
	factor = getArgReference_bat(stk, pci, 2);
	*factor = bfactor->batCacheid;

	bcompress = COLnew((oid)0, TYPE_lng,0, TRANSIENT);
	if( bcompress == NULL){
		BBPunfix(btech->batCacheid);
		BBPunfix(boutput->batCacheid);
		BBPunfix(bfactor->batCacheid);
		throw(SQL,"mosaicAnalysis", MAL_MALLOC_FAIL);
	}
	compress = getArgReference_bat(stk, pci, 3);
	*compress = bcompress->batCacheid;

	bdecompress = COLnew((oid)0, TYPE_lng,0, TRANSIENT);
	if( bdecompress == NULL){
		BBPunfix(btech->batCacheid);
		BBPunfix(boutput->batCacheid);
		BBPunfix(bfactor->batCacheid);
		BBPunfix(bcompress->batCacheid);
		throw(SQL,"mosaicAnalysis", MAL_MALLOC_FAIL);
	}
	decompress = getArgReference_bat(stk, pci, 4);
	*decompress = bdecompress->batCacheid;

	sch = *getArgReference_str(stk, pci, 5);
	tbl = *getArgReference_str(stk, pci, 6);
	col = *getArgReference_str(stk, pci, 7);
	if ( pci->argc == 9){
		// use a predefined collection of compression schemes.
		compressions = *getArgReference_str(stk,pci,8);
	}

#ifdef DEBUG_SQL_MOSAIC
	mnstr_printf(cntxt->fdout, "#mosaic analysis %s.%s.%s \n", sch, tbl, col);
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
						if (col && strcmp(bc->name, col))
							continue;
						// perform the analysis
						bn = store_funcs.bind_col(m->session->tr, c, 0);
						msg = MOSAnalysis(bn, btech, boutput, bfactor, bcompress, bdecompress, compressions);
						BBPunfix(bn->batCacheid);
						(void) c;

						if(msg != MAL_SUCCEED){
							BBPunfix(btech->batCacheid);
							BBPunfix(boutput->batCacheid);
							BBPunfix(bfactor->batCacheid);
							BBPunfix(bcompress->batCacheid);
							return msg;
						}
					}
			}
	}
	BBPkeepref(*tech);
	BBPkeepref(*output);
	BBPkeepref(*factor);
	BBPkeepref(*compress);
	BBPkeepref(*decompress);
	return MAL_SUCCEED;
}
