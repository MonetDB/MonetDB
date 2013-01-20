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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/

#include "monetdb_config.h"
#include "sciql.h"
#include "array.h"

#define MTRL_CLEANUP() \
{ \
	if (N) GDKfree(N); \
	if (M) GDKfree(M); \
	if (bids) { \
		for (i = 0; i < list_length(a->columns.set); i++) { \
			if (bids[i] != 0) \
			BBPunfix(bids[i]); \
		} \
		GDKfree(bids); \
	} \
}

/* copied from monetdb5/modules/mal/batcalc.c:mythrow
 */
static str
_rethrow(enum malexception type, const char *fcn, const char *msg)
{
	char *errbuf = GDKerrbuf;
	char *s;

	if (errbuf && *errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
		if (strchr(errbuf, '!') == errbuf + 5) {
			s = createException(type, fcn, "%s", errbuf);
		} else if ((s = strchr(errbuf, ':')) != NULL && s[1] == ' ') {
			s = createException(type, fcn, "%s", s + 2);
		} else {
			s = createException(type, fcn, "%s", errbuf);
		}
		*GDKerrbuf = 0;
		return s;
	}
	return createException(type, fcn, "%s", msg);
}

str
SCIQLmaterialise(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg = getSQLContext(cntxt, mb, &sql, NULL);
	str sname = *(str*) getArgReference(stk, pci, 1);
	str aname = *(str*) getArgReference(stk, pci, 2);
	sql_schema *s = NULL;
	sql_table *a = NULL;
	int i = 0, j = 0, *N = NULL, *M = NULL, *bids = NULL;
	BUN cnt = 0, cntall = 1; /* cntall must be initialised with 1! */
	node *n = NULL;

	/* for updating the SQL catalog */
	sql_trans *tr = NULL;
	sql_table *systable = NULL, *sysarray = NULL;
	oid rid = 0;
	sql_schema *syss = NULL;
	sqlid tid = 0;
	bit materialised = 1;

	if (msg)
		return msg;

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(MAL, "sciql.materialise", RUNTIME_OBJECT_MISSING);
	if (!(a = mvc_bind_table(sql, s, aname)))
		throw(MAL, "sciql.materialise", RUNTIME_OBJECT_MISSING);
	if (a->materialised)
		return MAL_SUCCEED;

	/* To compute N (the #times each value is repeated), multiply the size of
	 * dimensions defined after the current dimension.  For the last dimension,
	 * its N is 1.  To compute M (the #times each value group is repeated),
	 * multiply the size of dimensions defined before the current dimension.
	 * For the first dimension, its M is 1. */
	N = GDKmalloc(a->valence * sizeof(int)); /* #repeats of each value */
	M = GDKmalloc(a->valence * sizeof(int)); /* #repeats of each group of values */
	bids = GDKzalloc(list_length(a->columns.set) * sizeof(int)); /* BAT ids for each column */
	if (!N || !M || !bids) {
		MTRL_CLEANUP();
		throw(MAL, "sciql.materialise", MAL_MALLOC_FAIL);
	}
	for (i = 0; i < a->valence; i++)
		N[i] = M[i] = 1;

	for (n = a->columns.set->h, i = 0; n; n = n->next) {
		sql_column *sc = n->data;

		if (sc->dim) {
			/* TODO: overflow check, see gdk_calc.c */
			cnt = sc->dim->step > 0 ? (sc->dim->stop - sc->dim->strt + sc->dim->step - 1) / sc->dim->step :
				(sc->dim->strt - sc->dim->stop - sc->dim->step - 1) / -sc->dim->step;
			for (j = 0; j < i; j++) N[j] *= cnt;
			for (j = a->valence; j > i; j--) M[j] *= cnt;
			cntall *= cnt;
			i++;
		}
	}

	/* create and fill all columns */
	for (n = a->columns.set->h, i = 0; n; n = n->next, i++){
		sql_column *sc = n->data;
		BAT *bn = NULL;
		int tpe = sc->type.type->localtype;
		
		if (sc->dim) {
			switch (tpe) {
			case TYPE_bte:
				ARRAYseries_bte(&bids[i], (bte *)&sc->dim->strt, (bte *)&sc->dim->step, (bte *)&sc->dim->stop, &N[i], &M[i]);
				break;
			case TYPE_sht:
				ARRAYseries_sht(&bids[i], (sht *)&sc->dim->strt, (sht *)&sc->dim->step, (sht *)&sc->dim->stop, &N[i], &M[i]);
				break;                                                                        
			case TYPE_int:                                                                    
				ARRAYseries_int(&bids[i], (int *)&sc->dim->strt, (int *)&sc->dim->step, (int *)&sc->dim->stop, &N[i], &M[i]);
				break;                                                                        
			case TYPE_lng:                                                                    
				ARRAYseries_lng(&bids[i], (lng *)&sc->dim->strt, (lng *)&sc->dim->step, (lng *)&sc->dim->stop, &N[i], &M[i]);
				break;                                                                        
			case TYPE_flt:                                                                    
				ARRAYseries_flt(&bids[i], (flt *)&sc->dim->strt, (flt *)&sc->dim->step, (flt *)&sc->dim->stop, &N[i], &M[i]);
				break;                                                                        
			case TYPE_dbl:                                                                    
				ARRAYseries_dbl(&bids[i], (dbl *)&sc->dim->strt, (dbl *)&sc->dim->step, (dbl *)&sc->dim->stop, &N[i], &M[i]);
				break;
			default:
				MTRL_CLEANUP();
				throw(MAL, "sciql.materialise", "Unsupported dimension data type");
			}

			if (!(bn = BATdescriptor(bids[i]))) {
				MTRL_CLEANUP();
				throw(MAL, "sciql.materialise", "Cannot access descriptor");
			}
		} else {
			ValRecord src, dst;
			int ret = 0;
			
			src.vtype = TYPE_str;
			if (sc->def) {
				int l = strlen(sc->def);
				if (l == 4 && (sc->def[0] == 'n' || sc->def[0] == 'N') &&
						(sc->def[1] == 'u' || sc->def[1] == 'U') &&
						(sc->def[2] == 'l' || sc->def[2] == 'L') &&
						(sc->def[3] == 'l' || sc->def[3] == 'L') ) {
					src.val.sval = NULL;
					src.len = 0;
				} else {
					src.val.sval = sc->def;
					src.len = l;
				}
			} else {
				src.val.sval = sc->def;
				src.len = 0;
			}
			dst.vtype = tpe;

			/* rely on VARconvert to convert a NULL str to the corresponding
			 * NIL value */
			ret = VARconvert(&dst, &src, 1);
			if (ret != GDK_SUCCEED) {
				MTRL_CLEANUP();	
				return _rethrow(MAL, "sciql.materialise", "string conversion failed");
			}

			switch(tpe) { /* TODO: check for overflow */
			case TYPE_bte:
				bn = BATconstant(tpe, &dst.val.btval, cntall);
				break;
			case TYPE_sht:
				bn = BATconstant(tpe, &dst.val.shval, cntall);
				break;
			case TYPE_int:
				bn = BATconstant(tpe, &dst.val.ival, cntall);
				break;
			case TYPE_lng:
				bn = BATconstant(tpe, &dst.val.lval, cntall);
				break;
			case TYPE_flt:
				bn = BATconstant(tpe, &dst.val.fval, cntall);
				break;
			case TYPE_dbl:
				bn = BATconstant(tpe, &dst.val.dval, cntall);
				break;
			case TYPE_str:
				bn = BATconstant(tpe, dst.val.sval, cntall);
				break;
			default:
				throw(MAL, "sciql.materialise", "Unsupported data type %s", ATOMname(tpe));
			}

			if (bn == NULL) {
				MTRL_CLEANUP();
				throw(MAL, "sciql.materialise", "Failed to create new constant BAT");
			}
			bids[i] = bn->batCacheid;
			BBPkeepref(bids[i]);
		}
		store_funcs.append_col(sql->session->tr, sc, bn, TYPE_bat);
	}

	/* update SQL catalog for this array */
	tr = sql->session->tr;	
	syss = find_sql_schema(tr, isGlobal(a)?"sys":"tmp");
	systable = find_sql_table(syss, "_tables");
	sysarray = find_sql_table(syss, "_arrays");
	/* find 'id' of this array in _tables */
	rid = table_funcs.column_find_row(tr, find_sql_column(systable, "name"), aname, NULL);
	tid = *(sqlid*) table_funcs.column_find_value(tr, find_sql_column(systable, "id"), rid);
	/* update value in _arrays */
	rid = table_funcs.column_find_row(tr, find_sql_column(sysarray, "table_id"), &tid, NULL);
	table_funcs.column_update_value(tr, find_sql_column(sysarray, "materialised"), rid, &materialised);	
	a->materialised = 1;

	return MAL_SUCCEED;
}

