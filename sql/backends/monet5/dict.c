
#include "monetdb_config.h"
#include "sql.h"
#include "mal.h"
#include "mal_client.h"

#include "dict.h"

str
DICTcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	/* always assume one result */
	str msg = MAL_SUCCEED;
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *tname = *getArgReference_str(stk, pci, 2);
	const char *cname = *getArgReference_str(stk, pci, 3);
	backend *be = NULL;
	sql_trans *tr = NULL;

	if (!sname || !tname || !cname)
		throw(SQL, "sql.dict_compress", SQLSTATE(3F000) "dict compress: invalid column name");
	if ((msg = getBackendContext(cntxt, &be)) != MAL_SUCCEED)
		return msg;
	tr = be->mvc->session->tr;

	sql_schema *s = find_sql_schema(tr, sname);
	assert(s);
	sql_table *t = find_sql_table(tr, s, tname);
	assert(t);
	sql_column *c = find_sql_column(t, cname);
	assert(c);

	sqlstore *store = tr->store;
	BAT *b = store->storage_api.bind_col(tr, c, RDONLY);

	/* for now use all rows */
	BAT *u = BATunique(b, NULL);
	if (!u)
		throw(SQL, "sql.dict_compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BUN cnt = BATcount(u);
	/* create hash on u */
	int tt = (cnt<256)?TYPE_bte:(cnt<(64*1024))?TYPE_sht:TYPE_int;
	if (cnt > 2L*1024*1024*1024) {
		bat_destroy(u);
		bat_destroy(b);
		throw(SQL, "sql.dict_compress", SQLSTATE(3F000) "dict compress: too many values");
	}
	BAT *uv = BATproject(u, b); /* get values */
	bat_destroy(u);
	if (!uv) {
		bat_destroy(b);
		throw(SQL, "sql.dict_compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
    BAT *uu = COLcopy(uv, uv->ttype, true, PERSISTENT);
	if (!uu) {
		bat_destroy(uv);
		throw(SQL, "sql.dict_compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bat_destroy(uv);
	u = uu;

	BAT *o = COLnew(0, tt, BATcount(b), PERSISTENT);
	if (!o || BAThash(u) != GDK_SUCCEED) {
		bat_destroy(u);
		throw(SQL, "sql.dict_compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BUN p, q;
	BATiter bi = bat_iterator(b);
	BATiter ui = bat_iterator_nolock(u);
	if (tt == TYPE_bte) {
		bte *op = (bte*)Tloc(o, 0);
		BATloop(b, p, q) {
			BUN up = 0;
			HASHloop(ui, ui.b->thash, up, BUNtail(bi, p)) {
				op[p] = up;
			}
		}
		BATsetcount(o, BATcount(b));
		o->tsorted = (u->tsorted && b->tsorted);
		o->trevsorted = false;
		o->tnil = b->tnil;
		o->tnonil = b->tnonil;
		o->tkey = b->tkey;
		if (sql_trans_alter_storage(tr, c, "DICT") != LOG_OK || store->storage_api.col_dict(tr, c, o, u) != LOG_OK)
			throw(SQL, "sql.dict_compress", SQLSTATE(HY013) "alter_storage failed");
	} else if (tt == TYPE_sht) {
		sht *op = (sht*)Tloc(o, 0);
		BATloop(b, p, q) {
			BUN up = 0;
			HASHloop(ui, ui.b->thash, up, BUNtail(bi, p)) {
				op[p] = up;
			}
		}
		BATsetcount(o, BATcount(b));
		o->tsorted = (u->tsorted && b->tsorted);
		o->trevsorted = false;
		o->tnil = b->tnil;
		o->tnonil = b->tnonil;
		o->tkey = b->tkey;
		if (sql_trans_alter_storage(tr, c, "DICT") != LOG_OK || store->storage_api.col_dict(tr, c, o, u) != LOG_OK)
			throw(SQL, "sql.dict_compress", SQLSTATE(HY013) "alter_storage failed");
	} else {
		printf("implement int cases \n");
	}
	bat_iterator_end(&bi);
	bat_destroy(b);
	bat_destroy(u);
	bat_destroy(o);
	return MAL_SUCCEED;
}


str
DICTdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* b = project(o:bat[:bte], u) */
	/* b = project(o:bat[:sht], u) */
	/* b = project(o:bat[:int], u) */
	(void)cntxt;
	(void)mb;
	bat *r = getArgReference_bat(stk, pci, 0);
	bat O = *getArgReference_bat(stk, pci, 1);
	bat U = *getArgReference_bat(stk, pci, 2);

	BAT *o = BATdescriptor(O);
	BAT *u = BATdescriptor(U);
	if (!o || !u) {
		bat_destroy(o);
		bat_destroy(u);
		throw(SQL, "sql.dict_compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BAT *b = COLnew(0, u->ttype, BATcount(o), TRANSIENT);

	BUN p, q;
	BATiter oi = bat_iterator(o);
	BATiter ui = bat_iterator_nolock(u);
	//if (ATOMvarsized(u->ttype)) {
	if (o->ttype == TYPE_bte) {
		unsigned char *op = Tloc(o, 0);
		BATloop(o, p, q) {
			BUN up = op[p];
	        if (BUNappend(b, BUNtail(ui, up), false) != GDK_SUCCEED) {
				bat_iterator_end(&oi);
				bat_destroy(b);
				bat_destroy(o);
				bat_destroy(u);
				throw(SQL, "sql.dict_compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	} else if (o->ttype == TYPE_sht) {
		unsigned short *op = Tloc(o, 0);
		BATloop(o, p, q) {
			BUN up = op[p];
	        if (BUNappend(b, BUNtail(ui, up), false) != GDK_SUCCEED) {
				bat_iterator_end(&oi);
				bat_destroy(b);
				bat_destroy(o);
				bat_destroy(u);
				throw(SQL, "sql.dict_compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	} else if (o->ttype == TYPE_int) {
		assert(0);
	} else {
		bat_iterator_end(&oi);
		bat_destroy(b);
		bat_destroy(o);
		bat_destroy(u);
		throw(SQL, "sql.dict_compress", SQLSTATE(HY013) "unknown offset type");
	}
	bat_iterator_end(&oi);
	BBPkeepref(*r = b->batCacheid);
	bat_destroy(o);
	bat_destroy(u);
	return MAL_SUCCEED;
}

str
DICTconvert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* convert candidates into bte,sht,int offsets */
	(void)cntxt;
	bat *r = getArgReference_bat(stk, pci, 0);
	bat O = *getArgReference_bat(stk, pci, 1);
	int rt = getBatType(getArgType(mb, pci, 0));

	BAT *o = BATdescriptor(O);
	if (!o)
		throw(SQL, "sql.dict_compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BAT *b = COLnew(0, rt, BATcount(o), TRANSIENT);
	BATiter oi = bat_iterator(o);

	BUN p, q;
	if (rt == TYPE_bte) {
		unsigned char *rp = Tloc(b, 0);
		oid *op = Tloc(o, 0);
		BATloop(o, p, q) {
			rp[p] = op[p];
		}
	} else if (rt == TYPE_sht) {
		unsigned short *rp = Tloc(b, 0);
		oid *op = Tloc(o, 0);
		BATloop(o, p, q) {
			rp[p] = op[p];
		}
	} else {
		assert(0);
	}
	bat_iterator_end(&oi);
	BATsetcount(b, BATcount(o));
	/* TODO correct props and set min/max offset */
	BBPkeepref(*r = b->batCacheid);
	bat_destroy(o);
	return MAL_SUCCEED;
}
