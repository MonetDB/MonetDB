
#include "monetdb_config.h"
#include "sql.h"
#include "mal.h"
#include "mal_client.h"

#include "for.h"

static sql_column *
get_newcolumn(sql_trans *tr, sql_column *c)
{
	sql_table *t = find_sql_table_id(tr, c->t->s, c->t->base.id);
	if (t)
		return find_sql_column(t, c->base.name);
	return NULL;
}

static void
BATnegateprops(BAT *b)
{
	/* disable all properties here */
	b->tsorted = false;
	b->trevsorted = false;
	b->tnosorted = 0;
	b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
}

str
FORdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	bat *r = getArgReference_bat(stk, pci, 0);
	bat O = *getArgReference_bat(stk, pci, 1);
	int tt = getArgType(mb, pci, 2);

	if (
#ifdef HAVE_HGE
			tt != TYPE_hge &&
#endif
			tt != TYPE_lng && tt != TYPE_int)
		throw(SQL, "for.decompress", SQLSTATE(3F000) "for decompress: invalid offset type");

	BAT *o = BATdescriptor(O), *b = NULL;
	if (!o) {
		bat_destroy(o);
		throw(SQL, "for.decompress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (o->ttype != TYPE_bte && o->ttype != TYPE_sht) {
		bat_destroy(o);
		throw(SQL, "for.decompress", SQLSTATE(3F000) "for decompress: invalid type");
	}

	BUN cnt = BATcount(o);
	if (tt == TYPE_lng) {
		lng minval = *getArgReference_lng(stk, pci, 2);

		b = COLnew(o->hseqbase, TYPE_lng, cnt, PERSISTENT);
		if (!b) {
			bat_destroy(o);
			throw(SQL, "for.decompress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (o->ttype == TYPE_bte) {
			lng *ov = Tloc(b, 0);
			bte *iv = Tloc(o, 0);
			for(BUN i = 0; i<cnt; i++)
				ov[i] = minval + iv[i];
		} else {
			lng *ov = Tloc(b, 0);
			sht *iv = Tloc(o, 0);
			for(BUN i = 0; i<cnt; i++)
				ov[i] = minval + iv[i];
		}
	} else {
		bat_destroy(o);
		throw(SQL, "for.decompress", SQLSTATE(3F000) "offset type %s not yet implemented", ATOMname(tt));
	}
	bat_destroy(o);
	BATsetcount(b, cnt);
	BATnegateprops(b);
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

static str
FORcompress_intern(char **comp_min_val, BAT **r, BAT *b)
{
	BAT *o = NULL;
	char buf[64];
	int tt = b->ttype;
	ptr mn = NULL, mx = NULL;

	if (
#ifdef HAVE_HGE
			tt != TYPE_hge &&
#endif
			tt != TYPE_lng && tt != TYPE_int)
		throw(SQL, "for.compress", SQLSTATE(3F000) "for compress: invalid column type");

	/* For now we only handle hge, lng, and int -> sht and bte */
	if (!(mn = BATmin(NULL, b, NULL)))
		throw(SQL, "for.compress", GDK_EXCEPTION);
	if (!(mx = BATmax(NULL, b, NULL))) {
		GDKfree(mn);
		throw(SQL, "for.compress", GDK_EXCEPTION);
	}

	BUN cnt = BATcount(b);
	if (tt == TYPE_lng) {
		lng min_val = *(lng*)mn;
		lng max_val = *(lng*)mx;
		GDKfree(mn);
		GDKfree(mx);
		if ((max_val-min_val) > GDK_sht_max)
			throw(SQL, "for.compress", SQLSTATE(3F000) "for compress: too large value spread for 'for' compression");
		if ((max_val-min_val) < GDK_bte_max/2) {
			o = COLnew(b->hseqbase, TYPE_bte, cnt, PERSISTENT);
			if (!o)
				throw(SQL, "for.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			bte *ov = Tloc(o, 0);
			lng *iv = Tloc(b, 0);
			for(BUN i = 0; i<cnt; i++)
				ov[i] = (bte)(iv[i] - min_val);
		} else {
			o = COLnew(b->hseqbase, TYPE_sht, cnt, PERSISTENT);
			if (!o)
				throw(SQL, "for.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			sht *ov = Tloc(o, 0);
			lng *iv = Tloc(b, 0);
			for(BUN i = 0; i<cnt; i++)
				ov[i] = (sht)(iv[i] - min_val);
		}
		snprintf(buf, 64, "FOR-" LLFMT, min_val);
	} else {
		GDKfree(mn);
		GDKfree(mx);
		throw(SQL, "for.compress", SQLSTATE(3F000) "for compress: type %s not yet implemented", ATOMname(tt));
	}
	if (!(*comp_min_val = GDKstrdup(buf))) {
		bat_destroy(o);
		throw(SQL, "for.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATsetcount(o, cnt);
	BATnegateprops(o);
	*r = o;
	return NULL;
}

str
FORcompress_col(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
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
		throw(SQL, "for.compress", SQLSTATE(3F000) "for compress: invalid column name");
	if (strNil(sname))
		throw(SQL, "for.compress", SQLSTATE(42000) "Schema name cannot be NULL");
	if (strNil(tname))
		throw(SQL, "for.compress", SQLSTATE(42000) "Table name cannot be NULL");
	if (strNil(cname))
		throw(SQL, "for.compress", SQLSTATE(42000) "Column name cannot be NULL");
	if ((msg = getBackendContext(cntxt, &be)) != MAL_SUCCEED)
		return msg;
	tr = be->mvc->session->tr;

	sql_schema *s = find_sql_schema(tr, sname);
	if (!s)
		throw(SQL, "for.compress", SQLSTATE(3F000) "schema '%s' unknown", sname);
	sql_table *t = find_sql_table(tr, s, tname);
	if (!t)
		throw(SQL, "for.compress", SQLSTATE(3F000) "table '%s.%s' unknown", sname, tname);
	if (!isTable(t))
		throw(SQL, "for.compress", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	sql_column *c = find_sql_column(t, cname);
	if (!c)
		throw(SQL, "for.compress", SQLSTATE(3F000) "column '%s.%s.%s' unknown", sname, tname, cname);
	if (c->null)
		throw(SQL, "for.compress", SQLSTATE(3F000) "for compress: for 'for' compression column's cannot have NULL's");
	if (c->storage_type)
		throw(SQL, "for.compress", SQLSTATE(3F000) "column '%s.%s.%s' already compressed", sname, tname, cname);

	sqlstore *store = tr->store;
	BAT *b = store->storage_api.bind_col(tr, c, RDONLY), *o = NULL;
	if( b == NULL)
		throw(SQL,"for.compress", SQLSTATE(HY005) "Cannot access column descriptor");

	char *comp_min_val = NULL;
	msg = FORcompress_intern(&comp_min_val, &o, b);
	bat_destroy(b);
	if (msg == MAL_SUCCEED) {
		switch (sql_trans_alter_storage(tr, c, comp_min_val)) {
			case -1:
				msg = createException(SQL, "for.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			case -2:
			case -3:
				msg = createException(SQL, "for.compress", SQLSTATE(42000) "transaction conflict detected");
				break;
			default:
				break;
		}
		if (msg == MAL_SUCCEED && !(c = get_newcolumn(tr, c)))
			msg = createException(SQL, "for.compress", SQLSTATE(HY013) "alter_storage failed");
		if (msg == MAL_SUCCEED) {
			switch (store->storage_api.col_compress(tr, c, ST_FOR, o, NULL)) {
				case -1:
					msg = createException(SQL, "for.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					break;
				case -2:
				case -3:
					msg = createException(SQL, "for.compress", SQLSTATE(42000) "transaction conflict detected");
					break;
				default:
					break;
			}
		}
		GDKfree(comp_min_val);
		bat_destroy(o);
	}
	return msg;
}
