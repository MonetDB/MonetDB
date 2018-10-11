/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * authors M Kersten, N Nes
 * SQL catalog support implementation
 * This module contains the wrappers around the SQL catalog operations
 */
#include "monetdb_config.h"
#include "sql_cat.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_scenario.h"
#include "sql_mvc.h"
#include "sql_qc.h"
#include "sql_partition.h"
#include "mal_namespace.h"
#include "opt_prelude.h"
#include "querylog.h"
#include "mal_builder.h"
#include "mal_debugger.h"

#include "rel_select.h"
#include "rel_optimizer.h"
#include "rel_prop.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_bin.h"
#include "rel_dump.h"
#include "rel_remote.h"
#include "orderidx.h"

#define initcontext() \
    if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)\
        return msg;\
    if ((msg = checkSQLContext(cntxt)) != NULL)\
        return msg;\
    if (STORE_READONLY)\
        throw(SQL,"sql.cat",SQLSTATE(25006) "Schema statements cannot be executed on a readonly database.");

static char *
SaveArgReference(MalStkPtr stk, InstrPtr pci, int arg)
{   
    char *val = *getArgReference_str(stk, pci, arg);
    
    if (val && strcmp(val, str_nil) == 0)
        val = NULL;
    return val;
}

static int
table_has_updates(sql_trans *tr, sql_table *t)
{
	node *n;
	int cnt = 0;

	for ( n = t->columns.set->h; !cnt && n; n = n->next) {
		sql_column *c = n->data;
		BAT *b = store_funcs.bind_col(tr, c, RD_UPD_ID);
		if ( b == 0)
			return -1;
		cnt |= BATcount(b) > 0;
		if (isTable(t) && t->access != TABLE_READONLY && (t->base.flag != TR_NEW /* alter */ ) &&
		    t->persistence == SQL_PERSIST && !t->commit_action)
			cnt |= store_funcs.count_col(tr, c, 0) > 0;
		BBPunfix(b->batCacheid);
	}
	return cnt;
}

static char *
rel_check_tables(sql_table *nt, sql_table *nnt, const char* errtable)
{
	node *n, *m, *nn, *mm;

	if (cs_size(&nt->columns) != cs_size(&nnt->columns))
		throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table doesn't match %s TABLE definition", errtable, errtable);
	for (n = nt->columns.set->h, m = nnt->columns.set->h; n && m; n = n->next, m = m->next) {
		sql_column *nc = n->data;
		sql_column *mc = m->data;

		if (subtype_cmp(&nc->type, &mc->type) != 0)
			throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table column type doesn't match %s TABLE definition", errtable, errtable);
		if(isRangePartitionTable(nt) || isListPartitionTable(nt)) {
			if (nc->null != mc->null)
				throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table column NULL check doesn't match %s TABLE definition", errtable, errtable);
			if ((!nc->def && mc->def) || (nc->def && !mc->def) || (nc->def && mc->def && strcmp(nc->def, mc->def) != 0))
				throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table column DEFAULT value doesn't match %s TABLE definition", errtable, errtable);
		}
	}
	if(isNonPartitionedTable(nt)) {
		if (cs_size(&nt->idxs) != cs_size(&nnt->idxs))
			throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table index doesn't match %s TABLE definition", errtable, errtable);
		if (cs_size(&nt->idxs))
			for (n = nt->idxs.set->h, m = nnt->idxs.set->h; n && m; n = n->next, m = m->next) {
				sql_idx *ni = n->data;
				sql_idx *mi = m->data;

				if (ni->type != mi->type)
					throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table index type doesn't match %s TABLE definition", errtable, errtable);
			}
	} else { //for partitioned tables we allow indexes but the key set must be exactly the same
		if (cs_size(&nt->keys) != cs_size(&nnt->keys))
			throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table key doesn't match %s TABLE definition", errtable, errtable);
		if (cs_size(&nt->keys))
			for (n = nt->keys.set->h, m = nnt->keys.set->h; n && m; n = n->next, m = m->next) {
				sql_key *ni = n->data;
				sql_key *mi = m->data;

				if (ni->type != mi->type)
					throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table key type doesn't match %s TABLE definition", errtable, errtable);
				if (list_length(ni->columns) != list_length(mi->columns))
					throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table key type doesn't match %s TABLE definition", errtable, errtable);
				for (nn = ni->columns->h, mm = mi->columns->h; nn && mm; nn = nn->next, mm = mm->next) {
					sql_kc *nni = nn->data;
					sql_kc *mmi = mm->data;

					if (nni->c->colnr != mmi->c->colnr)
						throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table key's columns doesn't match %s TABLE definition", errtable, errtable);
				}
			}
	}

	for(sql_table *up = nt->p ; up ; up = up->p) {
		if(!strcmp(up->s->base.name, nnt->s->base.name) && !strcmp(up->base.name, nnt->base.name))
			throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s TABLE: to be added table is a parent of the %s TABLE", errtable, errtable);
	}
	return MAL_SUCCEED;
}

static char*
validate_alter_table_add_table(mvc *sql, char* call, char *msname, char *mtname, char *psname, char *ptname,
							   sql_table **mt, sql_table **pt, int update, const char* errtable)
{
	sql_schema *ms = mvc_bind_schema(sql, msname), *ps = mvc_bind_schema(sql, psname);
	sql_table *rmt = NULL, *rpt = NULL;

	if (ms)
		rmt = mvc_bind_table(sql, ms, mtname);
	if (ps)
		rpt = mvc_bind_table(sql, ps, ptname);
	*mt = rmt;
	*pt = rpt;
	if (!update && rmt && (!isMergeTable(rmt) && !isReplicaTable(rmt)))
		throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: cannot add table '%s.%s' to table '%s.%s'", psname, ptname, msname, mtname);
	if (rmt && rpt) {
		char *msg;
		node *n = cs_find_id(&rmt->members, rpt->base.id);

		if (n && !update)
			throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: table '%s.%s' is already part of the MERGE TABLE '%s.%s'", psname, ptname, msname, mtname);
		if(!n && update)
			throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: table '%s.%s' isn't part of the MERGE TABLE '%s.%s'", psname, ptname, msname, mtname);
		if ((msg = rel_check_tables(rmt, rpt, errtable)) != NULL)
			return msg;
		return MAL_SUCCEED;
	} else if (rmt) {
		throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", ptname, psname);
	} else {
		throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", mtname, msname);
	}
}

static char *
alter_table_add_table(mvc *sql, char *msname, char *mtname, char *psname, char *ptname)
{
	sql_table *mt = NULL, *pt = NULL;
	str msg = validate_alter_table_add_table(sql, "sql.alter_table_add_table", msname, mtname, psname, ptname, &mt, &pt, 0, "MERGE");

	if(msg == MAL_SUCCEED)
		sql_trans_add_table(sql->session->tr, mt, pt);

	return msg;
}

static char *
alter_table_add_range_partition(mvc *sql, char *msname, char *mtname, char *psname, char *ptname, ptr min, ptr max,
								int with_nills, int update)
{
	sql_table *mt = NULL, *pt = NULL;
	sql_part *err = NULL;
	str msg = MAL_SUCCEED, err_min = NULL, err_max = NULL, conflict_err_min = NULL, conflict_err_max = NULL;
	int tp1 = 0, errcode = 0, min_null = 0, max_null = 0;
	size_t length = 0;
	ssize_t (*atomtostr)(str *, size_t *, const void *);
	sql_subtype tpe;

	if((msg = validate_alter_table_add_table(sql, "sql.alter_table_add_range_partition", msname, mtname, psname, ptname,
											 &mt, &pt, update, "RANGE PARTITION"))) {
		return msg;
	} else if(!isRangePartitionTable(mt)) {
		msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
									"ALTER TABLE: cannot add range partition into a %s table",
									(mt->type == tt_merge_table)?"merge":"list partition");
		goto finish;
	} else if(!update && pt->p) {
		msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
							  "ALTER TABLE: table %s.%s is already part of another range partition table",
							  psname, ptname);
		goto finish;
	}

	find_partition_type(&tpe, mt);
	tp1 = tpe.type->localtype;
	min_null = ATOMcmp(tp1, min, ATOMnilptr(tp1)) == 0;
	max_null = ATOMcmp(tp1, max, ATOMnilptr(tp1)) == 0;
	if(min_null || max_null)
		with_nills = 1;

	if(!max_null && min_null) {
		msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000) "ALTER TABLE: range bound cannot be null");
		goto finish;
	} else if(!min_null && max_null) {
		msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000) "ALTER TABLE: range bound cannot be null");
		goto finish;
	} else if(!with_nills && ATOMcmp(tp1, min, max) > 0) {
		msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000) "ALTER TABLE: minimum value is higher than maximum value");
		goto finish;
	}

	errcode = sql_trans_add_range_partition(sql->session->tr, mt, pt, tpe, min, max, with_nills, update, &err);
	switch(errcode) {
		case 0:
			break;
		case -1:
			msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY001) MAL_MALLOC_FAIL);
			break;
		case -2:
			msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
									"ALTER TABLE: minimum value length is higher than %d", STORAGE_MAX_VALUE_LENGTH);
			break;
		case -3:
			msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
									"ALTER TABLE: maximum value length is higher than %d", STORAGE_MAX_VALUE_LENGTH);
			break;
		case -4:
			assert(err);
			if(with_nills && err->with_nills) {
				msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
										"ALTER TABLE: conflicting partitions: table %s.%s stores null values and only "
										"one partition can store null values at the time", err->t->s->base.name, err->t->base.name);
			} else {
				atomtostr = BATatoms[tp1].atomToStr;
				if(atomtostr(&conflict_err_min, &length, err->part.range.minvalue) < 0) {
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY001) MAL_MALLOC_FAIL);
				} else if(atomtostr(&conflict_err_max, &length, err->part.range.maxvalue) < 0) {
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY001) MAL_MALLOC_FAIL);
				} else if(atomtostr(&err_min, &length, min) < 0) {
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY001) MAL_MALLOC_FAIL);
				} else if(atomtostr(&err_max, &length, max) < 0) {
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY001) MAL_MALLOC_FAIL);
				} else {
					sql_table *errt = mvc_bind_table(sql, mt->s, err->base.name);
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
									  "ALTER TABLE: conflicting partitions: %s to %s and %s to %s from table %s.%s",
									  err_min, err_max, conflict_err_min, conflict_err_max, errt->s->base.name, errt->base.name);
				}
			}
			break;
		default:
			assert(0);
	}

finish:
	if(err_min)
		GDKfree(err_min);
	if(err_max)
		GDKfree(err_max);
	if(conflict_err_min)
		GDKfree(conflict_err_min);
	if(conflict_err_max)
		GDKfree(conflict_err_max);
	if(msg != MAL_SUCCEED)
		pt->p = NULL;
	return msg;
}

static char *
alter_table_add_value_partition(mvc *sql, MalStkPtr stk, InstrPtr pci, char *msname, char *mtname, char *psname,
								char *ptname, int with_nills, int update)
{
	sql_table *mt = NULL, *pt = NULL;
	str msg = MAL_SUCCEED;
	sql_part *err = NULL;
	int errcode = 0, i = 0, ninserts = 0;
	list *values = list_new(sql->session->tr->sa, (fdestroy) NULL);
	sql_subtype tpe;

	if((msg = validate_alter_table_add_table(sql, "sql.alter_table_add_value_partition", msname, mtname, psname, ptname,
											 &mt, &pt, update, "LIST PARTITION"))) {
		return msg;
	} else if(!isListPartitionTable(mt)) {
		msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
									"ALTER TABLE: cannot add value partition into a %s table",
									(mt->type == tt_merge_table)?"merge":"range partition");
		goto finish;
	} else if(!update && pt->p) {
		msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
							  "ALTER TABLE: table %s.%s is already part of another list partition table",
							  psname, ptname);
		goto finish;
	}

	find_partition_type(&tpe, mt);
	ninserts = pci->argc - pci->retc - 6;
	if(ninserts <= 0 && !with_nills) {
		msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000) "ALTER TABLE: no values in the list");
		goto finish;
	}
	for( i = pci->retc+6; i < pci->argc; i++){
		sql_part_value *nextv = NULL;
		ValRecord *vnext = &(stk)->stk[(pci)->argv[i]];
		ptr pnext = VALget(vnext);
		size_t len = ATOMlen(vnext->vtype, pnext);

		if(VALisnil(vnext)) { /* check for an eventual null value which cannot be */
			msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
																			"ALTER TABLE: list value cannot be null");
			goto finish;
		}

		nextv = SA_ZNEW(sql->session->tr->sa, sql_part_value); /* instantiate the part value */
		nextv->tpe = tpe;
		nextv->value = sa_alloc(sql->session->tr->sa, len);
		memcpy(nextv->value, pnext, len);
		nextv->length = len;

		if(list_append_sorted(values, nextv, sql_values_list_element_validate_and_insert) != NULL) {
			msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
									"ALTER TABLE: there are duplicated values in the list");
			goto finish;
		}
	}

	errcode = sql_trans_add_value_partition(sql->session->tr, mt, pt, tpe, values, with_nills, update, &err);
	switch(errcode) {
		case 0:
			break;
		case -1:
			msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
									"ALTER TABLE: the new partition is conflicting with the existing partition %s.%s",
									err->t->s->base.name, err->t->base.name);
			break;
		default:
			msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
									"ALTER TABLE: value at position %d length is higher than %d",
									(errcode * -1) - 1, STORAGE_MAX_VALUE_LENGTH);
			break;
	}

finish:
	if(msg != MAL_SUCCEED)
		pt->p = NULL;
	return msg;
}

static char *
alter_table_del_table(mvc *sql, char *msname, char *mtname, char *psname, char *ptname, int drop_action)
{
	sql_schema *ms = mvc_bind_schema(sql, msname), *ps = mvc_bind_schema(sql, psname);
	sql_table *mt = NULL, *pt = NULL;

	if (ms)
		mt = mvc_bind_table(sql, ms, mtname);
	if (ps)
		pt = mvc_bind_table(sql, ps, ptname);
	if (mt && pt) {
		node *n = NULL;

		if (!pt || (n = cs_find_id(&mt->members, pt->base.id)) == NULL)
			throw(SQL,"sql.alter_table_del_table",SQLSTATE(42S02) "ALTER TABLE: table '%s.%s' isn't part of the MERGE TABLE '%s.%s'", psname, ptname, msname, mtname);

		sql_trans_del_table(sql->session->tr, mt, pt, drop_action);
	} else if (mt) {
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", ptname, psname);
	} else {
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", mtname, msname);
	}
	return MAL_SUCCEED;
}

static char *
alter_table_set_access(mvc *sql, char *sname, char *tname, int access)
{
	sql_schema *s = mvc_bind_schema(sql, sname);
	sql_table *t = NULL;

	if (s)
		t = mvc_bind_table(sql, s, tname);
	if (t) {
		if (isMergeTable(t))
			throw(SQL,"sql.alter_table_set_access",SQLSTATE(42S02) "ALTER TABLE: read only MERGE TABLES are not supported");
		if (t->access != access) {
			if (access && table_has_updates(sql->session->tr, t))
				throw(SQL,"sql.alter_table_set_access",SQLSTATE(40000) "ALTER TABLE: set READ or INSERT ONLY not possible with outstanding updates (wait until updates are flushed)\n");

			mvc_access(sql, t, access);
		}
	} else {
		throw(SQL,"sql.alter_table_set_access",SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", tname, sname);
	}
	return MAL_SUCCEED;
}

static char *
create_trigger(mvc *sql, char *sname, char *tname, char *triggername, int time, int orientation, int event, char *old_name, char *new_name, char *condition, char *query)
{
	sql_trigger *tri = NULL;
	sql_schema *s = NULL;
	sql_table *t;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.create_trigger",SQLSTATE(3F000) "CREATE TRIGGER: no such schema '%s'", sname);
	if (!s)
		s = cur_schema(sql);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.create_trigger",SQLSTATE(3F000) "CREATE TRIGGER: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	if (mvc_bind_trigger(sql, s, triggername) != NULL)
		throw(SQL,"sql.create_trigger",SQLSTATE(3F000) "CREATE TRIGGER: name '%s' already in use", triggername);

	if (!(t = mvc_bind_table(sql, s, tname)))
		throw(SQL,"sql.create_trigger",SQLSTATE(3F000) "CREATE TRIGGER: unknown table '%s'", tname);

	if (isView(t))
		throw(SQL,"sql.create_trigger",SQLSTATE(3F000) "CREATE TRIGGER: cannot create trigger on view '%s'", tname);

	tri = mvc_create_trigger(sql, t, triggername, time, orientation, event, old_name, new_name, condition, query);
	if (tri) {
		char *buf;
		sql_rel *r = NULL;
		sql_allocator *sa = sql->sa;

		sql->sa = sa_create();
		if(!sql->sa)
			throw(SQL, "sql.catalog",SQLSTATE(HY001) MAL_MALLOC_FAIL);
		buf = sa_strdup(sql->sa, query);
		if(!buf)
			throw(SQL, "sql.catalog",SQLSTATE(HY001) MAL_MALLOC_FAIL);
		r = rel_parse(sql, s, buf, m_deps);
		if (r)
			r = rel_optimizer(sql, r, 0);
		if (r) {
			list *id_l = rel_dependencies(sql, r);

			mvc_create_dependencies(sql, id_l, tri->base.id, TRIGGER_DEPENDENCY);
		}
		sa_destroy(sql->sa);
		sql->sa = sa;
	}
	return MAL_SUCCEED;
}

static char *
drop_trigger(mvc *sql, char *sname, char *tname, int if_exists)
{
	sql_trigger *tri = NULL;
	sql_schema *s = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.drop_trigger",SQLSTATE(3F000) "DROP TRIGGER: no such schema '%s'", sname);
	if (!s)
		s = cur_schema(sql);
	assert(s);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.drop_trigger",SQLSTATE(3F000) "DROP TRIGGER: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);

	if ((tri = mvc_bind_trigger(sql, s, tname)) == NULL) {
		if(if_exists)
			return MAL_SUCCEED;
		throw(SQL,"sql.drop_trigger", SQLSTATE(3F000) "DROP TRIGGER: unknown trigger %s\n", tname);
	}
	if(mvc_drop_trigger(sql, s, tri))
		throw(SQL,"sql.drop_trigger", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static char *
drop_table(mvc *sql, char *sname, char *tname, int drop_action, int if_exists)
{
	sql_schema *s = NULL;
	sql_table *t = NULL;
	node *n;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.drop_table",SQLSTATE(3F000) "DROP TABLE: no such schema '%s'", sname);
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) {
		s = tmp_schema(sql);
		t = mvc_bind_table(sql, s, tname);
	}
	if (!t) {
		if (if_exists)
			return MAL_SUCCEED;
		throw(SQL,"sql.droptable", SQLSTATE(42S02) "DROP TABLE: no such table '%s'", tname);
	} else if (isView(t)) {
		throw(SQL,"sql.droptable", SQLSTATE(42000) "DROP TABLE: cannot drop VIEW '%s'", tname);
	} else if (t->system) {
		throw(SQL,"sql.droptable", SQLSTATE(42000) "DROP TABLE: cannot drop system table '%s'", tname);
	} else if (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && t->persistence == SQL_LOCAL_TEMP)) {
		throw(SQL,"sql.droptable",SQLSTATE(42000) "DROP TABLE: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	}
	if (!drop_action && t->keys.set) {
		for (n = t->keys.set->h; n; n = n->next) {
			sql_key *k = n->data;

			if (k->type == ukey || k->type == pkey) {
				sql_ukey *uk = (sql_ukey *) k;

				if (uk->keys && list_length(uk->keys)) {
					node *l = uk->keys->h;

					for (; l; l = l->next) {
						k = l->data;
						/* make sure it is not a self referencing key */
						if (k->t != t)
							throw(SQL,"sql.droptable", SQLSTATE(40000) "DROP TABLE: FOREIGN KEY %s.%s depends on %s", k->t->base.name, k->base.name, tname);
					}
				}
			}
		}
	}

	if (!drop_action && mvc_check_dependency(sql, t->base.id, TABLE_DEPENDENCY, NULL))
		throw (SQL,"sql.droptable",SQLSTATE(42000) "DROP TABLE: unable to drop table %s (there are database objects which depend on it)\n", t->base.name);

	return mvc_drop_table(sql, s, t, drop_action);
}

static char *
drop_view(mvc *sql, char *sname, char *tname, int drop_action, int if_exists)
{
	sql_table *t = NULL;
	sql_schema *ss = NULL;

	if (sname != NULL && (ss = mvc_bind_schema(sql, sname)) == NULL)
		throw(SQL,"sql.dropview", SQLSTATE(3F000) "DROP VIEW: no such schema '%s'", sname);

	if (ss == NULL)
		ss = cur_schema(sql);

	t = mvc_bind_table(sql, ss, tname);
	if (!mvc_schema_privs(sql, ss) && !(isTempSchema(ss) && t && t->persistence == SQL_LOCAL_TEMP)) {
		throw(SQL,"sql.dropview", SQLSTATE(42000) "DROP VIEW: access denied for %s to schema '%s'", stack_get_string(sql, "current_user"), ss->base.name);
	} else if (!t) {
		if(if_exists){
			return MAL_SUCCEED;
		}
		throw(SQL,"sql.drop_view",SQLSTATE(42S02) "DROP VIEW: unknown view '%s'", tname);
	} else if (!isView(t)) {
		throw(SQL,"sql.drop_view", SQLSTATE(42000) "DROP VIEW: unable to drop view '%s': is a table", tname);
	} else if (t->system) {
		throw(SQL,"sql.drop_view", SQLSTATE(42000) "DROP VIEW: cannot drop system view '%s'", tname);
	} else if (!drop_action && mvc_check_dependency(sql, t->base.id, VIEW_DEPENDENCY, NULL)) {
		throw(SQL,"sql.drop_view", SQLSTATE(42000) "DROP VIEW: cannot drop view '%s', there are database objects which depend on it", t->base.name);
	}
	return mvc_drop_table(sql, ss, t, drop_action);
}

static str
drop_key(mvc *sql, char *sname, char *kname, int drop_action)
{
	sql_key *key;
	sql_schema *ss = NULL;

	if (sname != NULL && (ss = mvc_bind_schema(sql, sname)) == NULL)
		throw(SQL,"drop_key", SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", sname);

	if (ss == NULL)
		ss = cur_schema(sql);

	if ((key = mvc_bind_key(sql, ss, kname)) == NULL)
		throw(SQL,"sql.drop_key", SQLSTATE(42000) "ALTER TABLE: no such constraint '%s'", kname);
	if (!drop_action && mvc_check_dependency(sql, key->base.id, KEY_DEPENDENCY, NULL))
		throw(SQL,"sql.drop_key", SQLSTATE(42000) "ALTER TABLE: cannot drop constraint '%s': there are database objects which depend on it", key->base.name);
	if(mvc_drop_key(sql, ss, key, drop_action))
		throw(SQL,"sql.drop_key", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
drop_index(Client cntxt, mvc *sql, char *sname, char *iname)
{
	sql_schema *s = NULL;
	sql_idx *i = NULL;

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.drop_index", SQLSTATE(3F000) "DROP INDEX: no such schema '%s'", sname);
	i = mvc_bind_idx(sql, s, iname);
	if (!i) {
		throw(SQL,"sql.drop_index", SQLSTATE(42S12) "DROP INDEX: no such index '%s'", iname);
	} else if (!mvc_schema_privs(sql, s)) {
		throw(SQL,"sql.drop_index", SQLSTATE(42000) "DROP INDEX: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else {
		if (i->type == ordered_idx) {
			sql_kc *ic = i->columns->h->data;
			BAT *b = mvc_bind(sql, s->base.name, ic->c->t->base.name, ic->c->base.name, 0);
			if(b) {
				OIDXdropImplementation(cntxt, b);
				BBPunfix(b->batCacheid);
			}
		}
		if (i->type == imprints_idx) {
			sql_kc *ic = i->columns->h->data;
			BAT *b = mvc_bind(sql, s->base.name, ic->c->t->base.name, ic->c->base.name, 0);
			if(b) {
				IMPSdestroy(b);
				BBPunfix(b->batCacheid);
			}
		}
		if(mvc_drop_idx(sql, s, i))
			throw(SQL,"sql.drop_index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	return NULL;
}

static str
create_seq(mvc *sql, char *sname, char *seqname, sql_sequence *seq)
{
	sql_schema *s = NULL;

	(void)seqname;
	if (sname && !(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.create_seq", SQLSTATE(3F000) "CREATE SEQUENCE: no such schema '%s'", sname);
	if (s == NULL)
		s = cur_schema(sql);
	if (find_sql_sequence(s, seq->base.name)) {
		throw(SQL,"sql.create_seq", SQLSTATE(42000) "CREATE SEQUENCE: name '%s' already in use", seq->base.name);
	} else if (!mvc_schema_privs(sql, s)) {
		throw(SQL,"sql.create_seq", SQLSTATE(42000) "CREATE SEQUENCE: insufficient privileges for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}
	sql_trans_create_sequence(sql->session->tr, s, seq->base.name, seq->start, seq->minvalue, seq->maxvalue, seq->increment, seq->cacheinc, seq->cycle, seq->bedropped);
	return NULL;
}

static str
alter_seq(mvc *sql, char *sname, char *seqname, sql_sequence *seq, lng *val)
{
	sql_schema *s = NULL;
	sql_sequence *nseq = NULL;

	(void)seqname;
	if (sname && !(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.alter_seq", SQLSTATE(3F000) "ALTER SEQUENCE: no such schema '%s'", sname);
	if (s == NULL)
		s = cur_schema(sql);
	if (!(nseq = find_sql_sequence(s, seq->base.name))) {
		throw(SQL,"sql.alter_seq", SQLSTATE(42000) "ALTER SEQUENCE: no such sequence '%s'", seq->base.name);
	} else if (!mvc_schema_privs(sql, s)) {
		throw(SQL,"sql.alter_seq", SQLSTATE(42000) "ALTER SEQUENCE: insufficient privileges for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}

	/* first alter the known values */
	sql_trans_alter_sequence(sql->session->tr, nseq, seq->minvalue, seq->maxvalue, seq->increment, seq->cacheinc, seq->cycle);
	if (val)
		sql_trans_sequence_restart(sql->session->tr, nseq, *val);
	return MAL_SUCCEED;
}

static str
drop_seq(mvc *sql, char *sname, char *name)
{
	sql_schema *s = NULL;
	sql_sequence *seq = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.drop_seq", SQLSTATE(3F000) "DROP SEQUENCE: no such schema '%s'", sname);
	if (!s)
		s = cur_schema(sql);
	if (!(seq = find_sql_sequence(s, name))) {
		throw(SQL,"sql.drop_seq", SQLSTATE(42M35) "DROP SEQUENCE: no such sequence '%s'", name);
	} else if (!mvc_schema_privs(sql, s)) {
		throw(SQL,"sql.drop_seq", SQLSTATE(42000) "DROP SEQUENCE: insufficient privileges for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}
	if (mvc_check_dependency(sql, seq->base.id, BEDROPPED_DEPENDENCY, NULL))
		throw(SQL,"sql.drop_seq", SQLSTATE(2B000) "DROP SEQUENCE: unable to drop sequence %s (there are database objects which depend on it)\n", seq->base.name);

	sql_trans_drop_sequence(sql->session->tr, s, seq, 0);
	return NULL;
}

static str
drop_func(mvc *sql, char *sname, char *name, int fid, int type, int action)
{
	sql_schema *s = NULL;
	char is_aggr = (type == F_AGGR);
	char is_func = (type != F_PROC);
	char *F = is_aggr ? "AGGREGATE" : (is_func ? "FUNCTION" : "PROCEDURE");
	char *f = is_aggr ? "aggregate" : (is_func ? "function" : "procedure");
	char *KF = type == F_FILT ? "FILTER " : type == F_UNION ? "UNION " : "";
	char *kf = type == F_FILT ? "filter " : type == F_UNION ? "union " : "";

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.drop_func", SQLSTATE(3F000) "DROP %s%s: no such schema '%s'", KF, F, sname);
	if (!s)
		s = cur_schema(sql);
	if (fid >= 0) {
		node *n = find_sql_func_node(s, fid);
		if (n) {
			sql_func *func = n->data;

			if (!mvc_schema_privs(sql, s)) {
				throw(SQL,"sql.drop_func", SQLSTATE(42000) "DROP %s%s: access denied for %s to schema ;'%s'", KF, F, stack_get_string(sql, "current_user"), s->base.name);
			}
			if (!action && mvc_check_dependency(sql, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY, NULL))
				throw(SQL,"sql.drop_func", SQLSTATE(42000) "DROP %s%s: there are database objects dependent on %s%s %s;", KF, F, kf, f, func->base.name);

			if(mvc_drop_func(sql, s, func, action))
				throw(SQL,"sql.drop_func", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	} else if(fid == -2) { //if exists option
		return MAL_SUCCEED;
	} else { //fid == -1
		node *n = NULL;
		list *list_func = schema_bind_func(sql, s, name, type);
		int res;

		if (!mvc_schema_privs(sql, s)) {
			list_destroy(list_func);
			throw(SQL,"sql.drop_func", SQLSTATE(42000) "DROP %s%s: access denied for %s to schema ;'%s'", KF, F, stack_get_string(sql, "current_user"), s->base.name);
		}
		for (n = list_func->h; n; n = n->next) {
			sql_func *func = n->data;

			if (!action && mvc_check_dependency(sql, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY, list_func)) {
				list_destroy(list_func);
				throw(SQL,"sql.drop_func", SQLSTATE(42000) "DROP %s%s: there are database objects dependent on %s%s %s;", KF, F, kf, f, func->base.name);
			}
		}
		res = mvc_drop_all_func(sql, s, list_func, action);
		list_destroy(list_func);
		if(res)
			throw(SQL,"sql.drop_func", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

static char *
create_func(mvc *sql, char *sname, char *fname, sql_func *f)
{
	sql_func *nf;
	sql_schema *s = NULL;
	char is_aggr = (f->type == F_AGGR);
	char is_func = (f->type != F_PROC);
	char *F = is_aggr ? "AGGREGATE" : (is_func ? "FUNCTION" : "PROCEDURE");
	char *KF = f->type == F_FILT ? "FILTER " : f->type == F_UNION ? "UNION " : "";

	(void)fname;
	if (sname && !(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.create_func", SQLSTATE(3F000) "CREATE %s%s: no such schema '%s'", KF, F, sname);
	if (!s)
		s = cur_schema(sql);
	nf = mvc_create_func(sql, NULL, s, f->base.name, f->ops, f->res, f->type, f->lang, f->mod, f->imp, f->query, f->varres, f->vararg, f->system);
	if (nf && nf->query && nf->lang <= FUNC_LANG_SQL) {
		char *buf;
		sql_rel *r = NULL;
		sql_allocator *sa = sql->sa;

		sql->sa = sa_create();
		if(!sql->sa)
			throw(SQL, "sql.catalog",SQLSTATE(HY001) MAL_MALLOC_FAIL);
		buf = sa_strdup(sql->sa, nf->query);
		if(!buf)
			throw(SQL, "sql.catalog",SQLSTATE(HY001) MAL_MALLOC_FAIL);
		r = rel_parse(sql, s, buf, m_deps);
		if (r)
			r = rel_optimizer(sql, r, 0);
		if (r) {
			node *n;
			list *id_l = rel_dependencies(sql, r);

			if (!f->vararg && f->ops) {
				for (n = f->ops->h; n; n = n->next) {
					sql_arg *a = n->data;

					if (a->type.type->s) 
						mvc_create_dependency(sql, a->type.type->base.id, nf->base.id, TYPE_DEPENDENCY);
				}
			}
			if (!f->varres && f->res) {
				for (n = f->res->h; n; n = n->next) {
					sql_arg *a = n->data;

					if (a->type.type->s) 
						mvc_create_dependency(sql, a->type.type->base.id, nf->base.id, TYPE_DEPENDENCY);
				}
			}
			mvc_create_dependencies(sql, id_l, nf->base.id, !IS_PROC(f) ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
		}
		sa_destroy(sql->sa);
		sql->sa = sa;
	} else if (nf->lang == FUNC_LANG_MAL) {
		if (!backend_resolve_function(sql, nf))
			throw(SQL,"sql.create_func", SQLSTATE(3F000) "CREATE %s%s: external name %s.%s not bound", KF, F, nf->mod, nf->base.name);
	}
	return MAL_SUCCEED;
}

static str
alter_table(Client cntxt, mvc *sql, char *sname, sql_table *t)
{
	sql_schema *s = mvc_bind_schema(sql, sname);
	sql_table *nt = NULL;
	node *n;

	if (!s)
		throw(SQL,"sql.alter_table", SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", sname);

	if ((nt = mvc_bind_table(sql, s, t->base.name)) == NULL) {
		throw(SQL,"sql.alter_table", SQLSTATE(42S02) "ALTER TABLE: no such table '%s'", t->base.name);
	} else if (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && t->persistence == SQL_LOCAL_TEMP)) {
		throw(SQL,"sql.alter_table", SQLSTATE(42000) "ALTER TABLE: insufficient privileges for user '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}

	/* First check if all the changes are allowed */
	if (t->idxs.set) {
		/* only one pkey */
		if (nt->pkey) {
			for (n = t->idxs.nelm; n; n = n->next) {
				sql_idx *i = n->data;
				if (i->key && i->key->type == pkey)
					throw(SQL,"sql.alter_table", SQLSTATE(40000) "CONSTRAINT PRIMARY KEY: a table can have only one PRIMARY KEY\n");
			}
		}
	}

	/* check for changes */
	if (t->columns.dset)
		for (n = t->columns.dset->h; n; n = n->next) {
			/* propagate alter table .. drop column */
			sql_column *c = n->data;
			sql_column *nc = mvc_bind_column(sql, nt, c->base.name);
			if(mvc_drop_column(sql, nt, nc, c->drop_action))
				throw(SQL,"sql.alter_table", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	/* check for changes on current cols */
	for (n = t->columns.set->h; n != t->columns.nelm; n = n->next) {

		/* null or default value changes */
		sql_column *c = n->data;
		sql_column *nc = mvc_bind_column(sql, nt, c->base.name);

		if (c->null != nc->null && isTable(nt)) {
			if (c->null && nt->pkey) { /* check for primary keys based on this column */
				node *m;
				for(m = nt->pkey->k.columns->h; m; m = m->next) {
					sql_kc *kc = m->data;

					if (kc->c->base.id == c->base.id)
						throw(SQL,"sql.alter_table", SQLSTATE(40000) "NOT NULL CONSTRAINT: cannot change NOT NULL CONSTRAINT for column '%s' as its part of the PRIMARY KEY\n", c->base.name);
				}
			}
			mvc_null(sql, nc, c->null);
			/* for non empty check for nulls */
			if (c->null == 0) {
				const void *nilptr = ATOMnilptr(c->type.type->localtype);
				rids *nils = table_funcs.rids_select(sql->session->tr, nc, nilptr, NULL, NULL);
				int has_nils = !is_oid_nil(table_funcs.rids_next(nils));

				table_funcs.rids_destroy(nils);
				if (has_nils)
					throw(SQL,"sql.alter_table", SQLSTATE(40002) "ALTER TABLE: NOT NULL constraint violated for column %s.%s", c->t->base.name, c->base.name);
			}
		}
		if (c->def != nc->def)
			mvc_default(sql, nc, c->def);

		if (c->storage_type != nc->storage_type) {
			if (c->t->access == TABLE_WRITABLE)
				throw(SQL,"sql.alter_table", SQLSTATE(40002) "ALTER TABLE: SET STORAGE for column %s.%s only allowed on READ or INSERT ONLY tables", c->t->base.name, c->base.name);
			nc->base.rtime = nc->base.wtime = sql->session->tr->wtime;
			mvc_storage(sql, nc, c->storage_type);
		}
	}
	for (; n; n = n->next) {
		/* propagate alter table .. add column */
		sql_column *c = n->data;
		mvc_copy_column(sql, nt, c);
	}
	if (t->idxs.set) {
		/* alter drop index */
		if (t->idxs.dset)
			for (n = t->idxs.dset->h; n; n = n->next) {
				sql_idx *i = n->data;
				sql_idx *ni = mvc_bind_idx(sql, s, i->base.name);
				if(mvc_drop_idx(sql, s, ni))
					throw(SQL,"sql.alter_table", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
		/* alter add index */
		for (n = t->idxs.nelm; n; n = n->next) {
			sql_idx *i = n->data;

			if (i->type == ordered_idx) {
				sql_kc *ic = i->columns->h->data;
				BAT *b = mvc_bind(sql, nt->s->base.name, nt->base.name, ic->c->base.name, 0);
				char *msg = OIDXcreateImplementation(cntxt, newBatType(b->ttype), b, -1);
				BBPunfix(b->batCacheid);
				if (msg != MAL_SUCCEED) {
					char *smsg = createException(SQL,"sql.alter_table", SQLSTATE(40002) "CREATE ORDERED INDEX: %s", msg);
					freeException(msg);
					return smsg;
				}
			}
			if (i->type == imprints_idx) {
				gdk_return r;
				sql_kc *ic = i->columns->h->data;
				BAT *b = mvc_bind(sql, nt->s->base.name, nt->base.name, ic->c->base.name, 0);
				r = BATimprints(b);
				BBPunfix(b->batCacheid);
				if (r != GDK_SUCCEED)
					throw(SQL, "sql.alter_table", GDK_EXCEPTION);
			}
			mvc_copy_idx(sql, nt, i);
		}
	}
	if (t->keys.set) {
		/* alter drop key */
		if (t->keys.dset)
			for (n = t->keys.dset->h; n; n = n->next) {
				sql_key *k = n->data;
				sql_key *nk = mvc_bind_key(sql, s, k->base.name);
				if (nk) {
					if(mvc_drop_key(sql, s, nk, k->drop_action))
						throw(SQL,"sql.alter_table", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
			}
		/* alter add key */
		for (n = t->keys.nelm; n; n = n->next) {
			sql_key *k = n->data;
			str err;
			if((err = sql_partition_validate_key(sql, t, k, "ALTER")))
				return err;
			mvc_copy_key(sql, nt, k);
		}
	}
	return MAL_SUCCEED;
}

/* the MAL wrappers */
str
SQLcreate_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	str seqname = *getArgReference_str(stk, pci, 2); 
	sql_sequence *s = *(sql_sequence **) getArgReference(stk, pci, 3);

	initcontext();
	msg = create_seq(sql, sname, seqname, s);
	return msg;
}

str
SQLalter_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	str sname = *getArgReference_str(stk, pci, 1); 
	str seqname = *getArgReference_str(stk, pci, 2); 
	sql_sequence *s = *(sql_sequence **) getArgReference(stk, pci, 3);
	lng *val = NULL;

	initcontext();
	if (getArgType(mb, pci, 4) == TYPE_lng)
		val = getArgReference_lng(stk, pci, 4);
	if (val == NULL || is_lng_nil(*val))
		msg = createException(SQL,"sql.alter_seq", SQLSTATE(42M36) "ALTER SEQUENCE: cannot (re)start with NULL");
	else
		msg = alter_seq(sql, sname, seqname, s, val);
	return msg;
}

str
SQLdrop_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	str sname = *getArgReference_str(stk, pci, 1); 
	str name = *getArgReference_str(stk, pci, 2);

	initcontext();
	msg = drop_seq(sql, sname, name);
	return msg;
}

str
SQLcreate_schema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	str sname = *getArgReference_str(stk, pci, 1);
	str name = SaveArgReference(stk, pci, 2);
	int auth_id;

	initcontext();
	auth_id = sql->role_id;
	if (name && (auth_id = sql_find_auth(sql, name)) < 0) {
		throw(SQL,"sql.create_schema", SQLSTATE(42M32) "CREATE SCHEMA: no such authorization '%s'", name);
	}
	if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN) {
		throw(SQL,"sql.create_schema", SQLSTATE(42000) "CREATE SCHEMA: insufficient privileges for user '%s'", stack_get_string(sql, "current_user"));
	}
	if (mvc_bind_schema(sql, sname)) {
		throw(SQL,"sql.create_schema", SQLSTATE(3F000) "CREATE SCHEMA: name '%s' already in use", sname);
	} else {
		(void) mvc_create_schema(sql, sname, auth_id, sql->user_id);
	}
	return msg;
}

str
SQLdrop_schema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg= MAL_SUCCEED;
	str sname = *getArgReference_str(stk, pci, 1); 
	str notused = *getArgReference_str(stk, pci, 2);
	int if_exists = *getArgReference_int(stk, pci, 3);
	int action = *getArgReference_int(stk, pci, 4);
	sql_schema *s;

	(void) notused;
	initcontext();
	s = mvc_bind_schema(sql, sname);
	if (!s) {
		if(!if_exists)
			throw(SQL,"sql.drop_schema",SQLSTATE(3F000) "DROP SCHEMA: name %s does not exist", sname);
	} else if (!mvc_schema_privs(sql, s)) {
		throw(SQL,"sql.drop_schema",SQLSTATE(42000) "DROP SCHEMA: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else if (s == cur_schema(sql)) {
		throw(SQL,"sql.drop_schema",SQLSTATE(42000) "DROP SCHEMA: cannot drop current schema");
	} else if (s->system) {
		throw(SQL,"sql.drop_schema",SQLSTATE(42000) "DROP SCHEMA: access denied for '%s'", sname);
	} else if (sql_schema_has_user(sql, s)) {
		throw(SQL,"sql.drop_schema",SQLSTATE(2BM37) "DROP SCHEMA: unable to drop schema '%s' (there are database objects which depend on it)", sname);
	} else if (!action /* RESTRICT */ && (
		!list_empty(s->tables.set) || !list_empty(s->types.set) ||
		!list_empty(s->funcs.set) || !list_empty(s->seqs.set))) {
		throw(SQL,"sql.drop_schema",SQLSTATE(2BM37) "DROP SCHEMA: unable to drop schema '%s' (there are database objects which depend on it)", sname);
	} else {
		if(mvc_drop_schema(sql, s, action))
			throw(SQL,"sql.drop_schema", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	return msg;
}

str
SQLcreate_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	str tname = *getArgReference_str(stk, pci, 2); 
	sql_table *t = *(sql_table **) getArgReference(stk, pci, 3);
	int temp = *getArgReference_int(stk, pci, 4);

	initcontext();
	msg = create_table_or_view(sql, sname, tname, t, temp);
	return msg;
}

str
SQLcreate_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	str vname = *getArgReference_str(stk, pci, 2); 
	sql_table *t = *(sql_table **) getArgReference(stk, pci, 3);
	int temp = *getArgReference_int(stk, pci, 4);

	initcontext();
	msg = create_table_or_view(sql, sname, vname, t, temp);
	return msg;
}

str
SQLdrop_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	str name = *getArgReference_str(stk, pci, 2);
	int if_exists = *getArgReference_int(stk, pci, 3);
	int action = *getArgReference_int(stk, pci, 4);

	initcontext();
	msg = drop_table(sql, sname, name, action, if_exists);
	return msg;
}

str
SQLdrop_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	str name = *getArgReference_str(stk, pci, 2);
	int if_exists = *getArgReference_int(stk, pci, 3);
	int action = *getArgReference_int(stk, pci, 4);

	initcontext();
	msg = drop_view(sql, sname, name, action, if_exists);
	return msg;
}

str
SQLdrop_constraint(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	str name = *getArgReference_str(stk, pci, 2);
	int action = *getArgReference_int(stk, pci, 4);
	(void) *getArgReference_int(stk, pci, 3); //the if_exists parameter is also passed but not used

	initcontext();
	msg = drop_key(sql, sname, name, action);
	return msg;
}

str
SQLalter_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	str tname = *getArgReference_str(stk, pci, 2); 
	sql_table *t = *(sql_table **) getArgReference(stk, pci, 3);

	(void)tname;
	initcontext();
	msg = alter_table(cntxt, sql, sname, t);
	return msg;
}

str
SQLcreate_type(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *name = *getArgReference_str(stk, pci, 2);
	char *impl = *getArgReference_str(stk, pci, 3);
	sql_schema *s;

	initcontext();
	s = mvc_bind_schema(sql, sname);
	if (!mvc_schema_privs(sql, sql->session->schema))
		throw(SQL,"sql.create_type", SQLSTATE(0D000) "CREATE TYPE: not enough privileges to create type '%s'", sname);
	if (!mvc_create_type(sql, s, name, 0, 0, 0, impl))
		throw(SQL,"sql.create_type", SQLSTATE(0D000) "CREATE TYPE: unknown external type '%s'", impl);
	return msg;
}

str
SQLdrop_type(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *name = *getArgReference_str(stk, pci, 2);
	int drop_action = *getArgReference_int(stk, pci, 3);
	sql_schema *s;
	sql_type *t;

	initcontext();
	s = mvc_bind_schema(sql, sname);
	t = schema_bind_type( sql, s, name);
	if (!t)
		throw(SQL,"sql.drop_type", SQLSTATE(0D000) "DROP TYPE: type '%s' does not exist", sname);
	else if (!mvc_schema_privs(sql, sql->session->schema))
		throw(SQL,"sql.drop_type", SQLSTATE(0D000) "DROP TYPE: not enough privileges to drop type '%s'", sname);
	else if (!drop_action && mvc_check_dependency(sql, t->base.id, TYPE_DEPENDENCY, NULL))
		throw(SQL,"sql.drop_type", SQLSTATE(42000) "DROP TYPE: unable to drop type %s (there are database objects which depend on it)\n", sname);
	else if (!mvc_drop_type(sql, sql->session->schema, t, drop_action))
		throw(SQL,"sql.drop_type", SQLSTATE(0D000) "DROP TYPE: failed to drop type '%s'", sname);
	return MAL_SUCCEED;
}

str
SQLgrant_roles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *auth = SaveArgReference(stk, pci, 2);
	int grantor = *getArgReference_int(stk, pci, 3);
	int admin = *getArgReference_int(stk, pci, 4);

	initcontext();
	msg = sql_grant_role(sql, sname /*grantee */ , auth, grantor, admin);
	return msg;
}

str
SQLrevoke_roles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *auth = SaveArgReference(stk, pci, 2);
	int grantor = *getArgReference_int(stk, pci, 3);
	int admin = *getArgReference_int(stk, pci, 4);

	initcontext();
	msg = sql_revoke_role(sql, sname /*grantee */ , auth, grantor, admin);
	return msg;
}

str
SQLgrant(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *tname = *getArgReference_str(stk, pci, 2);
	char *grantee = *getArgReference_str(stk, pci, 3);
	int privs = *getArgReference_int(stk, pci, 4);
	char *cname = SaveArgReference(stk, pci, 5);
	int grant = *getArgReference_int(stk, pci, 6);
	int grantor = *getArgReference_int(stk, pci, 7);

	initcontext();
	if (!tname || strcmp(tname, str_nil) == 0)
		msg = sql_grant_global_privs(sql, grantee, privs, grant, grantor);
	else
		msg = sql_grant_table_privs(sql, grantee, privs, sname, tname, cname, grant, grantor);
	return msg;
}

str SQLrevoke(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *tname = *getArgReference_str(stk, pci, 2);
	char *grantee = *getArgReference_str(stk, pci, 3);
	int privs = *getArgReference_int(stk, pci, 4);
	char *cname = SaveArgReference(stk, pci, 5);
	int grant = *getArgReference_int(stk, pci, 6);
	int grantor = *getArgReference_int(stk, pci, 7);

	initcontext();
	if (!tname || strcmp(tname, str_nil) == 0)
		msg = sql_revoke_global_privs(sql, grantee, privs, grant, grantor);
	else
		msg = sql_revoke_table_privs(sql, grantee, privs, sname, tname, cname, grant, grantor);
	return msg;
}

str
SQLgrant_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	int func_id = *getArgReference_int(stk, pci, 2);
	char *grantee = *getArgReference_str(stk, pci, 3);
	int privs = *getArgReference_int(stk, pci, 4);
	int grant = *getArgReference_int(stk, pci, 5);
	int grantor = *getArgReference_int(stk, pci, 6);

	initcontext();
	msg = sql_grant_func_privs(sql, grantee, privs, sname, func_id, grant, grantor);
	return msg;
}

str
SQLrevoke_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	int func_id = *getArgReference_int(stk, pci, 2);
	char *grantee = *getArgReference_str(stk, pci, 3);
	int privs = *getArgReference_int(stk, pci, 4);
	int grant = *getArgReference_int(stk, pci, 5);
	int grantor = *getArgReference_int(stk, pci, 6);

	initcontext();
	msg = sql_revoke_func_privs(sql, grantee, privs, sname, func_id, grant, grantor);
	return msg;
}

str
SQLcreate_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *passwd = *getArgReference_str(stk, pci, 2);
	int enc = *getArgReference_int(stk, pci, 3);
	char *schema = SaveArgReference(stk, pci, 4);
	char *fullname = SaveArgReference(stk, pci, 5);

	initcontext();
	msg = sql_create_user(sql, sname, passwd, enc, fullname, schema);
	return msg;
}

str
SQLdrop_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 

	initcontext();
	 msg = sql_drop_user(sql, sname);
	return msg;
}

str
SQLalter_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *passwd = SaveArgReference(stk, pci, 2);
	int enc = *getArgReference_int(stk, pci, 3);
	char *schema = SaveArgReference(stk, pci, 4);
	char *oldpasswd = SaveArgReference(stk, pci, 5);

	initcontext();
	msg = sql_alter_user(sql, sname, passwd, enc, schema, oldpasswd);

	return msg;
}

str
SQLrename_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *newuser = *getArgReference_str(stk, pci, 2);

	initcontext();
	msg = sql_rename_user(sql, sname, newuser);
	return msg;
}

str
SQLcreate_role(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *role = sname;
	int grantor = *getArgReference_int(stk, pci, 3);

	initcontext();
	msg = sql_create_role(sql, role, grantor);
	return msg;
}

str
SQLdrop_role(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *role = sname;

	initcontext();
	msg = sql_drop_role(sql, role);
	return msg;
}

str
SQLdrop_index(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *iname = *getArgReference_str(stk, pci, 2);

	initcontext();
	msg = drop_index(cntxt, sql, sname, iname);
	return msg;
}

str
SQLdrop_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *fname = *getArgReference_str(stk, pci, 2);
	int fid = *getArgReference_int(stk, pci, 3);
	int type = *getArgReference_int(stk, pci, 4);
	int action = *getArgReference_int(stk, pci, 5);

	initcontext();
	msg = drop_func(sql, sname, fname, fid, type, action);
	return msg;
}

str
SQLcreate_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	str fname = *getArgReference_str(stk, pci, 2); 
	sql_func *f = *(sql_func **) getArgReference(stk, pci, 3);

	initcontext();
	msg = create_func(sql, sname, fname, f);
	return msg;
}

str
SQLcreate_trigger(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *tname = *getArgReference_str(stk, pci, 2);
	char *triggername = *getArgReference_str(stk, pci, 3);
	int time = *getArgReference_int(stk, pci, 4);
	int orientation = *getArgReference_int(stk, pci, 5);
	int event = *getArgReference_int(stk, pci, 6);
	char *old_name = *getArgReference_str(stk, pci, 7);
	char *new_name = *getArgReference_str(stk, pci, 8);
	char *condition = *getArgReference_str(stk, pci, 9);
	char *query = *getArgReference_str(stk, pci, 10);

	initcontext();
	old_name=(!old_name || strcmp(old_name, str_nil) == 0)?NULL:old_name; 
	new_name=(!new_name || strcmp(new_name, str_nil) == 0)?NULL:new_name; 
	condition=(!condition || strcmp(condition, str_nil) == 0)?NULL:condition;
	msg = create_trigger(sql, sname, tname, triggername, time, orientation, event, old_name, new_name, condition, query);
	return msg;
}

str
SQLdrop_trigger(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *triggername = *getArgReference_str(stk, pci, 2);
	int if_exists = *getArgReference_int(stk, pci, 3);

	initcontext();
	msg = drop_trigger(sql, sname, triggername, if_exists);
	return msg;
}

str
SQLalter_add_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *mtname = SaveArgReference(stk, pci, 2);
	char *psname = SaveArgReference(stk, pci, 3);
	char *ptname = SaveArgReference(stk, pci, 4);

	initcontext();
	msg = alter_table_add_table(sql, sname, mtname, psname, ptname);
	return msg;
}

str
SQLalter_add_range_partition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	char *mtname = SaveArgReference(stk, pci, 2);
	char *psname = SaveArgReference(stk, pci, 3);
	char *ptname = SaveArgReference(stk, pci, 4);
	ValRecord *min = &(stk)->stk[(pci)->argv[5]];
	ValRecord *max = &(stk)->stk[(pci)->argv[6]];
	int with_nills = *getArgReference_int(stk, pci, 7);
	int update = *getArgReference_int(stk, pci, 8);

	initcontext();
	msg = alter_table_add_range_partition(sql, sname, mtname, psname, ptname, VALget(min), VALget(max), with_nills, update);
	return msg;
}

str
SQLalter_add_value_partition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	char *mtname = SaveArgReference(stk, pci, 2);
	char *psname = SaveArgReference(stk, pci, 3);
	char *ptname = SaveArgReference(stk, pci, 4);
	int with_nills = *getArgReference_int(stk, pci, 5);
	int update = *getArgReference_int(stk, pci, 6);

	initcontext();
	msg = alter_table_add_value_partition(sql, stk, pci, sname, mtname, psname, ptname, with_nills, update);
	return msg;
}

str
SQLalter_del_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *mtname = SaveArgReference(stk, pci, 2);
	char *psname = SaveArgReference(stk, pci, 3);
	char *ptname = SaveArgReference(stk, pci, 4);
	int drop_action = *getArgReference_int(stk, pci, 5);

	initcontext();
	msg= alter_table_del_table(sql, sname, mtname, psname, ptname, drop_action);
	return msg;
}

str
SQLalter_set_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) 
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1); 
	char *tname = SaveArgReference(stk, pci, 2);
	int access = *getArgReference_int(stk, pci, 3);

	initcontext();
	msg = alter_table_set_access(sql, sname, tname, access);

	return msg;
}

str
SQLcomment_on(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int objid = *getArgReference_int(stk, pci, 1);
	char *remark = *getArgReference_str(stk, pci, 2);
	sql_trans *tx;
	sql_schema *sys;
	sql_table *comments;
	sql_column *id_col, *remark_col;
	oid rid;
	int ok = LOG_OK;

	initcontext();

	// Manually insert the rows to circumvent permission checks.
	tx = sql->session->tr;
	sys = mvc_bind_schema(sql, "sys");
	if (!sys)
		throw(SQL, "sql.comment_on", SQLSTATE(3F000) "Internal error");
	comments = mvc_bind_table(sql, sys, "comments");
	if (!comments)
		throw(SQL, "sql.comment_on", SQLSTATE(3F000) "no table sys.comments");
	id_col = mvc_bind_column(sql, comments, "id");
	remark_col = find_sql_column(comments, "remark");
	if (!id_col || !remark_col)
		throw(SQL, "sql.comment_on", SQLSTATE(3F000) "no table sys.comments");
	rid = table_funcs.column_find_row(tx, id_col, &objid, NULL);
	if (remark != NULL && *remark && strcmp(remark, str_nil) != 0) {
		if (!is_oid_nil(rid)) {
			// have new remark and found old one, so update field
			/* UPDATE sys.comments SET remark = %s WHERE id = %d */
			ok = table_funcs.column_update_value(tx, remark_col, rid, remark);
		} else {
			// have new remark but found none so insert row
			/* INSERT INTO sys.comments (id, remark) VALUES (%d, %s) */
			ok = table_funcs.table_insert(tx, comments, &objid, remark);
		}
	} else {
		if (!is_oid_nil(rid)) {
			// have no remark but found one, so delete row
			/* DELETE FROM sys.comments WHERE id = %d */
			ok = table_funcs.table_delete(tx, comments, rid);
		}
	}
	if (ok != LOG_OK)
		throw(SQL, "sql.comment_on", SQLSTATE(3F000) "operation failed");
	return MAL_SUCCEED;
}
