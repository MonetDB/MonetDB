/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "sql_statistics.h"
#include "mal_namespace.h"
#include "opt_prelude.h"
#include "querylog.h"
#include "mal_builder.h"
#include "mal_debugger.h"

#include "rel_select.h"
#include "rel_unnest.h"
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
	if (store_readonly(sql->session->tr->store))\
		throw(SQL,"sql.cat",SQLSTATE(25006) "Schema statements cannot be executed on a readonly database.");

static char *
SaveArgReference(MalStkPtr stk, InstrPtr pci, int arg)
{
	char *val = *getArgReference_str(stk, pci, arg);

	if (strNil(val))
		val = NULL;
	return val;
}

static int
table_has_updates(sql_trans *tr, sql_table *t)
{
	node *n;
	int cnt = 0;
	sqlstore *store = tr->store;

	for ( n = ol_first_node(t->columns); !cnt && n; n = n->next) {
		sql_column *c = n->data;

		size_t upd = store->storage_api.count_col( tr, c, 2/* count updates */);
		cnt |= upd > 0;
	}
	return cnt;
}

static char *
rel_check_tables(mvc *sql, sql_table *nt, sql_table *nnt, const char *errtable)
{
	node *n, *m, *nn, *mm;

	if (ol_length(nt->columns) != ol_length(nnt->columns))
		throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table doesn't match %s definition", errtable, errtable);
	for (n = ol_first_node(nt->columns), m = ol_first_node(nnt->columns); n && m; n = n->next, m = m->next) {
		sql_column *nc = n->data;
		sql_column *mc = m->data;

		if (subtype_cmp(&nc->type, &mc->type) != 0)
			throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table column type doesn't match %s definition", errtable, errtable);
		if (isRangePartitionTable(nt) || isListPartitionTable(nt)) {
			if (nc->null != mc->null)
				throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table column NULL check doesn't match %s definition", errtable, errtable);
			if ((!nc->def && mc->def) || (nc->def && !mc->def) || (nc->def && mc->def && strcmp(nc->def, mc->def) != 0))
				throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table column DEFAULT value doesn't match %s definition", errtable, errtable);
		}
	}
	if (isNonPartitionedTable(nt)) {
		if (ol_length(nt->idxs) != ol_length(nnt->idxs))
			throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table index doesn't match %s definition", errtable, errtable);
		if (ol_length(nt->idxs))
			for (n = ol_first_node(nt->idxs), m = ol_first_node(nnt->idxs); n && m; n = n->next, m = m->next) {
				sql_idx *ni = n->data;
				sql_idx *mi = m->data;

				if (ni->type != mi->type)
					throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table index type doesn't match %s definition", errtable, errtable);
			}
	} else { //for partitioned tables we allow indexes but the key set must be exactly the same
		if (ol_length(nt->keys) != ol_length(nnt->keys))
			throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table key doesn't match %s definition", errtable, errtable);
		if (ol_length(nt->keys))
			for (n = ol_first_node(nt->keys), m = ol_first_node(nnt->keys); n && m; n = n->next, m = m->next) {
				sql_key *ni = n->data;
				sql_key *mi = m->data;

				if (ni->type != mi->type)
					throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table key type doesn't match %s definition", errtable, errtable);
				if (list_length(ni->columns) != list_length(mi->columns))
					throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table key type doesn't match %s definition", errtable, errtable);
				for (nn = ni->columns->h, mm = mi->columns->h; nn && mm; nn = nn->next, mm = mm->next) {
					sql_kc *nni = nn->data;
					sql_kc *mmi = mm->data;

					if (nni->c->colnr != mmi->c->colnr)
						throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table key's columns doesn't match %s definition", errtable, errtable);
				}
			}
	}

	if (nested_mergetable(sql->session->tr, nt/*mergetable*/, nnt->s->base.name, nnt->base.name/*parts*/))
		throw(SQL,"sql.rel_check_tables",SQLSTATE(3F000) "ALTER %s: to be added table is a parent of the %s", errtable, errtable);
	return MAL_SUCCEED;
}

static char*
validate_alter_table_add_table(mvc *sql, char* call, char *msname, char *mtname, char *psname, char *ptname,
							   sql_table **mt, sql_table **pt, int update)
{
	char *msg = MAL_SUCCEED;
	sql_schema *ms = NULL, *ps = NULL;
	sql_table *rmt = NULL, *rpt = NULL;

	if (!(ms = mvc_bind_schema(sql, msname)))
		throw(SQL,call,SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", msname);
	if (!(ps = mvc_bind_schema(sql, psname)))
		throw(SQL,call,SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", psname);
	if (!mvc_schema_privs(sql, ms))
		throw(SQL,call,SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), ms->base.name);
	if (!mvc_schema_privs(sql, ps))
		throw(SQL,call,SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), ps->base.name);
	if (!(rmt = mvc_bind_table(sql, ms, mtname)))
		throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", ms->base.name, mtname);
	if (!(rpt = mvc_bind_table(sql, ps, ptname)))
		throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", ps->base.name, ptname);

	const char *errtable = TABLE_TYPE_DESCRIPTION(rmt->type, rmt->properties);
	if (!update && (!isMergeTable(rmt) && !isReplicaTable(rmt)))
		throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: cannot add table '%s.%s' to %s '%s.%s'", psname, ptname, errtable, msname, mtname);
	node *n = members_find_child_id(rmt->members, rpt->base.id);
	if (isView(rpt))
		throw(SQL,call,SQLSTATE(42000) "ALTER TABLE: can't add a view into a %s", errtable);
	if (isDeclaredTable(rpt))
		throw(SQL,call,SQLSTATE(42000) "ALTER TABLE: can't add a declared table into a %s", errtable);
	if (isTempSchema(rpt->s))
		throw(SQL,call,SQLSTATE(42000) "ALTER TABLE: can't add a temporary table into a %s", errtable);
	if (ms->base.id != ps->base.id)
		throw(SQL,call,SQLSTATE(42000) "ALTER TABLE: all children tables of '%s.%s' must be part of schema '%s'", msname, mtname, msname);
	if (n && !update)
		throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: table '%s.%s' is already part of %s '%s.%s'", psname, ptname, errtable, msname, mtname);
	if (!n && update)
		throw(SQL,call,SQLSTATE(42S02) "ALTER TABLE: table '%s.%s' isn't part of %s '%s.%s'", psname, ptname, errtable, msname, mtname);
	if ((msg = rel_check_tables(sql, rmt, rpt, errtable)) != MAL_SUCCEED)
		return msg;

	*mt = rmt;
	*pt = rpt;
	return MAL_SUCCEED;
}

static char *
alter_table_add_table(mvc *sql, char *msname, char *mtname, char *psname, char *ptname)
{
	sql_table *mt = NULL, *pt = NULL;
	str msg = validate_alter_table_add_table(sql, "sql.alter_table_add_table", msname, mtname, psname, ptname, &mt, &pt, 0);

	if (msg == MAL_SUCCEED) {
		if (isRangePartitionTable(mt))
			return createException(SQL, "sql.alter_table_add_table",SQLSTATE(42000) "ALTER TABLE: a range partition is required while adding under a range partition table");
		if (isListPartitionTable(mt))
			return createException(SQL, "sql.alter_table_add_table",SQLSTATE(42000) "ALTER TABLE: a value partition is required while adding under a list partition table");
		sql_trans_add_table(sql->session->tr, mt, pt);
	}
	return msg;
}

static char *
alter_table_add_range_partition(mvc *sql, char *msname, char *mtname, char *psname, char *ptname, ptr min, ptr max,
								bit with_nills, int update)
{
	sql_table *mt = NULL, *pt = NULL;
	sql_part *err = NULL;
	str msg = MAL_SUCCEED, err_min = NULL, err_max = NULL, conflict_err_min = NULL, conflict_err_max = NULL;
	int tp1 = 0, errcode = 0, min_null = 0, max_null = 0;
	size_t length = 0;
	sql_subtype tpe;

	if ((msg = validate_alter_table_add_table(sql, "sql.alter_table_add_range_partition", msname, mtname, psname, ptname,
											 &mt, &pt, update))) {
		return msg;
	} else if (!isRangePartitionTable(mt)) {
		msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
									"ALTER TABLE: cannot add range partition into a %s table",
									(isListPartitionTable(mt))?"list partition":"merge");
		goto finish;
	} else if (!update && partition_find_part(sql->session->tr, pt, NULL)) {
		msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
							  "ALTER TABLE: table '%s.%s' is already part of another table",
							  psname, ptname);
		goto finish;
	}

	find_partition_type(&tpe, mt);
	tp1 = tpe.type->localtype;
	min_null = ATOMcmp(tp1, min, ATOMnilptr(tp1)) == 0;
	max_null = ATOMcmp(tp1, max, ATOMnilptr(tp1)) == 0;

	if (!min_null && !max_null && ATOMcmp(tp1, min, max) > 0) {
		msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000) "ALTER TABLE: minimum value is higher than maximum value");
		goto finish;
	}

	errcode = sql_trans_add_range_partition(sql->session->tr, mt, pt, tpe, min, max, with_nills, update, &err);
	switch (errcode) {
		case 0:
			break;
		case -1:
			msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
			if (is_bit_nil(err->with_nills)) {
				msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
										"ALTER TABLE: conflicting partitions: table %s.%s stores every possible value", err->t->s->base.name, err->base.name);
			} else if (with_nills && err->with_nills) {
				msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
										"ALTER TABLE: conflicting partitions: table %s.%s stores null values and only "
										"one partition can store null values at the time", err->t->s->base.name, err->base.name);
			} else {
				ssize_t (*atomtostr)(str *, size_t *, const void *, bool) = BATatoms[tp1].atomToStr;
				const void *nil = ATOMnilptr(tp1);
				sql_table *errt = mvc_bind_table(sql, mt->s, err->base.name);

				if (!errt) {
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
									  "ALTER TABLE: cannot find partition table %s.%s", err->t->s->base.name, err->base.name);
					goto finish;
				}
				if (!ATOMcmp(tp1, nil, err->part.range.minvalue)) {
					if (!(conflict_err_min = GDKstrdup("absolute min value")))
						msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY013) MAL_MALLOC_FAIL);
				} else if (atomtostr(&conflict_err_min, &length, err->part.range.minvalue, true) < 0) {
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				if (msg)
					goto finish;

				if (!ATOMcmp(tp1, nil, err->part.range.maxvalue)) {
					if (!(conflict_err_max = GDKstrdup("absolute max value")))
						msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY013) MAL_MALLOC_FAIL);
				} else if (atomtostr(&conflict_err_max, &length, err->part.range.maxvalue, true) < 0) {
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				if (msg)
					goto finish;

				if (!ATOMcmp(tp1, nil, min)) {
					if (!(err_min = GDKstrdup("absolute min value")))
						msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY013) MAL_MALLOC_FAIL);
				} else if (atomtostr(&err_min, &length, min, true) < 0) {
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				if (msg)
					goto finish;

				if (!ATOMcmp(tp1, nil, max)) {
					if (!(err_max = GDKstrdup("absolute max value")))
						msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY013) MAL_MALLOC_FAIL);
				} else if (atomtostr(&err_max, &length, max, true) < 0) {
					msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				if (msg)
					goto finish;

				msg = createException(SQL,"sql.alter_table_add_range_partition",SQLSTATE(42000)
									  "ALTER TABLE: conflicting partitions: %s to %s and %s to %s from table %s.%s",
									  err_min, err_max, conflict_err_min, conflict_err_max, errt->s->base.name, errt->base.name);
			}
			break;
		default:
			assert(0);
	}

finish:
	if (err_min)
		GDKfree(err_min);
	if (err_max)
		GDKfree(err_max);
	if (conflict_err_min)
		GDKfree(conflict_err_min);
	if (conflict_err_max)
		GDKfree(conflict_err_max);
	return msg;
}

static char *
alter_table_add_value_partition(mvc *sql, MalStkPtr stk, InstrPtr pci, char *msname, char *mtname, char *psname,
								char *ptname, bit with_nills, int update)
{
	sql_table *mt = NULL, *pt = NULL;
	str msg = MAL_SUCCEED;
	sql_part *err = NULL;
	int errcode = 0, i = 0, ninserts = 0;
	sql_subtype tpe;
	list *values = NULL;

	assert(with_nills == false || with_nills == true); /* No nills allowed here */
	if ((msg = validate_alter_table_add_table(sql, "sql.alter_table_add_value_partition", msname, mtname, psname, ptname,
											 &mt, &pt, update))) {
		return msg;
	} else if (!isListPartitionTable(mt)) {
		msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
									"ALTER TABLE: cannot add value partition into a %s table",
									(isRangePartitionTable(mt))?"range partition":"merge");
		goto finish;
	} else if (!update && partition_find_part(sql->session->tr, pt, NULL)) {
		msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
							  "ALTER TABLE: table '%s.%s' is already part of another table",
							  psname, ptname);
		goto finish;
	}

	find_partition_type(&tpe, mt);
	ninserts = pci->argc - pci->retc - 6;
	if (ninserts <= 0 && !with_nills) {
		msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000) "ALTER TABLE: no values in the list");
		goto finish;
	}
	values = list_new(sql->session->tr->sa, (fdestroy) &part_value_destroy);
	for ( i = pci->retc+6; i < pci->argc; i++){
		sql_part_value *nextv = NULL;
		ValRecord *vnext = &(stk)->stk[(pci)->argv[i]];
		ptr pnext = VALget(vnext);
		size_t len = ATOMlen(vnext->vtype, pnext);

		if (VALisnil(vnext)) { /* check for an eventual null value which cannot be */
			msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
																			"ALTER TABLE: list value cannot be null");
			list_destroy2(values, sql->session->tr->store);
			goto finish;
		}

		nextv = SA_ZNEW(sql->session->tr->sa, sql_part_value); /* instantiate the part value */
		nextv->value = SA_NEW_ARRAY(sql->session->tr->sa, char, len);
		memcpy(nextv->value, pnext, len);
		nextv->length = len;

		if (list_append_sorted(values, nextv, &tpe, sql_values_list_element_validate_and_insert) != NULL) {
			msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
									"ALTER TABLE: there are duplicated values in the list");
			list_destroy2(values, sql->session->tr->store);
			_DELETE(nextv->value);
			_DELETE(nextv);
			goto finish;
		}
	}

	errcode = sql_trans_add_value_partition(sql->session->tr, mt, pt, tpe, values, with_nills, update, &err);
	switch (errcode) {
		case 0:
			break;
		case -1:
			msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
									"ALTER TABLE: the new partition is conflicting with the existing partition %s.%s",
									err->t->s->base.name, err->base.name);
			break;
		default:
			msg = createException(SQL,"sql.alter_table_add_value_partition",SQLSTATE(42000)
									"ALTER TABLE: value at position %d length is higher than %d",
									(errcode * -1) - 1, STORAGE_MAX_VALUE_LENGTH);
			break;
	}

finish:
	return msg;
}

static char *
alter_table_del_table(mvc *sql, char *msname, char *mtname, char *psname, char *ptname, int drop_action)
{
	sql_schema *ms = NULL, *ps = NULL;
	sql_table *mt = NULL, *pt = NULL;
	node *n = NULL;

	if (!(ms = mvc_bind_schema(sql, msname)))
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", msname);
	if (!(ps = mvc_bind_schema(sql, psname)))
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", psname);
	if (!mvc_schema_privs(sql, ms))
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), ms->base.name);
	if (!mvc_schema_privs(sql, ps))
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), ps->base.name);
	if (!(mt = mvc_bind_table(sql, ms, mtname)))
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", ms->base.name, mtname);
	if (!(pt = mvc_bind_table(sql, ps, ptname)))
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", ps->base.name, ptname);
	const char *errtable = TABLE_TYPE_DESCRIPTION(mt->type, mt->properties);
	if (!isMergeTable(mt) && !isReplicaTable(mt))
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(42S02) "ALTER TABLE: cannot drop table '%s.%s' to %s '%s.%s'", psname, ptname, errtable, msname, mtname);
	if (!(n = members_find_child_id(mt->members, pt->base.id)))
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(42S02) "ALTER TABLE: table '%s.%s' isn't part of %s '%s.%s'", ps->base.name, ptname, errtable, ms->base.name, mtname);

	if (sql_trans_del_table(sql->session->tr, mt, pt, drop_action))
		throw(SQL,"sql.alter_table_del_table",SQLSTATE(42000) "ALTER TABLE: transaction conflict detected");
	return MAL_SUCCEED;
}

static char *
alter_table_set_access(mvc *sql, char *sname, char *tname, int access)
{
	sql_schema *s = NULL;
	sql_table *t = NULL;

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.alter_table_set_access",SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", sname);
	if (s && !mvc_schema_privs(sql, s))
		throw(SQL,"sql.alter_table_set_access",SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (!(t = mvc_bind_table(sql, s, tname)))
		throw(SQL,"sql.alter_table_set_access",SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", tname, s->base.name);
	if (isMergeTable(t))
		throw(SQL,"sql.alter_table_set_access",SQLSTATE(42S02) "ALTER TABLE: read only MERGE TABLES are not supported");
	if (t->access != access) {
		if (access && table_has_updates(sql->session->tr, t))
			throw(SQL,"sql.alter_table_set_access",SQLSTATE(40000) "ALTER TABLE: set READ or INSERT ONLY not possible with outstanding updates (wait until updates are flushed)\n");

		mvc_access(sql, t, access);
		if (access == 0)
			sql_drop_statistics(sql, t);
	}
	return MAL_SUCCEED;
}

static char *
create_trigger(mvc *sql, char *sname, char *tname, char *triggername, int time, int orientation, int event, char *old_name, char *new_name, char *condition, char *query)
{
	sql_trigger *tri = NULL;
	sql_schema *s = NULL;
	sql_table *t;

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.create_trigger",SQLSTATE(3F000) "CREATE TRIGGER: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.create_trigger",SQLSTATE(42000) "CREATE TRIGGER: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (mvc_bind_trigger(sql, s, triggername))
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

		sql->sa = sa_create(sql->pa);
		if (!sql->sa)
			throw(SQL, "sql.catalog",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		buf = sa_strdup(sql->sa, query);
		if (!buf)
			throw(SQL, "sql.catalog",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		r = rel_parse(sql, s, buf, m_deps);
		if (r)
			r = sql_processrelation(sql, r, 0, 0);
		if (r) {
			list *id_l = rel_dependencies(sql, r);
			mvc_create_dependencies(sql, id_l, tri->base.id, TRIGGER_DEPENDENCY);
		}
		sa_destroy(sql->sa);
		sql->sa = sa;
		if (!r) {
			if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
				throw(SQL, "sql.create_trigger", "%s", sql->errstr);
			else
				throw(SQL, "sql.create_trigger", SQLSTATE(42000) "%s", sql->errstr);
		}
	}
	return MAL_SUCCEED;
}

static char *
drop_trigger(mvc *sql, char *sname, char *tname, int if_exists)
{
	sql_trigger *tri = NULL;
	sql_schema *s = NULL;

	if (!(s = mvc_bind_schema(sql, sname))) {
		if (if_exists)
			return MAL_SUCCEED;
		throw(SQL,"sql.drop_trigger",SQLSTATE(3F000) "DROP TRIGGER: no such schema '%s'", sname);
	}
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.drop_trigger",SQLSTATE(42000) "DROP TRIGGER: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);

	if ((tri = mvc_bind_trigger(sql, s, tname)) == NULL) {
		if (if_exists)
			return MAL_SUCCEED;
		throw(SQL,"sql.drop_trigger", SQLSTATE(3F000) "DROP TRIGGER: unknown trigger %s\n", tname);
	}
	if (mvc_drop_trigger(sql, s, tri))
		throw(SQL,"sql.drop_trigger", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static char *
drop_table(mvc *sql, char *sname, char *tname, int drop_action, int if_exists)
{
	sql_schema *s = NULL;
	sql_table *t = NULL;

	if (!(s = mvc_bind_schema(sql, sname))) {
		if (if_exists)
			return MAL_SUCCEED;
		throw(SQL,"sql.drop_table",SQLSTATE(3F000) "DROP TABLE: no such schema '%s'", sname);
	}
	if (!(t = mvc_bind_table(sql, s, tname))) {
		if (if_exists)
			return MAL_SUCCEED;
		throw(SQL,"sql.drop_table", SQLSTATE(42S02) "DROP TABLE: no such table '%s'", tname);
	}
	if (isView(t))
		throw(SQL,"sql.drop_table", SQLSTATE(42000) "DROP TABLE: cannot drop VIEW '%s'", tname);
	if (t->system)
		throw(SQL,"sql.drop_table", SQLSTATE(42000) "DROP TABLE: cannot drop system table '%s'", tname);
	if (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && t->persistence == SQL_LOCAL_TEMP))
		throw(SQL,"sql.drop_table", SQLSTATE(42000) "DROP TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);

	if (!drop_action && t->keys) {
		for (node *n = ol_first_node(t->keys); n; n = n->next) {
			sql_key *k = n->data;

			if (k->type == ukey || k->type == pkey) {
				struct os_iter oi;
				os_iterator(&oi, k->t->s->keys, sql->session->tr, NULL);
				for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
					sql_key *fk = (sql_key*)b;
					sql_fkey *rk = (sql_fkey*)b;

					if (fk->type != fkey || rk->rkey != k->base.id)
						continue;

					/* make sure it is not a self referencing key */
					if (fk->t != t)
						throw(SQL,"sql.drop_table", SQLSTATE(40000) "DROP TABLE: FOREIGN KEY %s.%s depends on %s", k->t->base.name, k->base.name, tname);
				}
			}
		}
	}

	if (!drop_action && mvc_check_dependency(sql, t->base.id, TABLE_DEPENDENCY, NULL))
		throw (SQL,"sql.drop_table",SQLSTATE(42000) "DROP TABLE: unable to drop table %s (there are database objects which depend on it)\n", t->base.name);

	return mvc_drop_table(sql, s, t, drop_action);
}

static char *
drop_view(mvc *sql, char *sname, char *tname, int drop_action, int if_exists)
{
	sql_table *t = NULL;
	sql_schema *ss = NULL;

	if (!(ss = mvc_bind_schema(sql, sname))) {
		if (if_exists)
			return MAL_SUCCEED;
		throw(SQL,"sql.drop_view", SQLSTATE(3F000) "DROP VIEW: no such schema '%s'", sname);
	}
	if (!(t = mvc_bind_table(sql, ss, tname))) {
		if (if_exists)
			return MAL_SUCCEED;
		throw(SQL,"sql.drop_view",SQLSTATE(42S02) "DROP VIEW: unknown view '%s'", tname);
	}
	if (!mvc_schema_privs(sql, ss) && !(isTempSchema(ss) && t && t->persistence == SQL_LOCAL_TEMP))
		throw(SQL,"sql.drop_view", SQLSTATE(42000) "DROP VIEW: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), ss->base.name);
	if (!isView(t))
		throw(SQL,"sql.drop_view", SQLSTATE(42000) "DROP VIEW: unable to drop view '%s': is a table", tname);
	if (t->system)
		throw(SQL,"sql.drop_view", SQLSTATE(42000) "DROP VIEW: cannot drop system view '%s'", tname);
	if (!drop_action && mvc_check_dependency(sql, t->base.id, VIEW_DEPENDENCY, NULL))
		throw(SQL,"sql.drop_view", SQLSTATE(42000) "DROP VIEW: cannot drop view '%s', there are database objects which depend on it", t->base.name);
	return mvc_drop_table(sql, ss, t, drop_action);
}

static str
drop_key(mvc *sql, char *sname, char *tname, char *kname, int drop_action)
{
	node *n;
	sql_schema *s = cur_schema(sql);
	sql_table *t = NULL;
	sql_key *key;

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.drop_key", SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.drop_key", SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (!(t = mvc_bind_table(sql, s, tname)))
		throw(SQL,"sql.drop_key", SQLSTATE(42S02) "ALTER TABLE: no such table '%s'", tname);
	if (!(n = ol_find_name(t->keys, kname)))
		throw(SQL,"sql.drop_key", SQLSTATE(42000) "ALTER TABLE: no such constraint '%s'", kname);
	key = n->data;
	if (!drop_action && mvc_check_dependency(sql, key->base.id, KEY_DEPENDENCY, NULL))
		throw(SQL,"sql.drop_key", SQLSTATE(42000) "ALTER TABLE: cannot drop constraint '%s': there are database objects which depend on it", key->base.name);
	if (mvc_drop_key(sql, s, key, drop_action))
		throw(SQL,"sql.drop_key", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
drop_index(Client cntxt, mvc *sql, char *sname, char *iname)
{
	sql_schema *s = NULL;
	sql_idx *i = NULL;

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.drop_index", SQLSTATE(3F000) "DROP INDEX: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.drop_index", SQLSTATE(42000) "DROP INDEX: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (!(i = mvc_bind_idx(sql, s, iname)))
		throw(SQL,"sql.drop_index", SQLSTATE(42S12) "DROP INDEX: no such index '%s'", iname);
	if (i->key)
		throw(SQL,"sql.drop_index", SQLSTATE(42S12) "DROP INDEX: cannot drop index '%s', because the constraint '%s' depends on it", iname, i->key->base.name);
	if (i->type == ordered_idx) {
		sql_kc *ic = i->columns->h->data;
		BAT *b = mvc_bind(sql, s->base.name, ic->c->t->base.name, ic->c->base.name, 0);
		if (b) {
			OIDXdropImplementation(cntxt, b);
			BBPunfix(b->batCacheid);
		}
	}
	if (i->type == imprints_idx) {
		sql_kc *ic = i->columns->h->data;
		BAT *b = mvc_bind(sql, s->base.name, ic->c->t->base.name, ic->c->base.name, 0);
		if (b) {
			IMPSdestroy(b);
			BBPunfix(b->batCacheid);
		}
	}
	if (mvc_drop_idx(sql, s, i))
		throw(SQL,"sql.drop_index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return NULL;
}

static str
create_seq(mvc *sql, char *sname, char *seqname, sql_sequence *seq)
{
	sql_schema *s = NULL;

	(void)seqname;
	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.create_seq", SQLSTATE(3F000) "CREATE SEQUENCE: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.create_seq", SQLSTATE(42000) "CREATE SEQUENCE: insufficient privileges for '%s' in schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (find_sql_sequence(sql->session->tr, s, seq->base.name))
		throw(SQL,"sql.create_seq", SQLSTATE(42000) "CREATE SEQUENCE: name '%s' already in use", seq->base.name);
	if (is_lng_nil(seq->start) || is_lng_nil(seq->minvalue) || is_lng_nil(seq->maxvalue) ||
			   is_lng_nil(seq->increment) || is_lng_nil(seq->cacheinc) || is_bit_nil(seq->cycle))
		throw(SQL,"sql.create_seq", SQLSTATE(42000) "CREATE SEQUENCE: sequence properties must be non-NULL");
	if (seq->minvalue && seq->start < seq->minvalue)
		throw(SQL,"sql.create_seq", SQLSTATE(42000) "CREATE SEQUENCE: start value is lesser than the minimum ("LLFMT" < "LLFMT")", seq->start, seq->minvalue);
	if (seq->maxvalue && seq->start > seq->maxvalue)
		throw(SQL,"sql.create_seq", SQLSTATE(42000) "CREATE SEQUENCE: start value is higher than the maximum ("LLFMT" > "LLFMT")", seq->start, seq->maxvalue);
	if (seq->minvalue && seq->maxvalue && seq->maxvalue < seq->minvalue)
		throw(SQL,"sql.create_seq", SQLSTATE(42000) "CREATE SEQUENCE: maximum value is lesser than the minimum ("LLFMT" < "LLFMT")", seq->maxvalue, seq->minvalue);
	sql_trans_create_sequence(sql->session->tr, s, seq->base.name, seq->start, seq->minvalue, seq->maxvalue, seq->increment, seq->cacheinc, seq->cycle, seq->bedropped);
	return NULL;
}

static str
alter_seq(mvc *sql, char *sname, char *seqname, sql_sequence *seq, const lng *val)
{
	sql_schema *s = NULL;
	sql_sequence *nseq = NULL;

	(void)seqname;
	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.alter_seq", SQLSTATE(3F000) "ALTER SEQUENCE: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.alter_seq", SQLSTATE(42000) "ALTER SEQUENCE: insufficient privileges for '%s' in schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (!(nseq = find_sql_sequence(sql->session->tr, s, seq->base.name)))
		throw(SQL,"sql.alter_seq", SQLSTATE(42000) "ALTER SEQUENCE: no such sequence '%s'", seq->base.name);
	/* if seq properties hold NULL values, then they should be ignored during the update */
	/* first alter the known values */
	sql_trans_alter_sequence(sql->session->tr, nseq, seq->minvalue, seq->maxvalue, seq->increment, seq->cacheinc, seq->cycle);
	if (nseq->minvalue && nseq->maxvalue && nseq->maxvalue < seq->minvalue)
		throw(SQL, "sql.alter_seq", SQLSTATE(42000) "ALTER SEQUENCE: maximum value is lesser than the minimum ("LLFMT" < "LLFMT")", nseq->maxvalue, nseq->minvalue);
	if (val) {
		if (is_lng_nil(*val))
			throw(SQL,"sql.alter_seq", SQLSTATE(42000) "ALTER SEQUENCE: sequence value must be non-NULL");
		if (nseq->minvalue && *val < nseq->minvalue)
			throw(SQL,"sql.alter_seq", SQLSTATE(42000) "ALTER SEQUENCE: cannot set sequence start to a value lesser than the minimum ("LLFMT" < "LLFMT")", *val, nseq->minvalue);
		if (nseq->maxvalue && *val > nseq->maxvalue)
			throw(SQL,"sql.alter_seq", SQLSTATE(42000) "ALTER SEQUENCE: cannot set sequence start to a value higher than the maximum ("LLFMT" > "LLFMT")", *val, nseq->maxvalue);
		if (!sql_trans_sequence_restart(sql->session->tr, nseq, *val))
			throw(SQL,"sql.alter_seq", SQLSTATE(42000) "ALTER SEQUENCE: failed to restart sequence %s.%s", sname, nseq->base.name);
	}
	return MAL_SUCCEED;
}

static str
drop_seq(mvc *sql, char *sname, char *name)
{
	sql_schema *s = NULL;
	sql_sequence *seq = NULL;

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.drop_seq", SQLSTATE(3F000) "DROP SEQUENCE: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.drop_seq", SQLSTATE(42000) "DROP SEQUENCE: insufficient privileges for '%s' in schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (!(seq = find_sql_sequence(sql->session->tr, s, name)))
		throw(SQL,"sql.drop_seq", SQLSTATE(42M35) "DROP SEQUENCE: no such sequence '%s'", name);
	if (mvc_check_dependency(sql, seq->base.id, BEDROPPED_DEPENDENCY, NULL))
		throw(SQL,"sql.drop_seq", SQLSTATE(2B000) "DROP SEQUENCE: unable to drop sequence %s (there are database objects which depend on it)\n", seq->base.name);

	sql_trans_drop_sequence(sql->session->tr, s, seq, 0);
	return NULL;
}

static str
drop_func(mvc *sql, char *sname, char *name, sqlid fid, sql_ftype type, int action)
{
	sql_schema *s = NULL;
	char *F = NULL, *fn = NULL;

	FUNC_TYPE_STR(type, F, fn)

	if (!(s = mvc_bind_schema(sql, sname))) {
		if (fid == -2) /* if exists option */
			return MAL_SUCCEED;
		throw(SQL,"sql.drop_func", SQLSTATE(3F000) "DROP %s: no such schema '%s'", F, sname);
	}
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.drop_func", SQLSTATE(42000) "DROP %s: access denied for %s to schema '%s'", F, get_string_global_var(sql, "current_user"), s->base.name);
	if (fid >= 0) {
		sql_base *b = os_find_id(s->funcs, sql->session->tr, fid);
		if (b) {
			sql_func *func = (sql_func*)b;

			if (!action && mvc_check_dependency(sql, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY, NULL))
				throw(SQL,"sql.drop_func", SQLSTATE(42000) "DROP %s: there are database objects dependent on %s %s;", F, fn, func->base.name);
			if (mvc_drop_func(sql, s, func, action))
				throw(SQL,"sql.drop_func", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	} else if (fid == -2) { /* if exists option */
		return MAL_SUCCEED;
	} else { /* fid == -1 */
		list *list_func = sql_find_funcs_by_name(sql, s->base.name, name, type);
		int res;

		if (list_func)
			for (node *n = list_func->h; n; n = n->next) {
				sql_func *func = n->data;

				if (!action && mvc_check_dependency(sql, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY, list_func)) {
					list_destroy(list_func);
					throw(SQL,"sql.drop_func", SQLSTATE(42000) "DROP %s: there are database objects dependent on %s %s;", F, fn, func->base.name);
				}
			}
		res = mvc_drop_all_func(sql, s, list_func, action);
		list_destroy(list_func);
		if (res)
			throw(SQL,"sql.drop_func", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

static char *
create_func(mvc *sql, char *sname, char *fname, sql_func *f)
{
	sql_func *nf;
	sql_schema *s = NULL;
	int clientid = sql->clientid;
	char *F = NULL, *fn = NULL;

	FUNC_TYPE_STR(f->type, F, fn)

	(void) fn;
	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.create_func", SQLSTATE(3F000) "CREATE %s: no such schema '%s'", F, sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.create_func", SQLSTATE(42000) "CREATE %s: access denied for %s to schema '%s'", F, get_string_global_var(sql, "current_user"), s->base.name);
	if (strlen(fname) >= IDLENGTH)
		throw(SQL,"sql.create_func", SQLSTATE(42000) "CREATE %s: name '%s' too large for the backend", F, fname);
	nf = mvc_create_func(sql, NULL, s, f->base.name, f->ops, f->res, f->type, f->lang, f->mod, f->imp, f->query, f->varres, f->vararg, f->system);
	assert(nf);
	switch (nf->lang) {
	case FUNC_LANG_INT:
	case FUNC_LANG_MAL: /* shouldn't be reachable, but leave it here */
		if (!backend_resolve_function(&clientid, nf))
			throw(SQL,"sql.create_func", SQLSTATE(3F000) "CREATE %s: external name %s.%s not bound", F, nf->mod, nf->base.name);
		if (nf->query == NULL)
			break;
		/* fall through */
	case FUNC_LANG_SQL: {
		char *buf;
		sql_rel *r = NULL;
		sql_allocator *sa = sql->sa;

		assert(nf->query);
		if (!(sql->sa = sa_create(sql->pa)))
			throw(SQL, "sql.create_func", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if (!(buf = sa_strdup(sql->sa, nf->query)))
			throw(SQL, "sql.create_func", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		r = rel_parse(sql, s, buf, m_deps);
		if (r)
			r = sql_processrelation(sql, r, 0, 0);
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
		if (!r) {
			if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
				throw(SQL, "sql.create_func", "%s", sql->errstr);
			else
				throw(SQL, "sql.create_func", SQLSTATE(42000) "%s", sql->errstr);
		}
	}
	default:
		break;
	}
	return MAL_SUCCEED;
}

static str
alter_table(Client cntxt, mvc *sql, char *sname, sql_table *t)
{
	sql_schema *s = NULL;
	sql_table *nt = NULL;
	node *n;

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.alter_table", SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && t->persistence == SQL_LOCAL_TEMP))
		throw(SQL,"sql.alter_table", SQLSTATE(42000) "ALTER TABLE: insufficient privileges for user '%s' in schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (!(nt = mvc_bind_table(sql, s, t->base.name)))
		throw(SQL,"sql.alter_table", SQLSTATE(42S02) "ALTER TABLE: no such table '%s'", t->base.name);

	/* First check if all the changes are allowed */
	if (t->idxs) {
		/* only one pkey */
		if (nt->pkey) {
			for (n = ol_first_node(t->idxs); n; n = n->next) {
				sql_idx *i = n->data;
				if (!i->base.new || i->base.deleted)
					continue;
				if (i->key && i->key->type == pkey)
					throw(SQL,"sql.alter_table", SQLSTATE(40000) "CONSTRAINT PRIMARY KEY: a table can have only one PRIMARY KEY\n");
			}
		}
	}

	for (n = ol_first_node(t->columns); n; n = n->next) {

		/* null or default value changes */
		sql_column *c = n->data;

		if (c->base.new)
			break;

		sql_column *nc = mvc_bind_column(sql, nt, c->base.name);
		if (c->base.deleted) {
			if (mvc_drop_column(sql, nt, nc, c->drop_action))
				throw(SQL,"sql.alter_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			continue;
		}
		if (c->null != nc->null && isTable(nt)) {
			if (c->null && nt->pkey) { /* check for primary keys based on this column */
				node *m;
				for (m = nt->pkey->k.columns->h; m; m = m->next) {
					sql_kc *kc = m->data;

					if (kc->c->base.id == c->base.id)
						throw(SQL,"sql.alter_table", SQLSTATE(40000) "NOT NULL CONSTRAINT: cannot change NOT NULL CONSTRAINT for column '%s' as its part of the PRIMARY KEY\n", c->base.name);
				}
			}
			mvc_null(sql, nc, c->null);
			/* for non empty check for nulls */
			sqlstore *store = sql->session->tr->store;
			if (c->null == 0) {
				const void *nilptr = ATOMnilptr(c->type.type->localtype);
				rids *nils = store->table_api.rids_select(sql->session->tr, nc, nilptr, NULL, NULL);
				int has_nils = !is_oid_nil(store->table_api.rids_next(nils));

				store->table_api.rids_destroy(nils);
				if (has_nils)
					throw(SQL,"sql.alter_table", SQLSTATE(40002) "ALTER TABLE: NOT NULL constraint violated for column %s.%s", c->t->base.name, c->base.name);
			}
		}
		if (c->def != nc->def)
			mvc_default(sql, nc, c->def);

		if (c->storage_type != nc->storage_type) {
			if (c->t->access == TABLE_WRITABLE)
				throw(SQL,"sql.alter_table", SQLSTATE(40002) "ALTER TABLE: SET STORAGE for column %s.%s only allowed on READ or INSERT ONLY tables", c->t->base.name, c->base.name);
			mvc_storage(sql, nc, c->storage_type);
		}
	}
	/* handle new columns */
	for (; n; n = n->next) {
		/* propagate alter table .. add column */
		sql_column *c = n->data;

		if (c->base.deleted) /* skip */
			continue;
		if (mvc_copy_column(sql, nt, c) == NULL)
			throw(SQL,"sql.alter_table", SQLSTATE(40002) "ALTER TABLE: Failed to create column %s.%s", c->t->base.name, c->base.name);
	}
	if (t->idxs) {
		/* alter drop index */
		if (t->idxs)
			for (n = ol_first_node(t->idxs); n; n = n->next) {
				sql_idx *i = n->data;
				if (i->base.new || !i->base.deleted)
					continue;
				sql_idx *ni = mvc_bind_idx(sql, s, i->base.name);
				if (mvc_drop_idx(sql, s, ni))
					throw(SQL,"sql.alter_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		/* alter add index */
		for (n = ol_first_node(t->idxs); n; n = n->next) {
			sql_idx *i = n->data;

			if (!i->base.new || i->base.deleted)
				continue;

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
			if (mvc_copy_idx(sql, nt, i) == NULL)
				throw(SQL,"sql.alter_table", SQLSTATE(40002) "ALTER TABLE: Failed to create index %s.%s", i->t->base.name, i->base.name);
		}
	}
	if (t->keys) {
		/* alter drop key */
		for (n = ol_first_node(t->keys); n; n = n->next) {
			sql_key *k = n->data;

			if ((!k->base.new && !k->base.deleted) || (k->base.new && k->base.deleted))
				continue;
			if (k->base.deleted) {
				sql_key *nk = mvc_bind_key(sql, s, k->base.name);
				if (nk) {
					if (mvc_drop_key(sql, s, nk, k->drop_action))
						throw(SQL,"sql.alter_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			} else { /* new */
				str err;
				if ((err = sql_partition_validate_key(sql, t, k, "ALTER")))
					return err;
				mvc_copy_key(sql, nt, k);
			}
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
	BAT *b = NULL;

	initcontext();
	if (getArgType(mb, pci, 4) == TYPE_lng)
		val = getArgReference_lng(stk, pci, 4);
	else if (isaBatType(getArgType(mb, pci, 4))) {
		bat *bid = getArgReference_bat(stk, pci, 4);

		if (!(b = BATdescriptor(*bid)))
			throw(SQL, "sql.alter_seq", SQLSTATE(HY005) "Cannot access column descriptor");
		if (BATcount(b) != 1) {
			BBPunfix(b->batCacheid);
			throw(SQL, "sql.alter_seq", SQLSTATE(42000) "Only one value allowed to alter a sequence value");
		}
		if (getBatType(getArgType(mb, pci, 4)) == TYPE_lng)
			val = (lng*)Tloc(b, 0);
	}

	if (val == NULL || is_lng_nil(*val))
		msg = createException(SQL,"sql.alter_seq", SQLSTATE(42M36) "ALTER SEQUENCE: cannot (re)start with NULL");
	else
		msg = alter_seq(sql, sname, seqname, s, val);

	if (b)
		BBPunfix(b->batCacheid);
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
	sqlid auth_id;

	initcontext();
	auth_id = sql->role_id;
	if (!strNil(name) && (auth_id = sql_find_auth(sql, name)) < 0)
		throw(SQL,"sql.create_schema", SQLSTATE(42M32) "CREATE SCHEMA: no such authorization '%s'", name);
	if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN)
		throw(SQL,"sql.create_schema", SQLSTATE(42000) "CREATE SCHEMA: insufficient privileges for user '%s'", get_string_global_var(sql, "current_user"));
	if (mvc_bind_schema(sql, sname))
		throw(SQL,"sql.create_schema", SQLSTATE(3F000) "CREATE SCHEMA: name '%s' already in use", sname);
	(void) mvc_create_schema(sql, sname, auth_id, sql->user_id);
	return msg;
}

str
SQLdrop_schema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	str sname = *getArgReference_str(stk, pci, 1);
	int if_exists = *getArgReference_int(stk, pci, 2);
	int action = *getArgReference_int(stk, pci, 3);
	sql_schema *s;

	initcontext();
	s = mvc_bind_schema(sql, sname);
	if (!s) {
		if (!if_exists)
			throw(SQL,"sql.drop_schema",SQLSTATE(3F000) "DROP SCHEMA: name %s does not exist", sname);
		return MAL_SUCCEED;
	}
	sql_trans *tr = sql->session->tr;
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.drop_schema",SQLSTATE(42000) "DROP SCHEMA: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (s == cur_schema(sql))
		throw(SQL,"sql.drop_schema",SQLSTATE(42000) "DROP SCHEMA: cannot drop current schema");
	if (s->system)
		throw(SQL,"sql.drop_schema",SQLSTATE(42000) "DROP SCHEMA: access denied for '%s'", sname);
	if (sql_schema_has_user(sql, s))
		throw(SQL,"sql.drop_schema",SQLSTATE(2BM37) "DROP SCHEMA: unable to drop schema '%s' (there are database objects which depend on it)", sname);
	if (!action /* RESTRICT */ && (
		os_size(s->tables, tr) || os_size(s->types, tr) || os_size(s->funcs, tr) || os_size(s->seqs, tr)))
		throw(SQL,"sql.drop_schema",SQLSTATE(2BM37) "DROP SCHEMA: unable to drop schema '%s' (there are database objects which depend on it)", sname);

	if (mvc_drop_schema(sql, s, action))
		throw(SQL,"sql.drop_schema", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
SQLcreate_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	//str tname = *getArgReference_str(stk, pci, 2);
	sql_table *t = *(sql_table **) getArgReference(stk, pci, 3);
	int temp = *getArgReference_int(stk, pci, 4);

	initcontext();
	msg = create_table_or_view(sql, sname, t->base.name, t, temp);
	return msg;
}

str
SQLcreate_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	//str vname = *getArgReference_str(stk, pci, 2);
	sql_table *t = *(sql_table **) getArgReference(stk, pci, 3);
	int temp = *getArgReference_int(stk, pci, 4);

	initcontext();
	msg = create_table_or_view(sql, sname, t->base.name, t, temp);
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
	str tname = *getArgReference_str(stk, pci, 2);
	str kname = *getArgReference_str(stk, pci, 3);
	int action = *getArgReference_int(stk, pci, 5);
	(void) *getArgReference_int(stk, pci, 4); //the if_exists parameter is also passed but not used

	initcontext();
	msg = drop_key(sql, sname, tname, kname, action);
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
	sql_schema *s = NULL;

	initcontext();

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.create_type",SQLSTATE(3F000) "CREATE TYPE: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.create_type", SQLSTATE(42000) "CREATE TYPE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (schema_bind_type(sql, s, name))
		throw(SQL,"sql.create_type", SQLSTATE(42S02) "CREATE TYPE: type '%s' already exists", name);
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
	sql_schema *s = NULL;
	sql_type *t;

	initcontext();

	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.drop_type",SQLSTATE(3F000) "DROP TYPE: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL,"sql.drop_type", SQLSTATE(42000) "DROP TYPE:  access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (!(t = schema_bind_type(sql, s, name)))
		throw(SQL,"sql.drop_type", SQLSTATE(3F000) "DROP TYPE: type '%s' does not exist", name);
	if (!drop_action && mvc_check_dependency(sql, t->base.id, TYPE_DEPENDENCY, NULL))
		throw(SQL,"sql.drop_type", SQLSTATE(42000) "DROP TYPE: unable to drop type %s (there are database objects which depend on it)\n", name);
	if (!mvc_drop_type(sql, s, t, drop_action))
		throw(SQL,"sql.drop_type", SQLSTATE(0D000) "DROP TYPE: failed to drop type '%s'", name);
	return msg;
}

str
SQLgrant_roles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	char *auth = SaveArgReference(stk, pci, 2);
	sqlid grantor = (sqlid) *getArgReference_int(stk, pci, 3);
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
	sqlid grantor = (sqlid) *getArgReference_int(stk, pci, 3);
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
	sqlid grantor = (sqlid) *getArgReference_int(stk, pci, 7);

	initcontext();
	if (strNil(tname))
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
	sqlid grantor = (sqlid) *getArgReference_int(stk, pci, 7);

	initcontext();
	if (strNil(tname))
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
	sqlid func_id = (sqlid) *getArgReference_int(stk, pci, 2);
	char *grantee = *getArgReference_str(stk, pci, 3);
	int privs = *getArgReference_int(stk, pci, 4);
	int grant = *getArgReference_int(stk, pci, 5);
	sqlid grantor = (sqlid) *getArgReference_int(stk, pci, 6);

	initcontext();
	msg = sql_grant_func_privs(sql, grantee, privs, sname, func_id, grant, grantor);
	return msg;
}

str
SQLrevoke_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	mvc *sql = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 1);
	sqlid func_id = (sqlid) *getArgReference_int(stk, pci, 2);
	char *grantee = *getArgReference_str(stk, pci, 3);
	int privs = *getArgReference_int(stk, pci, 4);
	int grant = *getArgReference_int(stk, pci, 5);
	sqlid grantor = (sqlid) *getArgReference_int(stk, pci, 6);

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
	char *schema_path = SaveArgReference(stk, pci, 5);
	char *fullname = SaveArgReference(stk, pci, 6);

	initcontext();
	msg = sql_create_user(sql, sname, passwd, enc, fullname, schema, schema_path);
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
	char *schema_path = SaveArgReference(stk, pci, 5);
	char *oldpasswd = SaveArgReference(stk, pci, 6);

	initcontext();
	msg = sql_alter_user(sql, sname, passwd, enc, schema, schema_path, oldpasswd);

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
	sqlid grantor = (sqlid)*getArgReference_int(stk, pci, 3);

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
	sqlid fid = (sqlid)*getArgReference_int(stk, pci, 3);
	sql_ftype type = (sql_ftype) *getArgReference_int(stk, pci, 4);
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
	old_name=(strNil(old_name))?NULL:old_name;
	new_name=(strNil(new_name))?NULL:new_name;
	condition=(strNil(condition))?NULL:condition;
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
	bit with_nills = *getArgReference_bit(stk, pci, 7);
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
	bit with_nills = *getArgReference_bit(stk, pci, 5);
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
	sqlid objid = (sqlid) *getArgReference_int(stk, pci, 1);
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
	sqlstore *store = tx->store;
	rid = store->table_api.column_find_row(tx, id_col, &objid, NULL);
	if (!strNil(remark) && *remark) {
		if (!is_oid_nil(rid)) {
			// have new remark and found old one, so update field
			/* UPDATE sys.comments SET remark = %s WHERE id = %d */
			ok = store->table_api.column_update_value(tx, remark_col, rid, remark);
		} else {
			// have new remark but found none so insert row
			/* INSERT INTO sys.comments (id, remark) VALUES (%d, %s) */
			ok = store->table_api.table_insert(tx, comments, &objid, &remark);
		}
	} else {
		if (!is_oid_nil(rid)) {
			// have no remark but found one, so delete row
			/* DELETE FROM sys.comments WHERE id = %d */
			ok = store->table_api.table_delete(tx, comments, rid);
		}
	}
	if (ok != LOG_OK)
		throw(SQL, "sql.comment_on", SQLSTATE(3F000) "operation failed");
	return MAL_SUCCEED;
}

str
SQLrename_schema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	str old_name = *getArgReference_str(stk, pci, 1);
	str new_name = *getArgReference_str(stk, pci, 2);
	sql_schema *s;

	initcontext();
	sql_trans *tr = sql->session->tr;
	if (!(s = mvc_bind_schema(sql, old_name)))
		throw(SQL, "sql.rename_schema", SQLSTATE(42S02) "ALTER SCHEMA: no such schema '%s'", old_name);
	if (!mvc_schema_privs(sql, s))
		throw(SQL, "sql.rename_schema", SQLSTATE(42000) "ALTER SCHEMA: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), old_name);
	if (s->system)
		throw(SQL, "sql.rename_schema", SQLSTATE(3F000) "ALTER SCHEMA: cannot rename a system schema");
	if (os_size(s->tables, tr) || os_size(s->types, tr) || os_size(s->funcs, tr) || os_size(s->seqs, tr))
		throw(SQL, "sql.rename_schema", SQLSTATE(2BM37) "ALTER SCHEMA: unable to rename schema '%s' (there are database objects which depend on it)", old_name);
	if (strNil(new_name) || *new_name == '\0')
		throw(SQL, "sql.rename_schema", SQLSTATE(3F000) "ALTER SCHEMA: invalid new schema name");
	if (mvc_bind_schema(sql, new_name))
		throw(SQL, "sql.rename_schema", SQLSTATE(3F000) "ALTER SCHEMA: there is a schema named '%s' in the database", new_name);

	if (!sql_trans_rename_schema(sql->session->tr, s->base.id, new_name))
		throw(SQL, "sql.rename_schema",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (s == cur_schema(sql)) /* change current session schema name */
		if (!mvc_set_schema(sql, new_name))
			throw(SQL, "sql.rename_schema",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
SQLrename_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	str oschema_name = *getArgReference_str(stk, pci, 1);
	str nschema_name = *getArgReference_str(stk, pci, 2);
	str otable_name = *getArgReference_str(stk, pci, 3);
	str ntable_name = *getArgReference_str(stk, pci, 4);
	sql_schema *o, *s;
	sql_table *t;

	initcontext();

	if (strcmp(oschema_name, nschema_name) == 0) { //renaming the table itself
		if (!(s = mvc_bind_schema(sql, oschema_name)))
			throw(SQL, "sql.rename_table", SQLSTATE(42S02) "ALTER TABLE: no such schema '%s'", oschema_name);
		if (!mvc_schema_privs(sql, s))
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), oschema_name);
		if (!(t = mvc_bind_table(sql, s, otable_name)))
			throw(SQL, "sql.rename_table", SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", otable_name, oschema_name);
		if (t->system)
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: cannot rename a system table");
		if (isView(t))
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: cannot rename a view");
		if (isDeclaredTable(t))
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: cannot rename a declared table");
		if (mvc_check_dependency(sql, t->base.id, TABLE_DEPENDENCY, NULL))
			throw (SQL,"sql.rename_table", SQLSTATE(2BM37) "ALTER TABLE: unable to rename table '%s' (there are database objects which depend on it)", otable_name);
		if (strNil(ntable_name) || *ntable_name == '\0')
			throw(SQL, "sql.rename_table", SQLSTATE(3F000) "ALTER TABLE: invalid new table name");
		if (mvc_bind_table(sql, s, ntable_name))
			throw(SQL, "sql.rename_table", SQLSTATE(3F000) "ALTER TABLE: there is a table named '%s' in schema '%s'", ntable_name, oschema_name);

		if (!sql_trans_rename_table(sql->session->tr, s, t->base.id, ntable_name))
			throw(SQL, "sql.rename_table",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else { //changing the schema of the table
		assert(strcmp(otable_name, ntable_name) == 0);

		if (!(o = mvc_bind_schema(sql, oschema_name)))
			throw(SQL, "sql.rename_table", SQLSTATE(42S02) "ALTER TABLE: no such schema '%s'", oschema_name);
		if (!mvc_schema_privs(sql, o))
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), oschema_name);
		if (!(t = mvc_bind_table(sql, o, otable_name)))
			throw(SQL, "sql.rename_table", SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", otable_name, oschema_name);
		if (t->system)
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: cannot set schema of a system table");
		if (isTempSchema(o))
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: not possible to change a temporary table schema");
		if (isView(t))
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: not possible to change schema of a view");
		if (isDeclaredTable(t))
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: not possible to change schema of a declared table");
		if (mvc_check_dependency(sql, t->base.id, TABLE_DEPENDENCY, NULL) || list_length(t->members) || ol_length(t->triggers))
			throw(SQL, "sql.rename_table", SQLSTATE(2BM37) "ALTER TABLE: unable to set schema of table '%s' (there are database objects which depend on it)", otable_name);
		if (!(s = mvc_bind_schema(sql, nschema_name)))
			throw(SQL, "sql.rename_table", SQLSTATE(42S02) "ALTER TABLE: no such schema '%s'", nschema_name);
		if (!mvc_schema_privs(sql, s))
			throw(SQL, "sql.rename_table", SQLSTATE(42000) "ALTER TABLE: access denied for '%s' to schema '%s'", get_string_global_var(sql, "current_user"), nschema_name);
		if (isTempSchema(s))
			throw(SQL, "sql.rename_table", SQLSTATE(3F000) "ALTER TABLE: not possible to change table's schema to temporary");
		if (mvc_bind_table(sql, s, otable_name))
			throw(SQL, "sql.rename_table", SQLSTATE(42S02) "ALTER TABLE: table '%s' on schema '%s' already exists", otable_name, nschema_name);

		if (!sql_trans_set_table_schema(sql->session->tr, t->base.id, o, s))
			throw(SQL, "sql.rename_table",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	return msg;
}

str
SQLrename_column(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	str schema_name = *getArgReference_str(stk, pci, 1);
	str table_name = *getArgReference_str(stk, pci, 2);
	str old_name = *getArgReference_str(stk, pci, 3);
	str new_name = *getArgReference_str(stk, pci, 4);
	sql_schema *s;
	sql_table *t;
	sql_column *col;

	initcontext();
	if (!(s = mvc_bind_schema(sql, schema_name)))
		throw(SQL, "sql.rename_column", SQLSTATE(42S02) "ALTER TABLE: no such schema '%s'", schema_name);
	if (!mvc_schema_privs(sql, s))
		throw(SQL, "sql.rename_column", SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), schema_name);
	if (!(t = mvc_bind_table(sql, s, table_name)))
		throw(SQL, "sql.rename_column", SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", table_name, schema_name);
	if (t->system)
		throw(SQL, "sql.rename_column", SQLSTATE(42000) "ALTER TABLE: cannot rename a column in a system table");
	if (isView(t))
		throw(SQL, "sql.rename_column", SQLSTATE(42000) "ALTER TABLE: cannot rename column '%s': '%s' is a view", old_name, table_name);
	if (isDeclaredTable(t))
		throw(SQL, "sql.rename_column", SQLSTATE(42000) "ALTER TABLE: cannot rename column in a declared table");
	if (!(col = mvc_bind_column(sql, t, old_name)))
		throw(SQL, "sql.rename_column", SQLSTATE(42S22) "ALTER TABLE: no such column '%s' in table '%s'", old_name, table_name);
	if (mvc_check_dependency(sql, col->base.id, COLUMN_DEPENDENCY, NULL))
		throw(SQL, "sql.rename_column", SQLSTATE(2BM37) "ALTER TABLE: cannot rename column '%s' (there are database objects which depend on it)", old_name);
	if (strNil(new_name) || *new_name == '\0')
		throw(SQL, "sql.rename_column", SQLSTATE(3F000) "ALTER TABLE: invalid new column name");
	if (mvc_bind_column(sql, t, new_name))
		throw(SQL, "sql.rename_column", SQLSTATE(3F000) "ALTER TABLE: there is a column named '%s' in table '%s'", new_name, table_name);

	if (!sql_trans_rename_column(sql->session->tr, t, old_name, new_name))
		throw(SQL, "sql.rename_column",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}
