/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) M Kersten, N Nes
 * SQL support implementation
 * This module contains the wrappers around the SQL
 * multi-version-catalog and support routines.
 */
#ifndef _SQL_H
#define _SQL_H

#include <sql_mem.h>

#ifdef WIN32
#ifndef LIBSQL
#define sql5_export extern __declspec(dllimport)
#else
#define sql5_export extern __declspec(dllexport)
#endif
#else
#define sql5_export extern
#endif

#include "mal_backend.h"
#include "sql_mvc.h"
#include <sql_backend.h>
#include <mal_session.h>

#include <mal_function.h>
#include <mal_stack.h>
#include <mal_interpreter.h>

#include <tablet.h>
#include <streams.h>
#include <mtime.h>
#include <math.h>
#include <blob.h>
#include <mkey.h>
#include <str.h>
#include "sql_privileges.h"
#include "sql_decimal.h"
#include "sql_string.h"
#include "sql_qc.h"
#include "sql_env.h"
#include "sql_statement.h"
#include "querylog.h"

#include <bat/bat_storage.h>
#include <bat/bat_utils.h>

extern int sqlcleanup(mvc *c, int err);
extern sql_rel *sql_symbol2relation(mvc *c, symbol *sym);

extern BAT *mvc_bind(mvc *m, const char *sname, const char *tname, const char *cname, int access);
extern BAT *mvc_bind_idxbat(mvc *m, const char *sname, const char *tname, const char *iname, int access);

sql5_export str SQLmvc(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLabort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLshutdown_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLtransaction2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str mvc_append_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_append_column(sql_trans *t, sql_column *c, BAT *ins);

sql5_export str mvc_update_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bind_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bind_idxbat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_clear_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_delete_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLtid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str DELTAbat(bat *result, const bat *col, const bat *uid, const bat *uval, const bat *ins);
sql5_export str DELTAsub(bat *result, const bat *col, const bat *cid, const bat *uid, const bat *uval, const bat *ins);
sql5_export str DELTAproject(bat *result, const bat *select, const bat *col, const bat *uid, const bat *uval, const bat *ins);
sql5_export str DELTAbat2(bat *result, const bat *col, const bat *uid, const bat *uval);
sql5_export str DELTAsub2(bat *result, const bat *col, const bat *cid, const bat *uid, const bat *uval);
sql5_export str DELTAproject2(bat *result, const bat *select, const bat *col, const bat *uid, const bat *uval);

sql5_export str BATleftproject(bat *result, const bat *col, const bat *l, const bat *r);

sql5_export str mvc_table_result_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str mvc_export_row_wrap( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_table_wrap( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str mvc_declared_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_declared_table_column_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_drop_declared_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_drop_declared_tables_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str UPGdrop_func(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str UPGcreate_func(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str UPGcreate_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str mvc_affected_rows_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_result_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_head_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_chunk_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_operation_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_scalar_value_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_row_result_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_row_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bin_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str setVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str getVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_variables(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_logfile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bat_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_get_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_getVersion(lng *r, const int *clientid);
sql5_export str mvc_restart_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str zero_or_one(ptr ret, const bat *bid);
sql5_export str SQLall(ptr ret, const bat *bid);
sql5_export str not_unique(bit *ret, const bat *bid);
sql5_export str SQLshrink(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLreuse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLvacuum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLdrop_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLnewDictionary(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLdropDictionary(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLargRecord(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLoptimizersUpdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str month_interval_str(int *ret, const str *s, const int *ek, const int *sk);
sql5_export str second_interval_str(lng *res, const str *s, const int *ek, const int *sk);
sql5_export str dump_cache(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dump_opt_stats(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dump_trace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_sessions_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_storage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_querylog_catalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_querylog_calls(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_querylog_empty(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_rowid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_rank_grp(bat *rid, const bat *bid, const bat *gid, const bat *gpe);
sql5_export str sql_rank(bat *rid, const bat *bid);
sql5_export str sql_dense_rank_grp(bat *rid, const bat *bid, const bat *gid, const bat *gpe);
sql5_export str sql_dense_rank(bat *rid, const bat *bid);
sql5_export str SQLidentity(oid *rid, const void *i);
sql5_export str BATSQLidentity(bat *rid, const bat *bid);
sql5_export str PBATSQLidentity(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str create_table_or_view(mvc *sql, char *sname, sql_table *t, int temp);
sql5_export str create_table_from_emit(Client cntxt, char *sname, char *tname, sql_emit_col *columns, size_t ncols);

sql5_export str bte_dec_round_wrap(bte *res, const bte *v, const bte *r);
sql5_export str bte_bat_dec_round_wrap(bat *res, const bat *v, const bte *r);
sql5_export str bte_round_wrap(bte *res, const bte *v, const int *d, const int *s, const bte *r);
sql5_export str bte_bat_round_wrap(bat *res, const bat *v, const int *d, const int *s, const bte *r);
sql5_export str str_2dec_bte(bte *res, const str *val, const int *d, const int *sc);
sql5_export str str_2num_bte(bte *res, const str *v, const int *len);
sql5_export str batstr_2dec_bte(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batstr_2num_bte(bat *res, const bat *v, const int *len);
sql5_export str bte_dec2second_interval(lng *res, const int *sc, const bte *dec, const int *ek, const int *sk);

sql5_export str nil_2dec_bte(bte *res, const void *val, const int *d, const int *sc);
sql5_export str nil_2num_bte(bte *res, const void *v, const int *len);
sql5_export str batnil_2dec_bte(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batnil_2num_bte(bat *res, const bat *v, const int *len);

sql5_export str sht_dec_round_wrap(sht *res, const sht *v, const sht *r);
sql5_export str sht_bat_dec_round_wrap(bat *res, const bat *v, const sht *r);
sql5_export str sht_round_wrap(sht *res, const sht *v, const int *d, const int *s, const bte *r);
sql5_export str sht_bat_round_wrap(bat *res, const bat *v, const int *d, const int *s, const bte *r);
sql5_export str str_2dec_sht(sht *res, const str *val, const int *d, const int *sc);
sql5_export str str_2num_sht(sht *res, const str *v, const int *len);
sql5_export str batstr_2dec_sht(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batstr_2num_sht(bat *res, const bat *v, const int *len);
sql5_export str sht_dec2second_interval(lng *res, const int *sc, const sht *dec, const int *ek, const int *sk);

sql5_export str nil_2dec_sht(sht *res, const void *val, const int *d, const int *sc);
sql5_export str nil_2num_sht(sht *res, const void *v, const int *len);
sql5_export str batnil_2dec_sht(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batnil_2num_sht(bat *res, const bat *v, const int *len);

sql5_export str int_dec_round_wrap(int *res, const int *v, const int *r);
sql5_export str int_bat_dec_round_wrap(bat *res, const bat *v, const int *r);
sql5_export str int_round_wrap(int *res, const int *v, const int *d, const int *s, const bte *r);
sql5_export str int_bat_round_wrap(bat *res, const bat *v, const int *d, const int *s, const bte *r);
sql5_export str str_2dec_int(int *res, const str *val, const int *d, const int *sc);
sql5_export str str_2num_int(int *res, const str *v, const int *len);
sql5_export str batstr_2dec_int(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batstr_2num_int(bat *res, const bat *v, const int *len);
sql5_export str int_dec2second_interval(lng *res, const int *sc, const int *dec, const int *ek, const int *sk);

sql5_export str nil_2dec_int(int *res, const void *val, const int *d, const int *sc);
sql5_export str nil_2num_int(int *res, const void *v, const int *len);
sql5_export str batnil_2dec_int(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batnil_2num_int(bat *res, const bat *v, const int *len);

sql5_export str lng_dec_round_wrap(lng *res, const lng *v, const lng *r);
sql5_export str lng_bat_dec_round_wrap(bat *res, const bat *v, const lng *r);
sql5_export str lng_round_wrap(lng *res, const lng *v, const int *d, const int *s, const bte *r);
sql5_export str lng_bat_round_wrap(bat *res, const bat *v, const int *d, const int *s, const bte *r);
sql5_export str str_2dec_lng(lng *res, const str *val, const int *d, const int *sc);
sql5_export str str_2num_lng(lng *res, const str *v, const int *len);
sql5_export str batstr_2dec_lng(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batstr_2num_lng(bat *res, const bat *v, const int *len);
sql5_export str lng_dec2second_interval(lng *res, const int *sc, const lng *dec, const int *ek, const int *sk);

sql5_export str nil_2dec_lng(lng *res, const void *val, const int *d, const int *sc);
sql5_export str nil_2num_lng(lng *res, const void *v, const int *len);
sql5_export str batnil_2dec_lng(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batnil_2num_lng(bat *res, const bat *v, const int *len);

#ifdef HAVE_HGE
sql5_export str hge_dec_round_wrap(hge *res, const hge *v, const hge *r);
sql5_export str hge_bat_dec_round_wrap(bat *res, const bat *v, const hge *r);
sql5_export str hge_round_wrap(hge *res, const hge *v, const int *d, const int *s, const bte *r);
sql5_export str hge_bat_round_wrap(bat *res, const bat *v, const int *d, const int *s, const bte *r);
sql5_export str str_2dec_hge(hge *res, const str *val, const int *d, const int *sc);
sql5_export str str_2num_hge(hge *res, const str *v, const int *len);
sql5_export str batstr_2dec_hge(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batstr_2num_hge(bat *res, const bat *v, const int *len);
sql5_export str hge_dec2second_interval(lng *res, const int *sc, const hge *dec, const int *ek, const int *sk);

sql5_export str nil_2dec_hge(hge *res, const void *val, const int *d, const int *sc);
sql5_export str nil_2num_hge(hge *res, const void *v, const int *len);
sql5_export str batnil_2dec_hge(bat *res, const bat *val, const int *d, const int *sc);
sql5_export str batnil_2num_hge(bat *res, const bat *v, const int *len);
#endif

sql5_export str nil_2time_timestamp(timestamp *res, const void *v, const int *len);
sql5_export str batnil_2time_timestamp(bat *res, const bat *v, const int *len);
sql5_export str str_2time_timestamp(timestamp *res, const str *v, const int *len);
sql5_export str str_2time_timestamptz(timestamp *res, const str *v, const int *len, int *tz);
sql5_export str batstr_2time_timestamp(bat *res, const bat *v, const int *len);
sql5_export str batstr_2time_timestamptz(bat *res, const bat *v, const int *len, int *tz);
sql5_export str timestamp_2time_timestamp(timestamp *res, const timestamp *v, const int *len);
sql5_export str battimestamp_2time_timestamp(bat *res, const bat *v, const int *len);

sql5_export str nil_2time_daytime(daytime *res, const void *v, const int *len);
sql5_export str batnil_2time_daytime(bat *res, const bat *v, const int *len);
sql5_export str str_2time_daytime(daytime *res, const str *v, const int *len);
sql5_export str str_2time_daytimetz(daytime *res, const str *v, const int *len, int *tz);
sql5_export str batstr_2time_daytime(bat *res, const bat *v, const int *len);
sql5_export str batstr_2time_daytimetz(bat *res, const bat *v, const int *len, int *tz);
sql5_export str daytime_2time_daytime(daytime *res, const daytime *v, const int *len);
sql5_export str batdaytime_2time_daytime(bat *res, const bat *v, const int *len);

sql5_export str nil_2_timestamp(timestamp *res, const void *val);
sql5_export str batnil_2_timestamp(bat *res, const bat *val);
sql5_export str str_2_timestamp(timestamp *res, const str *val);
sql5_export str batstr_2_timestamp(bat *res, const bat *val);

sql5_export str nil_2_daytime(daytime *res, const void *val);
sql5_export str batnil_2_daytime(bat *res, const bat *val);
sql5_export str str_2_daytime(daytime *res, const str *val);
sql5_export str batstr_2_daytime(bat *res, const bat *val);

sql5_export str nil_2_date(date *res, const void *val);
sql5_export str batnil_2_date(bat *res, const bat *val);
sql5_export str str_2_date(date *res, const str *val);
sql5_export str batstr_2_date(bat *res, const bat *val);
sql5_export str SQLdate_2_str(str *res, const date *val);

sql5_export str str_2_sqlblob(sqlblob * *res, const str *val);
sql5_export str batstr_2_sqlblob(bat *res, const bat *val);
sql5_export str SQLsqlblob_2_str(str *res, const sqlblob * val);


sql5_export str SQLstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLbatstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str flt_dec_round_wrap(flt *res, const flt *v, const flt *r);
sql5_export str flt_bat_dec_round_wrap(bat *res, const bat *v, const flt *r);
sql5_export str flt_round_wrap(flt *res, const flt *v, const bte *r);
sql5_export str flt_bat_round_wrap(bat *res, const bat *v, const bte *r);
sql5_export str flt_trunc_wrap(flt *res, const flt *v, const int *r);

sql5_export str dbl_dec_round_wrap(dbl *res, const dbl *v, const dbl *r);
sql5_export str dbl_bat_dec_round_wrap(bat *res, const bat *v, const dbl *r);
sql5_export str dbl_round_wrap(dbl *res, const dbl *v, const bte *r);
sql5_export str dbl_bat_round_wrap(bat *res, const bat *v, const bte *r);
sql5_export str dbl_trunc_wrap(dbl *res, const dbl *v, const int *r);

#define radians(x)	((x) * 3.14159265358979323846 /180.0 )
#define degrees(x)	((x) * 180.0/3.14159265358979323846 )

sql5_export str SQLcst_alpha_cst(dbl *res, const dbl *decl, const dbl *theta);
sql5_export str SQLbat_alpha_cst(bat *res, const bat *decl, const dbl *theta);
sql5_export str SQLcst_alpha_bat(bat *res, const dbl *decl, const bat *theta);
sql5_export str month_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str second_interval_daytime(lng *res, const daytime *s, const int *ek, const int *sk);

#include "sql_cast.h"

sql5_export str checkSQLContext(Client cntxt);
sql5_export str getSQLContext(Client cntxt, MalBlkPtr mb, mvc **c, backend **b);

sql5_export void freeVariables(Client c, MalBlkPtr mb, MalStkPtr glb, int start);
sql5_export str second_interval_daytime(lng *res, const daytime *s, const int *ek, const int *sk);
sql5_export str second_interval_2_daytime(daytime *res, const lng *s, const int *d);
sql5_export str timestamp_2_daytime(daytime *res, const timestamp *v, const int *d);
sql5_export str date_2_timestamp(timestamp *res, const date *v, const int *d);
sql5_export str SQLcurrent_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcurrent_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str STRindex_int(int *res, const str *src, const bit *u);
sql5_export str BATSTRindex_int(bat *res, const bat *src, const bit *u);
sql5_export str STRindex_sht(sht *res, const str *src, const bit *u);
sql5_export str BATSTRindex_sht(bat *res, const bat *src, const bit *u);
sql5_export str STRindex_bte(bte *res, const str *src, const bit *u);
sql5_export str BATSTRindex_bte(bat *res, const bat *src, const bit *u);
sql5_export str STRstrings(str *res, const str *src);
sql5_export str BATSTRstrings(bat *res, const bat *src);

sql5_export str SQLflush_log(void *ret);

#endif /* _SQL_H */
