/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (author) M Kersten, N Nes
 * SQL support implementation
 * This module contains the wrappers around the SQL
 * multi-version-catalog and support routines.
 */
#ifndef _SQL_H
#define _SQL_H

#include "sql_mem.h"

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
#include "sql_backend.h"
#include "mal_session.h"

#include "mal_function.h"
#include "mal_stack.h"
#include "mal_interpreter.h"

#include "tablet.h"
#include "gdk_time.h"
#include "blob.h"
#include "str.h"
#include "sql_privileges.h"
#include "sql_decimal.h"
#include "sql_string.h"
#include "sql_qc.h"
#include "sql_env.h"
#include "sql_statement.h"
#include "querylog.h"

#include "bat/bat_storage.h"
#include "bat/bat_utils.h"

extern int sqlcleanup(backend *be, int err);
extern sql_rel *sql_symbol2relation(backend *be, symbol *sym);

extern BAT *mvc_bind(mvc *m, const char *sname, const char *tname, const char *cname, int access);
extern BAT *mvc_bind_idxbat(mvc *m, const char *sname, const char *tname, const char *iname, int access);

sql5_export str SQLmvc(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLabort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLtransaction2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str mvc_grow_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_claim_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_append_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_append_column(sql_trans *t, sql_column *c, size_t pos, BAT *ins);

sql5_export str mvc_update_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bind_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_delta_values(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bind_idxbat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_clear_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_delete_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLtid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str DELTAbat(bat *result, const bat *col, const bat *uid, const bat *uval);
sql5_export str DELTAsub(bat *result, const bat *col, const bat *cid, const bat *uid, const bat *uval);
sql5_export str DELTAproject(bat *result, const bat *select, const bat *col, const bat *uid, const bat *uval);

sql5_export str BATleftproject(bat *result, const bat *col, const bat *l, const bat *r);

sql5_export str mvc_table_result_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str mvc_export_table_wrap( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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
sql5_export str mvc_bin_import_column_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str setVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str getVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_variables(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_logfile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bat_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_get_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bat_get_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_getVersion(lng *r, const int *clientid);
sql5_export str mvc_restart_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bat_restart_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str not_unique(bit *ret, const bat *bid);
sql5_export str SQLdrop_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLargRecord(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLoptimizersUpdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str month_interval_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str second_interval_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dump_cache(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dump_opt_stats(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dump_trace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_rt_credentials_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
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
sql5_export str create_table_or_view(mvc *sql, char* sname, char *tname, sql_table *t, int temp);
sql5_export str create_table_from_emit(Client cntxt, char *sname, char *tname, sql_emit_col *columns, size_t ncols);
sql5_export str append_to_table_from_emit(Client cntxt, char *sname, char *tname, sql_emit_col *columns, size_t ncols);

sql5_export str bte_dec_round_wrap(bte *res, const bte *v, const bte *r);
sql5_export str bte_bat_dec_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str bte_bat_dec_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str bte_bat_dec_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str bte_round_wrap(bte *res, const bte *v, const bte *r, const int *d, const int *s);
sql5_export str bte_bat_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str bte_bat_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str bte_bat_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);


sql5_export str str_2dec_bte(bte *res, const str *val, const int *d, const int *sc);
sql5_export str batstr_2dec_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str bte_dec2second_interval(lng *res, const int *sc, const bte *dec, const int *ek, const int *sk);
sql5_export str bte_batdec2second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str nil_2dec_bte(bte *res, const void *val, const int *d, const int *sc);
sql5_export str batnil_2dec_bte(bat *res, const bat *val, const int *d, const int *sc);

sql5_export str sht_dec_round_wrap(sht *res, const sht *v, const sht *r);
sql5_export str sht_bat_dec_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sht_bat_dec_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sht_bat_dec_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sht_round_wrap(sht *res, const sht *v, const bte *r, const int *d, const int *s);
sql5_export str sht_bat_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sht_bat_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sht_bat_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str str_2dec_sht(sht *res, const str *val, const int *d, const int *sc);
sql5_export str batstr_2dec_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sht_dec2second_interval(lng *res, const int *sc, const sht *dec, const int *ek, const int *sk);
sql5_export str sht_batdec2second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str nil_2dec_sht(sht *res, const void *val, const int *d, const int *sc);
sql5_export str batnil_2dec_sht(bat *res, const bat *val, const int *d, const int *sc);

sql5_export str int_dec_round_wrap(int *res, const int *v, const int *r);
sql5_export str int_bat_dec_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str int_bat_dec_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str int_bat_dec_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str int_round_wrap(int *res, const int *v, const bte *r, const int *d, const int *s);
sql5_export str int_bat_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str int_bat_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str int_bat_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str str_2dec_int(int *res, const str *val, const int *d, const int *sc);
sql5_export str batstr_2dec_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str int_dec2second_interval(lng *res, const int *sc, const int *dec, const int *ek, const int *sk);
sql5_export str int_batdec2second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str nil_2dec_int(int *res, const void *val, const int *d, const int *sc);
sql5_export str batnil_2dec_int(bat *res, const bat *val, const int *d, const int *sc);

sql5_export str lng_dec_round_wrap(lng *res, const lng *v, const lng *r);
sql5_export str lng_bat_dec_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str lng_bat_dec_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str lng_bat_dec_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str lng_round_wrap(lng *res, const lng *v, const bte *r, const int *d, const int *s);
sql5_export str lng_bat_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str lng_bat_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str lng_bat_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str str_2dec_lng(lng *res, const str *val, const int *d, const int *sc);
sql5_export str batstr_2dec_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str lng_dec2second_interval(lng *res, const int *sc, const lng *dec, const int *ek, const int *sk);
sql5_export str lng_batdec2second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str nil_2dec_lng(lng *res, const void *val, const int *d, const int *sc);
sql5_export str batnil_2dec_lng(bat *res, const bat *val, const int *d, const int *sc);

#ifdef HAVE_HGE
sql5_export str hge_dec_round_wrap(hge *res, const hge *v, const hge *r);
sql5_export str hge_bat_dec_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str hge_bat_dec_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str hge_bat_dec_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str hge_round_wrap(hge *res, const hge *v, const bte *r, const int *d, const int *s);
sql5_export str hge_bat_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str hge_bat_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str hge_bat_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str str_2dec_hge(hge *res, const str *val, const int *d, const int *sc);
sql5_export str batstr_2dec_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str hge_dec2second_interval(lng *res, const int *sc, const hge *dec, const int *ek, const int *sk);
sql5_export str hge_batdec2second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str nil_2dec_hge(hge *res, const void *val, const int *d, const int *sc);
sql5_export str batnil_2dec_hge(bat *res, const bat *val, const int *d, const int *sc);
#endif

sql5_export str nil_2time_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str str_2time_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str str_2time_timestamptz(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str batstr_2time_timestamp(bat *res, const bat *v, const bat *s, const int *len);
sql5_export str batstr_2time_timestamptz(bat *res, const bat *v, const bat *s, const int *len, int *tz);
sql5_export str timestamp_2time_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str nil_2time_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str str_2time_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str str_2time_daytimetz(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str batstr_2time_daytime(bat *res, const bat *v, const bat *s, const int *len);
sql5_export str daytime_2time_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str bat_date_trunc(bat *res, const str *scale, const bat *v);
sql5_export str date_trunc(timestamp *res, const str *scale, const timestamp *v);

sql5_export str nil_2_date(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str str_2_date(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str batstr_2_date(bat *res, const bat *val, const bat *s);

sql5_export str str_2_blob(blob * *res, const str *val);
sql5_export str batstr_2_blob(bat *res, const bat *val, const bat *sid);

sql5_export str SQLstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLbatstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str flt_dec_round_wrap(flt *res, const flt *v, const flt *r);
sql5_export str flt_bat_dec_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str flt_bat_dec_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str flt_bat_dec_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str flt_round_wrap(flt *res, const flt *v, const bte *r);
sql5_export str flt_bat_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str flt_bat_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str flt_bat_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str flt_trunc_wrap(flt *res, const flt *v, const int *r);

sql5_export str dbl_dec_round_wrap(dbl *res, const dbl *v, const dbl *r);
sql5_export str dbl_bat_dec_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dbl_bat_dec_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dbl_bat_dec_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dbl_round_wrap(dbl *res, const dbl *v, const bte *r);
sql5_export str dbl_bat_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dbl_bat_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dbl_bat_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dbl_trunc_wrap(dbl *res, const dbl *v, const int *r);

#define radians(x)	((x) * (3.14159265358979323846 / 180.0))
#define degrees(x)	((x) * (180.0 / 3.14159265358979323846))

sql5_export str SQLcst_alpha_cst(dbl *res, const dbl *decl, const dbl *theta);
sql5_export str SQLbat_alpha_cst(bat *res, const bat *decl, const dbl *theta);
sql5_export str SQLcst_alpha_bat(bat *res, const dbl *decl, const bat *theta);
sql5_export str month_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str second_interval_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#include "sql_cast.h"

sql5_export str checkSQLContext(Client cntxt);
sql5_export str getSQLContext(Client cntxt, MalBlkPtr mb, mvc **c, backend **b);

sql5_export void freeVariables(Client c, MalBlkPtr mb, MalStkPtr glb, int oldvtop, int oldvid);
sql5_export str second_interval_2_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str timestamp_2_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str date_2_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
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

sql5_export str SQLflush_log(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLsuspend_log_flushing(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLresume_log_flushing(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLhot_snapshot(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLhot_snapshot_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str SQLsession_prepared_statements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLsession_prepared_statements_args(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str SQLunionfunc(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str getBackendContext(Client cntxt, backend **be);

#endif /* _SQL_H */
