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

#if SIZEOF_WRD == SIZEOF_INT
#define wrdToStr(sptr, lptr, p) intToStr(sptr, lptr, (int*)p)
#else
#define wrdToStr(sptr, lptr, p) lngToStr(sptr, lptr, (lng*)p)
#endif

extern int sqlcleanup(mvc *c, int err);
extern sql_rel *sql_symbol2relation(mvc *c, symbol *sym);
extern stmt *sql_relation2stmt(mvc *c, sql_rel *r);

extern BAT *mvc_bind_idxbat(mvc *m, char *sname, char *tname, char *iname, int access);

sql5_export str SQLmvc(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLtransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLabort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLshutdown_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLtransaction2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str mvc_append_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_update_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bind_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bind_idxbat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_clear_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_delete_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLtid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str DELTAbat(bat *result, bat *col, bat *uid, bat *uval, bat *ins);
sql5_export str DELTAsub(bat *result, bat *col, bat *cid, bat *uid, bat *uval, bat *ins);
sql5_export str DELTAproject(bat *result, bat *subselect, bat *col, bat *uid, bat *uval, bat *ins);
sql5_export str DELTAbat2(bat *result, bat *col, bat *uid, bat *uval);
sql5_export str DELTAsub2(bat *result, bat *col, bat *cid, bat *uid, bat *uval);
sql5_export str DELTAproject2(bat *result, bat *subselect, bat *col, bat *uid, bat *uval);
sql5_export str mvc_result_row_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_result_file_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_result_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_result_column_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_result_value_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str mvc_declared_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_declared_table_column_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_drop_declared_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_drop_declared_tables_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str mvc_affected_rows_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_result_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_head_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_chunk_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_operation_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_export_value_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_import_table_stdin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bin_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str setVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str getVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_variables(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_logfile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bat_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_get_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_getVersion(lng *r, int *clientid);
sql5_export str mvc_restart_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str zero_or_one(ptr ret, int *bid);
sql5_export str not_unique(bit *ret, int *bid);
sql5_export str not_unique_oids(bat *ret, bat *bid);
sql5_export str SQLcluster1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcluster2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLshrink(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLreuse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLvacuum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLdrop_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLnewDictionary(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLdropDictionary(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLgzcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLgzdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLtruncate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLexpand(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLoctopusBind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLargRecord(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLrdfShred(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLoptimizersUpdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str month_interval_str(int *ret, str *s, int *ek, int *sk);
sql5_export str second_interval_str(lng *res, str *s, int *ek, int *sk);
sql5_export str dump_cache(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dump_opt_stats(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str dump_trace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_sessions_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_storage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_querylog_catalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_querylog_calls(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_querylog_empty(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_rowid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_rank_grp(bat *rid, bat *bid, bat *gid, bat *gpe);
sql5_export str sql_rank(bat *rid, bat *bid);
sql5_export str sql_dense_rank_grp(bat *rid, bat *bid, bat *gid, bat *gpe);
sql5_export str sql_dense_rank(bat *rid, bat *bid);
sql5_export str SQLidentity(bat *rid, bat *bid);
sql5_export str BATSQLidentity(bat *rid, bat *bid);
sql5_export str PBATSQLidentity(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str bte_dec_round_wrap(bte *res, bte *v, bte *r);
sql5_export str bte_bat_dec_round_wrap(bat *res, bat *v, bte *r);
sql5_export str bte_round_wrap(bte *res, bte *v, int *d, int *s, bte *r);
sql5_export str bte_bat_round_wrap(bat *res, bat *v, int *d, int *s, bte *r);
sql5_export str str_2dec_bte(bte *res, str *val, int *d, int *sc);
sql5_export str str_2num_bte(bte *res, str *v, int *len);
sql5_export str batstr_2dec_bte(int *res, int *val, int *d, int *sc);
sql5_export str batstr_2num_bte(int *res, int *v, int *len);
sql5_export str bte_dec2second_interval(lng *res, int *sc, bte *dec, int *ek, int *sk);

sql5_export str nil_2dec_bte(bte *res, void *val, int *d, int *sc);
sql5_export str nil_2num_bte(bte *res, void *v, int *len);
sql5_export str batnil_2dec_bte(int *res, int *val, int *d, int *sc);
sql5_export str batnil_2num_bte(int *res, int *v, int *len);

sql5_export str sht_dec_round_wrap(sht *res, sht *v, sht *r);
sql5_export str sht_bat_dec_round_wrap(bat *res, bat *v, sht *r);
sql5_export str sht_round_wrap(sht *res, sht *v, int *d, int *s, bte *r);
sql5_export str sht_bat_round_wrap(bat *res, bat *v, int *d, int *s, bte *r);
sql5_export str str_2dec_sht(sht *res, str *val, int *d, int *sc);
sql5_export str str_2num_sht(sht *res, str *v, int *len);
sql5_export str batstr_2dec_sht(int *res, int *val, int *d, int *sc);
sql5_export str batstr_2num_sht(int *res, int *v, int *len);
sql5_export str sht_dec2second_interval(lng *res, int *sc, sht *dec, int *ek, int *sk);

sql5_export str nil_2dec_sht(sht *res, void *val, int *d, int *sc);
sql5_export str nil_2num_sht(sht *res, void *v, int *len);
sql5_export str batnil_2dec_sht(int *res, int *val, int *d, int *sc);
sql5_export str batnil_2num_sht(int *res, int *v, int *len);

sql5_export str int_dec_round_wrap(int *res, int *v, int *r);
sql5_export str int_bat_dec_round_wrap(bat *res, bat *v, int *r);
sql5_export str int_round_wrap(int *res, int *v, int *d, int *s, bte *r);
sql5_export str int_bat_round_wrap(bat *res, bat *v, int *d, int *s, bte *r);
sql5_export str str_2dec_int(int *res, str *val, int *d, int *sc);
sql5_export str str_2num_int(int *res, str *v, int *len);
sql5_export str batstr_2dec_int(int *res, int *val, int *d, int *sc);
sql5_export str batstr_2num_int(int *res, int *v, int *len);
sql5_export str int_dec2second_interval(lng *res, int *sc, int *dec, int *ek, int *sk);

sql5_export str nil_2dec_int(int *res, void *val, int *d, int *sc);
sql5_export str nil_2num_int(int *res, void *v, int *len);
sql5_export str batnil_2dec_int(int *res, int *val, int *d, int *sc);
sql5_export str batnil_2num_int(int *res, int *v, int *len);

sql5_export str wrd_dec_round_wrap(wrd *res, wrd *v, wrd *r);
sql5_export str wrd_bat_dec_round_wrap(bat *res, bat *v, wrd *r);
sql5_export str wrd_round_wrap(wrd *res, wrd *v, int *d, int *s, bte *r);
sql5_export str wrd_bat_round_wrap(bat *res, bat *v, int *d, int *s, bte *r);
sql5_export str str_2dec_wrd(wrd *res, str *val, int *d, int *sc);
sql5_export str str_2num_wrd(wrd *res, str *v, int *len);
sql5_export str batstr_2dec_wrd(int *res, int *val, int *d, int *sc);
sql5_export str batstr_2num_wrd(int *res, int *v, int *len);
sql5_export str wrd_dec2second_interval(lng *res, int *sc, wrd *dec, int *ek, int *sk);

sql5_export str nil_2dec_wrd(wrd *res, void *val, int *d, int *sc);
sql5_export str nil_2num_wrd(wrd *res, void *v, int *len);
sql5_export str batnil_2dec_wrd(int *res, int *val, int *d, int *sc);
sql5_export str batnil_2num_wrd(int *res, int *v, int *len);

sql5_export str lng_dec_round_wrap(lng *res, lng *v, lng *r);
sql5_export str lng_bat_dec_round_wrap(bat *res, bat *v, lng *r);
sql5_export str lng_round_wrap(lng *res, lng *v, int *d, int *s, bte *r);
sql5_export str lng_bat_round_wrap(bat *res, bat *v, int *d, int *s, bte *r);
sql5_export str str_2dec_lng(lng *res, str *val, int *d, int *sc);
sql5_export str str_2num_lng(lng *res, str *v, int *len);
sql5_export str batstr_2dec_lng(int *res, int *val, int *d, int *sc);
sql5_export str batstr_2num_lng(int *res, int *v, int *len);
sql5_export str lng_dec2second_interval(lng *res, int *sc, lng *dec, int *ek, int *sk);

sql5_export str nil_2dec_lng(lng *res, void *val, int *d, int *sc);
sql5_export str nil_2num_lng(lng *res, void *v, int *len);
sql5_export str batnil_2dec_lng(int *res, int *val, int *d, int *sc);
sql5_export str batnil_2num_lng(int *res, int *v, int *len);

sql5_export str nil_2time_timestamp(timestamp *res, void *v, int *len);
sql5_export str batnil_2time_timestamp(int *res, int *v, int *len);
sql5_export str str_2time_timestamp(timestamp *res, str *v, int *len);
sql5_export str batstr_2time_timestamp(int *res, int *v, int *len);
sql5_export str timestamp_2time_timestamp(timestamp *res, timestamp *v, int *len);
sql5_export str battimestamp_2time_timestamp(int *res, int *v, int *len);

sql5_export str nil_2time_daytime(daytime *res, void *v, int *len);
sql5_export str batnil_2time_daytime(int *res, int *v, int *len);
sql5_export str str_2time_daytime(daytime *res, str *v, int *len);
sql5_export str batstr_2time_daytime(int *res, int *v, int *len);
sql5_export str daytime_2time_daytime(daytime *res, daytime *v, int *len);
sql5_export str batdaytime_2time_daytime(int *res, int *v, int *len);

sql5_export str nil_2_timestamp(timestamp *res, void *val);
sql5_export str batnil_2_timestamp(int *res, int *val);
sql5_export str str_2_timestamp(timestamp *res, str *val);
sql5_export str batstr_2_timestamp(int *res, int *val);
sql5_export str SQLtimestamp_2_str(str *res, timestamp *val);

sql5_export str nil_2_daytime(daytime *res, void *val);
sql5_export str batnil_2_daytime(int *res, int *val);
sql5_export str str_2_daytime(daytime *res, str *val);
sql5_export str batstr_2_daytime(int *res, int *val);
sql5_export str SQLdaytime_2_str(str *res, daytime *val);

sql5_export str nil_2_date(date *res, void *val);
sql5_export str batnil_2_date(int *res, int *val);
sql5_export str str_2_date(date *res, str *val);
sql5_export str batstr_2_date(int *res, int *val);
sql5_export str SQLdate_2_str(str *res, date *val);

sql5_export str nil_2_sqlblob(sqlblob * *res, void *val);
sql5_export str batnil_2_sqlblob(int *res, int *val);
sql5_export str str_2_sqlblob(sqlblob * *res, str *val);
sql5_export str batstr_2_sqlblob(int *res, int *val);
sql5_export str SQLsqlblob_2_str(str *res, sqlblob * val);


sql5_export str SQLstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLbatstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str flt_dec_round_wrap(flt *res, flt *v, flt *r);
sql5_export str flt_bat_dec_round_wrap(bat *res, bat *v, flt *r);
sql5_export str flt_round_wrap(flt *res, flt *v, bte *r);
sql5_export str flt_bat_round_wrap(bat *res, bat *v, bte *r);
sql5_export str flt_trunc_wrap(flt *res, flt *v, int *r);

sql5_export str dbl_dec_round_wrap(dbl *res, dbl *v, dbl *r);
sql5_export str dbl_bat_dec_round_wrap(bat *res, bat *v, dbl *r);
sql5_export str dbl_round_wrap(dbl *res, dbl *v, bte *r);
sql5_export str dbl_bat_round_wrap(bat *res, bat *v, bte *r);
sql5_export str dbl_trunc_wrap(dbl *res, dbl *v, int *r);

#define radians(x)	((x) * 3.14159265358979323846 /180.0 )
#define degrees(x)	((x) * 180.0/3.14159265358979323846 )

sql5_export str SQLcst_alpha_cst(dbl *res, dbl *decl, dbl *theta);
sql5_export str SQLbat_alpha_cst(bat *res, bat *decl, dbl *theta);
sql5_export str SQLcst_alpha_bat(bat *res, dbl *decl, bat *theta);
sql5_export str month_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str month_interval_daytime(int *ret, daytime *s, int *ek, int *sk);
sql5_export str second_interval_daytime(lng *res, daytime *s, int *ek, int *sk);

#include "sql_cast.h"

sql5_export str checkSQLContext(Client cntxt);
sql5_export str getSQLContext(Client cntxt, MalBlkPtr mb, mvc **c, backend **b);

sql5_export void freeVariables(Client c, MalBlkPtr mb, MalStkPtr glb, int start);
sql5_export str second_interval_daytime(lng *res, daytime *s, int *ek, int *sk);
sql5_export str second_interval_2_daytime(daytime *res, lng *s, int *d);
sql5_export str timestamp_2_daytime(daytime *res, timestamp *v, int *d);
sql5_export str date_2_timestamp(timestamp *res, date *v, int *d);
sql5_export str SQLcurrent_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcurrent_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _SQL_H */
