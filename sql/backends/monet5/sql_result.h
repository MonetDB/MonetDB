/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef sql_result_H
#define sql_result_H

#include "mal_client.h"
#include "stream.h"
#include "sql.h"
#include "sql_mvc.h"
#include "sql_catalog.h"
#include "sql_qc.h"
#include "sql_parser.h"		/* sql_error */

extern int mvc_export_affrows(backend *b, stream *s, lng val, str w, oid query_id, lng starttime, lng maloptimizer);
extern int mvc_export_operation(backend *b, stream *s, str w, lng starttime, lng maloptimizer);
extern int mvc_export_result(backend *b, stream *s, int res_id, bool header, lng starttime, lng maloptimizer);
extern int mvc_export_head(backend *b, stream *s, int res_id, int only_header, int compute_lengths, lng starttime, lng maloptimizer);
extern int mvc_export_chunk(backend *b, stream *s, int res_id, BUN offset, BUN nr);

extern int mvc_export_prepare(backend *b, stream *s, str w);

extern str mvc_import_table(Client cntxt, BAT ***bats, mvc *c, bstream *s, sql_table *t, const char *sep, const char *rsep, const char *ssep, const char *ns, lng nr, lng offset, int best, bool from_stdin, bool escape);
sql5_export int mvc_result_table(backend *be, oid query_id, int nr_cols, mapi_query_t type, BAT *order);

sql5_export int mvc_result_column(backend *be, char *tn, char *name, char *typename, int digits, int scale, BAT *b);
extern int mvc_result_value(backend *be, const char *tn, const char *name, const char *typename, int digits, int scale, ptr *p, int mtype);

extern ssize_t convert2str(mvc *m, sql_class eclass, int d, int sc, int has_tz, ptr p, int mtype, char **buf, size_t *len);

#endif /* sql_result_H */
