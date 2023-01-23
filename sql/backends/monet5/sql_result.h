/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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

extern int mvc_affrows(mvc *c, stream *s, lng val, str w, oid query_id, lng last_id, lng starttime, lng maloptimizer, lng reloptimizer);
extern int mvc_export_affrows(backend *b, stream *s, lng val, str w, oid query_id, lng starttime, lng maloptimizer);
extern int mvc_export_operation(backend *b, stream *s, str w, lng starttime, lng maloptimizer);
extern int mvc_export_result(backend *b, stream *s, int res_id, bool header, lng starttime, lng maloptimizer);
extern int mvc_export_head(backend *b, stream *s, int res_id, int only_header, int compute_lengths, lng starttime, lng maloptimizer);
extern int mvc_export_chunk(backend *b, stream *s, int res_id, BUN offset, BUN nr);

extern int mvc_export_prepare(backend *b, stream *s);

extern str mvc_import_table(Client cntxt, BAT ***bats, mvc *c, bstream *s, sql_table *t, const char *sep, const char *rsep, const char *ssep, const char *ns, lng nr, lng offset, int best, bool from_stdin, bool escape);
sql5_export int mvc_result_table(backend *be, oid query_id, int nr_cols, mapi_query_t type, BAT *order);

sql5_export int mvc_result_column(backend *be, const char *tn, const char *name, const char *typename, int digits, int scale, BAT *b);
extern int mvc_result_value(backend *be, const char *tn, const char *name, const char *typename, int digits, int scale, ptr *p, int mtype);

/*
  The covered errors so far:

  -1 Allocation failure
  -2 BAT descriptor error
  -3 GDK error
  -4 Stream error
*/
extern const char *mvc_export_error(backend *be, stream *s, int err_code);

extern ssize_t convert2str(mvc *m, sql_class eclass, int d, int sc, int has_tz, const void *p, int mtype, char **buf, size_t *len);
extern int mvc_export(mvc *m, stream *s, res_table *t, BUN nr);

#endif /* sql_result_H */
