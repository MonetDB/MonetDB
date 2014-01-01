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
#ifndef sql_result_H
#define sql_result_H

#include "mal_client.h"
#include <stream.h>
#include <sql_mvc.h>
#include <sql_catalog.h>
#include <sql_qc.h>
#include <sql_parser.h>		/* sql_error */

extern int mvc_export_affrows(mvc *m, stream *s, lng val, str w);
extern int mvc_export_operation(mvc *m, stream *s, str w);
extern int mvc_export_value( mvc *m, stream *s, int qtype, str tn, str cn, str type, int d, int sc, int eclass, ptr p, int mtype, str w, str ns);
extern int mvc_export_result(mvc *c, stream *s, int res_id);
extern int mvc_export_head(mvc *c, stream *s, int res_id, int only_header);
extern int mvc_export_prepare(mvc *c, stream *s, cq *q, str w);
extern int mvc_export_chunk(mvc *m, stream *s, int res_id, BUN offset, BUN nr);

extern BAT **mvc_import_table(Client cntxt, mvc *c, bstream *s, char *sname, char *tname, char *sep, char *rsep, char *ssep, char *ns, lng nr, lng offset, int locked);
extern int mvc_result_table(mvc *m, int nr_cols, int type, BAT *order);

extern int mvc_result_column(mvc *m, char *tn, char *name, char *typename, int digits, int scale, BAT *b);
extern int mvc_result_value(mvc *m, char *tn, char *name, char *typename, int digits, int scale, ptr *p, int mtype);

extern int convert2str( mvc *m, int eclass, int d, int sc, int has_tz, ptr p, int mtype, char **buf, int len);

#endif /* sql_result_H */
