/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _SQL_PP_STATEMENT_H_
#define _SQL_PP_STATEMENT_H_

extern InstrPtr stmt_hash_new(backend *be, int tt, lng estimate, int parent);
extern InstrPtr stmt_part_new(backend *be, int nr_parts);
extern InstrPtr stmt_mat_new(backend *be, int tt, int nr_parts);

extern stmt *stmt_heapn_projection(backend *be, int sel, int del, int ins, stmt *c, stmt *all);

extern stmt *stmt_group_locked(backend *be, stmt *op1, stmt *grp, stmt *ext, stmt *cnt, stmt *pp);

extern stmt *stmt_slice(backend *be, stmt *col, stmt *limit);
extern stmt *stmt_slicer(backend *be, stmt *col, int slicer);
extern stmt *stmt_slices(backend *be, stmt *col); /* call mal nr of slices */

extern stmt *pp_create(backend *ba, int nrparts); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern stmt *pp_dynamic(backend *ba, int input); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern int pp_jump(backend *ba, stmt *pp, int nrparts);    /* redo: label := part < nrparts; */
extern int pp_end(backend *ba, stmt *pp);    /* exit: label ; */

#endif /* _SQL_PP_STATEMENT_H_ */
