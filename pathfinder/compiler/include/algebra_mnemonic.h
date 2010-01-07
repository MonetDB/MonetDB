/**
 * @file
 *
 * Mnemonic abbreviations for algebra constructors.
 * (Generic algebra constructors)
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/** atomic natural number construction */
#define lit_nat(n)      PFalg_lit_nat (n)

/** atomic integer construction */
#define lit_int(i)      PFalg_lit_int (i)

/** atomic string construction */
#define lit_str(s)      PFalg_lit_str (s)

/** atomic uA construction */
#define lit_uA(s)       PFalg_lit_uA (s)

/** atomic float construction */
#define lit_dec(f)      PFalg_lit_dec (f)

/** atomic double construction */
#define lit_dbl(d)      PFalg_lit_dbl (d)

/** atomic boolean construction */
#define lit_bln(b)      PFalg_lit_bln (b)

/** atomic QName construction */
#define lit_qname(b)    PFalg_lit_qname (b)

/** tuple construction */
#define tuple(...)      PFalg_tuple (__VA_ARGS__)

/** column list construction */
#define collist(...)    PFalg_collist_worker (__VA_ARGS__)

/** item in the projection list */
#define proj(a,b)       PFalg_proj ((a),(b))

/** simple schema constructor iter|pos|item */
#define ipi_schema(a) PFalg_iter_pos_item_schema((a))

/** simple schema constructor iter|pos|item|score */
#define ipis_schema(a) PFalg_iter_pos_item_score_schema((a))

/** simple schema constructor iter|item*/
#define ii_schema(a) PFalg_iter_item_schema((a))

/* vim:set shiftwidth=4 expandtab: */
