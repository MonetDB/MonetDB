/**
 * @file
 *
 * Parse output of the Haskell XQuery-to-Algebra Mapper into
 * Pathfinder algebra.
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2005 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *          Sabine Mayer <sbnmayer@inf.uni-konstanz.de>
 *
 * $Id$
 */

#ifndef HSK_PARSER_H
#define HSK_PARSER_H

#include "algebra.h"

/* function prototype from parse.c */
PFalg_op_t *PFhsk_parse (void);

#endif  /* HSK_PARSER_H */

/* vim:set shiftwidth=4 expandtab: */
