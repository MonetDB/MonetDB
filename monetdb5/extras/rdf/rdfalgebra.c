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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

/* This contains algebra functions used for RDF store only
*/

#include "monetdb_config.h"
#include "rdf.h"
#include "algebra.h"
#include <gdk.h>

static BAT* leftfetchjoin_sorted(BAT* left, BAT *right, BUN estimate) {
        BAT *bn = BATleftfetchjoin(left, right, estimate);
        if (bn) bn->tsorted = TRUE; /* OK: we must be sure of this, but you are, aren't you? */
        return bn;
}

str
RDFleftfetchjoin_sorted(bat *result, bat *lid, bat *rid)
{
        return ALGbinaryestimate(result, lid, rid, NULL, leftfetchjoin_sorted, "rdf.leftfetchjoin_sorted");
}

