/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 *  M. L. Kersten
 * Functions for the mosaic code base
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

gdk_return
MOSheapAlloc(BAT *bn, BUN cap)
{
    const char *nme = BBP_physical(bn->batCacheid);

    if ( (bn->T->mosaic = (Heap*)GDKzalloc(sizeof(Heap))) == NULL ||
        (bn->T->mosaic->filename = GDKfilepath(NOFARM, NULL, nme, "mosaic")) == NULL)
        return GDK_FAIL;
    if( HEAPalloc(bn->T->mosaic, cap , Tsize(bn)) != GDK_SUCCEED)
        return GDK_FAIL;
    bn->T->mosaic->parentid = bn->batCacheid;
    bn->T->mosaic->farmid = BBPselectfarm(bn->batRole, bn->ttype, varheap);
    return GDK_SUCCEED;
}

void
MOSheapDestroy(BAT *bn)
{	Heap *h;
	
	if( bn && bn->T->mosaic && !VIEWtparent(bn)){
		h= bn->T->mosaic;
		bn->T->mosaic = NULL;
		if( HEAPdelete(h, BBP_physical(bn->batCacheid), "mosaic"))
			IODEBUG fprintf(stderr,"#MOSheapDestroy (%s) failed", BATgetId(bn));
		bn->T->mosaic = NULL;
		GDKfree(h);
	}
}
