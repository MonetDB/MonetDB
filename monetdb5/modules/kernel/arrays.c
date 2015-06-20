#include "monetdb_config.h"
#include "mal_exception.h"
#include "arrays.h"
#include <gdk_arrays.h>

str dimension_leftfetchjoin(bat *result,  ptr *dims, int dimNum) {
	BAT *resBAT= NULL;

	(void)*dims;
	(void)dimNum;
	/*find the dimension in dimNum and create the BAT out of it*/

    *result = resBAT->batCacheid;
    BBPkeepref(*result);
    return MAL_SUCCEED;

}

static str
ALGbinary(bat *result, const bat *lid, const bat *rid, BAT* (*func)(BAT *, BAT *), const char *name)
{
    BAT *left, *right,*bn= NULL;

    if ((left = BATdescriptor(*lid)) == NULL) {
        throw(MAL, name, RUNTIME_OBJECT_MISSING);
    }
    if ((right = BATdescriptor(*rid)) == NULL) {
        BBPunfix(left->batCacheid);
        throw(MAL, name, RUNTIME_OBJECT_MISSING);
    }
    bn = (*func)(left, right);
    BBPunfix(left->batCacheid);
    BBPunfix(right->batCacheid);
    if (bn == NULL)
        throw(MAL, name, GDK_EXCEPTION);
    if (!(bn->batDirty&2))
        BATsetaccess(bn, BAT_READ);
    *result = bn->batCacheid;
    BBPkeepref(*result);
    return MAL_SUCCEED;
}

str ALGdimensionLeftfetchjoin(bat *result, const bat *lid, const bat *rid) {
    return ALGbinary(result, lid, rid, dimensionBATproject_wrap, "algebra.dimension_leftfetchjoin");
}

str ALGmbrproject(bat *result, const bat *bid, const bat *sid, const bat* rid) {
    BAT *b, *s, *r, *bn;

    if ((b = BATdescriptor(*bid)) == NULL) {
        throw(MAL, "algebra.mbrproject", RUNTIME_OBJECT_MISSING);
    }
    if ((s = BATdescriptor(*sid)) == NULL) {
        BBPunfix(b->batCacheid);
        throw(MAL, "algebra.mbrproject", RUNTIME_OBJECT_MISSING);
    }
    if ((r = BATdescriptor(*rid)) == NULL) {
        BBPunfix(b->batCacheid);
        BBPunfix(s->batCacheid);
        throw(MAL, "algebra.mbrproject", RUNTIME_OBJECT_MISSING);
    }
    if(BATmbrproject(&bn, b, s, r) != GDK_SUCCEED)
        bn = NULL;
    BBPunfix(b->batCacheid);
    BBPunfix(s->batCacheid);
    BBPunfix(r->batCacheid);

    if (bn == NULL)
        throw(MAL, "algebra.mbrproject", GDK_EXCEPTION);
    if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);
    *result = bn->batCacheid;
    BBPkeepref(bn->batCacheid);
    return MAL_SUCCEED;

}

str ALGmbrsubselect(bat *result, const bat *bid, const bat *sid, const bat *cid) {
    BAT *b, *s = NULL, *c = NULL, *bn;

    if ((b = BATdescriptor(*bid)) == NULL) {
        throw(MAL, "algebra.mbrsubselect", RUNTIME_OBJECT_MISSING);
    }
    if (sid && *sid != bat_nil && (s = BATdescriptor(*sid)) == NULL) {
        BBPunfix(b->batCacheid);
        throw(MAL, "algebra.mbrsubselect", RUNTIME_OBJECT_MISSING);
    }
    if (cid && *cid != bat_nil && (c = BATdescriptor(*cid)) == NULL) {
        BBPunfix(b->batCacheid);
        BBPunfix(s->batCacheid);
        throw(MAL, "algebra.mbrsubselect", RUNTIME_OBJECT_MISSING);
    }
    if(BATmbrsubselect(&bn, b, s, c) != GDK_SUCCEED)
        bn = NULL;
    BBPunfix(b->batCacheid);
    BBPunfix(s->batCacheid);
    if (c)
        BBPunfix(c->batCacheid);
    if (bn == NULL)
        throw(MAL, "algebra.mbrsubselect", GDK_EXCEPTION);
    if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);
    *result = bn->batCacheid;
    BBPkeepref(bn->batCacheid);
    return MAL_SUCCEED;
}

str ALGmbrsubselect2(bat *result, const bat *bid, const bat *sid) {
    return ALGmbrsubselect(result, bid, sid, NULL);
}

