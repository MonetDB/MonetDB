/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * GDKtracer exposes routines where an occuring failure should reach the
 * client immediately. For that reason, GDKtracer reports those errors
 * directly to the stream.
 *
 */

#include "monetdb_config.h"
#include "tracer.h"


str
TRACERflush_buffer(void *ret)
{
    (void) ret;
    GDKtracer_flush_buffer();
    return MAL_SUCCEED;
}


str
TRACERset_component_level(void *ret, str *comp_id, str *lvl_id)
{
    (void) ret;
    if (GDKtracer_set_component_level(*comp_id, *lvl_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT);

    return MAL_SUCCEED;
}


str
TRACERreset_component_level(void *ret, str *comp_id)
{
    (void) ret;
    if (GDKtracer_reset_component_level(*comp_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT"\n");

    return MAL_SUCCEED;
}


str
TRACERset_layer_level(void *ret, str *layer_id, str *lvl_id)
{
    (void) ret;
    if (GDKtracer_set_layer_level(*layer_id, *lvl_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT"\n");

    return MAL_SUCCEED;
}


str
TRACERreset_layer_level(void *ret, str *layer_id)
{
    (void) ret;
    if (GDKtracer_reset_layer_level(*layer_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT"\n");

    return MAL_SUCCEED;
}


str
TRACERset_flush_level(void *ret, str *lvl_id)
{
    (void) ret;
    if (GDKtracer_set_flush_level(*lvl_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT"\n");

    return MAL_SUCCEED;
}


str
TRACERreset_flush_level(void *ret)
{
    (void) ret;
    if (GDKtracer_reset_flush_level() != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, _OPERATION_FAILED"\n");

    return MAL_SUCCEED;
}


str
TRACERset_adapter(void *ret, str *adapter_id)
{
    (void) ret;
    if (GDKtracer_set_adapter(*adapter_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT"\n");

    return MAL_SUCCEED;
}


str
TRACERreset_adapter(void *ret)
{
    (void) ret;
    if (GDKtracer_reset_adapter() != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, _OPERATION_FAILED"\n");

    return MAL_SUCCEED;
}


str
TRACERcomp_info(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    (void) cntxt;
    (void) mb;

    BAT *id, *component, *log_level;
    bat *i = getArgReference_bat(stk, pci, 0);
    bat *c = getArgReference_bat(stk, pci, 1);
    bat *l = getArgReference_bat(stk, pci, 2);
    str msg = MAL_SUCCEED;

    id = COLnew(0, TYPE_int, (BUN) COMPONENTS_COUNT, TRANSIENT);
    component = COLnew(0, TYPE_str, (BUN) COMPONENTS_COUNT, TRANSIENT);
    log_level = COLnew(0, TYPE_str, (BUN) COMPONENTS_COUNT, TRANSIENT);

    if ( id == NULL || component == NULL || log_level == NULL )
    {
        BBPreclaim(id);
        BBPreclaim(component);
        BBPreclaim(log_level);
        throw(MAL, __FUNCTION__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
    }

    // Fill the BATs
    MT_lock_set(&mal_delayLock);
        if(GDKtracer_fill_comp_info(id, component, log_level) == GDK_FAIL)
            goto bailout;
    MT_lock_unset(&mal_delayLock);

    BBPkeepref(*i = id->batCacheid);
    BBPkeepref(*c = component->batCacheid);
    BBPkeepref(*l = log_level->batCacheid);
    return MAL_SUCCEED;

    bailout:
        MT_lock_unset(&mal_delayLock);
        BBPunfix(id->batCacheid);
        BBPunfix(component->batCacheid);
        BBPunfix(log_level->batCacheid);
        return msg ? msg : createException(MAL, __FUNCTION__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
}
