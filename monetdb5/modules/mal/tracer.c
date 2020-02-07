/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 * 
 * 
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
TRACERset_component_level(void *ret, int *comp_id, int *lvl_id)
{
    (void) ret;
    if (GDKtracer_set_component_level(*comp_id, *lvl_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT"\n");

    return MAL_SUCCEED; 
}


str
TRACERreset_component_level(void *ret, int *comp_id)
{
    (void) ret;
    if (GDKtracer_reset_component_level(*comp_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT"\n");

    return MAL_SUCCEED;
}


str
TRACERset_layer_level(void *ret, int *layer_id, int *lvl_id)
{
    (void) ret;
    if (GDKtracer_set_layer_level(*layer_id, *lvl_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT"\n");

    return MAL_SUCCEED; 
}


str
TRACERreset_layer_level(void *ret, int *layer_id)
{
    (void) ret;
    if (GDKtracer_reset_layer_level(*layer_id) != GDK_SUCCEED)
        throw(MAL, __FUNCTION__, ILLEGAL_ARGUMENT"\n");

    return MAL_SUCCEED;
}


str
TRACERset_flush_level(void *ret, int *lvl_id)
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
TRACERset_adapter(void *ret, int *adapter_id)
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
TRACERshow_comp_info(void *ret)
{
    (void) ret;
    GDKtracer_show_comp_info();
    return MAL_SUCCEED;
}
