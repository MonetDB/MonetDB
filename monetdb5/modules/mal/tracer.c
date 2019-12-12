/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 * 
 * 
 * GDKtracer exposes routines where an occuring failure should not reach the client but 
 * should be made known only to the DBA. On the other hand, there are exposed routines
 * where the failure must also reach the client. 
 * 
 * For both cases the failures are being logged to the active adapter, or to the fallback
 * mechanism (mserver5). To cover the needs of the second category, an exception is thrown. 
 * Exceptions apart from being logged by GDKtracer are also percolating up to the client. 
 * 
 */

#include "monetdb_config.h"
#include "tracer.h"


int GDK_result = 0;

str 
TRACERflush_buffer(void)
{
    GDKtracer_flush_buffer();
    return MAL_SUCCEED;
}


str
TRACERset_component_level(void *ret, int *comp_id, int *lvl_id)
{
    (void) ret;
    GDK_result = GDKtracer_set_component_level(comp_id, lvl_id);
    if(GDK_result == GDK_FAIL)
        throw(TRACER, __FUNCTION__, ILLEGAL_ARGUMENT); 

    return MAL_SUCCEED; 
}


str
TRACERreset_component_level(void *ret, int *comp_id)
{
    (void) ret;
    GDK_result = GDKtracer_reset_component_level(comp_id);
    if(GDK_result == GDK_FAIL)
        throw(TRACER, __FUNCTION__, _OPERATION_FAILED); 

    return MAL_SUCCEED;
}


str
TRACERset_layer_level(void *ret, int *layer_id, int *lvl_id)
{
    (void) ret;
    GDK_result = GDKtracer_set_layer_level(layer_id, lvl_id);
    if(GDK_result == GDK_FAIL)
        throw(TRACER, __FUNCTION__, ILLEGAL_ARGUMENT); 

    return MAL_SUCCEED; 
}


str
TRACERreset_layer_level(void *ret, int *layer_id)
{
    (void) ret;
    GDK_result = GDKtracer_reset_layer_level(layer_id);
    if(GDK_result == GDK_FAIL)
        throw(TRACER, __FUNCTION__, _OPERATION_FAILED); 

    return MAL_SUCCEED;
}


str
TRACERset_flush_level(void *ret, int *lvl_id)
{
    (void) ret;
    GDK_result = GDKtracer_set_flush_level(lvl_id);
    if(GDK_result == GDK_FAIL)
        throw(TRACER, __FUNCTION__, ILLEGAL_ARGUMENT); 

    return MAL_SUCCEED;
}


str
TRACERreset_flush_level(void)
{
    GDK_result = GDKtracer_reset_flush_level();
    if(GDK_result == GDK_FAIL)
        throw(TRACER, __FUNCTION__, _OPERATION_FAILED); 

    return MAL_SUCCEED;
}


str
TRACERset_adapter(void *ret, int *adapter_id)
{
    (void) ret;
    GDK_result = GDKtracer_set_adapter(adapter_id);
    if(GDK_result == GDK_FAIL)
        throw(TRACER, __FUNCTION__, ILLEGAL_ARGUMENT); 

    return MAL_SUCCEED;
}


str
TRACERreset_adapter(void)
{
    GDK_result = GDKtracer_reset_adapter();
    if(GDK_result == GDK_FAIL)
        throw(TRACER, __FUNCTION__, _OPERATION_FAILED); 

    return MAL_SUCCEED;
}


str
TRACERshow_info(void)
{
    GDKtracer_show_info();
    return MAL_SUCCEED;
}


// Exposed only in MAL layer - for testing
str
TRACERlog(void)
{
    TRC_CRITICAL(SQL_BAT, "A CRITICAL message\n");
    TRC_INFO(MAL_DATAFLOW, "An INFO message\n");
    
    return MAL_SUCCEED;
}
