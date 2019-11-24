/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "tracer.h"
#include "gdk_tracer.h"


int GDK_result = 0;

str 
TRACERflush_buffer(void)
{
    GDK_result = GDKtracer_flush_buffer();
    // if(GDK_result == GDK_FAIL)
    
        
    return MAL_SUCCEED;
}


str
TRACERset_component_level(void *ret, int *comp, int *lvl)
{
    (void) ret;
    GDK_result = GDKtracer_set_component_level(comp, lvl);
    // if(GDK_result == GDK_FAIL)
    //     throw(TRACER, __FILE__, "%s:%s", __func__, OPERATION_FAILED);

    return MAL_SUCCEED; 
}


str
TRACERreset_component_level(int *comp)
{
    GDK_result = GDKtracer_reset_component_level(comp);
    // if(GDK_result == GDK_FAIL)
    //     throw(TRACER, __FILE__, "%s:%s", __func__, OPERATION_FAILED);

    return MAL_SUCCEED;
}


str
TRACERset_layer_level(void *ret, int *layer, int *lvl)
{
    (void) ret;
    GDK_result = GDKtracer_set_layer_level(layer, lvl);
    // if(GDK_result == GDK_FAIL)
    //     throw(TRACER, __FILE__, "%s:%s", __func__, OPERATION_FAILED);

    return MAL_SUCCEED; 
}


str
TRACERreset_layer_level(int *layer)
{
    GDK_result = GDKtracer_reset_layer_level(layer);
    // if(GDK_result == GDK_FAIL)
    //     throw(TRACER, __FILE__, "%s:%s", __func__, OPERATION_FAILED);

    return MAL_SUCCEED;
}


str
TRACERset_flush_level(void *ret, int *lvl)
{
    (void) ret;
    GDK_result = GDKtracer_set_flush_level(lvl);
    // if(GDK_result == GDK_FAIL)
    //     throw(TRACER, __FILE__, "%s:%s", __func__, OPERATION_FAILED);

    return MAL_SUCCEED;
}


str
TRACERreset_flush_level(void)
{
    GDK_result = GDKtracer_reset_flush_level();
    // if(GDK_result == GDK_FAIL)
    //     throw(TRACER, __FILE__, "%s:%s", __func__, OPERATION_FAILED);

    return MAL_SUCCEED;
}


str
TRACERset_adapter(void *ret, int *adapter)
{
    (void) ret;
    GDK_result = GDKtracer_set_adapter(adapter);
    // if(GDK_result == GDK_FAIL)
    //     throw(TRACER, __FILE__, "%s:%s", __func__, OPERATION_FAILED);

    return MAL_SUCCEED;
}


str
TRACERreset_adapter(void)
{
    GDK_result = GDKtracer_reset_adapter();
    // if(GDK_result == GDK_FAIL)
    //     throw(TRACER, __FILE__, "%s:%s", __func__, OPERATION_FAILED);

    return MAL_SUCCEED;
}
