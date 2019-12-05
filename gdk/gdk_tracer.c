/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_tracer.h"

// We need to get rid of macros defined in gdk.h. Those are using GDKtracer in order to produce 
// messages. At the point malloc is called in gdk_tracer.c (in function _GDKtracer_fill_tracer)
// a lock has already being acquired. Using the macro malloc in gdk.h a call to GDKtracer 
// acquires another lock and results in deadlock.
#ifdef malloc
    #undef malloc
#endif
#ifdef free
    #undef free
#endif

static gdk_tracer tracer = { .id = 0, .allocated_size = 0 };
static gdk_tracer *active_tracer = &tracer;
MT_Lock lock = MT_LOCK_INITIALIZER("GDKtracer_1");

static FILE *output_file;
static bool USE_STREAM = true;

static ATOMIC_TYPE CUR_ADAPTER = DEFAULT_ADAPTER;

static LOG_LEVEL CUR_FLUSH_LEVEL = DEFAULT_FLUSH_LEVEL;
static bool GDK_TRACER_STOP = false;

static const char *LAYER_STR[] = {
    FOREACH_LAYER(GENERATE_STRING)
};

static const char *ADAPTER_STR[] = {
    FOREACH_ADPTR(GENERATE_STRING)
};

LOG_LEVEL LVL_PER_COMPONENT[] = {
    FOREACH_COMP(GENERATE_LOG_LEVEL)
};



// Prepares a file in order to write the contents of the buffer when necessary
static gdk_return
_GDKtracer_init_basic_adptr(void)
{
    char file_name[FILENAME_MAX];
    sprintf(file_name, "%s%c%s%c%s%s", GDKgetenv("gdk_dbpath"), DIR_SEP, FILE_NAME, NAME_SEP, GDKtracer_get_timestamp("%Y-%m-%dT%H:%M:%S"), ".log");

    output_file = fopen(file_name, "w");

    if(!output_file)
    {
        GDK_TRACER_REPORT_EXCEPTION("Failed to initialize BASIC adapter. Could not open file: %s\n", file_name);
        return GDK_FAIL;
    }

    return GDK_SUCCEED;
}


static bool
_GDKtracer_adapter_exists(int *adapter)
{
    if(*adapter == ADAPTERS_COUNT)
        return false;

    if(*adapter >= 0 && *adapter < ADAPTERS_COUNT)
        return true;

    return false;
}


static bool
_GDKtracer_level_exists(int *lvl)
{
    if(*lvl == LOG_LEVELS_COUNT)
        return false;

    if(*lvl >= 0 && *lvl < LOG_LEVELS_COUNT)
        return true;

    return false;
}


static bool
_GDKtracer_layer_exists(int *layer)
{
    if(*layer == LAYERS_COUNT)
        return false;
    
    if(*layer >= 0 && *layer < LAYERS_COUNT)
        return true;

    return false;   
}


static bool
_GDKtracer_component_exists(int *comp)
{
    if(*comp == COMPONENTS_COUNT)
        return false;
    
    if(*comp >= 0 && *comp < COMPONENTS_COUNT)
        return true;

    return false;   
}


// Candidate for 'gnu_printf' format attribute [-Werror=suggest-attribute=format]
static int 
_GDKtracer_fill_tracer(gdk_tracer *sel_tracer, char *fmt, va_list va) __attribute__ ((format (printf, 2, 0)));

static int 
_GDKtracer_fill_tracer(gdk_tracer *sel_tracer, char *fmt, va_list va)
{
    char *tmp = NULL;
    const char* msg = NULL;
    size_t fmt_len = strlen(fmt);
    int bytes_written = 0;

    // Add \n if it doesn't exist
    if(fmt[fmt_len - 1] != NEW_LINE)
    {
        tmp = malloc(sizeof(char) * (fmt_len + 2));
        if(!tmp)
        {
            // Let GDKtracer_log to know about the failure
            GDK_TRACER_REPORT_EXCEPTION("Memory allocation failed\n");
            return -1;
        }
        else
        {
            strcpy(tmp, fmt);
            tmp[fmt_len] = NEW_LINE;
            tmp[fmt_len + 1] = NULL_CHAR;
            msg = tmp;    
        }    
    }
    else
    {
        msg = fmt;
    }
        
    if(msg)
        // vsnprintf(char *str, size_t count, ...) -> including null terminating character
        bytes_written = vsnprintf(sel_tracer->buffer + sel_tracer->allocated_size, BUFFER_SIZE - sel_tracer->allocated_size, msg, va);
        
    if(tmp)
        free(tmp);

    // Let GDKtracer_log to know about the failure
    if(bytes_written < 0)
        return -1;

    // vsnprintf returned value -> does not include the null terminating character
    return bytes_written++;
}


static gdk_return
_GDKtracer_layer_level_helper(int *layer, int *lvl)
{
    char *tmp = NULL;
    char *tok = NULL;
    LOG_LEVEL level = (LOG_LEVEL)*lvl;

    for(int i = 0; i < COMPONENTS_COUNT; i++)
    {
        if(*layer == MDB_ALL)
        {
            if(LVL_PER_COMPONENT[i] != level)
                LVL_PER_COMPONENT[i] = level;
        }
        else
        {
            tmp = strdup(COMPONENT_STR[i]);
            if(!tmp)
                return GDK_FAIL;

            tok = strtok(tmp, "_");
            if(!tok)
                return GDK_FAIL;

            switch(*layer)
            {
                case SQL_ALL:
                    if(strcmp(tok, "SQL") == 0)
                        if(LVL_PER_COMPONENT[i] != level)
                            LVL_PER_COMPONENT[i] = level;
                    break;
                case MAL_ALL:
                    if(strcmp(tok, "MAL") == 0)
                        if(LVL_PER_COMPONENT[i] != level)
                            LVL_PER_COMPONENT[i] = level;
                    break;
                case GDK_ALL:
                    if(strcmp(tok, "GDK") == 0)
                        if(LVL_PER_COMPONENT[i] != level)
                            LVL_PER_COMPONENT[i] = level;
                    break;
                default:
                    break;
            }
        }
    }
    
/* CHECK 
 * this should only be called if you wish to trace the GDKtracer
    GDKtracer_show_info();
*/

    return GDK_SUCCEED;
}



/**
 * 
 * API CALLS
 * 
 */ 
char*
GDKtracer_get_timestamp(char* fmt)
{
    static char datetime[20];
    time_t now = time(NULL);
    struct tm *tmp = localtime(&now);
    strftime(datetime, sizeof(datetime), fmt, tmp);
    return datetime;
}


gdk_return
GDKtracer_init(void)
{
    return _GDKtracer_init_basic_adptr();
}


gdk_return
GDKtracer_stop(void)
{
    GDK_TRACER_STOP = true;
    return GDKtracer_flush_buffer();
}


gdk_return
GDKtracer_set_component_level(int *comp, int *lvl)
{
    LOG_LEVEL level = (LOG_LEVEL)*lvl;

    if(LVL_PER_COMPONENT[*comp] == level)
        return GDK_SUCCEED;

    if(!_GDKtracer_component_exists(comp))
        return GDK_FAIL;

    if(!_GDKtracer_level_exists(lvl))
        return GDK_FAIL;
        
    LVL_PER_COMPONENT[*comp] = level;
/* CHECK 
 * this should only be called if you wish to trace the GDKtracer
    GDKtracer_show_info();
*/
    return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_component_level(int *comp)
{  
    if(LVL_PER_COMPONENT[*comp] == DEFAULT_LOG_LEVEL)
        return GDK_SUCCEED;
   
    if(!_GDKtracer_component_exists(comp))
        return GDK_FAIL;

    LVL_PER_COMPONENT[*comp] = DEFAULT_LOG_LEVEL; 
    return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_layer_level(int *layer, int *lvl)
{
    if(!_GDKtracer_layer_exists(layer))
        return GDK_FAIL;

    if(!_GDKtracer_level_exists(lvl))
        return GDK_FAIL;
    
    return _GDKtracer_layer_level_helper(layer, lvl);
}


gdk_return
GDKtracer_reset_layer_level(int *layer)
{  
    if(!_GDKtracer_layer_exists(layer))
        return GDK_FAIL;

    int tmp = DEFAULT_LOG_LEVEL;
    int *lvl = &tmp;
    return _GDKtracer_layer_level_helper(layer, lvl);
}


gdk_return
GDKtracer_set_flush_level(int *lvl)
{
    LOG_LEVEL level = (LOG_LEVEL)*lvl;

    if(CUR_FLUSH_LEVEL == level)
        return GDK_SUCCEED;

    if(!_GDKtracer_level_exists(lvl))
        return GDK_FAIL;
        
    CUR_FLUSH_LEVEL = level;

    return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_flush_level(void)
{
    if(CUR_FLUSH_LEVEL == DEFAULT_FLUSH_LEVEL)
        return GDK_SUCCEED;

    CUR_FLUSH_LEVEL = DEFAULT_FLUSH_LEVEL;

    return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_adapter(int *adapter)
{
    if((int) ATOMIC_GET(&CUR_ADAPTER) == *adapter)
        return GDK_SUCCEED;

    // Here when switching between adapters we can open/close the file
    // But it is not so important to keep it open in case the adapter switches
    // From BASIC to other => close the file
    // From other to BASIC => open the file
    
    if(!_GDKtracer_adapter_exists(adapter))
        return GDK_FAIL;

    ATOMIC_SET(&CUR_ADAPTER, *adapter);

    return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_adapter(void)
{
    if(ATOMIC_GET(&CUR_ADAPTER) == DEFAULT_ADAPTER)
        return GDK_SUCCEED;

    ATOMIC_SET(&CUR_ADAPTER, DEFAULT_ADAPTER);

    return GDK_SUCCEED;
}


gdk_return
GDKtracer_log(LOG_LEVEL level, char *fmt, ...)
{      
    int bytes_written = 0;

    MT_lock_set(&lock);
    
    va_list va;
    va_start(va, fmt);
    bytes_written = _GDKtracer_fill_tracer(active_tracer, fmt, va);
    va_end(va);
    
    if(bytes_written >= 0)
    {
        // The message fits the buffer OR the buffer is empty but the message does not fit (we cut it off)
        if(bytes_written < (BUFFER_SIZE - active_tracer->allocated_size) || 
            active_tracer->allocated_size == 0)
        {
            active_tracer->allocated_size += bytes_written;
            MT_lock_unset(&lock);
        }
        else
        {
            MT_lock_unset(&lock);

            GDKtracer_flush_buffer();
            
            MT_lock_set(&lock);
            va_list va;
            va_start(va, fmt);
            bytes_written = _GDKtracer_fill_tracer(active_tracer, fmt, va);
            va_end(va);

            if(bytes_written >= 0)
            {
                // The second buffer will always be empty at start
                // So if the message does not fit we cut it off
                // message might be > BUFFER_SIZE
                active_tracer->allocated_size += bytes_written;  
                MT_lock_unset(&lock);
            }
            else
            {
                MT_lock_unset(&lock);

                GDK_TRACER_OSTREAM("Failed to write to the buffer (bytes_written = %d)\n", bytes_written);
                // Fallback logging mechanism 
                // va_list va;
                // va_start(va, fmt);
                // GDK_TRACER_OSTREAM(fmt, va);
                // va_end(va);
            }
        }
    }
    else
    {
        MT_lock_unset(&lock);

        GDK_TRACER_OSTREAM("Failed to write to the buffer (bytes_written = %d)\n", bytes_written);
        // Fallback logging mechanism 
        // va_list va;
        // va_start(va, fmt);
        // GDK_TRACER_OSTREAM("%s", fmt);
        // va_end(va);
    }

    // Flush the current buffer in case the event is 
    // important depending on the flush-level
    // Always flush CRITICAL and ERROR messages - prevent cases 
    // like mserver5 refusing to start due to allocated port 
    // and the error is never reported to the user because it 
    // is still in the buffer which it never gets flushed.
    if(level == CUR_FLUSH_LEVEL || 
       level == M_CRITICAL      || 
       level == M_ERROR)
    {
        GDKtracer_flush_buffer();
    }

    return GDK_SUCCEED;
}


gdk_return
GDKtracer_flush_buffer(void)
{
    // No reason to flush a buffer with no content 
    MT_lock_set(&lock);
    if(active_tracer->allocated_size == 0)
    {
        MT_lock_unset(&lock);
        return GDK_SUCCEED;
    }
    MT_lock_unset(&lock);

    if(ATOMIC_GET(&CUR_ADAPTER) == BASIC)
    {
        MT_lock_set(&lock);
        {
            // Check if file is open - if not send the output to GDKstdout. There are cases that 
            // this is needed - e.g: on startup of mserver5 GDKmalloc is called before GDKinit. 
            // In GDKinit GDKtracer is getting initialized (open_file and initialize log level 
            // per component). Since the file is not open yet and there is an assert, we need 
            // to do something - and as a backup plan we send the logs to GDKstdout.
            if(output_file)
            {
                size_t nitems = 1;
                size_t w = fwrite(&active_tracer->buffer, active_tracer->allocated_size, nitems, output_file);

                if(w == nitems)
                {   
                    USE_STREAM = false;
                    fflush(output_file);
                }
            }

            // fwrite failed for whatever reason 
            // (e.g: disk is full) fallback to stream
            if(USE_STREAM)
            {
                GDK_TRACER_OSTREAM("%s", active_tracer->buffer);
            }
                
            // Reset buffer
            memset(active_tracer->buffer, 0, BUFFER_SIZE);
            active_tracer->allocated_size = 0;
        }
        MT_lock_unset(&lock);
    }
    else
    {
        MT_lock_set(&lock);
        memset(active_tracer->buffer, 0, BUFFER_SIZE);
        active_tracer->allocated_size = 0;
        MT_lock_unset(&lock);

        GDK_TRACER_OSTREAM("Using adapter: %s\n", ADAPTER_STR[(int) ATOMIC_GET(&CUR_ADAPTER)]);
    }

    // The file is kept open no matter the adapter
    // When GDKtracer stops we need also to close the file
    if(GDK_TRACER_STOP)
        if(output_file)
            fclose(output_file);

    return GDK_SUCCEED;
}


gdk_return
GDKtracer_show_info(void)
{
    int i = 0;
    int max_width = 0;
    int space = 0;
    int comp_width = 0;

    // Find max width from components
    for(i = 0; i < COMPONENTS_COUNT; i++)
    {
        comp_width = strlen(COMPONENT_STR[i]);
        if(comp_width > max_width)
            max_width = comp_width;
    }

    GDK_TRACER_OSTREAM("# LOG level per component\n");
    for(i = 0; i < COMPONENTS_COUNT; i++)
    {
        space = (int) (max_width - strlen(COMPONENT_STR[i]) + 30);
        GDK_TRACER_OSTREAM("# %s %*s\n", COMPONENT_STR[i], space, LEVEL_STR[LVL_PER_COMPONENT[i]]);
    }

    GDK_TRACER_OSTREAM("# You can use one of the following layers to massively set the LOG level\n");
    for(i = 0; i < LAYERS_COUNT; i++)
    {
        GDK_TRACER_OSTREAM("# %s\n", LAYER_STR[i]);
    }

    return GDK_SUCCEED;
}
