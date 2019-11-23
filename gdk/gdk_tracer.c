/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 *
 * The tracer is the general logging system for the MonetDB stack.
 * It is modelled after well-known logging schems, eg. Python
 *
 * Internally, the logger uses a dual buffer to capture log messages
 * before they are written to a file. This way we avoid serial execution.
 *
 * The logger files come in two as well, where we switch them 
 * once the logger is full.
 * The logger file format is "tracer_YY-MM-DDTHH:MM:SS_number.log"
 * An option to consider is we need a rotating scheme over 2 files only,
 * Moreover, old log files might be sent in the background to long term storage as well.
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


static gdk_tracer tracer = { .allocated_size = 0, .id = 0, .lock = MT_LOCK_INITIALIZER("GDKtracerL") };
static gdk_tracer secondary_tracer = { .allocated_size = 0, .id = 1, .lock = MT_LOCK_INITIALIZER("GDKtracerL2") };
static ATOMIC_TYPE SELECTED_tracer_ID = 0;

static FILE *output_file;
static ATOMIC_TYPE CUR_ADAPTER = DEFAULT_ADAPTER;

static LOG_LEVEL CUR_FLUSH_LEVEL = DEFAULT_FLUSH_LEVEL;
LOG_LEVEL LVL_PER_COMPONENT[COMPONENTS_COUNT];
static bool GDK_TRACER_STOP = false;


// Output error from snprintf of vsnprintf
static void 
_GDKtracer_log_output_error(int bytes_written)
{
    assert(bytes_written >= 0);
}


static void 
_GDKtracer_file_is_open(FILE *file)
{
    assert(file);
}


// Prepares a file in order to write the contents of the buffer when necessary
static void 
_GDKtracer_create_file(void)
{
    char id[INT_MAX_LEN]; 
    snprintf(id, INT_MAX_LEN, "%d", 1);

    char file_name[FILENAME_MAX];
    sprintf(file_name, "%s%c%s%c%s%c%s%s", GDKgetenv("gdk_dbpath"), DIR_SEP, FILE_NAME, NAME_SEP, GDKtracer_get_timestamp("%Y-%m-%dT%H:%M:%S"), NAME_SEP, id, ".log");

    output_file = fopen(file_name, "w");

    _GDKtracer_file_is_open(output_file);
}


static void
_GDKtracer_init_components(void)
{
    for(int i = 0; i < COMPONENTS_COUNT; i++)
    {
        LVL_PER_COMPONENT[i] = DEFAULT_LOG_LEVEL;
    }
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
_GDKtracer_level_exists(int *level)
{
    if(*level == LOG_LEVELS_COUNT)
        return false;

    if(*level >= 0 && *level < LOG_LEVELS_COUNT)
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
        tmp = GDKmalloc(sizeof(char) * (fmt_len + 2));
        strcpy(tmp, fmt);
        tmp[fmt_len] = NEW_LINE;
        tmp[fmt_len + 1] = NULL_CHAR;
        msg = tmp;
    }
    else
    {
        msg = fmt;
    }

    if(msg)
    {
        // vsnprintf(char *str, size_t count, ...) -> including null terminating character
        bytes_written = vsnprintf(sel_tracer->buffer + sel_tracer->allocated_size, BUFFER_SIZE - sel_tracer->allocated_size, msg, va);

        if(tmp)
            GDKfree(tmp);    
    }

    _GDKtracer_log_output_error(bytes_written);

    // vsnprintf returned value -> does not include the null terminating character
    return bytes_written++;
}


static gdk_return
_GDKtracer_layer_level_helper(int *layer, int *level)
{
    char *tmp = NULL;
    char *tok = NULL;
    for(int i = 0; i < COMPONENTS_COUNT; i++)
    {
        if(*layer == MDB_ALL)
        {
            if(LVL_PER_COMPONENT[i] != *level)
                LVL_PER_COMPONENT[i] = *level;
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
                        if(LVL_PER_COMPONENT[i] != *level)
                            LVL_PER_COMPONENT[i] = *level;
                    break;
                case MAL_ALL:
                    if(strcmp(tok, "MAL") == 0)
                        if(LVL_PER_COMPONENT[i] != *level)
                            LVL_PER_COMPONENT[i] = *level;
                    break;
                case GDK_ALL:
                    if(strcmp(tok, "GDK") == 0)
                        if(LVL_PER_COMPONENT[i] != *level)
                            LVL_PER_COMPONENT[i] = *level;
                    break;
                default:
                    break;
            }
        }
    }

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
    _GDKtracer_init_components();
    _GDKtracer_create_file();
    return GDK_SUCCEED;
}


gdk_return
GDKtracer_stop(void)
{
    GDK_TRACER_STOP = true;
    return GDKtracer_flush_buffer();
}


gdk_return
GDKtracer_set_component_level(int *comp, int *level)
{
    if(LVL_PER_COMPONENT[*comp] == *level)
        return GDK_SUCCEED;

    if(!_GDKtracer_component_exists(comp))
        return GDK_FAIL;

    if(!_GDKtracer_level_exists(level))
        return GDK_FAIL;
        
    LVL_PER_COMPONENT[*comp] = *level;
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
GDKtracer_set_layer_level(int *layer, int *level)
{
    if(!_GDKtracer_layer_exists(layer))
        return GDK_FAIL;

    if(!_GDKtracer_level_exists(level))
        return GDK_FAIL;
    
    return _GDKtracer_layer_level_helper(layer, level);
}


gdk_return
GDKtracer_reset_layer_level(int *layer)
{  
    if(!_GDKtracer_layer_exists(layer))
        return GDK_FAIL;

    int tmp = DEFAULT_LOG_LEVEL;
    int *level = &tmp;
    return _GDKtracer_layer_level_helper(layer, level);
}


gdk_return
GDKtracer_set_flush_level(int *level)
{
    if(CUR_FLUSH_LEVEL == *level)
        return GDK_SUCCEED;

    if(!_GDKtracer_level_exists(level))
        return GDK_FAIL;
        
    CUR_FLUSH_LEVEL = *level;

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
    // Select a tracer
    gdk_tracer *fill_tracer;
    int GDK_result;
    bool SWITCH_tracer = true;
    int bytes_written = 0;        

    if((int) ATOMIC_GET(&SELECTED_tracer_ID) == tracer.id)
        fill_tracer = &tracer;
    else
        fill_tracer = &secondary_tracer;

    MT_lock_set(&fill_tracer->lock);
    {
        va_list va;
        va_start(va, fmt);
        bytes_written = _GDKtracer_fill_tracer(fill_tracer, fmt, va);
        va_end(va);

        // The message fits the buffer OR the buffer is empty (we don't care if it fits - just cut it off)
        if(bytes_written < (BUFFER_SIZE - fill_tracer->allocated_size) || 
            fill_tracer->allocated_size == 0)
        {
            fill_tracer->allocated_size += bytes_written;
            SWITCH_tracer = false;
        }
    }
    MT_lock_unset(&fill_tracer->lock);

    if(SWITCH_tracer)
    {       
        // Switch tracer
        if((int) ATOMIC_GET(&SELECTED_tracer_ID) == tracer.id)
            fill_tracer = &secondary_tracer;
        else
            fill_tracer = &tracer;
            
        MT_lock_set(&fill_tracer->lock);
        {
            // Flush current tracer
            MT_Id tid;
            
            if(MT_create_thread(&tid, (void(*) (void*)) GDKtracer_flush_buffer, NULL, MT_THR_JOINABLE, "GDKtracerFlush") < 0)
                return GDK_FAIL;
            
            va_list va;
            va_start(va, fmt);
            bytes_written = _GDKtracer_fill_tracer(fill_tracer, fmt, va);
            va_end(va);

            // The second buffer will always be empty at start
            // So if the message does not fit we cut it off
            // message might be > BUFFER_SIZE
            fill_tracer->allocated_size += bytes_written;

            GDK_result = MT_join_thread(tid);
            if(GDK_result == GDK_FAIL)
                return GDK_FAIL;

            // Set the new selected tracer 
            ATOMIC_SET(&SELECTED_tracer_ID, fill_tracer->id);
        }
        MT_lock_unset(&fill_tracer->lock);
    }
        
    // Flush the current buffer in case the event is 
    // important depending on the flush-level
    if(level == CUR_FLUSH_LEVEL)
    {
        GDK_result = GDKtracer_flush_buffer();
        if(GDK_result == GDK_FAIL)
            return GDK_FAIL;
    }

    return GDK_SUCCEED;
}


gdk_return
GDKtracer_flush_buffer(void)
{
    // Select a tracer
    gdk_tracer *fl_tracer;
    if((int) ATOMIC_GET(&SELECTED_tracer_ID) == tracer.id)
        fl_tracer = &tracer;
    else
        fl_tracer = &secondary_tracer;
        
    // No reason to flush a buffer with no content 
    if(fl_tracer->allocated_size == 0)
        return GDK_SUCCEED;

    if(ATOMIC_GET(&CUR_ADAPTER) == BASIC)
    {
        // Check if file is open
        _GDKtracer_file_is_open(output_file);
        
        MT_lock_set(&fl_tracer->lock);
        {
            fwrite(&fl_tracer->buffer, fl_tracer->allocated_size, 1, output_file);
            fflush(output_file);
            
            // Reset buffer
            memset(fl_tracer->buffer, 0, BUFFER_SIZE);
            fl_tracer->allocated_size = 0;
        }
        MT_lock_unset(&fl_tracer->lock);
    }
    else
    {
        fprintf(stderr, "Using adapter: %s", ADAPTER_STR[(int) ATOMIC_GET(&CUR_ADAPTER)]);
    }

    // The file is kept open no matter the adapter
    // When GDKtracer stops we need also to close the file
    if(GDK_TRACER_STOP)
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

    fprintf(stderr, "# LOG level per component\n");
    for(i = 0; i < COMPONENTS_COUNT; i++)
    {
        space = (int) (max_width - strlen(COMPONENT_STR[i]) + 30);
        fprintf(stderr, "# %s %*s\n", COMPONENT_STR[i], space, LEVEL_STR[LVL_PER_COMPONENT[i]]);
    }

    fprintf(stderr, "# You can use one of the following layers to massively set the LOG level\n");
    for(i = 0; i < LAYERS_COUNT; i++)
    {
        fprintf(stderr, "# %s\n", LAYER_STR[i]);
    }

    return GDK_SUCCEED;
}
