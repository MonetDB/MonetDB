/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#ifndef _GDK_TRACER_H_
#define _GDK_TRACER_H_

#define INT_MAX_LEN ((__CHAR_BIT__ * sizeof(int) - 1) / 3  + 2)
#define BUFFER_SIZE 64000

#define DEFAULT_ADAPTER BASIC
#define DEFAULT_LOG_LEVEL M_CRITICAL
#define DEFAULT_FLUSH_LEVEL M_ERROR

#define FILE_NAME "trace"
#define NAME_SEP '_'
#define NULL_CHAR '\0'
#define NEW_LINE '\n'

#define __FILENAME__ (__builtin_strrchr(__FILE__, DIR_SEP) ? __builtin_strrchr(__FILE__, DIR_SEP) + 1 : __FILE__)

#define GENERATE_ENUM(ENUM) ENUM,
#define GENERATE_STRING(STRING) #STRING,



// ADAPTERS
#define FOREACH_ADPTR(ADPTR)     \
        ADPTR( BASIC )           \
        ADPTR( PROFILER )        \
                                 \
        ADPTR( ADAPTERS_COUNT )  \

typedef enum { 
    FOREACH_ADPTR(GENERATE_ENUM)
} ADAPTER;

static const char *ADAPTER_STR[] = {
    FOREACH_ADPTR(GENERATE_STRING)
};


// LOG LEVELS
#define FOREACH_LEVEL(LEVEL)      \
        LEVEL( M_CRITICAL )       \
        LEVEL( M_ERROR )          \
        LEVEL( M_WARNING )        \
        LEVEL( M_INFO )           \
        LEVEL( M_DEBUG )          \
                                  \
        LEVEL( LOG_LEVELS_COUNT ) \

typedef enum { 
    FOREACH_LEVEL(GENERATE_ENUM)
} LOG_LEVEL;

static const char *LEVEL_STR[] = {
    FOREACH_LEVEL(GENERATE_STRING)
};


// LAYERS
#define FOREACH_LAYER(LAYER)      \
        LAYER( MDB_ALL )          \
        LAYER( SQL_ALL )          \
        LAYER( MAL_ALL )          \
        LAYER( GDK_ALL )          \
                                  \
        LAYER( LAYERS_COUNT )     \

typedef enum { 
    FOREACH_LAYER(GENERATE_ENUM)
} LAYER;

static const char *LAYER_STR[] = {
    FOREACH_LAYER(GENERATE_STRING)
};


// COMPONENTS
#define FOREACH_COMP(COMP)            \
        COMP( ALLOC )                 \
        COMP( PAR )                   \
        COMP( ALGO )                  \
        COMP( GEOM )                  \
        COMP( LIDAR )                 \
        COMP( BAM )                   \
        COMP( FITS )                  \
        COMP( SHP )                   \
                                      \
        COMP( SQL_ATOM_TR )           \
        COMP( SQL_STATISTICS )        \
        COMP( SQL_ORDERIDX )          \
        COMP( SQL_OPTIMIZER )         \
        COMP( SQL_WLR )               \
        COMP( SQL_USER )              \
        COMP( SQL_SCENARIO )          \
        COMP( SQL_CACHE_TR )          \
        COMP( SQL_SYMBOL )            \
        COMP( SQL_MVC )               \
        COMP( SQL_STORE )             \
        COMP( SQL_UPGRADES )          \
        COMP( SQL_RELATION )          \
        COMP( SQL_RESULT )            \
        COMP( SQL_STATEMENT )         \
        COMP( SQL_BAT )               \
                                      \
        COMP( MAL_MEMO )              \
        COMP( MAL_SESSION )           \
        COMP( MAL_INTERPRETER )       \
        COMP( MAL_SCHEDULER )         \
        COMP( MAL_LISTING )           \
        COMP( MAL_FACTORY )           \
        COMP( MAL_PARSER )            \
        COMP( MAL_WLC )               \
        COMP( MAL_REDUCE )            \
        COMP( MAL_CLIENT )            \
        COMP( MAL_OIDX )              \
        COMP( MAL_REMOTE )            \
        COMP( MAL_INET )              \
        COMP( MAL_GROUPBY )           \
        COMP( MAL_TABLET )            \
        COMP( MAL_OLTP )              \
        COMP( MAL_MANIFOLD )          \
        COMP( MAL_RESOLVE )           \
        COMP( MAL_FCN )               \
        COMP( MAL_DATAFLOW )          \
        COMP( MAL_MODULE )            \
        COMP( MAL_SERVER )            \
        COMP( MAL_EXCEPTION )         \
        COMP( MAL_NAMESPACE )         \
        COMP( MAL_PROFILER )          \
        COMP( MAL_MAL )               \
        COMP( MAL_DEBUGGER )          \
                                      \
        COMP( MAL_OPT_ALIASES )       \
        COMP( MAL_OPT_CANDIDATES )    \
        COMP( MAL_OPT_COERCION )      \
        COMP( MAL_OPT_COMMONTERMS )   \
        COMP( MAL_OPT_PIPES )         \
        COMP( MAL_OPT_REMAP )         \
        COMP( MAL_OPT_DATAFLOW )      \
        COMP( MAL_OPT_EVALUATE )      \
        COMP( MAL_OPT_INLINE )        \
        COMP( MAL_OPT_JIT )           \
        COMP( MAL_OPT_MULTIPLEX )     \
        COMP( MAL_OPT_REORDER )       \
        COMP( MAL_OPT_PROJECTIONPATH )\
        COMP( MAL_OPT_WLC )           \
        COMP( MAL_OPT_CONSTANTS )     \
        COMP( MAL_OPT_COSTMODEL )     \
        COMP( MAL_OPT_DEADCODE )      \
        COMP( MAL_OPT_EMPTYBIND )     \
        COMP( MAL_OPT_GENERATOR )     \
        COMP( MAL_OPT_JSON )          \
        COMP( MAL_OPT_MATPACK )       \
        COMP( MAL_OPT_GC )            \
        COMP( MAL_OPT_MERGETABLE )    \
        COMP( MAL_OPT_MITOSIS )       \
        COMP( MAL_OPT_PUSHSELECT )    \
        COMP( MAL_OPT_QUERYLOG )      \
        COMP( MAL_OPT_OLTP )          \
        COMP( MAL_OPT_PROFILER )      \
        COMP( MAL_OPT_REDUCE )        \
        COMP( MAL_OPT_REMOTE )        \
        COMP( MAL_OPT_VOLCANO )       \
        COMP( MAL_OPT_MACRO )         \
        COMP( MAL_OPT_POSTFIX )       \
                                      \
        COMP( COMPONENTS_COUNT )      \

typedef enum { 
    FOREACH_COMP(GENERATE_ENUM)
} COMPONENT;

static const char *COMPONENT_STR[] = {
    FOREACH_COMP(GENERATE_STRING)
};



/*
 * Logging macros 
 */
extern LOG_LEVEL LVL_PER_COMPONENT[COMPONENTS_COUNT];

// If the LOG_LEVEL of the message is one of the following: CRITICAL, ERROR or WARNING 
// it is logged no matter the component. In any other case the component is taken into account (needs fix)
#define GDK_TRACER_LOG(LOG_LEVEL, COMP, MSG, ...)                        \
    if(LOG_LEVEL == M_CRITICAL ||                                        \
       LOG_LEVEL == M_ERROR    ||                                        \
       LOG_LEVEL == M_WARNING  ||                                        \
       (LVL_PER_COMPONENT[COMP] >= LOG_LEVEL))                           \
    {                                                                    \
            GDKtracer_log(LOG_LEVEL,                                     \
                        "[%s] %s %s:%d %s %s %s # "MSG,                  \
                        GDKtracer_get_timestamp("%Y-%m-%d %H:%M:%S"),    \
                        __FILENAME__,                                    \
                        __FUNCTION__,                                    \
                        __LINE__,                                        \
                        LEVEL_STR[LOG_LEVEL],                            \
                        COMPONENT_STR[COMP],                             \
                        MT_thread_getname(),                             \
                        ## __VA_ARGS__);                                 \
    }                                                                    \

#define CRITICAL(COMP, MSG, ...)                                         \
    GDK_TRACER_LOG(M_CRITICAL, COMP, MSG, ## __VA_ARGS__)                \

#define ERROR(COMP, MSG, ...)                                            \
    GDK_TRACER_LOG(M_ERROR, COMP, MSG, ## __VA_ARGS__)                   \

#define WARNING(COMP, MSG, ...)                                          \
    GDK_TRACER_LOG(M_WARNING, COMP, MSG, ## __VA_ARGS__)                 \

#define INFO(COMP, MSG, ...)                                             \
    GDK_TRACER_LOG(M_INFO, COMP, MSG, ## __VA_ARGS__)                    \

#define DEBUG(COMP, MSG, ...)                                            \
    GDK_TRACER_LOG(M_DEBUG, COMP, MSG, ## __VA_ARGS__)                   \


// GDKtracer Buffer
typedef struct GDKtracer
{
    int id;
    char buffer[BUFFER_SIZE];
    int allocated_size;
    MT_Lock lock;
}
gdk_tracer;



/*
 *  GDKtracer API
 */
// Returns the timestamp in the form of datetime
char* GDKtracer_get_timestamp(char* fmt);


// Initialize tracer
gdk_return GDKtracer_init(void);


gdk_return GDKtracer_stop(void);


gdk_return GDKtracer_set_component_level(int *comp, int *level);


gdk_return GDKtracer_reset_component_level(int *comp);


gdk_return GDKtracer_set_layer_level(int *layer, int *level);


gdk_return GDKtracer_reset_layer_level(int *layer);


// Sets the minimum flush level that an event will trigger the logger to flush the buffer
gdk_return GDKtracer_set_flush_level(int *level);


// Resets the flush level to the default (ERROR)
gdk_return GDKtracer_reset_flush_level(void);


// Sets the adapter used when flush buffer takes place
gdk_return GDKtracer_set_adapter(int *adapter);


// Resets the adapter to the default (BASIC) when flush buffer takes place 
gdk_return GDKtracer_reset_adapter(void);


// Candidate for 'gnu_printf' format attribute [-Werror=suggest-attribute=format] 
gdk_return GDKtracer_log(LOG_LEVEL level, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));


// Flush the buffer to the file
gdk_return GDKtracer_flush_buffer(void);


gdk_return GDKtracer_show_info(void);

#endif
