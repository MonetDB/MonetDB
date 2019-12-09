/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 * 
 * REWRITE ----->
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
 * 
 */

#ifndef _GDK_TRACER_H_
#define _GDK_TRACER_H_

#define INT_MAX_LEN ((__CHAR_BIT__ * sizeof(int) - 1) / 3  + 2)
#define BUFFER_SIZE 64000

#define DEFAULT_ADAPTER BASIC
#define DEFAULT_LOG_LEVEL M_ERROR
#define DEFAULT_FLUSH_LEVEL M_INFO

#define FILE_NAME "trace"
#define NAME_SEP '_'
#define NULL_CHAR '\0'
#define NEW_LINE '\n'
#define MXW "20"

#define AS_STR(x) #x
#define STR(x) AS_STR(x)

#define __FILENAME__ (__builtin_strrchr(__FILE__, DIR_SEP) ? __builtin_strrchr(__FILE__, DIR_SEP) + 1 : __FILE__)

#define GENERATE_ENUM(ENUM) ENUM,
#define GENERATE_STRING(STRING) #STRING,
#define GENERATE_LOG_LEVEL(COMP) DEFAULT_LOG_LEVEL,


// ADAPTERS
#define FOREACH_ADPTR(ADPTR)     \
        ADPTR( BASIC )           \
        ADPTR( PROFILER )        \
                                 \
        ADPTR( ADAPTERS_COUNT )  \

typedef enum { 
    FOREACH_ADPTR(GENERATE_ENUM)
} ADAPTER;



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



// COMPONENTS
#define FOREACH_COMP(COMP)            \
        COMP( ALLOC )                 \
        COMP( PAR )                   \
        COMP( ALGO )                  \
        COMP( DELTA )                 \
        COMP( IO_ )                   \
        COMP( TEM )                   \
        COMP( HEAP )                  \
        COMP( BAT_ )                  \
        COMP( ACCEL )                 \
        COMP( TRACE )                 \
        COMP( CHECK_ )                \
        COMP( THRD )                  \
                                      \
        COMP( GEOM )                  \
        COMP( LIDAR )                 \
        COMP( BAM_ )                  \
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
        COMP( GDK_TM )                \
        COMP( GDK_UTILS )             \
        COMP( GDK_POSIX )             \
        COMP( GDK_SYSTEM )            \
        COMP( GDK_IMPRINTS )          \
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
extern LOG_LEVEL LVL_PER_COMPONENT[];

// If the LOG_LEVEL of the message is one of the following: CRITICAL, ERROR or WARNING 
// it is logged no matter the component. In any other case the component is taken into account
#define IF_GDK_TRACER_LOG(LOG_LEVEL, COMP)                                \
    if(LOG_LEVEL == M_CRITICAL ||                                         \
       LOG_LEVEL == M_ERROR    ||                                         \
       LOG_LEVEL == M_WARNING  ||                                         \
       (LVL_PER_COMPONENT[COMP] >= LOG_LEVEL))                            \

#define GDK_TRACER_LOG_BODY(LOG_LEVEL, COMP, MSG, ...)                    \
          GDKtracer_log(LOG_LEVEL,                                        \
                        "[%s] "                                           \
                        "%-"MXW"s "                                       \
                        "%"MXW"s:%d "                                     \
                        "%"MXW"s "                                        \
                        "%-"MXW"s "                                       \
                        "%-"MXW"s # "MSG,                                 \
                        GDKtracer_get_timestamp("%Y-%m-%d %H:%M:%S"),     \
                        __FILENAME__,                                     \
                        __FUNCTION__,                                     \
                        __LINE__,                                         \
                        LEVEL_STR[LOG_LEVEL],                             \
                        COMPONENT_STR[COMP],                              \
                        MT_thread_getname(),                              \
                        ## __VA_ARGS__);                                  \

#define GDK_TRACER_LOG(LOG_LEVEL, COMP, MSG, ...)                         \
    do {                                                                  \
            IF_GDK_TRACER_LOG(LOG_LEVEL, COMP)                            \
            {                                                             \
                GDK_TRACER_LOG_BODY(LOG_LEVEL, COMP, MSG, ## __VA_ARGS__) \
            }                                                             \
    } while (0)                                                           \


#define TRC_CRITICAL(COMP, MSG, ...)                                      \
    GDK_TRACER_LOG(M_CRITICAL, COMP, MSG, ## __VA_ARGS__)                 \

#define TRC_ERROR(COMP, MSG, ...)                                         \
    GDK_TRACER_LOG(M_ERROR, COMP, MSG, ## __VA_ARGS__)                    \

#define TRC_WARNING(COMP, MSG, ...)                                       \
    GDK_TRACER_LOG(M_WARNING, COMP, MSG, ## __VA_ARGS__)                  \

#define TRC_INFO(COMP, MSG, ...)                                          \
    GDK_TRACER_LOG(M_INFO, COMP, MSG, ## __VA_ARGS__)                     \

#define TRC_DEBUG(COMP, MSG, ...)                                         \
    GDK_TRACER_LOG(M_DEBUG, COMP, MSG, ## __VA_ARGS__)                    \




// Conditional logging - Example usage
// NOTE: When using the macro with *_IF always use the macro with *_ENDIF for 
// logging. Avoiding to do that will result into checking the LOG_LEVEL of the 
// the COMPONENT 2 times. Also NEVER use the *_ENDIF macro without before 
// performing a check with *_IF macro. Such an action will have as a consequence
// logging everything without taking into account the LOG_LEVEL of the COMPONENT.
/*  
    TRC_CRITICAL_IF(SQL_STORE)
    {
        TRC_CRITICAL_ENDIF(SQL_STORE, "Test\n")
    }
*/

#define TRC_CRITICAL_IF(COMP)                                             \
    IF_GDK_TRACER_LOG(M_CRITICAL, COMP)                                   \

#define TRC_ERROR_IF(COMP)                                                \
    IF_GDK_TRACER_LOG(M_ERROR, COMP)                                      \

#define TRC_WARNING_IF(COMP)                                              \
    IF_GDK_TRACER_LOG(M_WARNING, COMP)                                    \

#define TRC_INFO_IF(COMP)                                                 \
    IF_GDK_TRACER_LOG(M_INFO, COMP)                                       \

#define TRC_DEBUG_IF(COMP)                                                \
    IF_GDK_TRACER_LOG(M_DEBUG, COMP)                                      \


#define TRC_CRITICAL_ENDIF(COMP, MSG, ...)                                \
    GDK_TRACER_LOG_BODY(M_CRITICAL, COMP, MSG, ## __VA_ARGS__)            \

#define TRC_ERROR_ENDIF(COMP, MSG, ...)                                   \
    GDK_TRACER_LOG_BODY(M_ERROR, COMP, MSG, ## __VA_ARGS__)               \

#define TRC_WARNING_ENDIF(COMP, MSG, ...)                                 \
    GDK_TRACER_LOG_BODY(M_WARNING, COMP, MSG, ## __VA_ARGS__)             \

#define TRC_INFO_ENDIF(COMP, MSG, ...)                                    \
    GDK_TRACER_LOG_BODY(M_INFO, COMP, MSG, ## __VA_ARGS__)                \

#define TRC_DEBUG_ENDIF(COMP, MSG, ...)                                   \
    GDK_TRACER_LOG_BODY(M_DEBUG, COMP, MSG, ## __VA_ARGS__)               \



// GDKtracer Buffer
typedef struct GDKtracer
{
    int id;
    char buffer[BUFFER_SIZE];
    int allocated_size;
}
gdk_tracer;



/*
 * GDKtracer Stream Macros
 */
// Exception
#define GDK_TRACER_REPORT_EXCEPTION(MSG, ...)                               \
    mnstr_printf(GDKstdout,                                                 \
                           "[%s] "                                          \
                           "%-"MXW"s "                                      \
                           "%"MXW"s:%d "                                    \
                           "%"MXW"s "                                       \
                           "%-"MXW"s "                                      \
                           "%-"MXW"s # "MSG,                                \
                            GDKtracer_get_timestamp("%Y-%m-%d %H:%M:%S"),   \
                            __FILENAME__,                                   \
                            __FUNCTION__,                                   \
                            __LINE__,                                       \
                            STR(M_CRITICAL),                                \
                            STR(GDK_TRACER),                                \
                            MT_thread_getname(),                            \
                            ## __VA_ARGS__);                                \


// mnstr_printf(GDKstdout, MSG, ## __VA_ARGS__);   \
// Produces messages to the output stream. It is also used as a fallback mechanism 
// in case GDKtracer fails to log for whatever reason.
#define GDK_TRACER_OSTREAM(MSG, ...)                \
    fprintf(stderr, MSG, ## __VA_ARGS__);           \



/*
 * GDKtracer API
 * For the allowed log_levels, components and layers see the 
 * LOG_LEVEL, COMPONENT and LAYER enum respectively.
 */
// Returns the timestamp in the form of datetime
char* GDKtracer_get_timestamp(char* fmt);


gdk_return GDKtracer_init(void);


gdk_return GDKtracer_stop(void);


gdk_return GDKtracer_set_component_level(int *comp, int *lvl);


gdk_return GDKtracer_reset_component_level(int *comp);


gdk_return GDKtracer_set_layer_level(int *layer, int *lvl);


gdk_return GDKtracer_reset_layer_level(int *layer);


// Sets the minimum flush level that an event will trigger the logger to flush the buffer
gdk_return GDKtracer_set_flush_level(int *lvl);


gdk_return GDKtracer_reset_flush_level(void);


// Sets the adapter used when flush buffer takes place
gdk_return GDKtracer_set_adapter(int *adapter);


// Resets the adapter to the default
gdk_return GDKtracer_reset_adapter(void);


// Candidate for 'gnu_printf' format attribute [-Werror=suggest-attribute=format] 
gdk_return GDKtracer_log(LOG_LEVEL level, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));


// Flush the buffer to the file
gdk_return GDKtracer_flush_buffer(void);


gdk_return GDKtracer_show_info(void);

#endif
