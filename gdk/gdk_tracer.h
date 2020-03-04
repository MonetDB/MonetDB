/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* // TODO: Complete it when documentation is accepted
 *
 * Tracer is the general logging system for the MonetDB stack modelled
 * after the well-known logging schemes (e.g: Python). It provides a
 * number of logging levels and options to increase or reduce the
 * verbosity either of individual code parts or of the codebase as a
 * whole. It allows users to focus on logging messages related to
 * certain steps of execution, which can be proved handy when it comes
 * to debugging. The behavior of Tracer can be controlled at runtime
 * using the SQL API described later on. Certain calls require an "id"
 * to operate which can be found on the list of each section below.
 *
 * Internally, the logger uses a buffer to capture log messages before
 * they are forwarded to the specific adapter.
 *
 * - Sets the minimum flush level that an event will trigger the
 *   logger to flush the buffer
 * - Produces messages to the output stream. It is also used as a
 *   fallback mechanism in case GDKtracer fails to log for whatever
 *   reason.
 * - Struct buffer with allocated space etc.
 * - Flush buffer sends the messages to the selected adapter
 * - Write about the log structure (e.g: MT_thread_get_name + datetime
 *   + blah blah)
 */

#ifndef _GDK_TRACER_H_
#define _GDK_TRACER_H_

#define MXW "20"

#define GENERATE_ENUM(ENUM) ENUM,


// ADAPTERS
#define FOREACH_ADPTR(ADPTR)			\
	ADPTR( BASIC )				\
	ADPTR( PROFILER )			\
						\
	ADPTR( ADAPTERS_COUNT )

typedef enum {
	FOREACH_ADPTR(GENERATE_ENUM)
} ADAPTER;



// LOG LEVELS
#define FOREACH_LEVEL(LEVEL)			\
	LEVEL( M_CRITICAL )			\
	LEVEL( M_ERROR )			\
	LEVEL( M_WARNING )			\
	LEVEL( M_INFO )				\
	LEVEL( M_DEBUG )			\
						\
	LEVEL( LOG_LEVELS_COUNT )

typedef enum {
	FOREACH_LEVEL(GENERATE_ENUM)
} LOG_LEVEL;

gdk_export const char *LEVEL_STR[];



// LAYERS
#define FOREACH_LAYER(LAYER)			\
	LAYER( MDB_ALL )			\
	LAYER( SQL_ALL )			\
	LAYER( MAL_ALL )			\
	LAYER( GDK_ALL )			\
						\
	LAYER( LAYERS_COUNT )

typedef enum {
	FOREACH_LAYER(GENERATE_ENUM)
} LAYER;




/*
 *
 * NOTE: Adding/Removing components will affect the test tracer00.mal
 * See the test file for more details.
 *
 */
// COMPONENTS
#define FOREACH_COMP(COMP)			\
	COMP( ACCELERATOR )			\
	COMP( ALGO )				\
	COMP( ALLOC )				\
	COMP( BAT_ )				\
	COMP( CHECK_ )				\
	COMP( DELTA )				\
	COMP( HEAP )				\
	COMP( IO_ )				\
	COMP( PAR )				\
	COMP( PERF )				\
	COMP( TEM )				\
	COMP( THRD )				\
	COMP( TRACE )				\
						\
	COMP( GEOM )				\
	COMP( LIDAR )				\
	COMP( FITS )				\
	COMP( SHP )				\
						\
	COMP( SQL_PARSER )			\
	COMP( SQL_TRANS )			\
	COMP( SQL_REWRITER )			\
	COMP( SQL_EXECUTION )			\
	COMP( SQL_STORE )			\
						\
	COMP( MAL_WLC )				\
	COMP( MAL_REMOTE )			\
	COMP( MAL_MAPI )			\
	COMP( MAL_SERVER )			\
						\
	COMP( MAL_OPTIMIZER )			\
						\
	COMP( GDK )				\
						\
	COMP( COMPONENTS_COUNT )

typedef enum {
	FOREACH_COMP(GENERATE_ENUM)
} COMPONENT;

gdk_export const char *COMPONENT_STR[];



/*
 * Logging macros
 */
gdk_export LOG_LEVEL LVL_PER_COMPONENT[];

// If the LOG_LEVEL of the message is one of the following: CRITICAL,
// ERROR or WARNING it is logged no matter the component. In any other
// case the component is taken into account
#define GDK_TRACER_TEST(LOG_LEVEL, COMP)	\
	(LOG_LEVEL == M_CRITICAL ||		\
	 LOG_LEVEL == M_ERROR    ||		\
	 LOG_LEVEL == M_WARNING  ||		\
	 LVL_PER_COMPONENT[COMP] >= LOG_LEVEL)

#define IF_GDK_TRACER_LOG(LOG_LEVEL, COMP)	\
	if (GDK_TRACER_TEST(LOG_LEVEL, COMP))

#define GDK_TRACER_LOG_BODY(LOG_LEVEL, COMP, MSG, ...)			\
	GDKtracer_log(LOG_LEVEL,					\
		      "%s "						\
		      "%-"MXW"s "					\
		      "%"MXW"s:%d "					\
		      "%"MXW"s "					\
		      "%-"MXW"s "					\
		      "%-"MXW"s # "MSG,					\
		      GDKtracer_get_timestamp("%Y-%m-%d %H:%M:%S",	\
					      (char[20]){0}, 20),	\
		      __FILE__,						\
		      __func__,						\
		      __LINE__,						\
		      LEVEL_STR[LOG_LEVEL],				\
		      COMPONENT_STR[COMP],				\
		      MT_thread_getname(),				\
		      ## __VA_ARGS__)

#define GDK_TRACER_LOG(LOG_LEVEL, COMP, MSG, ...)			\
	do {								\
		IF_GDK_TRACER_LOG(LOG_LEVEL, COMP) {			\
			GDK_TRACER_LOG_BODY(LOG_LEVEL, COMP, MSG,	\
					    ## __VA_ARGS__);		\
		}							\
	} while (0)


#define TRC_CRITICAL(COMP, MSG, ...)				\
	GDK_TRACER_LOG(M_CRITICAL, COMP, MSG, ## __VA_ARGS__)

#define TRC_ERROR(COMP, MSG, ...)				\
	GDK_TRACER_LOG(M_ERROR, COMP, MSG, ## __VA_ARGS__)

#define TRC_WARNING(COMP, MSG, ...)				\
	GDK_TRACER_LOG(M_WARNING, COMP, MSG, ## __VA_ARGS__)

#define TRC_INFO(COMP, MSG, ...)				\
	GDK_TRACER_LOG(M_INFO, COMP, MSG, ## __VA_ARGS__)

#define TRC_DEBUG(COMP, MSG, ...)				\
	GDK_TRACER_LOG(M_DEBUG, COMP, MSG, ## __VA_ARGS__)




// Conditional logging - Example usage
// NOTE: When using the macro with *_IF always use the macro with
// *_ENDIF for logging. Avoiding to do that will result into checking
// the LOG_LEVEL of the the COMPONENT 2 times. Also NEVER use the
// *_ENDIF macro without before performing a check with *_IF
// macro. Such an action will have as a consequence logging everything
// without taking into account the LOG_LEVEL of the COMPONENT.
/*
    TRC_CRITICAL_IF(SQL_STORE)
    {
	TRC_CRITICAL_ENDIF(SQL_STORE, "Test\n")
    }
*/
#define TRC_CRITICAL_IF(COMP)			\
	IF_GDK_TRACER_LOG(M_CRITICAL, COMP)

#define TRC_ERROR_IF(COMP)			\
	IF_GDK_TRACER_LOG(M_ERROR, COMP)

#define TRC_WARNING_IF(COMP)			\
	IF_GDK_TRACER_LOG(M_WARNING, COMP)

#define TRC_INFO_IF(COMP)			\
	IF_GDK_TRACER_LOG(M_INFO, COMP)

#define TRC_DEBUG_IF(COMP)			\
	IF_GDK_TRACER_LOG(M_DEBUG, COMP)


#define TRC_CRITICAL_ENDIF(COMP, MSG, ...)				\
	GDK_TRACER_LOG_BODY(M_CRITICAL, COMP, MSG, ## __VA_ARGS__)

#define TRC_ERROR_ENDIF(COMP, MSG, ...)				\
	GDK_TRACER_LOG_BODY(M_ERROR, COMP, MSG, ## __VA_ARGS__)

#define TRC_WARNING_ENDIF(COMP, MSG, ...)				\
	GDK_TRACER_LOG_BODY(M_WARNING, COMP, MSG, ## __VA_ARGS__)

#define TRC_INFO_ENDIF(COMP, MSG, ...)				\
	GDK_TRACER_LOG_BODY(M_INFO, COMP, MSG, ## __VA_ARGS__)

#define TRC_DEBUG_ENDIF(COMP, MSG, ...)				\
	GDK_TRACER_LOG_BODY(M_DEBUG, COMP, MSG, ## __VA_ARGS__)



/*
 * GDKtracer API
 * For the allowed log_levels, components and layers see the
 * LOG_LEVEL, COMPONENT and LAYER enum respectively.
 */
// Returns the timestamp in the form of datetime
gdk_export char *GDKtracer_get_timestamp(const char *fmt, char *buf, size_t sz);


// Used for logrotate
gdk_export void GDKtracer_reinit_basic(int sig);


gdk_export gdk_return GDKtracer_stop(void);


gdk_export gdk_return GDKtracer_set_component_level(const char *comp, const char *lvl);


gdk_export gdk_return GDKtracer_reset_component_level(const char *comp);


gdk_export gdk_return GDKtracer_set_layer_level(const char *layer, const char *lvl);


gdk_export gdk_return GDKtracer_reset_layer_level(const char *layer);


gdk_export gdk_return GDKtracer_set_flush_level(const char *lvl);


gdk_export gdk_return GDKtracer_reset_flush_level(void);


gdk_export gdk_return GDKtracer_set_adapter(const char *adapter);


gdk_export gdk_return GDKtracer_reset_adapter(void);


gdk_export gdk_return GDKtracer_log(LOG_LEVEL level, const char *fmt, ...)
	__attribute__((__format__(__printf__, 2, 3)));


gdk_export gdk_return GDKtracer_flush_buffer(void);


gdk_export gdk_return GDKtracer_fill_comp_info(BAT *id, BAT *component, BAT *log_level);

#endif /* _GDK_TRACER_H_ */
