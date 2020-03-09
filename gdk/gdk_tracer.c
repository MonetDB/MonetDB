/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_tracer.h"

#define DEFAULT_ADAPTER BASIC
#define DEFAULT_LOG_LEVEL M_ERROR
#define DEFAULT_FLUSH_LEVEL M_INFO

#define FILE_NAME "mdbtrace.log"

#define OPENFILE_FAILED "Failed to open "FILE_NAME
#define GDKTRACER_FAILED "Failed to write logs"

#define AS_STR(x) #x
#define STR(x) AS_STR(x)

#define GENERATE_STRING(STRING) #STRING,

static FILE *active_tracer;
MT_Lock lock = MT_LOCK_INITIALIZER("GDKtracer_1");

static char file_name[FILENAME_MAX];

static ATOMIC_TYPE CUR_ADAPTER = ATOMIC_VAR_INIT(DEFAULT_ADAPTER);

static LOG_LEVEL CUR_FLUSH_LEVEL = DEFAULT_FLUSH_LEVEL;

#define GENERATE_LOG_LEVEL(COMP) DEFAULT_LOG_LEVEL,
LOG_LEVEL LVL_PER_COMPONENT[] = {
	FOREACH_COMP(GENERATE_LOG_LEVEL)
};

const char *ADAPTER_STR[] = {
	FOREACH_ADPTR(GENERATE_STRING)
};

const char *LAYER_STR[] = {
	FOREACH_LAYER(GENERATE_STRING)
};

const char *COMPONENT_STR[] = {
	FOREACH_COMP(GENERATE_STRING)
};

const char *LEVEL_STR[] = {
	FOREACH_LEVEL(GENERATE_STRING)
};



/*
 * GDKtracer Stream Macros
 */
// Exception
#define GDK_TRACER_EXCEPTION(MSG, ...)					\
	fprintf(stderr,							\
		     "%s "						\
		     "%-"MXW"s "					\
		     "%"MXW"s:%d "					\
		     "%"MXW"s "						\
		     "%-"MXW"s "					\
		     "%-"MXW"s # "MSG,					\
		     GDKtracer_get_timestamp("%Y-%m-%d %H:%M:%S",	\
					     (char[20]){0}, 20),	\
		     __FILE__,						\
		     __func__,						\
		     __LINE__,						\
		     STR(M_CRITICAL),					\
		     STR(GDK_TRACER),					\
		     MT_thread_getname(),				\
		     ## __VA_ARGS__);




// When BASIC adapter is active, all the log messages are getting printed to a file.
// This function prepares a file in order to write the contents of the buffer when necessary.
static gdk_return
_GDKtracer_init_basic_adptr(void)
{
	const char *trace_path;

	trace_path = GDKgetenv("gdk_dbtrace");
	if (trace_path == NULL)
		trace_path = GDKgetenv("gdk_dbpath");
	if (trace_path == NULL) {
		active_tracer = stderr;
		return GDK_SUCCEED;
	}

	snprintf(file_name, sizeof(file_name), "%s%c%s", trace_path, DIR_SEP, FILE_NAME);
	active_tracer = fopen(file_name, "a");
	
	if (active_tracer == NULL) {
		GDK_TRACER_EXCEPTION(OPENFILE_FAILED);
		file_name[0] = 0; /* uninitialize */
		active_tracer = stderr;
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}


static void
_GDKtracer_layer_level_helper(int layer, int lvl)
{
	const char *tok = NULL;
	LOG_LEVEL level = (LOG_LEVEL) lvl;

	for (int i = 0; i < COMPONENTS_COUNT; i++) {
		if (layer == MDB_ALL) {
			if (LVL_PER_COMPONENT[i] != level)
				LVL_PER_COMPONENT[i] = level;
		} else {
			tok = COMPONENT_STR[i];

			switch (layer) {
			case SQL_ALL:
				if (strncmp(tok, "SQL_", 4) == 0)
					if (LVL_PER_COMPONENT[i] != level)
						LVL_PER_COMPONENT[i] = level;
				break;
			case MAL_ALL:
				if (strncmp(tok, "MAL_", 4) == 0)
					if (LVL_PER_COMPONENT[i] != level)
						LVL_PER_COMPONENT[i] = level;
				break;
			case GDK_ALL:
				if (strncmp(tok, "GDK", 3) == 0)
					if (LVL_PER_COMPONENT[i] != level)
						LVL_PER_COMPONENT[i] = level;
				break;
			default:
				break;
			}
		}
	}
}

static inline ADAPTER
find_adapter(const char *adptr)
{
	if (adptr == NULL)
		return ADAPTERS_COUNT;

	for (int i = 0; i < (int) ADAPTERS_COUNT; i++) {
		if (strcasecmp(ADAPTER_STR[i], adptr) == 0) {
			return (ADAPTER) i;
		}
	}
	return ADAPTERS_COUNT;
}

static inline LOG_LEVEL
find_level(const char *lvl)
{
	if (lvl == NULL)
		return LOG_LEVELS_COUNT;

	for (int i = 0; i < (int) LOG_LEVELS_COUNT; i++) {
		if (strcasecmp(LEVEL_STR[i] + 2, lvl) == 0) {
			return (LOG_LEVEL) i;
		}
	}
	return LOG_LEVELS_COUNT;
}

static inline LAYER
find_layer(const char *layer)
{
	if (layer == NULL)
		return LAYERS_COUNT;
	for (int i = 0; i < (int) LAYERS_COUNT; i++) {
		if (strcasecmp(LAYER_STR[i], layer) == 0) {
			return (LAYER) i;
		}
	}
	return LAYERS_COUNT;
}

static inline COMPONENT
find_component(const char *comp)
{
	/* special case for the (currently) three components that end in _ */
	if (comp == NULL || *comp == 0 || comp[strlen(comp) - 1] == '_')
		return COMPONENTS_COUNT;
	if (strcasecmp(comp, "io") == 0)
		comp = "io_";
	else if (strcasecmp(comp, "bat") == 0)
		comp = "bat_";
	else if (strcasecmp(comp, "check") == 0)
		comp = "check_";

	for (int i = 0; i < (int) COMPONENTS_COUNT; i++) {
		if (strcasecmp(COMPONENT_STR[i], comp) == 0) {
			return (COMPONENT) i;
		}
	}
	return COMPONENTS_COUNT;
}



/**
 *
 * API CALLS
 *
 */
char *
GDKtracer_get_timestamp(const char *fmt, char *datetime, size_t dtsz)
{
	time_t now = time(NULL);
	struct tm tmp;
	(void) localtime_r(&now, &tmp);
	strftime(datetime, dtsz, fmt, &tmp);

	return datetime;
}


void
GDKtracer_reinit_basic(int sig)
{
	(void) sig;

	// GDKtracer needs to reopen the file only in 
	// case the adapter is BASIC
	if ((int) ATOMIC_GET(&CUR_ADAPTER) != BASIC)
		return;

	// Make sure that GDKtracer is not trying to flush the buffer
	MT_lock_set(&lock);

	if (active_tracer) {
		if (active_tracer != stderr)
			fclose(active_tracer);
		else
			fflush(active_tracer);
		active_tracer = NULL;
	}
	_GDKtracer_init_basic_adptr();

	MT_lock_unset(&lock);
}


gdk_return
GDKtracer_stop(void)
{
	_GDKtracer_layer_level_helper(MDB_ALL, DEFAULT_LOG_LEVEL);
	return GDKtracer_flush_buffer();
}

gdk_return
GDKtracer_set_component_level(const char *comp, const char *lvl)
{
	LOG_LEVEL level = find_level(lvl);
	COMPONENT component = find_component(comp);

	if (level == LOG_LEVELS_COUNT) {
		GDKerror("%s: unknown level\n", __func__);
		return GDK_FAIL;
	}
	if (component == COMPONENTS_COUNT) {
		GDKerror("%s: unknown component\n", __func__);
		return GDK_FAIL;
	}

	LVL_PER_COMPONENT[component] = level;

	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_component_level(const char *comp)
{
	COMPONENT component = find_component(comp);

	if (component == COMPONENTS_COUNT) {
		GDKerror("%s: unknown component\n", __func__);
		return GDK_FAIL;
	}
	LVL_PER_COMPONENT[component] = DEFAULT_LOG_LEVEL;
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_layer_level(const char *layer, const char *lvl)
{
	LAYER lyr = find_layer(layer);
	LOG_LEVEL level = find_level(lvl);
	if (level == LOG_LEVELS_COUNT) {
		GDKerror("%s: unknown level\n", __func__);
		return GDK_FAIL;
	}
	if (lyr == LAYERS_COUNT) {
		GDKerror("%s: unknown layer\n", __func__);
		return GDK_FAIL;
	}

	_GDKtracer_layer_level_helper(lyr, level);
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_layer_level(const char *layer)
{
	LAYER lyr = find_layer(layer);
	if (lyr == LAYERS_COUNT) {
		GDKerror("%s: unknown layer\n", __func__);
		return GDK_FAIL;
	}

	_GDKtracer_layer_level_helper(lyr, DEFAULT_LOG_LEVEL);
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_flush_level(const char *lvl)
{
	LOG_LEVEL level = find_level(lvl);
	if (level == LOG_LEVELS_COUNT) {
		GDKerror("%s: unknown level\n", __func__);
		return GDK_FAIL;
	}

	CUR_FLUSH_LEVEL = level;
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_flush_level(void)
{
	CUR_FLUSH_LEVEL = DEFAULT_FLUSH_LEVEL;
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_adapter(const char *adapter)
{
	ADAPTER adptr = find_adapter(adapter);
	if (adptr == ADAPTERS_COUNT) {
		GDKerror("%s: unknown adapter\n", __func__);
		return GDK_FAIL;
	}

	// Here when switching between adapters we can open/close the file
	// But it is not so important to keep it open in case the adapter switches
	// From BASIC to other => close the file
	// From other to BASIC => open the file

	ATOMIC_SET(&CUR_ADAPTER, adptr);

	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_adapter(void)
{
	ATOMIC_SET(&CUR_ADAPTER, DEFAULT_ADAPTER);
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_log(LOG_LEVEL level, const char *fmt, ...)
{
	int bytes_written;
	char buffer[512];	/* should be plenty big enough for a message */

	va_list va;
	va_start(va, fmt);
	bytes_written = vsnprintf(buffer, sizeof(buffer), fmt, va);
	va_end(va);
	if (bytes_written < 0) {
		GDK_TRACER_EXCEPTION(GDKTRACER_FAILED "\n");
		return GDK_FAIL;
	}
	if (bytes_written >= (int) sizeof(buffer) - 1) {
		/* message is truncated */
		bytes_written = (int) sizeof(buffer) - 2;
	}
	/* make sure message ends with a newline */
	if (buffer[bytes_written - 1] != '\n') {
		buffer[bytes_written++] = '\n';
		buffer[bytes_written] = '\0';
	}

	MT_lock_set(&lock);
	if (file_name[0] == 0) {
		_GDKtracer_init_basic_adptr();
	}
	MT_lock_unset(&lock);
	fprintf(active_tracer, "%s", buffer);

	// Flush the current buffer in case the event is
	// important depending on the flush-level
	// Always flush CRITICAL and ERROR messages - prevent cases
	// like mserver5 refusing to start due to allocated port
	// and the error is never reported to the user because it
	// is still in the buffer which it never gets flushed.
	if (level == CUR_FLUSH_LEVEL || level == M_CRITICAL || level == M_ERROR) {
		fflush(active_tracer);
		if (level == M_CRITICAL && active_tracer != stderr)
			fprintf(stderr, "%s", buffer);
	}
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_flush_buffer(void)
{
	if (active_tracer)
		fflush(active_tracer);
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_fill_comp_info(BAT *id, BAT *component, BAT *log_level)
{
	for (int i = 0; i < COMPONENTS_COUNT; i++) {
		if (BUNappend(id, &i, false) != GDK_SUCCEED)
			return GDK_FAIL;

		if (BUNappend(component, COMPONENT_STR[i], false) != GDK_SUCCEED)
			return GDK_FAIL;

		if (BUNappend(log_level, LEVEL_STR[LVL_PER_COMPONENT[i]], false) != GDK_SUCCEED) 
			return GDK_FAIL;
	}

	return GDK_SUCCEED;
}
