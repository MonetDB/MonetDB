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

// GDKtracer struct - Buffer and info
#define BUFFER_SIZE 64000
typedef struct GDKtracer {
	int id;
	char buffer[BUFFER_SIZE];
	int allocated_size;
} gdk_tracer;

#define DEFAULT_ADAPTER BASIC
#define DEFAULT_LOG_LEVEL M_ERROR
#define DEFAULT_FLUSH_LEVEL M_INFO

#define FILE_NAME "mdbtrace.log"

#define OPENFILE_FAILED "Failed to open "FILE_NAME
#define GDKTRACER_FAILED "Failed to write logs"

#define AS_STR(x) #x
#define STR(x) AS_STR(x)

#define GENERATE_STRING(STRING) #STRING,

static gdk_tracer tracer = {.id = 0, .allocated_size = 0};

static gdk_tracer *active_tracer = &tracer;
MT_Lock lock = MT_LOCK_INITIALIZER("GDKtracer_1");

static FILE *output_file;
static char file_name[FILENAME_MAX];

static ATOMIC_TYPE CUR_ADAPTER = ATOMIC_VAR_INIT(DEFAULT_ADAPTER);
static bool INIT_BASIC_ADAPTER = false;
static bool LOG_EXC_REP = false;

static LOG_LEVEL CUR_FLUSH_LEVEL = DEFAULT_FLUSH_LEVEL;
static bool GDK_TRACER_STOP = false;

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
	mnstr_printf(GDKstdout,						\
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
	const char* TRACE_PATH = GDKgetenv("gdk_dbpath");

	if(GDKgetenv("gdk_dbtrace") != NULL)
		TRACE_PATH = GDKgetenv("gdk_dbtrace");

	snprintf(file_name, sizeof(file_name), "%s%c%s", TRACE_PATH, DIR_SEP, FILE_NAME);
	output_file = fopen(file_name, "a");
	
	// Even if creating the file failed, the adapter has 
	// still tried to initialize and we shouldn't retry it
	INIT_BASIC_ADAPTER = true;

	if(!output_file)
	{
		GDK_TRACER_EXCEPTION(OPENFILE_FAILED);
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}


// Candidate for 'gnu_printf' format attribute [-Werror=suggest-attribute=format]
static int _GDKtracer_fill_tracer(gdk_tracer *sel_tracer, const char *fmt, va_list va)
	__attribute__((__format__(__printf__, 2, 0)));

static int
_GDKtracer_fill_tracer(gdk_tracer *sel_tracer, const char *fmt, va_list va)
{
	size_t fmt_len = strlen(fmt);
	int bytes_written = 0;

	// vsnprintf(char *str, size_t count, ...) -> including null terminating character
	bytes_written = vsnprintf(sel_tracer->buffer +sel_tracer->allocated_size, BUFFER_SIZE - sel_tracer->allocated_size, fmt, va);
	// Add \n if it doesn't exist
	if (bytes_written && fmt[fmt_len - 1] != '\n')
		bytes_written += snprintf(sel_tracer->buffer +sel_tracer->allocated_size, BUFFER_SIZE - sel_tracer->allocated_size, "\n");

	// Let GDKtracer_log to know about the failure
	if (bytes_written < 0)
		return -1;

	// vsnprintf returned value -> does not include the null terminating character
	return bytes_written++;
}


static gdk_return
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
				if (strncmp(tok, "GDK_", 4) == 0)
					if (LVL_PER_COMPONENT[i] != level)
						LVL_PER_COMPONENT[i] = level;
				break;
			default:
				break;
			}
		}
	}
	return GDK_SUCCEED;
}

static gdk_return
_GDKtracer_flush_buffer_locked(void)
{
	// No reason to flush a buffer with no content
	if (active_tracer->allocated_size == 0) {
		return GDK_SUCCEED;
	}

	if (ATOMIC_GET(&CUR_ADAPTER) == BASIC) {
		// Initialize the BASIC adapter. It is used in order to avoid cases with files being
		// created and no logs being produced. Even if the creating the file fails the function
		// is going to be called only once.
		if(!INIT_BASIC_ADAPTER)
			_GDKtracer_init_basic_adptr();

		if (output_file) {
			size_t nitems = 1;
			size_t w = fwrite(&active_tracer->buffer, active_tracer->allocated_size, nitems, output_file);

			if (w == nitems)
				fflush(output_file);
			else
				// fwrite failed for whatever reason (e.g: disk is full)
				if(!LOG_EXC_REP) {
					GDK_TRACER_EXCEPTION(GDKTRACER_FAILED "\n");
					LOG_EXC_REP = true;
				}
		}

		// Reset buffer
		memset(active_tracer->buffer, 0, BUFFER_SIZE);
		active_tracer->allocated_size = 0;
	} else {
		memset(active_tracer->buffer, 0, BUFFER_SIZE);
		active_tracer->allocated_size = 0;

		// Here we are supposed to send the logs to the profiler
	}

	// The file is kept open no matter the adapter
	// When GDKtracer stops we need also to close the file
	if (GDK_TRACER_STOP && output_file) {
		fclose(output_file);
		output_file = NULL;
	}

	return GDK_SUCCEED;
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
	// BASIC adapter has been initialized already and file is open
	if(INIT_BASIC_ADAPTER && output_file) {
		// Close file
		fclose(output_file);
		output_file = NULL;

		// Open a new file in append mode
		output_file = fopen(file_name, "a");
		if(!output_file)
			GDK_TRACER_EXCEPTION(OPENFILE_FAILED);
	}
	MT_lock_unset(&lock);
}


gdk_return
GDKtracer_stop(void)
{
	GDK_TRACER_STOP = true;
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

	return _GDKtracer_layer_level_helper(lyr, level);
}


gdk_return
GDKtracer_reset_layer_level(const char *layer)
{
	LAYER lyr = find_layer(layer);
	if (lyr == LAYERS_COUNT) {
		GDKerror("%s: unknown layer\n", __func__);
		return GDK_FAIL;
	}

	return _GDKtracer_layer_level_helper(lyr, DEFAULT_LOG_LEVEL);
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
	int bytes_written = 0;

	MT_lock_set(&lock);

	va_list va;
	va_start(va, fmt);
	bytes_written = _GDKtracer_fill_tracer(active_tracer, fmt, va);
	va_end(va);

	if (bytes_written >= 0) {
		// The message fits the buffer OR the buffer is empty but the message does not fit (we cut it off)
		if (bytes_written < (BUFFER_SIZE - active_tracer->allocated_size) || active_tracer->allocated_size == 0) {
			active_tracer->allocated_size += bytes_written;
		} else {
			_GDKtracer_flush_buffer_locked();

			va_list va;
			va_start(va, fmt);
			bytes_written = _GDKtracer_fill_tracer(active_tracer, fmt, va);
			va_end(va);

			if (bytes_written >= 0) {
				// The second buffer will always be empty at start
				// So if the message does not fit we cut it off
				// message might be > BUFFER_SIZE
				active_tracer->allocated_size += bytes_written;
			} else {

				// Failed to write to the buffer - bytes_written < 0
				if(!LOG_EXC_REP)
				{
					GDK_TRACER_EXCEPTION(GDKTRACER_FAILED "\n");
					LOG_EXC_REP = true;
				}
			}
		}
	} else {

		// Failed to write to the buffer - bytes_written < 0
		if(!LOG_EXC_REP)
		{
			GDK_TRACER_EXCEPTION(GDKTRACER_FAILED "\n");
			LOG_EXC_REP = true;
		}
	}

	// Flush the current buffer in case the event is
	// important depending on the flush-level
	// Always flush CRITICAL and ERROR messages - prevent cases
	// like mserver5 refusing to start due to allocated port
	// and the error is never reported to the user because it
	// is still in the buffer which it never gets flushed.
	if (level == CUR_FLUSH_LEVEL || level == M_CRITICAL || level == M_ERROR) {
		_GDKtracer_flush_buffer_locked();
	}
	MT_lock_unset(&lock);
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_flush_buffer(void)
{
	gdk_return rc;

	// No reason to flush a buffer with no content
	MT_lock_set(&lock);
	rc = _GDKtracer_flush_buffer_locked();
	MT_lock_unset(&lock);
	return rc;
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
