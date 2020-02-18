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

LOG_LEVEL LVL_PER_COMPONENT[] = {
	FOREACH_COMP(GENERATE_LOG_LEVEL)
};



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


static bool
_GDKtracer_adapter_exists(int adapter)
{
	if (adapter == ADAPTERS_COUNT)
		return false;

	if (adapter >= 0 && adapter < ADAPTERS_COUNT)
		return true;

	return false;
}


static bool
_GDKtracer_level_exists(int lvl)
{
	if (lvl == LOG_LEVELS_COUNT)
		return false;

	if (lvl >= 0 && lvl < LOG_LEVELS_COUNT)
		return true;

	return false;
}


static bool
_GDKtracer_layer_exists(int layer)
{
	if (layer == LAYERS_COUNT)
		return false;

	if (layer >= 0 && layer < LAYERS_COUNT)
		return true;

	return false;
}


static bool
_GDKtracer_component_exists(int comp)
{
	if (comp == COMPONENTS_COUNT)
		return false;

	if (comp >= 0 && comp < COMPONENTS_COUNT)
		return true;

	return false;
}


// Candidate for 'gnu_printf' format attribute [-Werror=suggest-attribute=format]
static int _GDKtracer_fill_tracer(gdk_tracer *sel_tracer, const char *fmt, va_list va)
	__attribute__((format(printf, 2, 0)));

static int
_GDKtracer_fill_tracer(gdk_tracer *sel_tracer, const char *fmt, va_list va)
{
	size_t fmt_len = strlen(fmt);
	int bytes_written = 0;

	// vsnprintf(char *str, size_t count, ...) -> including null terminating character
	bytes_written = vsnprintf(sel_tracer->buffer +sel_tracer->allocated_size, BUFFER_SIZE - sel_tracer->allocated_size, fmt, va);
	// Add \n if it doesn't exist
	if (bytes_written && fmt[fmt_len - 1] != NEW_LINE)
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

	// BASIC adapter has been initialized already and file is open
	if(INIT_BASIC_ADAPTER && output_file) {
		// Make sure that GDKtracer is not trying to flush the buffer
		MT_lock_set(&lock);
		{
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
}


gdk_return
GDKtracer_stop(void)
{
	GDK_TRACER_STOP = true;
	return GDKtracer_flush_buffer();
}


gdk_return
GDKtracer_set_component_level(int comp, int lvl)
{
	LOG_LEVEL level = (LOG_LEVEL) lvl;

	if (LVL_PER_COMPONENT[comp] == level)
		return GDK_SUCCEED;

	if (!_GDKtracer_component_exists(comp))
		return GDK_FAIL;

	if (!_GDKtracer_level_exists(lvl))
		return GDK_FAIL;

	LVL_PER_COMPONENT[comp] = level;

	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_component_level(int comp)
{
	if (LVL_PER_COMPONENT[comp] == DEFAULT_LOG_LEVEL)
		return GDK_SUCCEED;

	if (!_GDKtracer_component_exists(comp))
		return GDK_FAIL;

	LVL_PER_COMPONENT[comp] = DEFAULT_LOG_LEVEL;
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_layer_level(int layer, int lvl)
{
	if (!_GDKtracer_layer_exists(layer))
		return GDK_FAIL;

	if (!_GDKtracer_level_exists(lvl))
		return GDK_FAIL;

	return _GDKtracer_layer_level_helper(layer, lvl);
}


gdk_return
GDKtracer_reset_layer_level(int layer)
{
	if (!_GDKtracer_layer_exists(layer))
		return GDK_FAIL;

	return _GDKtracer_layer_level_helper(layer, DEFAULT_LOG_LEVEL);
}


gdk_return
GDKtracer_set_flush_level(int lvl)
{
	LOG_LEVEL level = (LOG_LEVEL) lvl;

	if (CUR_FLUSH_LEVEL == level)
		return GDK_SUCCEED;

	if (!_GDKtracer_level_exists(lvl))
		return GDK_FAIL;

	CUR_FLUSH_LEVEL = level;

	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_flush_level(void)
{
	if (CUR_FLUSH_LEVEL == DEFAULT_FLUSH_LEVEL)
		return GDK_SUCCEED;

	CUR_FLUSH_LEVEL = DEFAULT_FLUSH_LEVEL;

	return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_adapter(int adapter)
{
	if ((int) ATOMIC_GET(&CUR_ADAPTER) == adapter)
		return GDK_SUCCEED;

	// Here when switching between adapters we can open/close the file
	// But it is not so important to keep it open in case the adapter switches
	// From BASIC to other => close the file
	// From other to BASIC => open the file

	if (!_GDKtracer_adapter_exists(adapter))
		return GDK_FAIL;

	ATOMIC_SET(&CUR_ADAPTER, adapter);

	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_adapter(void)
{
	if (ATOMIC_GET(&CUR_ADAPTER) == DEFAULT_ADAPTER)
		return GDK_SUCCEED;

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
			MT_lock_unset(&lock);
		} else {
			MT_lock_unset(&lock);

			GDKtracer_flush_buffer();

			MT_lock_set(&lock);
			va_list va;
			va_start(va, fmt);
			bytes_written = _GDKtracer_fill_tracer(active_tracer, fmt, va);
			va_end(va);

			if (bytes_written >= 0) {
				// The second buffer will always be empty at start
				// So if the message does not fit we cut it off
				// message might be > BUFFER_SIZE
				active_tracer->allocated_size += bytes_written;
				MT_lock_unset(&lock);
			} else {
				MT_lock_unset(&lock);

				// Failed to write to the buffer - bytes_written < 0
				if(!LOG_EXC_REP)
				{
					GDK_TRACER_EXCEPTION(GDKTRACER_FAILED "\n");
					LOG_EXC_REP = true;
				}
			}
		}
	} else {
		MT_lock_unset(&lock);

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
		GDKtracer_flush_buffer();
	}
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_flush_buffer(void)
{
	// No reason to flush a buffer with no content
	MT_lock_set(&lock);
	if (active_tracer->allocated_size == 0) {
		MT_lock_unset(&lock);
		return GDK_SUCCEED;
	}
	MT_lock_unset(&lock);

	if (ATOMIC_GET(&CUR_ADAPTER) == BASIC) {
		// Initialize the BASIC adapter. It is used in order to avoid cases with files being
		// created and no logs being produced. Even if the creating the file fails the function
		// is going to be called only once.
		if(!INIT_BASIC_ADAPTER)
			_GDKtracer_init_basic_adptr();

		MT_lock_set(&lock); 
		{
			if (output_file) {
				size_t nitems = 1;
				size_t w = fwrite(&active_tracer->buffer, active_tracer->allocated_size, nitems, output_file);

				if (w == nitems)
					fflush(output_file);
				else
					// fwrite failed for whatever reason (e.g: disk is full)
					if(!LOG_EXC_REP)
					{
						GDK_TRACER_EXCEPTION(GDKTRACER_FAILED "\n");
						LOG_EXC_REP = true;
					}
			}

			// Reset buffer
			memset(active_tracer->buffer, 0, BUFFER_SIZE);
			active_tracer->allocated_size = 0;
		}
		MT_lock_unset(&lock);
	} else {
		MT_lock_set(&lock);
		memset(active_tracer->buffer, 0, BUFFER_SIZE);
		active_tracer->allocated_size = 0;
		MT_lock_unset(&lock);

		// Here we are supposed to send the logs to the profiler
	}

	// The file is kept open no matter the adapter
	// When GDKtracer stops we need also to close the file
	if (GDK_TRACER_STOP)
		if (output_file)
			fclose(output_file);

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
