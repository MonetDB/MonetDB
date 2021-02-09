/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_tracer.h"
#include "gdk_private.h"
#include "mutils.h"

#define DEFAULT_ADAPTER BASIC
#define DEFAULT_LOG_LEVEL M_ERROR
#define DEFAULT_FLUSH_LEVEL M_INFO

#define FILE_NAME "mdbtrace.log"

#define AS_STR(x) #x
#define STR(x) AS_STR(x)

#define GENERATE_STRING(STRING) #STRING,

static FILE *active_tracer;
MT_Lock GDKtracer_lock = MT_LOCK_INITIALIZER(GDKtracer_lock);

static char file_name[FILENAME_MAX];

static ATOMIC_TYPE cur_adapter = ATOMIC_VAR_INIT(DEFAULT_ADAPTER);

static log_level_t cur_flush_level = DEFAULT_FLUSH_LEVEL;

#define GENERATE_LOG_LEVEL(COMP) DEFAULT_LOG_LEVEL,
log_level_t lvl_per_component[] = {
	FOREACH_COMP(GENERATE_LOG_LEVEL)
};

static const char *adapter_str[] = {
	FOREACH_ADPTR(GENERATE_STRING)
};

static const char *layer_str[] = {
	FOREACH_LAYER(GENERATE_STRING)
};

static const char *component_str[] = {
	FOREACH_COMP(GENERATE_STRING)
};

static const char *level_str[] = {
	FOREACH_LEVEL(GENERATE_STRING)
};


#define MXW "20"

#define TS_SIZE	((size_t) 20)	/* buffer size needed for timestamp */


/*
 * GDKtracer Stream Macros
 */
// Exception
#define GDK_TRACER_EXCEPTION(MSG, ...)				\
	fprintf(stderr,						\
		"%s "						\
		"%-"MXW"s "					\
		"%"MXW"s:%d "					\
		"%"MXW"s "					\
		"%-"MXW"s "					\
		"%-"MXW"s # "MSG,				\
		get_timestamp((char[TS_SIZE]){0}, TS_SIZE),	\
		__FILE__,					\
		__func__,					\
		__LINE__,					\
		STR(M_CRITICAL),				\
		STR(GDK_TRACER),				\
		MT_thread_getname(),				\
		## __VA_ARGS__);


static inline char *
get_timestamp(char *datetime, size_t dtsz)
{
	time_t now = time(NULL);
	struct tm tmp;
	(void) localtime_r(&now, &tmp);
	strftime(datetime, dtsz, "%Y-%m-%d %H:%M:%S", &tmp);

	return datetime;
}


// When BASIC adapter is active, all the log messages are getting printed to a file.
// This function prepares a file in order to write the contents of the buffer when necessary.
static gdk_return
GDKtracer_init_trace_file(const char *dbpath, const char *dbtrace)
{
	if (dbtrace == NULL) {
		if (dbpath == NULL) {
			active_tracer = stderr;
			return GDK_SUCCEED;
		}
		if (strconcat_len(file_name, sizeof(file_name),
				  dbpath, DIR_SEP_STR, FILE_NAME, NULL)
		    >= sizeof(file_name)) {
			goto too_long;
		}
	} else {
		if (strcpy_len(file_name, dbtrace, sizeof(file_name))
		    >= sizeof(file_name)) {
			goto too_long;
		}
	}

	active_tracer = MT_fopen(file_name, "a");

	if (active_tracer == NULL) {
		GDK_TRACER_EXCEPTION("Failed to open %s: %s\n", file_name,
				     GDKstrerror(errno, (char[64]){0}, 64));
		file_name[0] = 0; /* uninitialize */
		active_tracer = stderr;
		return GDK_FAIL;
	}

	return GDK_SUCCEED;

  too_long:
	GDK_TRACER_EXCEPTION("path name for dbtrace file too long\n");
	file_name[0] = 0; /* uninitialize */
	active_tracer = stderr;
	return GDK_FAIL;
}

static gdk_return
_GDKtracer_init_basic_adptr(void)
{
	return GDKtracer_init_trace_file(GDKgetenv("gdk_dbpath"),
					 GDKgetenv("gdk_dbtrace"));
}


static void
set_level_for_layer(int layer, int lvl)
{
	const char *tok = NULL;
	log_level_t level = (log_level_t) lvl;

	// make sure we initialize before changing the component level
	MT_lock_set(&GDKtracer_lock);
	if (file_name[0] == 0) {
		_GDKtracer_init_basic_adptr();
	}
	MT_lock_unset(&GDKtracer_lock);

	for (int i = 0; i < COMPONENTS_COUNT; i++) {
		if (layer == MDB_ALL) {
			lvl_per_component[i] = level;
		} else {
			tok = component_str[i];

			switch (layer) {
			case SQL_ALL:
				if (strncmp(tok, "SQL_", 4) == 0)
					lvl_per_component[i] = level;
				break;
			case MAL_ALL:
				if (strncmp(tok, "MAL_", 4) == 0)
					lvl_per_component[i] = level;
				break;
			case GDK_ALL:
				if (strncmp(tok, "GDK", 3) == 0)
					lvl_per_component[i] = level;
				break;
			default:
				break;
			}
		}
	}
}

static inline adapter_t
find_adapter(const char *adptr)
{
	if (adptr == NULL)
		return ADAPTERS_COUNT;

	for (int i = 0; i < (int) ADAPTERS_COUNT; i++) {
		if (strcasecmp(adapter_str[i], adptr) == 0) {
			return (adapter_t) i;
		}
	}
	return ADAPTERS_COUNT;
}

static inline log_level_t
find_level(const char *lvl)
{
	if (lvl == NULL)
		return LOG_LEVELS_COUNT;

	for (int i = 0; i < (int) LOG_LEVELS_COUNT; i++) {
		if (strcasecmp(level_str[i] + 2, lvl) == 0) {
			return (log_level_t) i;
		}
	}
	return LOG_LEVELS_COUNT;
}

static inline layer_t
find_layer(const char *layer)
{
	if (layer == NULL)
		return LAYERS_COUNT;
	for (int i = 0; i < (int) LAYERS_COUNT; i++) {
		if (strcasecmp(layer_str[i], layer) == 0) {
			return (layer_t) i;
		}
	}
	return LAYERS_COUNT;
}

static inline component_t
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
		if (strcasecmp(component_str[i], comp) == 0) {
			return (component_t) i;
		}
	}
	return COMPONENTS_COUNT;
}



/**
 *
 * API CALLS
 *
 */
void
GDKtracer_reinit_basic(int sig)
{
	(void) sig;

	// GDKtracer needs to reopen the file only in
	// case the adapter is BASIC
	if ((adapter_t) ATOMIC_GET(&cur_adapter) != BASIC)
		return;

	// Make sure that GDKtracer is not trying to flush the buffer
	MT_lock_set(&GDKtracer_lock);

	if (active_tracer) {
		if (active_tracer != stderr)
			fclose(active_tracer);
		else
			fflush(active_tracer);
		active_tracer = NULL;
	}
	_GDKtracer_init_basic_adptr();

	MT_lock_unset(&GDKtracer_lock);
}


gdk_return
GDKtracer_stop(void)
{
	set_level_for_layer(MDB_ALL, DEFAULT_LOG_LEVEL);
	return GDKtracer_flush_buffer();
}

gdk_return
GDKtracer_set_component_level(const char *comp, const char *lvl)
{
	log_level_t level = find_level(lvl);
	component_t component = find_component(comp);

	if (level == LOG_LEVELS_COUNT) {
		GDKerror("unknown level\n");
		return GDK_FAIL;
	}
	if (component == COMPONENTS_COUNT) {
		GDKerror("unknown component\n");
		return GDK_FAIL;
	}

	// make sure we initialize before changing the component level
	MT_lock_set(&GDKtracer_lock);
	if (file_name[0] == 0) {
		_GDKtracer_init_basic_adptr();
	}
	MT_lock_unset(&GDKtracer_lock);

	lvl_per_component[component] = level;

	return GDK_SUCCEED;
}

const char *
GDKtracer_get_component_level(const char *comp)
{
	component_t component = find_component(comp);

	if (component == COMPONENTS_COUNT) {
		GDKerror("unknown component\n");
		return NULL;
	}
	return level_str[(int) lvl_per_component[component]];
}


gdk_return
GDKtracer_reset_component_level(const char *comp)
{
	component_t component = find_component(comp);

	if (component == COMPONENTS_COUNT) {
		GDKerror("unknown component\n");
		return GDK_FAIL;
	}
	lvl_per_component[component] = DEFAULT_LOG_LEVEL;
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_layer_level(const char *layer, const char *lvl)
{
	layer_t lyr = find_layer(layer);
	log_level_t level = find_level(lvl);
	if (level == LOG_LEVELS_COUNT) {
		GDKerror("unknown level\n");
		return GDK_FAIL;
	}
	if (lyr == LAYERS_COUNT) {
		GDKerror("unknown layer\n");
		return GDK_FAIL;
	}

	set_level_for_layer(lyr, level);
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_layer_level(const char *layer)
{
	layer_t lyr = find_layer(layer);
	if (lyr == LAYERS_COUNT) {
		GDKerror("unknown layer\n");
		return GDK_FAIL;
	}

	set_level_for_layer(lyr, DEFAULT_LOG_LEVEL);
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_flush_level(const char *lvl)
{
	log_level_t level = find_level(lvl);
	if (level == LOG_LEVELS_COUNT) {
		GDKerror("unknown level\n");
		return GDK_FAIL;
	}

	cur_flush_level = level;
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_flush_level(void)
{
	cur_flush_level = DEFAULT_FLUSH_LEVEL;
	return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_adapter(const char *adapter)
{
	adapter_t adptr = find_adapter(adapter);
	if (adptr == ADAPTERS_COUNT) {
		GDKerror("unknown adapter\n");
		return GDK_FAIL;
	}

	// Here when switching between adapters we can open/close the file
	// But it is not so important to keep it open in case the adapter switches
	// From BASIC to other => close the file
	// From other to BASIC => open the file

	ATOMIC_SET(&cur_adapter, adptr);

	return GDK_SUCCEED;
}


gdk_return
GDKtracer_reset_adapter(void)
{
	ATOMIC_SET(&cur_adapter, DEFAULT_ADAPTER);
	return GDK_SUCCEED;
}

static bool add_ts;		/* add timestamp to error message to stderr */

void
GDKtracer_init(const char *dbpath, const char *dbtrace)
{
#ifdef _MSC_VER
	add_ts = GetFileType(GetStdHandle(STD_ERROR_HANDLE)) != FILE_TYPE_PIPE;
#else
	add_ts = isatty(2) || lseek(2, 0, SEEK_CUR) != (off_t) -1 || errno != ESPIPE;
#endif
	(void) GDKtracer_init_trace_file(dbpath, dbtrace);
}

void
GDKtracer_log(const char *file, const char *func, int lineno,
	      log_level_t level, component_t comp,
	      const char *syserr,
	      const char *fmt, ...)
{
	if ((adapter_t) ATOMIC_GET(&cur_adapter) == MBEDDED)
		return;

	int bytes_written;
	char buffer[512];	/* should be plenty big enough for a message */
	va_list va;
	char ts[TS_SIZE];
	char *msg = NULL;
	static char file_prefix[] = __FILE__;
	static size_t prefix_length = (size_t) -1;

	if (prefix_length == (size_t) -1) {
		/* first time, calculate prefix of file name */
		msg = strstr(file_prefix, "gdk" DIR_SEP_STR "gdk_tracer.c");
		if (msg == NULL)
			prefix_length = 0;
		else
			prefix_length = (size_t) (msg - file_prefix);
	}
	if (prefix_length != 0 &&
	    strncmp(file, file_prefix, prefix_length) == 0)
		file += prefix_length;

	va_start(va, fmt);
	int pad = (int) strlen(file);
	pad = (pad > 40) ? 0 : 40 - pad;
	bytes_written = snprintf(buffer, sizeof(buffer),
				 "%s "		/* timestamp */
				 "%10s "	/* level */
				 "%-8s "	/* component */
				 "%-20s "	/* thread name */
				 "%s:%-5d %*s"	/* file, lineno, pad */
				 "%-20s ",	/* function */
				 get_timestamp(ts, sizeof(ts)),
				 level_str[level],
				 component_str[comp],
				 MT_thread_getname(),
				 file, lineno, pad, "",
				 func);
	if (bytes_written > 0 && bytes_written < (int) sizeof(buffer)) {
		msg = buffer + bytes_written;
		bytes_written = vsnprintf(msg,
					  sizeof(buffer) - bytes_written,
					  fmt, va);
	}
	va_end(va);
	if (bytes_written < 0) {
		GDK_TRACER_EXCEPTION("Failed to write logs\n");
		return;
	}
	char *p;
	if ((p = strchr(buffer, '\n')) != NULL)
		*p = '\0';

	if (comp == GDK && level <= M_ERROR) {
		/* append message to GDKerrbuf (if set) */
		char *buf = GDKerrbuf;
		if (buf) {
			size_t n = strlen(buf);
			snprintf(buf + n, GDKMAXERRLEN - n,
				 "%s%s: %s%s%s\n",
				 GDKERROR, func, msg,
				 syserr ? ": " : "",
				 syserr ? syserr : "");
		}
	}

	if (level <= M_WARNING) {
		fprintf(stderr, "#%s%s%s: %s: %s%s%s%s\n",
			add_ts ? ts : "",
			add_ts ? ": " : "",
			MT_thread_getname(), func, GDKERROR,
			msg, syserr ? ": " : "",
			syserr ? syserr : "");
		if (active_tracer == NULL || active_tracer == stderr)
			return;
	}
	if (active_tracer == NULL)
		return;
	if (syserr)
		fprintf(active_tracer, "%s: %s\n", buffer, syserr);
	else
		fprintf(active_tracer, "%s\n", buffer);

	// Flush the current buffer in case the event is
	// important depending on the flush-level
	// Always flush CRITICAL and ERROR messages - prevent cases
	// like mserver5 refusing to start due to allocated port
	// and the error is never reported to the user because it
	// is still in the buffer which it never gets flushed.
	if (level == cur_flush_level || level <= M_ERROR)
		fflush(active_tracer);
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

		if (BUNappend(component, component_str[i], false) != GDK_SUCCEED)
			return GDK_FAIL;

		if (BUNappend(log_level, level_str[lvl_per_component[i]], false) != GDK_SUCCEED)
			return GDK_FAIL;
	}

	return GDK_SUCCEED;
}
