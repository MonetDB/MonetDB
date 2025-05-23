/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "mutils.h"

#define DEFAULT_ADAPTER TRC_NAME(BASIC)
#define DEFAULT_LOG_LEVEL TRC_NAME(M_ERROR)
#define DEFAULT_FLUSH_LEVEL TRC_NAME(M_DEBUG)

#define FILE_NAME "mdbtrace.log"

#define AS_STR(x) #x
#define STR(x) AS_STR(x)

#define GENERATE_STRING(STRING) #STRING,

static FILE *active_tracer;
MT_Lock GDKtracer_lock = MT_LOCK_INITIALIZER(GDKtracer_lock);

static char *file_name;

static ATOMIC_TYPE cur_adapter = ATOMIC_VAR_INIT(DEFAULT_ADAPTER);

static log_level_t cur_flush_level = DEFAULT_FLUSH_LEVEL;

static bool write_to_tracer = false;

#define GENERATE_LOG_LEVEL(COMP) ATOMIC_VAR_INIT((ATOMIC_BASE_TYPE) DEFAULT_LOG_LEVEL),
ATOMIC_TYPE lvl_per_component[] = {
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
#define GDK_TRACER_EXCEPTION(MSG)				\
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
		MT_thread_getname());
#define GDK_TRACER_EXCEPTION2(MSG, ...)				\
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
		__VA_ARGS__);


#define GDK_TRACER_RESET_OUTPUT()					\
	do {								\
		write_to_tracer = false;				\
		for (int i = 0; !write_to_tracer && i < (int) TRC_NAME(COMPONENTS_COUNT); i++) {	\
			write_to_tracer = (log_level_t) ATOMIC_GET(&lvl_per_component[i]) > DEFAULT_LOG_LEVEL; \
		}							\
	} while(0)

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
	char *fn;

	/* we use malloc/free instead of GDKmalloc/GDKfree to avoid
	 * possible recursion */
#undef malloc
#undef free
	if (dbtrace == NULL) {
		write_to_tracer = false;
		if (dbpath == NULL) {
			active_tracer = stdout;
			return GDK_SUCCEED;
		}
		size_t fnl = strlen(dbpath) + strlen(DIR_SEP_STR) + strlen(FILE_NAME) + 1;
		fn = malloc(fnl);
		if (fn == NULL) {
			GDK_TRACER_EXCEPTION("malloc failure\n");
			active_tracer = stdout;
			return GDK_FAIL;
		}
		if (strconcat_len(fn, fnl, dbpath, DIR_SEP_STR, FILE_NAME, NULL)
		    >= fnl) {
			/* cannot happen */
			goto too_long;
		}
	} else if (strcmp(dbtrace, "stdout") == 0) {
		write_to_tracer = false;
		active_tracer = stdout;
		return GDK_SUCCEED;
	} else {
		write_to_tracer = true;
		size_t fnl = strlen(dbtrace) + 1;
		fn = malloc(fnl);
		if (fn == NULL) {
			GDK_TRACER_EXCEPTION("malloc failure\n");
			active_tracer = stdout;
			return GDK_FAIL;
		}
		if (strcpy_len(fn, dbtrace, fnl)
		    >= fnl) {
			/* cannot happen */
			goto too_long;
		}
	}
	free(file_name);
	file_name = fn;

	active_tracer = MT_fopen(file_name, "a");

	if (active_tracer == NULL) {
		GDK_TRACER_EXCEPTION2("Failed to open %s: %s\n", file_name,
				      GDKstrerror(errno, (char[64]){0}, 64));
		/* uninitialize */
		free(file_name);
		file_name = NULL;
		active_tracer = stdout;
		return GDK_FAIL;
	}

	return GDK_SUCCEED;

  too_long:
	GDK_TRACER_EXCEPTION("path name for dbtrace file too long\n");
	/* uninitialize */
	free(fn);
	free(file_name);
	file_name = NULL;
	active_tracer = stdout;
	return GDK_FAIL;
}

static gdk_return
_GDKtracer_init_basic_adptr(void)
{
	return GDKtracer_init_trace_file(GDKgetenv("gdk_dbpath"),
					 GDKgetenv("gdk_dbtrace"));
}

static void
set_level_for_layer(int layer, log_level_t level)
{
	const char *tok = NULL;

	// make sure we initialize before changing the component level
	MT_lock_set(&GDKtracer_lock);
	if (file_name == NULL) {
		_GDKtracer_init_basic_adptr();
	}
	MT_lock_unset(&GDKtracer_lock);

	for (int i = 0; i < TRC_NAME(COMPONENTS_COUNT); i++) {
		if (layer == TRC_NAME(MDB_ALL)) {
			ATOMIC_SET(&lvl_per_component[i], (ATOMIC_BASE_TYPE) level);
		} else {
			tok = component_str[i];

			switch (layer) {
			case TRC_NAME(SQL_ALL):
				if (strncmp(tok, "SQL_", 4) == 0)
					ATOMIC_SET(&lvl_per_component[i], (ATOMIC_BASE_TYPE) level);
				break;
			case TRC_NAME(MAL_ALL):
				if (strncmp(tok, "MAL_", 4) == 0)
					ATOMIC_SET(&lvl_per_component[i], (ATOMIC_BASE_TYPE) level);
				break;
			case TRC_NAME(GDK_ALL):
				if (strncmp(tok, "GDK", 3) == 0)
					ATOMIC_SET(&lvl_per_component[i], (ATOMIC_BASE_TYPE) level);
				break;
			default:
				break;
			}
		}
	}
	MT_lock_set(&GDKtracer_lock);
	GDK_TRACER_RESET_OUTPUT();
	MT_lock_unset(&GDKtracer_lock);
}

static inline adapter_t
find_adapter(const char *adptr)
{
	if (adptr == NULL)
		return TRC_NAME(ADAPTERS_COUNT);

	for (int i = 0; i < (int) TRC_NAME(ADAPTERS_COUNT); i++) {
		if (strcasecmp(adapter_str[i], adptr) == 0) {
			return (adapter_t) i;
		}
	}
	return TRC_NAME(ADAPTERS_COUNT);
}

static inline log_level_t
find_level(const char *lvl)
{
	if (lvl == NULL)
		return TRC_NAME(LOG_LEVELS_COUNT);

	for (int i = 0; i < (int) TRC_NAME(LOG_LEVELS_COUNT); i++) {
		if (strcasecmp(level_str[i] + 2, lvl) == 0) {
			return (log_level_t) i;
		}
	}
	return TRC_NAME(LOG_LEVELS_COUNT);
}

static inline layer_t
find_layer(const char *layer)
{
	if (layer == NULL)
		return TRC_NAME(LAYERS_COUNT);
	for (int i = 0; i < (int) TRC_NAME(LAYERS_COUNT); i++) {
		if (strcasecmp(layer_str[i], layer) == 0) {
			return (layer_t) i;
		}
	}
	return TRC_NAME(LAYERS_COUNT);
}

static inline component_t
find_component(const char *comp)
{
	/* special case for the (currently) three components that end in _ */
	if (comp == NULL || *comp == 0 || comp[strlen(comp) - 1] == '_')
		return TRC_NAME(COMPONENTS_COUNT);

	for (int i = 0; i < (int) TRC_NAME(COMPONENTS_COUNT); i++) {
		if (strcasecmp(component_str[i], comp) == 0) {
			return (component_t) i;
		}
	}
	return TRC_NAME(COMPONENTS_COUNT);
}



/**
 *
 * API CALLS
 *
 */
static volatile sig_atomic_t interrupted = 0;

void
GDKtracer_reinit_basic(int sig)
{
	(void) sig;
	interrupted = 1;
}

static void
reinit(void)
{
	/* called locked */

	interrupted = 0;

	// GDKtracer needs to reopen the file only in
	// case the adapter is BASIC
	if ((adapter_t) ATOMIC_GET(&cur_adapter) != TRC_NAME(BASIC))
		return;

	if (active_tracer) {
		if (active_tracer != stdout)
			fclose(active_tracer);
		else
			fflush(active_tracer);
		active_tracer = NULL;
	}
	_GDKtracer_init_basic_adptr();
}


gdk_return
GDKtracer_stop(void)
{
	set_level_for_layer(TRC_NAME(MDB_ALL), DEFAULT_LOG_LEVEL);
	if (active_tracer) {
		if (active_tracer != stdout)
			fclose(active_tracer);
		else
			fflush(active_tracer);
		active_tracer = NULL;
	}
	return GDK_SUCCEED;
}

gdk_return
GDKtracer_set_component_level(const char *comp, const char *lvl)
{
	log_level_t level = find_level(lvl);
	component_t component = find_component(comp);

	if (level == TRC_NAME(LOG_LEVELS_COUNT)) {
		GDKerror("unknown level\n");
		return GDK_FAIL;
	}
	if (component == TRC_NAME(COMPONENTS_COUNT)) {
		GDKerror("unknown component\n");
		return GDK_FAIL;
	}

	// make sure we initialize before changing the component level
	MT_lock_set(&GDKtracer_lock);
	if (file_name == NULL) {
		_GDKtracer_init_basic_adptr();
	}
	write_to_tracer |= level > DEFAULT_LOG_LEVEL;
	MT_lock_unset(&GDKtracer_lock);

	ATOMIC_SET(&lvl_per_component[component], (ATOMIC_BASE_TYPE) level);

	return GDK_SUCCEED;
}

const char *
GDKtracer_get_component_level(const char *comp)
{
	component_t component = find_component(comp);

	if (component == TRC_NAME(COMPONENTS_COUNT)) {
		GDKerror("unknown component\n");
		return NULL;
	}
	return level_str[ATOMIC_GET(&lvl_per_component[component])];
}


gdk_return
GDKtracer_reset_component_level(const char *comp)
{
	component_t component = find_component(comp);

	if (component == TRC_NAME(COMPONENTS_COUNT)) {
		GDKerror("unknown component\n");
		return GDK_FAIL;
	}
	ATOMIC_SET(&lvl_per_component[component], (ATOMIC_BASE_TYPE) DEFAULT_LOG_LEVEL);
	MT_lock_set(&GDKtracer_lock);
	GDK_TRACER_RESET_OUTPUT();
	MT_lock_unset(&GDKtracer_lock);

	return GDK_SUCCEED;
}


gdk_return
GDKtracer_set_layer_level(const char *layer, const char *lvl)
{
	layer_t lyr = find_layer(layer);
	log_level_t level = find_level(lvl);
	if (level == TRC_NAME(LOG_LEVELS_COUNT)) {
		GDKerror("unknown level\n");
		return GDK_FAIL;
	}
	if (lyr == TRC_NAME(LAYERS_COUNT)) {
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
	if (lyr == TRC_NAME(LAYERS_COUNT)) {
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
	if (level == TRC_NAME(LOG_LEVELS_COUNT)) {
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
	if (adptr == TRC_NAME(ADAPTERS_COUNT)) {
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

static bool add_ts;		/* add timestamp to error message to stdout */

gdk_return
GDKtracer_init(const char *dbpath, const char *dbtrace)
{
	MT_lock_set(&GDKtracer_lock);
#ifdef _MSC_VER
	add_ts = GetFileType(GetStdHandle(STD_ERROR_HANDLE)) != FILE_TYPE_PIPE;
#else
	add_ts = isatty(2) || lseek(2, 0, SEEK_CUR) != (off_t) -1 || errno != ESPIPE;
#endif
	gdk_return rc = GDKtracer_init_trace_file(dbpath, dbtrace);
	MT_lock_unset(&GDKtracer_lock);
	return rc;
}

gdk_return
GDKtracer_set_tracefile(const char *tracefile)
{
	return GDKtracer_init(NULL, tracefile);
}

void
GDKtracer_log(const char *file, const char *func, int lineno,
	      log_level_t level, component_t comp,
	      const char *syserr,
	      const char *fmt, ...)
{
	int bytes_written;
	char buffer[512];	/* should be plenty big enough for a message */
	va_list va;
	char ts[TS_SIZE];
	char *msg = NULL;
	bool isexit;
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
	} else {
		/* exceedingly unlikely that we ever come here */
		msg = buffer;
		bytes_written = 0;
	}
	bytes_written = vsnprintf(msg,
				  sizeof(buffer) - bytes_written,
				  fmt, va);
	isexit = strstr(msg, EXITING_MSG) != NULL;
	va_end(va);
	if (bytes_written < 0) {
		if ((adapter_t) ATOMIC_GET(&cur_adapter) != TRC_NAME(MBEDDED))
			GDK_TRACER_EXCEPTION("Failed to write logs\n");
		return;
	}
	char *p;
	if ((p = strchr(buffer, '\n')) != NULL)
		*p = '\0';

	if (level <= TRC_NAME(M_ERROR) && (comp == TRC_NAME(GDK) || comp == TRC_NAME(GEOM))) {
		/* append message to GDKerrbuf (if set) */
		char *buf = GDKerrbuf;
		if (buf) {
			size_t n = strlen(buf);
			snprintf(buf + n, GDKMAXERRLEN - n,
				 "%s%s: %s%s%s\n",
				 isexit ? "" : GDKERROR,
				 func, msg,
				 syserr ? ": " : "",
				 syserr ? syserr : "");
		}
	}

	/* don't write to file in embedded case, but set the GDK error buffer */
	if ((adapter_t) ATOMIC_GET(&cur_adapter) == TRC_NAME(MBEDDED))
		return;

	MT_lock_set(&GDKtracer_lock);
	if (interrupted)
		reinit();

	if (level <= TRC_NAME(M_WARNING) ||
	    active_tracer == stdout ||
	    (ATOMIC_GET(&GDKdebug) & TESTINGMASK)) {
		fprintf(level <= TRC_NAME(M_ERROR) && !isexit ? stderr : stdout,
			"#%s%s%s: %s: %s: %s%s%s\n",
			add_ts ? ts : "",
			add_ts ? ": " : "",
			MT_thread_getname(), func, level_str[level] + 2,
			msg, syserr ? ": " : "",
			syserr ? syserr : "");
		if (active_tracer == NULL || active_tracer == stdout || !write_to_tracer) {
			MT_lock_unset(&GDKtracer_lock);
			return;
		}
	}
	if (active_tracer == NULL) {
		MT_lock_unset(&GDKtracer_lock);
		return;
	}
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
	if (level == cur_flush_level || level <= TRC_NAME(M_ERROR))
		fflush(active_tracer);
	MT_lock_unset(&GDKtracer_lock);
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
	for (int i = 0; i < TRC_NAME(COMPONENTS_COUNT); i++) {
		if (BUNappend(id, &i, false) != GDK_SUCCEED)
			return GDK_FAIL;

		if (BUNappend(component, component_str[i], false) != GDK_SUCCEED)
			return GDK_FAIL;

		if (BUNappend(log_level, level_str[ATOMIC_GET(&lvl_per_component[i])], false) != GDK_SUCCEED)
			return GDK_FAIL;
	}

	return GDK_SUCCEED;
}
